"""Home Assistant MQTT Discovery publisher.

Generates and publishes HA MQTT discovery payloads for all evohome zones
and devices. Enabled via HA_DISCOVERY_ENABLED in evogateway.cfg.

Discovery topics are retained so HA picks them up after restart.
Zone / system state topics (written by MessageRouter) are the live data
sources that HA subscribes to.
"""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING

from .utils import to_snake_case

if TYPE_CHECKING:
    from .registry import DeviceRegistry
    from .config import MqttConfig
    from .services import MQTTService


# Maps device type code → list of sensor descriptors to publish
DEVICE_SENSORS: dict[str, list[dict]] = {
    "04": [  # HR92 / HR80 TRV
        {"code": "temperature",   "name": "Temperature",  "class": "temperature", "unit": "°C",  "state_class": "measurement"},
        {"code": "battery_state", "name": "Battery",      "class": "battery",     "unit": "%",   "state_class": "measurement"},
        {"code": "window_open",   "name": "Window Open",  "class": "window",      "binary": True},
        {"code": "heat_demand",   "name": "Heat Demand",  "unit": "%",            "state_class": "measurement"},
    ],
    "34": [  # T87RF wall thermostat
        {"code": "temperature",   "name": "Temperature",  "class": "temperature", "unit": "°C",  "state_class": "measurement"},
        {"code": "battery_state", "name": "Battery",      "class": "battery",     "unit": "%",   "state_class": "measurement"},
    ],
    "07": [  # CS92 DHW sensor
        {"code": "temperature",   "name": "Temperature",  "class": "temperature", "unit": "°C",  "state_class": "measurement"},
        {"code": "battery_state", "name": "Battery",      "class": "battery",     "unit": "%",   "state_class": "measurement"},
    ],
    "13": [  # BDR91 boiler/zone relay
        {"code": "actuator_state", "name": "Actuator",    "class": "running",    "binary": True},
        {"code": "heat_demand",    "name": "Heat Demand", "unit": "%",           "state_class": "measurement"},
    ],
    "10": [  # R8810A OpenTherm bridge
        {"code": "heat_demand",    "name": "Boiler Demand", "unit": "%",         "state_class": "measurement"},
        {"code": "actuator_state", "name": "Boiler",        "class": "running",  "binary": True},
    ],
    "02": [  # HCC80R UFH controller
        {"code": "heat_demand",    "name": "Heat Demand", "unit": "%",           "state_class": "measurement"},
        {"code": "actuator_state", "name": "Actuator",    "class": "running",    "binary": True},
    ],
}

# HA-friendly mode names accepted by the climate entity
ZONE_HVAC_MODES = ["heat", "off"]

# DHW-specific climate modes
DHW_HVAC_MODES = ["auto", "on", "off", "temporary"]

# Evohome system-level modes exposed as a select entity
SYSTEM_MODES = ["auto", "away", "heating_off", "day_off", "auto_with_reset", "custom"]

# Zone IDs that are not real heating zones
_PSEUDO_ZONES = frozenset({"SYS", "FA", "FC", "F9"})


class HADiscovery:
    """Publishes Home Assistant MQTT discovery config messages."""

    def __init__(
        self,
        *,
        registry: "DeviceRegistry",
        mqtt_topics: "MqttConfig.TopicLayout",
        mqtt_service: "MQTTService",
        ha_prefix: str,
        gateway_name: str,
    ) -> None:
        self.registry = registry
        self.topics = mqtt_topics
        self.mqtt = mqtt_service
        self.ha_prefix = ha_prefix.rstrip("/")
        self.gateway_name = gateway_name
        self.root = (mqtt_topics.root or "evogateway").rstrip("/")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_all(self) -> None:
        """Publish discovery payloads for all zones and devices."""
        for zone_id, zone_name in self.registry.zone_names.items():
            if zone_id in _PSEUDO_ZONES:
                continue
            self._publish_zone_climate(zone_id, zone_name)
            self._publish_zone_heat_demand(zone_id, zone_name)

        self._publish_system_mode()

        if self.topics.dhw_is_zone:
            self._publish_dhw_climate()

        for dev_id, dev_type in self.registry.type_of_id.items():
            if dev_type in ("01", "18"):
                continue
            zone_id = self.registry.zone_of(dev_id)
            if not zone_id or zone_id in _PSEUDO_ZONES:
                continue
            zone_name = self.registry.zone_name(zone_id)
            if not zone_name:
                continue
            self._publish_device_sensors(dev_id, dev_type, zone_id, zone_name)

    async def remove_all(self) -> None:
        """Remove all discovery entries by scanning the broker first.

        Subscribes to the HA discovery namespace, waits briefly for the broker to
        deliver all matching retained messages, then publishes empty retained payloads
        to exactly those topics. Only topics that actually exist on the broker are
        touched, so no ghost entries are created.
        """
        found: set[str] = set()
        sub_topic = f"{self.ha_prefix}/+/+/config"

        def _collect(client, userdata, message):
            if message.retain and message.payload and "evogateway_" in message.topic:
                found.add(message.topic)

        client = self.mqtt._client
        client.message_callback_add(sub_topic, _collect)
        client.subscribe(sub_topic, qos=1)

        await asyncio.sleep(0.5)

        client.unsubscribe(sub_topic)
        client.message_callback_remove(sub_topic)

        for topic in found:
            self.mqtt.publish(topic, "", retain=True, raw=True)

    # ------------------------------------------------------------------
    # Zone entities
    # ------------------------------------------------------------------

    def _publish_zone_climate(self, zone_id: str, zone_name: str) -> None:
        zone_slug = to_snake_case(zone_name).lower()
        state_topic = self._zone_state_topic(zone_slug)
        cmd_topic = self._cmd_topic()
        zone_id_hex = zone_id  # already in hex format (e.g. "00", "01")

        payload = {
            "name": zone_name,
            "unique_id": f"evogateway_zone_{zone_id}",
            "device": self._zone_device(zone_id, zone_name),
            "current_temperature_topic": state_topic,
            "current_temperature_template": "{{ value_json.temperature }}",
            "temperature_state_topic": state_topic,
            "temperature_state_template": "{{ value_json.setpoint }}",
            "temperature_command_topic": cmd_topic,
            "temperature_command_template": (
                f'{{"command": "set_zone_setpoint", "zone_idx": "{zone_id_hex}", "setpoint": {{{{ value }}}}}}'
            ),
            "modes": ZONE_HVAC_MODES,
            "mode_state_topic": state_topic,
            "mode_state_template": "{{ value_json.mode | default('heat') | lower }}",
            "mode_command_topic": cmd_topic,
            "mode_command_template": (
                f'{{"command": "set_zone_mode", "zone_idx": "{zone_id_hex}", "mode": "{{{{ value }}}}"  }}'
            ),
            "min_temp": 5,
            "max_temp": 35,
            "temp_step": 0.5,
            "availability_topic": self._status_topic(),
            "payload_available": "Online",
            "payload_not_available": "Offline",
        }
        disc_topic = f"{self.ha_prefix}/climate/evogateway_zone_{zone_id}/config"
        self._publish_raw(disc_topic, payload)

    def _publish_zone_heat_demand(self, zone_id: str, zone_name: str) -> None:
        zone_slug = to_snake_case(zone_name).lower()
        state_topic = self._zone_state_topic(zone_slug)

        payload = {
            "name": f"{zone_name} Heat Demand",
            "unique_id": f"evogateway_zone_{zone_id}_heat_demand",
            "device": self._zone_device(zone_id, zone_name),
            "state_topic": state_topic,
            "value_template": "{{ value_json.heat_demand }}",
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "availability_topic": self._status_topic(),
            "payload_available": "Online",
            "payload_not_available": "Offline",
        }
        disc_topic = f"{self.ha_prefix}/sensor/evogateway_zone_{zone_id}_heat_demand/config"
        self._publish_raw(disc_topic, payload)

    def _publish_dhw_climate(self) -> None:
        dhw_slug = to_snake_case(self.topics.dhw).lower()
        state_topic = self._zone_state_topic(dhw_slug)
        cmd_topic = self._cmd_topic()

        payload = {
            "name": "Hot Water",
            "unique_id": "evogateway_dhw",
            "device": self._zone_device("HW", "Hot Water"),
            "current_temperature_topic": state_topic,
            "current_temperature_template": "{{ value_json.temperature }}",
            "temperature_state_topic": state_topic,
            "temperature_state_template": "{{ value_json.setpoint }}",
            "temperature_command_topic": cmd_topic,
            "temperature_command_template": '{"command": "set_dhw_temp", "setpoint": {{ value }}}',
            "modes": DHW_HVAC_MODES,
            "mode_state_topic": state_topic,
            "mode_state_template": "{{ value_json.mode | default('auto') | lower }}",
            "mode_command_topic": cmd_topic,
            "mode_command_template": '{"command": "set_dhw_mode", "mode": "{{ value }}"}',
            "min_temp": 30,
            "max_temp": 85,
            "temp_step": 1,
            "availability_topic": self._status_topic(),
            "payload_available": "Online",
            "payload_not_available": "Offline",
        }
        disc_topic = f"{self.ha_prefix}/climate/evogateway_dhw/config"
        self._publish_raw(disc_topic, payload)

    # ------------------------------------------------------------------
    # System entities
    # ------------------------------------------------------------------

    def _publish_system_mode(self) -> None:
        system_state_topic = f"{self.root}/system/state"
        cmd_topic = self._cmd_topic()

        payload = {
            "name": f"{self.gateway_name} System Mode",
            "unique_id": "evogateway_system_mode",
            "device": self._gateway_device(),
            "state_topic": system_state_topic,
            "value_template": "{{ value_json.system_mode }}",
            "command_topic": cmd_topic,
            "command_template": '{"command": "set_system_mode", "mode": "{{ value }}"}',
            "options": SYSTEM_MODES,
            "availability_topic": self._status_topic(),
            "payload_available": "Online",
            "payload_not_available": "Offline",
        }
        disc_topic = f"{self.ha_prefix}/select/evogateway_system_mode/config"
        self._publish_raw(disc_topic, payload)

    # ------------------------------------------------------------------
    # Device entities
    # ------------------------------------------------------------------

    def _publish_device_sensors(
        self, dev_id: str, dev_type: str, zone_id: str, zone_name: str
    ) -> None:
        sensors = DEVICE_SENSORS.get(dev_type)
        if not sensors:
            return

        zone_slug = to_snake_case(zone_name).lower()
        dev_slug = self._dev_slug(dev_id)
        dev_alias = to_snake_case(self.registry.friendly_name_of(dev_id)).lower()
        zones_root = getattr(self.topics, "zones", "zones")

        # HA device object scoped to this physical device
        dev_alias_display = self.registry.alias_of(dev_id) or dev_id
        device_obj = {
            "identifiers": [f"evogateway_{dev_id.replace(':', '_')}"],
            "name": dev_alias_display,
            "model": self._device_model(dev_type),
            "manufacturer": "Honeywell evohome",
            "via_device": f"evogateway_{self.gateway_name}",
        }

        for sensor in sensors:
            code = sensor["code"]
            is_binary = sensor.get("binary", False)
            entity_type = "binary_sensor" if is_binary else "sensor"
            obj_id = f"evogateway_{dev_slug}_{code}"

            state_topic = f"{self.root}/{zones_root}/{zone_slug}/{dev_alias}/{code}"

            entry: dict = {
                "name": sensor["name"],
                "unique_id": obj_id,
                "device": device_obj,
                "state_topic": state_topic,
                "availability_topic": self._status_topic(),
                "payload_available": "Online",
                "payload_not_available": "Offline",
            }

            if is_binary:
                entry["value_template"] = f"{{{{ value_json.{code} }}}}"
                entry["payload_on"] = True
                entry["payload_off"] = False
            else:
                entry["value_template"] = f"{{{{ value_json.{code} }}}}"
                if "unit" in sensor:
                    entry["unit_of_measurement"] = sensor["unit"]
                if "class" in sensor:
                    entry["device_class"] = sensor["class"]
                if "state_class" in sensor:
                    entry["state_class"] = sensor["state_class"]

            disc_topic = f"{self.ha_prefix}/{entity_type}/{obj_id}/config"
            self._publish_raw(disc_topic, entry)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _zone_state_topic(self, zone_slug: str) -> str:
        zones_root = getattr(self.topics, "zones", "zones")
        return f"{self.root}/{zones_root}/{zone_slug}/state"

    def _cmd_topic(self) -> str:
        return f"{self.root}/{self.topics.cmd}"

    def _status_topic(self) -> str:
        return f"{self.root}/status"

    def _dev_slug(self, dev_id: str) -> str:
        return dev_id.replace(":", "_")

    def _gateway_device(self) -> dict:
        return {
            "identifiers": [f"evogateway_{self.gateway_name}"],
            "name": self.gateway_name,
            "model": "evoGateway",
            "manufacturer": "Honeywell evohome",
        }

    def _zone_device(self, zone_id: str, zone_name: str) -> dict:
        return {
            "identifiers": [f"evogateway_{self.gateway_name}_zone_{zone_id}"],
            "name": zone_name,
            "model": "evohome Zone",
            "manufacturer": "Honeywell evohome",
            "via_device": f"evogateway_{self.gateway_name}",
        }

    def _device_model(self, dev_type: str) -> str:
        models = {
            "04": "HR92/HR80 TRV",
            "34": "T87RF Thermostat",
            "07": "CS92 DHW Sensor",
            "13": "BDR91 Relay",
            "10": "R8810A OpenTherm Bridge",
            "02": "HCC80R UFH Controller",
        }
        return models.get(dev_type, f"Type {dev_type}")

    def _publish_raw(self, full_topic: str, payload) -> None:
        self.mqtt.publish(full_topic, payload, retain=True, raw=True)
