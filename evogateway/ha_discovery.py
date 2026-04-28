"""Home Assistant MQTT Discovery publisher.

Generates and publishes HA MQTT discovery payloads for all evohome zones
and devices. Enabled via HA_DISCOVERY_ENABLED in evogateway.cfg.

Uses HA device-mode discovery: one retained topic per logical device, with
all entities for that device bundled under a 'components' key. This keeps
the discovery namespace compact and groups related entities automatically.

Topic pattern: {ha_prefix}/device/{device_id}/config
"""

from __future__ import annotations
import asyncio
from typing import TYPE_CHECKING

from .utils import to_snake_case

if TYPE_CHECKING:
    from .registry import DeviceRegistry
    from .config import MqttConfig
    from .services import MQTTService


# Maps device type code → list of sensor descriptors.
# msg_code: ramses_rf CODE_NAMES value — becomes the sub-path in the MQTT topic.
# field:    key inside the JSON payload at that topic — used in value_template.
DEVICE_SENSORS: dict[str, list[dict]] = {
    "04": [  # HR92 / HR80 TRV
        {"msg_code": "temperature",    "field": "temperature",    "name": "Temperature", "class": "temperature", "unit": "°C", "state_class": "measurement"},
        {"msg_code": "device_battery", "field": "battery_level",  "name": "Battery",     "class": "battery",     "unit": "%",  "state_class": "measurement", "value_template": "{{ (value_json.battery_level * 100) | int }}"},
        {"msg_code": "window_state",   "field": "window_open",    "name": "Window Open", "class": "window",      "binary": True},
        {"msg_code": "heat_demand",    "field": "heat_demand",    "name": "Heat Demand", "unit": "%",            "state_class": "measurement"},
    ],
    "34": [  # T87RF wall thermostat
        {"msg_code": "temperature",    "field": "temperature",    "name": "Temperature", "class": "temperature", "unit": "°C", "state_class": "measurement"},
        {"msg_code": "device_battery", "field": "battery_level",  "name": "Battery",     "class": "battery",     "unit": "%",  "state_class": "measurement", "value_template": "{{ (value_json.battery_level * 100) | int }}"},
    ],
    "07": [  # CS92 DHW sensor
        {"msg_code": "dhw_temp",       "field": "temperature",    "name": "Temperature", "class": "temperature", "unit": "°C", "state_class": "measurement"},
        {"msg_code": "device_battery", "field": "battery_level",  "name": "Battery",     "class": "battery",     "unit": "%",  "state_class": "measurement", "value_template": "{{ (value_json.battery_level * 100) | int }}"},
    ],
    "13": [  # BDR91 boiler/zone relay
        {"msg_code": "actuator_state", "field": "actuator_state", "name": "Actuator",    "class": "running",     "binary": True},
        {"msg_code": "heat_demand",    "field": "heat_demand",    "name": "Heat Demand", "unit": "%",            "state_class": "measurement"},
    ],
    "10": [  # R8810A OpenTherm bridge
        {"msg_code": "heat_demand",    "field": "heat_demand",    "name": "Boiler Demand", "unit": "%",          "state_class": "measurement"},
        {"msg_code": "actuator_state", "field": "actuator_state", "name": "Boiler",       "class": "running",    "binary": True},
    ],
    "02": [  # HCC80R UFH controller
        {"msg_code": "heat_demand",    "field": "heat_demand",    "name": "Heat Demand", "unit": "%",            "state_class": "measurement"},
        {"msg_code": "actuator_state", "field": "actuator_state", "name": "Actuator",    "class": "running",     "binary": True},
    ],
}

# HA-friendly HVAC mode names.
# Evohome zones don't have a per-zone off; "heat" covers all zone modes.
ZONE_HVAC_MODES = ["heat"]

# DHW: follow_schedule → auto; permanent_override active/inactive → heat/off
DHW_HVAC_MODES = ["heat", "auto", "off"]

# System-level modes — must match ramses_rf SYS_MODE_MAP values exactly
SYSTEM_MODES = ["auto", "heat_off", "eco_boost", "away", "day_off", "day_off_eco", "auto_with_reset", "custom"]

# Zone IDs that are not real heating zones
_PSEUDO_ZONES = frozenset({"SYS", "FA", "FC", "F9"})


class HADiscovery:
    """Publishes Home Assistant MQTT discovery config messages using device mode."""

    def __init__(
        self,
        *,
        registry: "DeviceRegistry",
        mqtt_topics: "MqttConfig.TopicLayout",
        mqtt_service: "MQTTService",
        ha_prefix: str,
        gateway_name: str,
        id_prefix: str,
        zone_climate_prefix: str = "",
        zone_climate_suffix: str = "Heating",
        dhw_temp_subtopic: str = "",
        dhw_params_subtopic: str = "",
        dhw_mode_subtopic: str = "",
    ) -> None:
        self.registry = registry
        self.topics = mqtt_topics
        self.mqtt = mqtt_service
        self.ha_prefix = ha_prefix.rstrip("/")
        self.gateway_name = gateway_name
        self.id_prefix = id_prefix.strip("_") or "evogateway"
        self.zone_climate_prefix = zone_climate_prefix.strip()
        self.zone_climate_suffix = zone_climate_suffix.strip()
        self.root = (mqtt_topics.root or "evogateway").rstrip("/")
        self.dhw_temp_subtopic = dhw_temp_subtopic.strip("/")
        self.dhw_params_subtopic = dhw_params_subtopic.strip("/")
        self.dhw_mode_subtopic = dhw_mode_subtopic.strip("/")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_all(self) -> None:
        """Publish device-mode discovery payloads for all zones and devices."""
        ctl_slug = self._find_device_slug("01")  # resolved once, shared across zones

        self._publish_gateway_device()

        for zone_id, zone_name in self.registry.zone_names.items():
            if zone_id in _PSEUDO_ZONES:
                continue
            self._publish_zone_device(zone_id, zone_name, ctl_slug)

        if self.topics.dhw_is_zone:
            self._publish_dhw_device()

        for dev_id, dev_type in self.registry.type_of_id.items():
            if dev_type in ("01", "18"):
                continue
            zone_id = self.registry.zone_of(dev_id)
            if zone_id and zone_id not in _PSEUDO_ZONES:
                zone_name = self.registry.zone_name(zone_id)
                if zone_name:
                    self._publish_physical_device(dev_id, dev_type, zone_id, zone_name)
            else:
                # No zone or pseudo-zone (boiler relay, OTB, etc.) → system device
                self._publish_physical_device(dev_id, dev_type, None, None)

    async def remove_all(self) -> None:
        """Remove all discovery entries by scanning the broker first.

        Subscribes to both the legacy per-entity namespace and the new device
        namespace, waits briefly for retained messages, then clears exactly
        those topics. Handles migration from old-style to device-mode topics.
        """
        found: set[str] = set()

        def _collect(client, userdata, message):
            if message.retain and message.payload and self.id_prefix in message.topic:
                found.add(message.topic)

        client = self.mqtt._client
        # Match both old-style (homeassistant/{type}/{id}/config)
        # and new device-mode (homeassistant/device/{id}/config)
        for sub_topic in (f"{self.ha_prefix}/+/+/config", f"{self.ha_prefix}/device/+/config"):
            client.message_callback_add(sub_topic, _collect)
            client.subscribe(sub_topic, qos=1)

        await asyncio.sleep(0.5)

        for sub_topic in (f"{self.ha_prefix}/+/+/config", f"{self.ha_prefix}/device/+/config"):
            client.unsubscribe(sub_topic)
            client.message_callback_remove(sub_topic)

        for topic in found:
            self.mqtt.publish(topic, "", retain=True, raw=True)

    # ------------------------------------------------------------------
    # Device-mode publishers
    # ------------------------------------------------------------------

    def _publish_gateway_device(self) -> None:
        """Gateway device: system mode select entity."""
        components = {
            "system_mode": {
                "platform": "select",
                "name": "System Mode",
                "unique_id": f"{self.id_prefix}_system_mode",
                "object_id": f"{self.id_prefix}_system_mode",
                "state_topic": f"{self.root}/system/state",
                "value_template": "{{ value_json.system_mode }}",
                "command_topic": self._cmd_topic(),
                "command_template": '{"command": "set_system_mode", "mode": "{{ value }}"}',
                "options": SYSTEM_MODES,
                **self._availability(),
            }
        }
        payload = {
            "device": {
                "identifiers": [self.id_prefix],
                "name": self.gateway_name,
                "model": "evoGateway",
                "manufacturer": "Honeywell evohome",
            },
            "origin": self._origin(),
            "components": components,
        }
        self._publish_device(self.id_prefix, payload)

    def _publish_zone_device(self, zone_id: str, zone_name: str, ctl_slug: str | None) -> None:
        """Zone device: climate entity + heat demand sensor."""
        zone_slug = to_snake_case(zone_name).lower()
        zones_root = getattr(self.topics, "zones", "zones")
        cmd_topic = self._cmd_topic()

        # Controller-reported topics are authoritative; fall back to aggregated zone state
        if ctl_slug:
            temp_topic = f"{self.root}/{zones_root}/{zone_slug}/{ctl_slug}/temperature"
            setpoint_topic = f"{self.root}/{zones_root}/{zone_slug}/{ctl_slug}/setpoint"
        else:
            temp_topic = self._zone_state_topic(zone_slug)
            setpoint_topic = temp_topic

        climate_name = " ".join(
            p for p in [self.zone_climate_prefix, zone_name, self.zone_climate_suffix] if p
        )
        zone_dev_id = f"{self.id_prefix}_zone_{zone_id}"

        components = {
            "climate": {
                "platform": "climate",
                "name": climate_name,
                "unique_id": f"{self.id_prefix}_zone_{zone_id}",
                "object_id": f"{self.id_prefix}_zone_{zone_id}",
                "current_temperature_topic": temp_topic,
                "current_temperature_template": "{{ value_json.temperature }}",
                "temperature_state_topic": setpoint_topic,
                "temperature_state_template": "{{ value_json.setpoint }}",
                "temperature_command_topic": cmd_topic,
                "temperature_command_template": (
                    f'{{"command": "set_zone_setpoint", "zone_idx": "{zone_id}", "setpoint": {{{{ value }}}}}}'
                ),
                "modes": ZONE_HVAC_MODES,
                "mode_state_topic": temp_topic,
                # All evohome zone modes mean the zone is heating; static template.
                "mode_state_template": "{{ 'heat' }}",
                "min_temp": 5,
                "max_temp": 35,
                "temp_step": 0.5,
                **self._availability(),
            },
            "heat_demand": {
                "platform": "sensor",
                "name": "Heat Demand",
                "unique_id": f"{self.id_prefix}_zone_{zone_id}_heat_demand",
                "object_id": f"{self.id_prefix}_zone_{zone_id}_heat_demand",
                "state_topic": self._zone_state_topic(zone_slug),
                "value_template": "{{ value_json.heat_demand }}",
                "unit_of_measurement": "%",
                "state_class": "measurement",
                **self._availability(),
            },
        }
        payload = {
            "device": {
                "identifiers": [zone_dev_id],
                "name": f"{self.gateway_name}: {zone_name} Zone",
                "model": "evohome Zone",
                "manufacturer": "Honeywell evohome",
                "via_device": self.id_prefix,
            },
            "origin": self._origin(),
            "components": components,
        }
        self._publish_device(zone_dev_id, payload)

    def _publish_dhw_device(self) -> None:
        """DHW device: climate entity with heat/auto/off modes."""
        dhw_slug = to_snake_case(self.topics.dhw).lower()
        zones_root = getattr(self.topics, "zones", "zones")
        dhw_zone_root = f"{self.root}/{zones_root}/{dhw_slug}"
        fallback_state = self._zone_state_topic(dhw_slug)
        cmd_topic = self._cmd_topic()

        sensor_slug = self._find_device_slug("07")  # CS92 DHW sensor
        ctl_slug = self._find_device_slug("01")     # CTL controller

        # Resolve topics: explicit config override → auto-detect → zone state fallback
        temp_topic = (
            f"{dhw_zone_root}/{self.dhw_temp_subtopic}" if self.dhw_temp_subtopic
            else f"{dhw_zone_root}/{sensor_slug}/dhw_temp" if sensor_slug
            else fallback_state
        )
        setpoint_topic = (
            f"{dhw_zone_root}/{self.dhw_params_subtopic}" if self.dhw_params_subtopic
            else f"{dhw_zone_root}/{sensor_slug}/dhw_params" if sensor_slug
            else fallback_state
        )
        mode_topic = (
            f"{dhw_zone_root}/{self.dhw_mode_subtopic}" if self.dhw_mode_subtopic
            else f"{dhw_zone_root}/{ctl_slug}/dhw_mode" if ctl_slug
            else fallback_state
        )

        components = {
            "climate": {
                "platform": "climate",
                "name": "Hot Water",
                "unique_id": f"{self.id_prefix}_dhw",
                "object_id": f"{self.id_prefix}_dhw",
                "current_temperature_topic": temp_topic,
                "current_temperature_template": "{{ value_json.temperature }}",
                "temperature_state_topic": setpoint_topic,
                "temperature_state_template": "{{ value_json.setpoint }}",
                "temperature_command_topic": cmd_topic,
                "temperature_command_template": '{"command": "set_dhw_temp", "setpoint": {{ value }}}',
                "modes": DHW_HVAC_MODES,
                "mode_state_topic": mode_topic,
                # follow_schedule → auto; permanent_override uses active flag for heat/off
                "mode_state_template": (
                    "{% if value_json.mode == 'follow_schedule' %}auto"
                    "{% elif value_json.mode == 'permanent_override' %}"
                    "{{ 'heat' if value_json.active else 'off' }}"
                    "{% else %}auto{% endif %}"
                ),
                "mode_command_topic": cmd_topic,
                # Map HA HVAC modes → ramses_rf set_dhw_mode arguments
                "mode_command_template": (
                    "{% if value == 'auto' %}"
                    '{"command": "set_dhw_mode", "mode": "follow_schedule"}'
                    "{% elif value == 'heat' %}"
                    '{"command": "set_dhw_mode", "mode": "permanent_override", "active": true}'
                    "{% elif value == 'off' %}"
                    '{"command": "set_dhw_mode", "mode": "permanent_override", "active": false}'
                    "{% endif %}"
                ),
                "min_temp": 30,
                "max_temp": 85,
                "temp_step": 1,
                **self._availability(),
            }
        }
        dhw_dev_id = f"{self.id_prefix}_zone_HW"
        payload = {
            "device": {
                "identifiers": [dhw_dev_id],
                "name": f"{self.gateway_name}: Hot Water",
                "model": "evohome DHW",
                "manufacturer": "Honeywell evohome",
                "via_device": self.id_prefix,
            },
            "origin": self._origin(),
            "components": components,
        }
        self._publish_device(dhw_dev_id, payload)

    def _publish_physical_device(
        self, dev_id: str, dev_type: str, zone_id: str | None, zone_name: str | None
    ) -> None:
        """Physical device (TRV, thermostat, relay, etc.) with all sensor entities."""
        sensors = DEVICE_SENSORS.get(dev_type)
        if not sensors:
            return

        dev_alias_slug = to_snake_case(self.registry.friendly_name_of(dev_id)).lower()
        dev_slug = self._dev_slug(dev_id)
        zones_root = getattr(self.topics, "zones", "zones")

        alias = self.registry.alias_of(dev_id)
        type_label = self._device_type_label(dev_type)

        is_system = zone_id is None
        if is_system:
            dev_name = (
                f"{self.gateway_name}: {type_label} ({alias})" if alias
                else f"{self.gateway_name}: System {type_label} ({dev_id})"
            )
            via_device = self.id_prefix
        else:
            dev_name = (
                f"{self.gateway_name}: {type_label} ({alias})" if alias
                else f"{self.gateway_name}: {zone_name} {type_label} ({dev_id})"
            )
            via_device = f"{self.id_prefix}_zone_{zone_id}"

        components: dict = {}
        for sensor in sensors:
            msg_code = sensor["msg_code"]
            field = sensor["field"]
            is_binary = sensor.get("binary", False)

            if is_system:
                state_topic = f"{self.root}/system/{dev_alias_slug}/{msg_code}"
            else:
                zone_slug = to_snake_case(zone_name).lower()
                state_topic = f"{self.root}/{zones_root}/{zone_slug}/{dev_alias_slug}/{msg_code}"

            uid = f"{self.id_prefix}_{dev_slug}_{field}"
            component: dict = {
                "platform": "binary_sensor" if is_binary else "sensor",
                "name": sensor["name"],
                "unique_id": uid,
                "object_id": uid,
                "state_topic": state_topic,
                **self._availability(),
            }

            if is_binary:
                component["value_template"] = f"{{{{ value_json.{field} | lower }}}}"
                component["payload_on"] = "true"
                component["payload_off"] = "false"
                if "class" in sensor:
                    component["device_class"] = sensor["class"]
            else:
                component["value_template"] = sensor.get("value_template", f"{{{{ value_json.{field} }}}}")
                if "unit" in sensor:
                    component["unit_of_measurement"] = sensor["unit"]
                if "class" in sensor:
                    component["device_class"] = sensor["class"]
                if "state_class" in sensor:
                    component["state_class"] = sensor["state_class"]

            components[field] = component

        phys_dev_id = f"{self.id_prefix}_{dev_slug}"
        payload = {
            "device": {
                "identifiers": [phys_dev_id],
                "name": dev_name,
                "model": self._device_model(dev_type),
                "manufacturer": "Honeywell evohome",
                "via_device": via_device,
            },
            "origin": self._origin(),
            "components": components,
        }
        self._publish_device(phys_dev_id, payload)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _availability(self) -> dict:
        return {
            "availability_topic": self._status_topic(),
            "availability_template": "{{ value_json.status }}",
            "payload_available": "Online",
            "payload_not_available": "Offline",
        }

    def _origin(self) -> dict:
        return {"name": "evoGateway"}

    def _zone_state_topic(self, zone_slug: str) -> str:
        zones_root = getattr(self.topics, "zones", "zones")
        return f"{self.root}/{zones_root}/{zone_slug}/state"

    def _cmd_topic(self) -> str:
        return f"{self.root}/{self.topics.cmd}"

    def _status_topic(self) -> str:
        return f"{self.root}/status"

    def _find_device_slug(self, type_code: str) -> str | None:
        """Return the snake_case slug of the first registered device of the given type."""
        for dev_id, dev_type in self.registry.type_of_id.items():
            if dev_type == type_code:
                return to_snake_case(self.registry.friendly_name_of(dev_id)).lower()
        return None

    def _dev_slug(self, dev_id: str) -> str:
        return dev_id.replace(":", "_")

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

    def _device_type_label(self, dev_type: str) -> str:
        labels = {
            "04": "TRV",
            "34": "Thermostat",
            "07": "DHW Sensor",
            "13": "Relay",
            "10": "Boiler",
            "02": "UFH Controller",
        }
        return labels.get(dev_type, "Device")

    def _publish_device(self, device_id: str, payload: dict) -> None:
        topic = f"{self.ha_prefix}/device/{device_id}/config"
        self.mqtt.publish(topic, payload, retain=True, raw=True)
