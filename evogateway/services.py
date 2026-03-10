"""Core Application Services.

This module contains the primary logic and communication components of evoGateway. 
It includes:
- MQTTService: Manages the threaded Paho MQTT connection.
- RamsesService: Encapsulates the asynchronous RamsesRF Gateway for RF communication.
- PersistenceService: Handles structured file operations (schema, devices, zones).
- ScheduleHandler: Implements specific logic for interacting with TCC schedules.
"""

from __future__ import annotations
import asyncio
import datetime as _dt
import json
import logging
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Awaitable

import paho.mqtt.client as mqtt

from ramses_rf import Gateway, GracefulExit
from ramses_rf.version import VERSION as RAMSES_RF_VERSION
from ramses_tx import Command, Priority
from ramses_tx.address import HGI_DEVICE_ID, NON_DEVICE_ID, DEV_TYPE_MAP
from ramses_tx.exceptions import RamsesException
from ramses_tx.message import CODE_NAMES
from ramses_rf.exceptions import ExpiredCallbackError 

from .registry import DeviceRegistry

from .config import (
    MQTT_OFFLINE,
    MQTT_ONLINE,
    SEND_STATUS_FAILED,
    SEND_STATUS_SUCCESS,
    SEND_STATUS_TRANSMITTED,
    DEFAULT_MIN_ROW_LENGTH,
    GET_SCHED_WAIT_PERIOD,
    GET_SCHED,
    SET_SCHED,
)

from .utils import make_fake_ramses_message


class MQTTService:
    """Manages the threaded Paho MQTT client connection and bridging to asyncio."""
    def __init__(
        self,
        *,
        server: str,
        user: str,
        password: str,
        client_id: str,
        cmd_topic: str,
        root_topic: str,
        status_subtopic: str,
        on_message_async: Callable[[dict], Awaitable[None]] | None,
        loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
    ) -> None:
        self.server = server
        self.user = user
        self.password = password
        self.client_id = client_id
        self.cmd_topic = cmd_topic
        self.root_topic = root_topic
        self.status_subtopic = status_subtopic
        self._on_message_async = on_message_async
        self._loop = loop
        self.log = logger

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        if self.user:
            self._client.username_pw_set(self.user, self.password)
        
        # Robust topic joining for Last Will
        will_topic = "/".join(filter(None, [self.root_topic.strip("/"), self.status_subtopic.strip("/")]))
        self._client.will_set(
            will_topic,
            payload=json.dumps(self.format_status_payload(MQTT_OFFLINE), indent=4),
            qos=1,
            retain=True,
        )

    def start(self) -> bool:
        try:
            self._client.connect(self.server)
        except Exception as e:
            self.log.error(f"MQTT connect failed: {e}", exc_info=True)
            return False
        self._client.loop_start()
        self.publish_status(MQTT_ONLINE)
        return True

    def stop(self) -> None:
        try:
            self.publish_status(MQTT_OFFLINE)
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.log.info(f"Connected to MQTT broker with reason code: {reason_code}")
        if self.cmd_topic:
            # Robust topic joining for command subscription
            sub_topic = "/".join(filter(None, [self.root_topic.strip("/"), self.cmd_topic.strip("/")]))
            client.subscribe(sub_topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.log.warning(f"Disconnected from MQTT broker with reason code: {reason_code}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            self.log.error("MQTT message is not JSON")
            return
        if self._on_message_async:
            asyncio.run_coroutine_threadsafe(self._on_message_async(payload), self._loop)

    def publish_status(self, status: str) -> None:
        # Use pure formatter, then publish (no behaviour change)
        payload = self.format_status_payload(status)
        self.publish(self.status_subtopic, payload, retain=True)

    def publish(self, subtopic: str, payload: Any, retain: bool = True) -> None:
        # Robust topic joining
        parts = [self.root_topic, subtopic]
        topic = "/".join(filter(None, [p.strip("/") for p in parts]))
        
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        
        # Use qos=1 for retained messages to ensure they are received by the broker correctly
        # Retained messages are often critical state, so qos=1 is appropriate.
        qos = 1 if retain else 0
        
        self.log.debug(f"MQTT Publish: topic={topic}, retain={retain}, qos={qos}, payload_type={type(payload)}")
        self._client.publish(topic, payload, qos=qos, retain=retain)

    # Pure formatter for status payload (keeps _build_status_payload as alias)
    @staticmethod
    def format_status_payload(status: str) -> dict:
        """Pure formatting for status payload."""
        return {"status": status, "status_ts": _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}

    @staticmethod
    def _build_status_payload(status: str) -> dict:
        """
        Backwards-compatible alias: delegate to format_status_payload() to preserve behaviour.
        """
        return MQTTService.format_status_payload(status)


@dataclass
class DeviceRecord:
    alias: str
    zone_id: Optional[str] = None

class CommandSendError(Exception):
    """Raised when an RF command fails to send or validate."""
    pass

class RamsesService:
    """Encapsulates the ramses_rf.Gateway and all RF command/message handling."""
    def __init__(
        self,
        *,
        serial_port: str,
        lib_kwargs: Dict[str, Any],
        logger: logging.Logger,
        registry: DeviceRegistry,
        on_message: Callable[[Any], None],
        publish_schema: Callable[[], None],
        on_sys_config: Callable[[str], None] | None = None,        
        colors: Dict[str, str] | None = None,
        min_row_length: int = DEFAULT_MIN_ROW_LENGTH,
    ) -> None:
        self.serial_port = serial_port
        self.lib_kwargs = lib_kwargs
        self.log = logger
        self.registry = registry
        self._on_message = on_message
        self._publish_schema = publish_schema
        self._handle_sys_config = on_sys_config
        self._colors = colors or {}
        self._min_row_length = min_row_length

        # Discovery counters (for auto-sync)
        self._last_dev_count = 0
        self._last_zone_count = 0

        self.gwy: Optional[Gateway] = None
        self.devices: Dict[str, DeviceRecord] = {}
        self.zones: Dict[str, str] = {}
        self.ufh_circuits: Dict[str, Dict[str, Any]] = {}

    async def start(self) -> None:
        self.log.debug("LIB KWARGS SCHEMA:", json.dumps(self.lib_kwargs.get("schema"), indent=2))

        self.gwy = Gateway(self.serial_port, **self.lib_kwargs)
        self.gwy.add_msg_handler(self._handle_gwy_message)
        self._refresh_devices()
        self._refresh_zones()        
        await self.gwy.start()
        
        self._publish_schema()

        # TODO! Check why zone names not coming through after gateway start. Temp fix for now
        self._update_zone_names_from_schema()

        # Load UFH mappings from schema as these also do not always come through live
        self._load_ufh_mapping_from_schema()

        # Initial sync from gwy after gwy is started/populated
        self.sync_registry_from_gwy()

        # Set counters
        self._last_dev_count = len(self.gwy.device_by_id)
        self._last_zone_count = len(self.gwy.tcs.zone_by_idx) if self.gwy.tcs else 0

    async def stop(self) -> None:
        pass

    def _check_discovery_updates(self):
        dev_count = len(self.gwy.device_by_id)
        if dev_count != self._last_dev_count:
            self.registry.update_from_gateway(self.gwy)
            self._last_dev_count = dev_count

    def device_alias(self, device_id: str) -> str:
        """Return the alias stored in the Ramses schema, or '' if not set."""
        try:
            if not self.gwy:
                return ""
            entry = self.gwy.known_list.get(device_id, {})
            alias = entry.get("alias", "")
            return alias or ""
        except Exception:
            return ""

    def device_type(self, device_id: str) -> str:
        """Return device type code based on the HEX prefix."""
        try:
            prefix = device_id.split(":")[0]
            return DEV_TYPE_MAP.get(prefix, "")
        except Exception:
            return ""

    def device_zone(self, device_id: str) -> str | None:
        """Return zone index string (e.g. '07') of the device."""
        try:
            dev = self.gwy.get_device(device_id)
            return getattr(getattr(dev, "zone", None), "zone_idx", None)
        except Exception:
            return None

    def zone_name(self, zone_id: Optional[str]) -> Optional[str]:
        if not zone_id:
            return None
        return self.zones.get(zone_id)

    def device_label(self, device_id: str) -> str:
        """Return 'TRV Bedroom' style label for console/log display."""
        alias = self.registry.alias(device_id)
        dev_type = self.registry.device_type(device_id)
        if alias:
            return f"{dev_type} {alias}"
        return dev_type

    def check_schema_changed(self) -> bool:
        """Detect whether ramses_rf has disocvered new devices or zones."""
        schema = self.gwy.schema  
        
        dev_count = len(schema.devices)
        zone_count = len(schema.zones)

        changed = False
        
        if dev_count != self._last_dev_count:
            changed = True
            self._last_dev_count = dev_count

        if zone_count != self._last_zone_count:
            changed = True
            self._last_zone_count = zone_count

        if changed:
            # Rebuild registry objects for new devices/zones
            self.registry.rebuild_from_schema(schema)

        return changed

    def _update_zone_names_from_schema(self):
        """Update zone names in the gateway TCS zones from the schema data, as they 
           don't always seem to come through from the gateway."""

        # Make sure we actually have a valid schema/tcs
        if not (self.gwy.tcs and self.gwy.tcs.id):
            self.log.warn("GWY does not have a valid schema from which to update zone names")
            return
        
        for zone_id in self.lib_kwargs[self.gwy.tcs.id]["zones"]:
            zone = self.lib_kwargs[self.gwy.tcs.id]["zones"][zone_id]
            self.gwy.tcs.zones[int(zone_id,16)]._name = zone.get("_name", f"Zone {zone_id}")

    def _load_ufh_mapping_from_schema(self):
        """Load UFH circuit zone mappings directly from the schema file
           as they don't always seem to come through from the gateway."""
        
        # reg.ufh_map.clear()

        # ufh_schema = self.lib_kwargs[self.gwy.tcs.id]["underfloor_heating"]
        # self.gwy.tcs.schema["underfloor_heating"] = ufh_schema
        try:
            ufh_schema = self.lib_kwargs[self.gwy.tcs.id]
            if ufh_schema:
                self.registry.set_ufh_map_from_schema(ufh_schema)
        except Exception:
            self.log.exception("Failed to load UFH circuit mappings from local schema")

    def sync_registry_from_gwy(self) -> None:
        """Extract all device/zone/ufh metadata from ramses and push into registry."""

        reg = self.registry
        gwy = self.gwy

        # ID of the connected HGI 
        if getattr(gwy, "hgi", None) and getattr(gwy.hgi, "id", None):
            self.registry.set_hgi(gwy.hgi.id)

        # Schema alias import (known_devices or known_list) 
        known = getattr(gwy, "known_devices", None) or getattr(gwy, "known_list", {})
        for dev_id, meta in known.items():
            alias = meta.get("alias", None)
            if alias:
                reg.update_alias(dev_id, alias)

        # Live discovery: device types and zones 
        for dev_id, dev in gwy.device_by_id.items():

            # Device Type (e.g. "04", "10")
            dev_type = getattr(dev, "type", None)
            if dev_type:
                reg.update_device_type(dev_id, dev_type)

            # Zone Index
            zone_obj = getattr(dev, "zone", None)
            zone_id = getattr(zone_obj, "idx", None) if zone_obj else None
            if zone_id:
                reg.update_zone(dev_id, zone_id)

            # Zone Name (if available)
            try:
                zone_name =  getattr(self.gwy.tcs.zones[int(zone_id,16)], "_name", None) if (self.gwy.tcs and zone_id) else None
                if zone_id and zone_name:
                    reg.update_zone_name(zone_id, zone_name)
            except Exception:
                pass

        # UFH Circuit -> Zone mappings (consistent style)
        reg.ufh_map.clear()

        schema = getattr(gwy, "schema", None) or {}
        ufh = schema.get("underfloor_heating", {}) if isinstance(schema, dict) else {}

        for ufh_dev_id, data in ufh.items():
            circuits = (data or {}).get("circuits", {}) or {}
            for circuit_id, cinfo in circuits.items():
                zone_idx = (cinfo or {}).get("zone_idx")
                if zone_idx:
                    reg.ufh_map[(ufh_dev_id, str(circuit_id))] = zone_idx

        # Update internal counters for discovery auto-sync 
        self._last_dev_count = len(gwy.device_by_id)

        tcs = getattr(gwy, "tcs", None)
        zone_by_idx = getattr(tcs, "zone_by_idx", {}) if tcs else {}
        self._last_zone_count = len(zone_by_idx)


    # Process commands received via MQTT
    async def process_command(
        self,
        payload: Dict[str, Any],
        publish_status: Callable[[Optional[str], str, Optional[str]], None],
    ) -> None:

        if not self.gwy:
            self.log.error("Gateway not started")
            publish_status(None, "Failed", error="Gateway not started")
            return

        # Identify command name early (best effort)
        cmd_name = None

        try:
            # Handle sys_config commands
            if "sys_config" in payload:
                cmd_name = payload["sys_config"]
                publish_status(cmd_name, "Transmitted")

                cmd = str(cmd_name).upper().strip()
                if cmd in ("POST_SCHEMA", "SAVE_SCHEMA"):
                    self._refresh_zones()
                    self._refresh_devices()
                    self._publish_schema()
                    if self._handle_sys_config:
                        self._handle_sys_config(cmd)
                    publish_status(cmd_name, "Successful")
                else:
                    self.log.warning("Unknown sys_config command: %s", cmd)
                    publish_status(cmd_name, "Failed", error="Unknown sys_config command")
                return

            #  Handle low-level 'code' styled commands
            if "code" in payload:
                cmd_name = f"code_{payload.get('code')}"
                publish_status(cmd_name, "Transmitted")

                code = payload["code"]
                if isinstance(code, int):
                    code = hex(code).upper().replace("0X", "")

                verb = payload.get("verb")
                pl = payload.get("payload", {})
                dest_id = payload.get("dest_id") or (self.gwy.tcs.id if self.gwy.tcs else None)

                if not (verb and pl):
                    error = "Invalid command: missing verb/payload"
                    self.log.error(error)
                    publish_status(cmd_name, "Failed", error=error)
                    return

                gw_cmd = self.gwy.create_cmd(verb, dest_id, code, pl)

                try:
                    await self._send_cmd(gw_cmd)
                except CommandSendError as e:
                    publish_status(cmd_name, "Failed", error=str(e))
                    return

                publish_status(cmd_name, "Successful")
                return

            #  Handle high-level 'command' styled commands 
            if "command" in payload:
                cmd_name = payload["command"]
                publish_status(cmd_name, "Transmitted")

                name = cmd_name
                if name == "ping":
                    name = "get_system_time"

                ctor = getattr(Command, name)
                sig_keys = sorted(list(inspect.signature(ctor).parameters.keys()))

                kwargs = {k: v for k, v in payload.items() if k != "command"}

                if "dst_id" in sig_keys and "dst_id" not in kwargs and self.gwy.tcs:
                    kwargs["dst_id"] = self.gwy.tcs.id
                if "ctl_id" in sig_keys and "ctl_id" not in kwargs and self.gwy.tcs:
                    kwargs["ctl_id"] = self.gwy.tcs.id

                try:
                    gw_cmd = ctor(**kwargs)
                except Exception as ex:
                    msg = f"Error building command '{name}': {ex}"
                    publish_status(cmd_name, "Failed", error=msg)
                    return

                try:
                    await self._send_cmd(gw_cmd)
                except CommandSendError as e:
                    publish_status(cmd_name, "Failed", error=str(e))
                    return

                publish_status(cmd_name, "Successful")
                return

            # Invalid command 
            self.log.error("Invalid MQTT payload: missing 'command' or 'code'")
            publish_status(None, "Failed", error="Missing 'command' or 'code'")

        except Exception as ex:
            # Final safety net: unexpected errors get reported
            self.log.exception("Exception while processing MQTT command")
            publish_status(cmd_name, "Failed", error=str(ex))

    async def _send_cmd(self, gw_cmd: Command) -> None:
        """Sends commands and validates response"""
        if not self.gwy:
            raise CommandSendError("Gateway is not initialized")

        try:
            await self.gwy.send_cmd(
                gw_cmd,
                wait_for_reply=True,
                priority=Priority.HIGH
            )
        except Exception as ex:
            raise CommandSendError(f"RF transmission failed: {ex}") from ex

        # Validate response using rx_header property of sent command (contains any reply received)
        try:
            rx = getattr(gw_cmd, "rx_header", None)
            if rx:
                reply_code, reply_verb, _rest = rx.split("|", maxsplit=2)
                if reply_code == gw_cmd.code and reply_verb.strip() in ("RP", "I"):
                    return  # SUCCESS
        except Exception:
            pass  # fall through

        # If here, reply header invalid or missing
        rx = getattr(gw_cmd, "rx_header", None)
        msg = f"Invalid or missing reply header for '{CODE_NAMES.get(gw_cmd.code, 'Unknown')}'"
        if rx:
            msg += f" (rx_header='{rx}')"

        raise CommandSendError(msg)

    def _handle_gwy_message(self, msg) -> None:
        try:
            msg.code_name = CODE_NAMES.get(msg.code, getattr(msg, "code_name", str(msg.code)))
        except Exception:
            pass
        # Registry auto-sync when new devices/zones discovered by ramses
        try:
            dev_count = len(self.gwy.device_by_id)
            tcs = getattr(self.gwy, "tcs", None)
            zone_by_idx = getattr(tcs, "zone_by_idx", {}) if tcs else {}
            zone_count = len(zone_by_idx)

            if dev_count != self._last_dev_count or zone_count != self._last_zone_count:
                self.sync_registry_from_gwy()
                self._publish_schema()

            self._on_message(msg)
        except Exception:
            self.log.exception("Registry sync failed")

    def _refresh_devices(self) -> None:
        if not self.gwy:
            return
        schema = self.gwy.tcs.schema if self.gwy.tcs else self.gwy.schema
        try:
            ctl_id = self.gwy.tcs.id if self.gwy.tcs else (self.gwy.schema.get("main_tcs") if self.gwy.schema else None)
            if ctl_id and ctl_id not in self.devices:
                self.devices[ctl_id] = DeviceRecord(alias="Controller")
        except Exception:
            pass

        try:
            zones = schema.get("zones", {})
            for zone_id, zone_items in zones.items():
                sensor_id = zone_items.get("sensor")
                if sensor_id:
                    self.devices.setdefault(sensor_id, DeviceRecord(alias=f"{self._device_type_code(sensor_id)}", zone_id=zone_id))
                for dev_id in zone_items.get("devices", []) or []:
                    if dev_id:
                        self.devices.setdefault(dev_id, DeviceRecord(alias=f"{self._device_type_code(dev_id)}", zone_id=zone_id))
        except Exception:
            pass

    def _refresh_zones(self) -> None:
        if not self.gwy:
            return
        schema = self.gwy.tcs.schema if self.gwy.tcs else self.gwy.schema
        params = self.gwy.tcs.params if self.gwy.tcs else self.gwy.params
        try:
            zones = schema.get("zones", {})
            for zid in zones:
                name = (params.get("zones", {}).get(zid, {}).get("name") if isinstance(params, dict) else None)
                if name:
                    self.zones[zid] = name
        except Exception:
            pass
        try:
            if "ufh_system" in schema:
                for _ufc_id, u in schema["ufh_system"].items():
                    for c, data in (u.get("circuits", {}) or {}).items():
                        if c not in self.ufh_circuits:
                            self.ufh_circuits[c] = data
        except Exception:
            pass

    @staticmethod
    def _device_type_code(device_id: str) -> str:
        try:
            typ = device_id.split(":")[0]
            return DEV_TYPE_MAP.get(typ, typ)
        except Exception:
            return "Device"


@dataclass
class PersistenceService:
    def __init__(
        self,
        schema_file: Path,
        logger: logging.Logger,
        max_backups: int = 10,        
    ):
        self.schema_file = schema_file
        self.max_backups = max_backups
        self.log = logger

    def load_schema(self) -> Dict[str, Any]:
        """
        We are standardizing on using the ramses_rf schema as single master source of truth,
        and have removed the previous separate devices/zones files etc

        As such, do not mutate/normalize here to avoid introducing incompatibilities with
        ramses_rf's expected formats. If the file doesn't exist or is invalid JSON,
        return an empty dict so the caller can decide whether to enable discovery/eavesdrop.
        """
        try:
            data = self.load_json(self.schema_file)
            return data if isinstance(data, dict) else {}
        except Exception as ex:
            # Corrupt or unreadable — treat as no schema
            self.log.exception("Failed to load schema file: %s", ex)
            return {}

    def save_schema(self, schema: Dict[str, Any]) -> None:
        """
        Persist the full schema as-is with rotation.
        Caller should pass exactly what ramses_rf expects/produces.
        """
        self.save_json_with_rotation(self.schema_file, schema, max_backups=self.max_backups)

    def schema_exists(self) -> bool:
        try:
            return self.schema_file.exists() and self.schema_file.is_file()
        except Exception:
            return False

    def save_json_with_rotation(self, path: Path, data: Any, *, max_backups: int = 9) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            for i in range(max_backups, 0, -1):
                src = path.with_suffix(path.suffix + f".{i}")
                dst = path.with_suffix(path.suffix + f".{i+1}")
                if src.exists():
                    if i == max_backups:
                        src.unlink(missing_ok=True)
                    else:
                        src.rename(dst)
            path.rename(path.with_suffix(path.suffix + ".1"))
        path.write_text(json.dumps(data, indent=4, sort_keys=True))

    def load_json(self, path: Path) -> dict:
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            pass
        return {}


class ScheduleError(Exception):
    """Raised when GET/SET schedule operations fail."""
    pass

class ScheduleHandler:
    """Handles logic for GET/SET schedule commands via the RamsesRF Gateway."""
    def __init__(
            self, 
            *, 
            ramses: "RamsesService", 
            router: Any, 
            delay_seconds: int = GET_SCHED_WAIT_PERIOD, 
            logger: logging.Logger,
        ) -> None:
        self.ramses = ramses
        self.router = router
        self.delay_seconds = delay_seconds
        self.log = logger

    async def handle_command(self, payload: dict, publish_send_status: Callable[[Optional[str], str], None]) -> None:
        
        cmd = payload.get("command")
        
        try:
            zone_idx = payload.get("zone_idx")
            if zone_idx is None:
                raise ScheduleError(f"Zone not found: {zone_idx}")
            
            publish_send_status(cmd, "Transmitted")

            if cmd == GET_SCHED:
                force_io = bool(payload.get("force_refresh"))
                await self.get_schedule(zone_idx, force_io)
            elif cmd == SET_SCHED:
                if "schedule" not in payload:
                    raise ScheduleError("set_schedule failed as no schedule given")
                await self.set_schedule(zone_idx, payload.get("schedule"))
        except ScheduleError as e:
            publish_send_status(cmd, "Failed", error=str(e))
            return

        except Exception as e:
            publish_send_status(cmd, "Failed", error=str(e))
            return

        # No exception: success
        publish_send_status(cmd, "Successful")

    async def get_schedule(self, zone_idx: str, force_io: bool = False) -> None:

        gwy = self.ramses.gwy
        if not gwy or not getattr(gwy, "tcs", None):
            raise ScheduleError("No controller available")

        if zone_idx == "HW":
            dhw = gwy.tcs.dhw
            zone: Any = dhw
        else:
            zone = gwy.tcs.zone_by_idx.get(zone_idx)
        if not zone:
            raise ScheduleError(f"Zone not found: {zone_idx}")

        # Attempt schedule fetch
        try:
            if not getattr(zone, "schedule", None) or force_io:
                await zone.get_schedule(force_io=force_io)

        except ExpiredCallbackError as e:
            raise ScheduleError(
                f"Timeout while requesting schedule for zone {zone_idx}: {e}"
            ) from e

        except Exception as e:
            raise ScheduleError(
                f"Error requesting schedule for zone {zone_idx}: {e}"
            ) from e

        # Wait for fragments to populate the schedule
        await asyncio.sleep(self.delay_seconds)

        # If schedule still missing, this is a failure
        if not getattr(zone, "schedule", None):
            raise ScheduleError(
                f"Schedule not received after timeout for zone {zone_idx}"
            )

        # Publish the schedule normally
        self._publish_schedule_for_zone(zone)


    async def set_schedule(self, zone_idx: str, schedule: Any) -> None:

        gwy = self.ramses.gwy
        if not gwy or not getattr(gwy, "tcs", None):
            raise ScheduleError("No controller available")

        if zone_idx == "HW":
            dhw = gwy.tcs.dhw
            zone: Any = dhw
        else:
            zone = gwy.tcs.zone_by_idx.get(zone_idx)
        if not zone:
            raise ScheduleError(f"Zone not found: {zone_idx}")

        # Attempt sending the schedule
        try:
            await zone.set_schedule(schedule)

        except ExpiredCallbackError as e:
            raise ScheduleError(
                f"Timeout while sending schedule for zone {zone_idx}: {e}"
            ) from e

        except Exception as e:
            raise ScheduleError(
                f"Error sending schedule for zone {zone_idx}: {e}"
            ) from e

        # Give ramses_rf time to populate the schedule fragments
        await asyncio.sleep(self.delay_seconds)

        # If the schedule didn't update on the zone, treat it as a failure
        if not getattr(zone, "schedule", None):
            raise ScheduleError(
                f"Schedule update not confirmed for zone {zone_idx}"
            )

        # Publish the schedule normally
        self._publish_schedule_for_zone(zone)

    def _publish_schedule_for_zone(self, zone) -> None:
        if not zone or not getattr(zone, "schedule", None):
            return
        gwy = self.ramses.gwy
        ctl_id = gwy.tcs.id if gwy and getattr(gwy, "tcs", None) else None
        ctl_type = gwy.get_device(ctl_id).type if ctl_id else "01"

        fake_msg = make_fake_ramses_message(
            "zone_schedule", ctl_id, ctl_type, zone,
            {"schedule": zone.schedule, "zone_idx": zone.idx}
        )
        self.router.handle_message(fake_msg)
