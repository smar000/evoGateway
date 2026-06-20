from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from datetime import datetime as _dt
from .utils import to_snake_case
from .registry import DeviceRegistry
from .config import MqttConfig

# Classification Tables for Evohome/Ramses Messages
# Temperature-related codes
TEMPERATURE_CODES = {
    "temperature",
    "dhw_temp",
    "dhw_temperature",
    "sensor_temperature",
    "outside_temperature",
    "relay_temperature",
}

# Demand / Actuator State
DEMAND_CODES = {
    "heat_demand",
    "actuator_state",
    "boiler_relay",
    "zone_demand",
}

# OpenTherm / HVAC sync messages
SYNC_CODES = {
    "opentherm_sync",
    "ot_sync",
    "ot_config",
    "opentherm_config",
}

# Zone configuration messages
ZONE_CONFIG_CODES = {
    "zone_name",
    "zone_config",
    "zone_parameters",
    "zone_schedule",
}

# Device identity / type / info messages
DEVICE_INFO_CODES = {
    "device_info",
    "device_version",
    "device_type",
    "binding_info",
    "binding_list",
}

# Schedule messages
SCHEDULE_CODES = {
    "schedule_fragment",
    "schedule_part",
    "zone_schedule",
}

# Fault / Error / Diagnostic messages
FAULT_CODES = {
    "fault",
    "system_fault",
    "zone_fault",
    "device_fault",
    "sensor_fault",
}

# Heartbeat / keepalive / presence
HEARTBEAT_CODES = {
    "heartbeat",
    "tick",
    "sync",
}

# Unknown or miscellaneous
MISC_CODES = {
    "unknown",
    "misc",
}

@dataclass(frozen=True)
class ParsedMessage:
    raw: Any                     
    payload: Dict[str, Any]     # raw payload dict (immutable copy)
    registry: DeviceRegistry
    verb: str
    code: str
    timestamp: str

    # Optional 
    src_id: Optional[str] = None
    dst_id: Optional[str] = None

    dtm: Optional[str] = None

    zone_id: Optional[str] = None
    zone_name: Optional[str] = None
    device_type: Optional[str] = None

    rssi: Optional[str] = None

    def __post_init__(self):
        """Compute zone_id and zone_name after dataclass init."""
        # Only resolve zone_id if router didn't explicitly set it
        if self.zone_id is None:
            zid = self._resolve_zone_id(self.raw, self.payload)
            object.__setattr__(self, "zone_id", zid)

        # Now compute zone_name
        if self.zone_id:
            zname = self.registry.zone_name(self.zone_id)
            object.__setattr__(self, "zone_name", zname)
        else:
            object.__setattr__(self, "zone_name", None)


    def _resolve_zone_id(self, msg, payload) -> str | None:
        """Determine the logical target zone for this message.

        1) Prefer payload zone hints
        2) Handle special relay zones (F9 = DHW, FA = Radiators and FC = UFH)
        3) Fall back to the source device's zone
        4) Handle controllers, HGI, DHW, relays
        """

        try:
            reg = self.registry
            src = getattr(msg, "src", None)
            src_type = getattr(src, "type", "")
            src_id = getattr(src, "id", "")
            
            # # DHW wireless sensor is type 07
            # if src_type == "07":
            #     return "HW"
            
            # Otherwise prefer payload target zone if given (check for ufh_idx first as this requires mapping)
            ufh_idx = payload.get("ufh_idx")
            if ufh_idx is not None and src_type == "02":         # UFH controller
                mapped = reg.ufh_zone_for(src_id, str(ufh_idx))
                if mapped:
                    return mapped
            zone_idx = (
                payload.get("zone_idx")
                or payload.get("parent_idx")
                or payload.get("domain_id")
            )

            # Zone is BDR or UFH controller (F9 = DHW BDR, FA = Radiator BDR, FC = UFH controller)
            if isinstance(zone_idx, str) and zone_idx.strip().lower() in ("fa", "fc", "f9"):
                return zone_idx.upper()

            # Controllers / HGI produce system / independent messages
            if not zone_idx and src_type in ("01", "18"):
                return "SYS"

            # If payload gave no zone, use the source device's declared zone
            if not zone_idx and src_id:
                zone_idx = reg.zone_of(src_id)


            # # Relay-types (BDR = 02, OTB = 10, UFH = 13) 
            # if src_type in ("02", "10", "13"):
            #     return "FA"   # heating relay pseudo-zone

            # Normalise zone indexes to hex if numeric 
            if zone_idx:
                try:
                    zone_name = reg.zone_name(zone_idx) if zone_idx else None
                    # if zone_idx.isdigit():
                    #     zone_idx = f"{int(zone_idx):02X}"
                except Exception:
                    pass
                return zone_idx

            return None

        except Exception as ex:
            print(f"Excpetion: {ex}")
            return None

   
    # Classification Properties
    @property
    def is_temperature(self) -> bool:
        return self.code in TEMPERATURE_CODES

    @property
    def is_demand(self) -> bool:
        return self.code in DEMAND_CODES

    @property
    def is_sync(self) -> bool:
        return self.code in SYNC_CODES

    @property
    def is_zone_config(self) -> bool:
        return self.code in ZONE_CONFIG_CODES

    @property
    def is_device_info(self) -> bool:
        return self.code in DEVICE_INFO_CODES

    @property
    def is_schedule_fragment(self) -> bool:
        return self.code in SCHEDULE_CODES

    @property
    def is_fault(self) -> bool:
        return self.code in FAULT_CODES

    @property
    def is_heartbeat(self) -> bool:
        return self.code in HEARTBEAT_CODES

    @property
    def is_misc(self) -> bool:
        return self.code in MISC_CODES

    @property
    def is_sensor_message(self) -> bool:
        return self.is_temperature or self.code.startswith("sensor_")

    @property
    def is_actuator_message(self) -> bool:
        return self.is_demand or self.is_sync

    @property
    def is_zone_message(self) -> bool:
        return self.zone_id is not None

    @property
    def is_device_message(self) -> bool:
        return self.zone_id is None

    @property
    def family(self) -> str:
        """High-level category used for routing, logging etc"""
        if self.is_temperature:
            return "temperature"
        if self.is_demand:
            return "demand"
        if self.is_sync:
            return "sync"
        if self.is_zone_config:
            return "zone_config"
        if self.is_schedule_fragment:
            return "schedule"
        if self.is_device_info:
            return "device_info"
        if self.is_fault:
            return "fault"
        if self.is_heartbeat:
            return "heartbeat"
        return "misc"

    # Registry-based lookup helpers
    def src_alias(self) -> str | None:
        return self.registry.alias_of(self.src_id)

    def dst_alias(self) -> str | None:
        return self.registry.alias_of(self.dst_id)

    def device_type(self) -> str | None:
        if not self.registry:
            return None
        return self.registry.device_type(self.src_id)

    def zone_id_resolved(self) -> str | None:
        if getattr(self, "zone_id", None):
            return self.zone_id
        if not self.registry:
            return None
        return self.registry.zone_of(self.src_id)

    def zone_name(self) -> str | None:
        zid = self.zone_id_resolved()
        if not zid:
            return None
        if not self.registry:
            return None
        return self.registry.zone_name(zid)


    # MQTT Topic Helpers
    def is_controller_message(self) -> bool:
        """Detect messages from the central controller that should not be
        treated as belonging to another zone."""
        
        if not self.src_id or self.device_type != "01":
            return False

        msg_to_different_zone = self.src_id != self.dst_id
        
        # If msg src is from a controller but the message
        # zone is to a different zone, it's a controller message
        return msg_to_different_zone

    def topic_device(self) -> str:
        """Return device ID or alias as snake_case."""
        raw = self.src_alias() or self.src_id
        return to_snake_case(raw)
    
    def topic_code(self) -> str:
        """Return message code as snake_case."""
        return to_snake_case(self.code)

    def topic_zone(self) -> str | None:
        zid = self.zone_id
        if not zid:
            return None

        zname = self.registry.zone_name(zid)
        if not zname:
            return None

        return to_snake_case(zname)

    def topic_base(self, *, topics: MqttConfig.TopicLayout, group_by_zone: bool) -> str:
        """
        Build MQTT topic base according to device zone and type,
        unless group_by_zone=False, in which case we flatten topics completely.
        """

        RELAY_DEVICE_TYPES = {"02", "10", "13"}
        RELAY_ZONE_IDS = {"F9": "dhw", "FA": "radiators", "FC": "ufh"}

        # Flattended mode – bypass ALL grouping logic
        if not group_by_zone:
            device_path_snake = to_snake_case(
                self.registry.friendly_name_of(self.src_id)
            ).lower()

            # Simple code handling (same rules as structured mode)
            dev_type = self.registry.type_of(self.src_id) or "device"
            if self.zone_id in RELAY_ZONE_IDS:   # relay pseudo-zones
                code = f"{self.code.lower()}/_domain_{self.zone_id.upper()}_{RELAY_ZONE_IDS[self.zone_id].lower()}"
            elif dev_type == "10" and "msg_name" in self.payload:
                code = f"{self.code.lower()}/{to_snake_case(self.payload['msg_name'])}"
            else:
                code = self.code
            code_snake = to_snake_case(code)

            # Return flat legacy-style topic
            return f"{device_path_snake}/{code_snake}"

        # Safe topic setting extraction
        zone_independent_topic = getattr(topics, "system", "system")
        unknown_zone_topic     = getattr(topics, "zone_unknown",  "_unknown")
        zones_root_topic       = getattr(topics, "zones", "")
        relays_root_topic      = getattr(topics, "relays", "relays")
        controllers_root_topic = getattr(topics, "controller", "") 
        dhw_root_topic         = getattr(topics, "dhw", "_dhw") or "_dhw"
        dhw_is_zone            = getattr(topics, "dhw_is_zone", True)
        use_legacy             = getattr(topics, "use_legacy", True)

        dev_type = self.registry.type_of(self.src_id) or "device"

        # DHW classification
        is_dhw = (
            self.zone_id == "HW"
            or (self.code and "dhw" in self.code.lower())
        )

        # Determine base 'top'
        if is_dhw and dhw_is_zone:
            # registry name wins if present
            zname = self.registry.zone_name(self.zone_id)
            top = to_snake_case(zname).lower() if zname else to_snake_case(dhw_root_topic).lower()

        elif is_dhw and not dhw_is_zone:
            top = dhw_root_topic

        elif self.zone_id and self.zone_name:
            top = to_snake_case(self.zone_name).lower()

        elif self.device_type in ("01", "18"):
            top = f"{zone_independent_topic}/{controllers_root_topic}"

        elif (
            self.zone_id is None
            or dev_type in RELAY_DEVICE_TYPES
            or self.zone_id in RELAY_ZONE_IDS
        ):
            top = f"{zone_independent_topic}/{relays_root_topic}"

        else:
            top = unknown_zone_topic

        # Device path
        device_path_snake = to_snake_case(self.registry.friendly_name_of(self.src_id)).lower()

        # Work out code
        if self.zone_id in RELAY_ZONE_IDS:
            code = f"{self.code.lower()}/_domain_{self.zone_id.upper()}_{RELAY_ZONE_IDS[self.zone_id].lower()}"
        elif dev_type == "10" and "msg_name" in self.payload:
            code = f"{self.code.lower()}/{to_snake_case(self.payload['msg_name'])}"
        else:
            code = self.code

        code_snake = to_snake_case(code)

        # Wrap zones for new layout
        if not use_legacy:
            if is_dhw and dhw_is_zone:
                # registry name if available
                zname = self.registry.zone_name(self.zone_id)
                name = to_snake_case(zname).lower() if zname else to_snake_case(dhw_root_topic).lower()
                top = f"{zones_root_topic}/{name}" if zones_root_topic else name

            elif self.zone_id and self.zone_name:
                zone_name_snake = to_snake_case(self.zone_name).lower()
                top = f"{zones_root_topic}/{zone_name_snake}" if zones_root_topic else zone_name_snake

        return f"{top}/{device_path_snake}/{code_snake}"

