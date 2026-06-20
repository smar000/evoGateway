from __future__ import annotations
from typing import Dict, Optional, Any
from ramses_tx.address import DEV_TYPE_MAP, ALL_DEV_ADDR

class DeviceRegistry:
    """
    Unified source of truth for device metadata, zone names, aliases and types.

    Populated from:
      • Local schema (if present)
      • RamsesRF discovery/eavesdropping (live updates)
      • Explicit updates from services/router (future use)
    """

    def __init__(
        self,
        merge_unknown_otb: bool = True,
        merge_unknown_hgi: bool = True,
        merge_unknown_ctl: bool = False,
    ) -> None:
        # Device metadata
        self.alias_of_id: Dict[str, str] = {}          # e.g. {"34:112233": "living_room_thermostat"}
        self.type_of_id: Dict[str, str] = {}           # all device IDs and their types
        self.zone_of_id: Dict[str, str] = {}           # zone index -> device ID mappings
        self.zone_names: Dict[str, str] = {}           # zone index -> human-readable name
        self.ufh_map: dict[tuple[str, str], str] = {}  # (ufh_dev_id, ufh_circuit) -> zone_idx
        self.hgi_id: str = ""                          # Connected HGI id from gwy
        self.this_gateway_name = None                  # Our friendly name for the HGI/evogateway

        # Build the set of type prefixes eligible for singleton ID merging
        self._merge_types: frozenset[str] = frozenset(
            t for t, flag in (("10", merge_unknown_otb), ("18", merge_unknown_hgi), ("01", merge_unknown_ctl))
            if flag
        )

    # Public lookup helpers used by router and models
    

    def alias_of(self, dev_id: str) -> str:
        """Return the alias for a device, or generate a fallback based on device ID"""
        if not dev_id:
            return ""

        # Check for connected HGI
        if self.hgi_id and dev_id == self.hgi_id:
            if self.this_gateway_name:
                return self.this_gateway_name

        # Well-known sentinel addresses
        if dev_id == ALL_DEV_ADDR.id:
            return f"Broadcast ({dev_id})"

        # Otherwise alias from schema/user
        alias = self.alias_of_id.get(dev_id)
        if alias:
            return alias

        # Fallback: detect friendly device type
        dev_type = None

        # Fallback to device type; if not yet registered use the ID prefix
        type_id = self.type_of_id.get(dev_id) or (dev_id.split(":")[0] if ":" in dev_id else None)

        if type_id == "18":
            # Ramses gives THM for this type id; Ours are normally HGI devices...
            dev_type = "HGI"
        elif type_id in DEV_TYPE_MAP:
            dev_type = DEV_TYPE_MAP[type_id].name

        if not dev_type:
            dev_type = type_id

        # Fallback if still unknown
        if not dev_type:
            dev_type = "DEV"

        compact_id = dev_id.split(":")[-1]
        return f"{dev_type} ({compact_id})"


    def type_of(self, dev_id: str | None) -> Optional[str]:
        if dev_id is None:
            return None
        return self.type_of_id.get(str(dev_id))

    def canonical_id(self, device_id: str | None) -> str | None:
        """Return the canonical device ID, merging unknown singleton-type devices.

        If a device ID is unrecognised but its type prefix is in the active merge
        set and exactly one device of that type is registered, returns that known
        device's ID. Otherwise returns the original ID unchanged.
        """
        if not device_id or not self._merge_types:
            return device_id
        if device_id in self.type_of_id:
            return device_id  # already known — no remap needed
        prefix = device_id.split(":")[0] if ":" in device_id else None
        if not prefix or prefix not in self._merge_types:
            return device_id
        candidates = [k for k, v in self.type_of_id.items() if v == prefix]
        return candidates[0] if len(candidates) == 1 else device_id

    def friendly_name_of(self, dev_id: str) -> str:
        """Return device type and alias combination """
        if not dev_id:
            return ""

        dev_type_code = self.device_type_code(dev_id)
        
        # device alias from schema/user
        dev_alias = self.alias_of_id.get(dev_id) #if dev_type_code != "CTL" else "Controller"
        if not dev_alias:
            dev_alias = dev_id.split(":")[-1]  # take last segment of ID

        return f"{dev_type_code} {dev_alias}"

    def zone_of(self, dev_id: str | None) -> Optional[str]:
        if dev_id is None:
            return None
        return self.zone_of_id.get(str(dev_id))

    def zone_name(self, zone_id: str | None) -> Optional[str]:
        if zone_id is None:
            return None
        return self.zone_names.get(str(zone_id))

    def ufh_zone_for(self, ufh_dev_id: str, circuit_id: str) -> str | None:
        return self.ufh_map.get((ufh_dev_id, circuit_id))

    def device_type(self, dev_id: str | None) -> str | None:
        return self.type_of(dev_id)

    def device_type_code(self, dev_id: str | None) -> str | None:
        if dev_id in self.type_of_id:
            type_id = self.type_of_id[dev_id]
        else:
            type_id = dev_id.split(":")[0]
        
        return DEV_TYPE_MAP[type_id].name if type_id in DEV_TYPE_MAP else type_id

    def update_device_type(self, dev_id: str, dev_type: str) -> None:
        if dev_id and dev_type:
            self.type_of_id[str(dev_id)] = dev_type

    def update_alias(self, dev_id: str | None, alias: str | None) -> None:
        if dev_id and alias:
            self.alias_of_id[str(dev_id)] = alias

    def update_zone(self, dev_id: str | None, zone_id: str | None) -> None:
        if dev_id and zone_id:
            self.zone_of_id[str(dev_id)] = zone_id

    def update_zone_name(self, zone_id: str, name: str) -> None:
        if zone_id and name:
            self.zone_names[zone_id] = name

    def set_hgi(self, hgi_id: str, friendly_name: str | None = None):
        self.hgi_id = hgi_id
        self.this_gateway_name = friendly_name
        if hgi_id:
            if "18" in self._merge_types:
                # Purge any stale phantom HGI entries so canonical_id() finds exactly one
                for k in [k for k, v in self.type_of_id.items() if v == "18" and k != hgi_id]:
                    del self.type_of_id[k]
            self.type_of_id[hgi_id] = "18"  # ensure canonical_id() can find it

    def set_ufh_map_from_schema(self, schema: dict) -> None:
        """Populate (ufh_dev_id, circuit_id) → zone_idx from a schema dict."""
        self.ufh_map.clear()       

        if not isinstance(schema, dict):
            return

        ufh_section = (schema.get("underfloor_heating") or {})
        if not isinstance(ufh_section, dict):
            return

        for ufh_dev_id, data in ufh_section.items():
            circuits = (data or {}).get("circuits") or {}
            if not isinstance(circuits, dict):
                continue
            for circuit_id, cinfo in circuits.items():
                zone_idx = (cinfo or {}).get("zone_idx")
                if zone_idx:
                    # normalise key shapes
                    self.ufh_map[(str(ufh_dev_id), str(circuit_id))] = str(zone_idx).upper()

        


        