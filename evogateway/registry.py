from __future__ import annotations
from typing import Dict, Optional, Any
from ramses_tx.address import DEV_TYPE_MAP

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

        # Otherwise alias from schema/user
        alias = self.alias_of_id.get(dev_id)
        if alias:
            return alias

        # Fallback: detect friendly device type
        dev_type = None

        # Fallback to device type
        type_id = self.type_of_id[dev_id] if dev_id in self.type_of_id else None

        if type_id == "18":
            # Ramses gives THM for this type id; Ours are normally HGI devices...
            dev_type = "HGI"
        elif type_id in DEV_TYPE_MAP:
            dev_type = DEV_TYPE_MAP[dev_type].name

        if not dev_type:
            dev_type = type_id

        # Fallback if still unknown
        if not dev_type:
            dev_type = "DEV"

        # Build friendly deterministic alias
        compact_id = dev_id.split(":")[-1]  # take last segment of ID
        return f"{dev_type} ({dev_id})"


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

    # Back-compat alias (some places call device_type) TODO: deprecate
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

    def count_zones_in_gwy(self, gwy) -> int:
        tcs = getattr(gwy, "tcs", None)
        if tcs and hasattr(tcs, "zones"):
            return len(tcs.zones)
        return 0

    # def sync_from_gwy(self, gwy) -> None:
    #     """Populate aliases/types/zones/zone_names from the gateway."""
    #     try:
    #         # Aliases are only from the schema / in known_list
    #         known = getattr(gwy, "known_list", {})  

    #         if isinstance(known, dict):
    #             for dev_id, meta in known.items():
    #                 dev_id = str(dev_id)
    #                 alias = None
    #                 # various shapes we might see
    #                 if hasattr(meta, "alias"):
    #                     alias = getattr(meta, "alias", None)
    #                 elif isinstance(meta, dict):
    #                     alias = meta.get("alias") or meta.get("name") or meta.get("label")
    #                 elif isinstance(meta, (str, bytes)):
    #                     alias = str(meta)
    #                 if alias:
    #                     self.alias_of_id[dev_id] = alias

    #         # Full device objects (inc zone index etc) are from device_by_id
    #         for dev_id, dev in getattr(gwy, "device_by_id", {}).items():
    #             dev_id = str(dev_id)
    #             # type may be 'type' or '_type' depending on object
    #             dev_type = getattr(dev, "type", None) or getattr(dev, "_type", None)
    #             if dev_type:
    #                 self.type_of_id[dev_id] = dev_type

    #             zone_obj = getattr(dev, "zone", None)
    #             zone_id = getattr(zone_obj, "idx", None) if zone_obj else None
    #             if zone_id:
    #                 self.zone_of_id[dev_id] = zone_id

    #         # Zone names (via TCS if present)
    #         tcs = getattr(gwy, "tcs", None)
    #         zones = getattr(tcs, "zones", {}) if tcs else {}
    #         for zid, zone in zones.items():
    #             zid_s = str(zid)
    #             name = getattr(zone, "_name", None) or getattr(zone, "name", None)
    #             if name:
    #                 self.zone_names[zid_s] = name

    #     except Exception:
    #         # Never allow registry issues to crash the gateway
    #         pass


    #Loading from schema at startup
    def load_from_schema(self, gwy: Any) -> None:
        """
        Load device/zone metadata using a pre-existing schema (schema.json).
        Runs once at startup IF schema was loaded.
        """

        try:
            schema = gwy.schema or {}
            known = getattr(gwy, "known_list", {})

            # Load aliases (if present)
            devices = getattr(gwy, "devices", {}) or getattr(gwy, "device_by_id", {})

            for dev_id, dev in devices.items():
                dev_id = str(dev_id)
                alias = getattr(dev, "alias", None)
                devtype = getattr(dev, "type", None)
                zone = getattr(dev, "zone_id", None)

                if alias:
                    self.alias_of_id[dev_id] = alias
                if devtype:
                    self.type_of_id[dev_id] = devtype
                if zone:
                    self.zone_of_id[dev_id] = zone

            # Load zone names
            zones = schema.get("zones", {})
            for zid, zdata in zones.items():
                if isinstance(zdata, dict) and "name" in zdata:
                    self.zone_names[str(zid)] = zdata["name"]

            # gwy.tcs.schema["underfloor_heating"][gwy.tcs._ufh_ctls()[0].id]["circuits"]["00"]
            ufh_schema = schema.get("underfloor_heating", {})
            for ufh_dev_id, ufh_data in ufh_schema.items():
                circuits = ufh_data.get("circuits", {})
                for circuit_id, circuit_data in circuits.items():
                    zone_idx = circuit_data.get("zone_idx")
                    if zone_idx:
                        self.ufh_map[(str(ufh_dev_id), str(circuit_id))] = str(zone_idx)
                        
        except Exception:
            # Never let registry failure kill the gateway
            pass

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

        


        