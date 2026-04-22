"""Ramses Message Routing and Presentation.

This module defines the MessageRouter, responsible for handling inbound packets from the
RamsesRF Gateway. Its core functions are to translate raw message data into:
1. Formatted, human-readable console output.
2. Structured MQTT publications to the broker.
It decouples the core communication services from presentation logic.
"""

from __future__ import annotations
import datetime as _dt
import json
import logging
from typing import Any, Dict, Optional, Callable, Awaitable
from colorama import init as colorama_init, Fore, Back, Style

from ramses_tx.message import CODE_NAMES

from .utils import to_snake_case, clean_display_text, print_formatted_row, local_now
from .utils import apply_address_aliases, zone_group
from .utils import mqtt_safe_value
from .models import ParsedMessage
from .registry import DeviceRegistry  
from .services import MQTTService
from .config import MqttConfig


class MessageRouter:
    def __init__(
        self,
        *,
        mqtt: MQTTService,
        color_scheme: Dict[str, str],
        min_row_length: int,
        registry: DeviceRegistry,
        group_by_zone: bool,
        mqtt_topics: MqttConfig.TopicLayout,
        pub_json_only: bool,
        pub_kv_with_json: bool,
        log_events_with_device_names: bool,
        logger: logging.Logger,
        use_local_time: bool = False,
    ) -> None:

        # MQTT interface
        self.mqtt = mqtt

        # Colour scheme for console display
        self.colors = color_scheme
        self.min_row_length = min_row_length

        # Registry for device name lookups
        self.registry = registry

        # Behaviour flags
        self.group_by_zone = group_by_zone

        # Unified topic layout object
        self.mqtt_topics = mqtt_topics  

        # publishing mode flags:
        self.pub_json_only = pub_json_only
        self.pub_kv_with_json = pub_kv_with_json   

        # Logging
        self.log_events_with_device_names = log_events_with_device_names
        self.log = logger
        self.use_local_time = use_local_time

        # Aggregated state caches for HA-friendly topics
        self._zone_state: dict[str, dict] = {}   # zone_id → latest aggregated state
        self._system_state: dict = {}             # system-level state

        # Dispatcher: family → handler
        self.family_handlers = {
            "temperature": self.handle_temperature,
            "demand": self.handle_demand,
            "sync": self.handle_sync,
            "zone_config": self.handle_zone_config,
            "schedule": self.handle_schedule,
            "device_info": self.handle_device_info,
            "fault": self.handle_fault,
            "heartbeat": self.handle_heartbeat,
            "misc": self.handle_misc,
        }


    def parse_message(self, msg: Any, item: Dict[str, Any]) -> ParsedMessage:
        """Extract and normalise all message-related metadata into a single model."""

        src_id = getattr(msg.src, "id", None)
        dst_id = getattr(msg.dst, "id", None)

        # Basic metadata 
        verb = getattr(msg, "verb", "")
        code = getattr(msg, "code_name", "")
        dtm_obj = getattr(msg, "dtm", None)
        dtm = f"{dtm_obj:%H:%M:%S.%f}"[:-3] if dtm_obj else None

        #  Device type 
        device_type = self.registry.type_of(src_id)

        #  RSSI 
        rssi = getattr(getattr(msg, "_pkt", None), "_rssi", None)

        #  Timestamp 
        timestamp = self.format_timestamp()

        #  Build ParsedMessage 
        return ParsedMessage(
            raw=msg,
            payload=item.copy(),
            registry=self.registry,
            src_id=src_id,
            dst_id=dst_id,
            verb=verb,
            code=code,
            dtm=dtm,
            device_type=device_type,
            rssi=rssi,
            timestamp=timestamp,
        )

    # Formatting helpers 
    def format_display_row(self, parsed: ParsedMessage, suffix: str = "") -> str:
        """Formatter for the simplified display row"""
        filtered_text = clean_display_text(parsed.raw, parsed.payload.copy())

        zn = f"@ {parsed.zone_name:<20}" if parsed.zone_name else ""
        zid = f"[Zone {parsed.zone_id:<3}]" if parsed.zone_id else ""

        main_txt = f"{filtered_text if filtered_text else '-': <45} {zn:<25}"
        return f"{main_txt: <75} {zid} {suffix}"

    def format_display_row_metadata(self, parsed: ParsedMessage) -> Dict[str, Any]:
        """Pure formatter: metadata for print_formatted_row."""
        src_alias = parsed.src_alias()
        src_display = src_alias or parsed.src_id or "-"
        dst_alias = parsed.dst_alias()
        dst_display = dst_alias or parsed.dst_id or ""

        return {
            "src": src_display,
            "dst": dst_display,
            "verb": parsed.verb or "",
            "cmd": parsed.code or "",
            "rssi": parsed.rssi or "   ",
            "style_prefix": self._style_for_message(parsed),
        }

    def _style_for_message(self, parsed: ParsedMessage) -> str:
        """Resolve colour/style based on message metadata."""        
        if parsed.verb:
            return self.colors.get(parsed.verb, "")
        if parsed.code.lower() in self.colors:
            return self.colors.get(parsed.code)        
        if getattr(parsed.raw.src, "type", "") == "18":
            return self.colors.get("RP", "")

        return ""


    def format_timestamp(self) -> str:
        """Return ISO-like timestamp string."""
        return local_now(self.use_local_time).strftime("%Y-%m-%dT%H:%M:%S")
            
    def format_mqtt_payload(self, parsed: ParsedMessage) -> Dict[str, Any]:
        return parsed.payload | {"timestamp": parsed.timestamp}

    # Rendering for console output
    def _display_row(self, parsed: ParsedMessage, suffix: str = "") -> None:
        row_text = self.format_display_row(parsed, suffix=suffix)
        metadata = self.format_display_row_metadata(parsed)

        print_formatted_row(
            row_text,
            min_row_length=self.min_row_length,
            local_time=self.use_local_time,
            **metadata,
        )
        
    # Zone / system state aggregation
    _ZONE_TRACKED = frozenset({
        "temperature", "sensor_temperature", "dhw_temperature",
        "setpoint", "mode", "heat_demand",
    })
    _SYSTEM_TRACKED = frozenset({
        "system_mode", "heat_demand", "boiler_status", "flame",
    })

    def _update_and_publish_state(self, parsed: ParsedMessage) -> None:
        """Maintain aggregated per-zone and system state topics consumed by HA discovery."""
        updated = False

        if parsed.zone_id and parsed.zone_id not in ("SYS", "FA", "FC", "F9"):
            state = self._zone_state.setdefault(parsed.zone_id, {})
            for k, v in parsed.payload.items():
                if k in self._ZONE_TRACKED:
                    state[k] = v
                    updated = True
            if updated:
                state["timestamp"] = parsed.timestamp
                zone_slug = parsed.topic_zone()
                if zone_slug:
                    zones_root = getattr(self.mqtt_topics, "zones", "zones")
                    subtopic = f"{zones_root}/{zone_slug}/state"
                    self.mqtt.publish(subtopic, dict(state), retain=False)

        if parsed.zone_id == "SYS" or parsed.zone_id is None:
            sys_updated = False
            for k, v in parsed.payload.items():
                if k in self._SYSTEM_TRACKED:
                    self._system_state[k] = v
                    sys_updated = True
            if sys_updated:
                self._system_state["timestamp"] = parsed.timestamp
                self.mqtt.publish("system/state", dict(self._system_state), retain=False)

    # MQTT publishing
    def publish_mqtt(self, topic: str, payload: Any) -> None:
        """Thin wrapper so MessageRouter never calls MQTTService directly."""
        if topic is None:
            return  # skip if formatting decided topic is invalid
        self.mqtt.publish(topic, payload)

    def _publish_received_payload(self, parsed: ParsedMessage) -> None:
        """Publish MQTT output, supporting JSON-only or JSON+KV modes."""

        topic = parsed.topic_base(
            topics=self.mqtt_topics,
            group_by_zone=self.group_by_zone
        )

        payload = parsed.payload.copy()
        timestamp = parsed.timestamp

        # Publish full JSON 
        if self.pub_json_only or self.pub_kv_with_json:
            payload["timestamp"] = timestamp
            self.mqtt.publish(topic, payload)

        # KV publishing 
        if self.pub_kv_with_json or not self.pub_json_only and isinstance(parsed.payload, dict):
            for key, value in parsed.payload.items():
                try:
                    kv_topic = f"{topic}/{key}"
                    safe_value = mqtt_safe_value(value)
                    self.mqtt.publish(kv_topic, safe_value)
                except Exception as ex:
                    print(f"Exception: {ex}")
                    print(f"key: {key}, value: {value}")
                    self.log.error(ex)

        # timestamp topic
        ts_topic = f"{topic}/{parsed.topic_code()}_ts"
        self.mqtt.publish(ts_topic, timestamp)

        self._update_and_publish_state(parsed)

    # def _publish_received_payload(self, parsed: ParsedMessage) -> None:
    #     """Publish MQTT output using the structured ParsedMessage helpers."""

    #     # Build the topic using ParsedMessage helpers
    #     topic = parsed.topic_base(topics=self.mqtt_topics)
    #     payload = parsed.payload.copy()
        
    #     payload["timestamp"] = parsed.timestamp
    #     self.mqtt.publish(topic, payload)

    #     # Timestamp in case we are not publishing the whole json
    #     ts_topic = f"{topic}/{parsed.topic_code()}_ts"
    #     self.mqtt.publish(ts_topic, parsed.timestamp)  



    # Extras
    def display_device_list(self, ramses):
        """Pretty-print the list of devices grouped by zone."""

        print_formatted_row(text="")
        print_formatted_row(text="  ---   Devices from schema file   ---")

        zones: dict[str, list] = {}

        for dev in ramses.gwy.device_by_id.values():
            zone_devs = zone_group(dev)
            zones.setdefault(zone_devs, []).append(dev)

        # Sort zones numerically
        def zone_sort_key(group: str):
            if group.startswith("Z"):
                return (0, int(group[1:]))  # numeric zones first
            if group == "DHW":
                return (1, 0)
            return (2, group)  # SYS and any others

        for zone_id in sorted(zones.keys(), key=zone_sort_key):
            zone_devs = zones[zone_id]
            zone_obj = getattr(zone_devs[0], "zone", None)

            if zone_obj:
                zone_name = getattr(zone_obj, "_name", "") or ""
            else:
                zone_name = ""

            zone_num = int(zone_id.replace("Z", "")) if zone_id.startswith("Z") else zone_id

            # Let's change DHW to "Hot Water" for clarity
            if zone_id.upper() in ("DHW"):
                zone_label = "Hot Water"
            else:
                zone_label = zone_name or "System Devices"

            # Add some colour!
            ztitle = (
                f"{Fore.CYAN}Zone {zone_num}"
                f"{Style.RESET_ALL}"
                f" {Fore.YELLOW}({zone_label}){Style.RESET_ALL}" if zone_label else ""
            )
            print_formatted_row(text=f"  {ztitle}")

            # Sort devices by type
            def dev_sort_key(d):
                prefix = d.id.split(":")[0]       # e.g. '04'
                return (prefix, d.id)

            for dev in sorted(zone_devs, key=dev_sort_key):
                dev_type = self.registry.type_of(dev.id)
                alias = self.registry.alias_of(dev.id) or dev.id

                # RSSI if available
                rssi = getattr(dev, "rssi", None)
                rssi_str = f"RSSI={rssi:3d}" if isinstance(rssi, int) else ""

                # Highlight device type
                dtype_col = f"{Fore.GREEN}{dev_type:6}{Style.RESET_ALL}"

                print_formatted_row(
                    text=f"      {dtype_col}  {dev.id:12}  {alias:20} {rssi_str}"
                )
            print_formatted_row(text=f"")
            # print_formatted_row(text="--")

    def format_device_name(self, addr) -> str:
        """Accept device_id string OR Ramses Address object."""
        if not addr:
            return ""
        device_id = addr.id if hasattr(addr, "id") else str(addr)
        alias = self.registry.alias_of(device_id)
        dtype = self.registry.type_of(device_id)
        if dtype and alias:
            return f"{dtype} {alias}"
        if alias:
            return alias
        if dtype:
            return dtype
        return device_id


    def handle_temperature(self, parsed: ParsedMessage):
        """Handle temperature and sensor messages."""
        # Currently: just display + publish (no special logic yet)
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_demand(self, parsed: ParsedMessage):
        """Handle heat/actuator demand messages."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_sync(self, parsed: ParsedMessage):
        """Handle OpenTherm / sync messages."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_zone_config(self, parsed: ParsedMessage):
        """Handle zone config / zone parameters."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_schedule(self, parsed: ParsedMessage):
        """Handle schedule fragments."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_device_info(self, parsed: ParsedMessage):
        """Handle identity/version/type/binding messages."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_fault(self, parsed: ParsedMessage):
        """Handle fault/error/diagnostic messages."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_heartbeat(self, parsed: ParsedMessage):
        """Handle heartbeat/tick/presence messages."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_misc(self, parsed: ParsedMessage):
        """Handle messages that fall into no major category."""
        self._display_row(parsed)
        self._publish_received_payload(parsed)

    def handle_message(self, msg: Any) -> None:
        """Log to event file (string representation of msg automatically formatted by ramses lib)
           Mirroring to console is configured in logging, subject to EVENTS_CONSOLE_OUTPUT setting
        """
        if self.log_events_with_device_names:
            msg_with_device_names = apply_address_aliases(
                msg,
                self.registry.alias_of,
                self.registry.type_of
            )
            self.log.info(msg_with_device_names)
        else:
            self.log.info(str(msg))

        try:
            msg.code_name = CODE_NAMES.get(msg.code, getattr(msg, "code_name", str(msg.code)))
        except Exception:
            pass

        payload = msg.payload if isinstance(msg.payload, list) else [msg.payload]

        for item in payload:
            parsed = self.parse_message(msg, item)

            # family dispatch
            family = parsed.family
            handler = self.family_handlers.get(family, self.handle_misc)

            try:
                handler(parsed)
            except Exception as ex:
                # print(f"Exception: {ex}")
                self.log.exception(f"Error handling message family: {family}")
