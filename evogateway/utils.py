"""General Utility Functions.

This module contains a collection of common, reusable helper functions that support various 
parts of the application. This includes string formatting (e.g., snake_case conversion, 
truncation), console output formatting, and operating system-agnostic JSON file I/O utilities.
"""

from __future__ import annotations
import datetime as _dt
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from colorama import Style # For print_formatted_row final reset

from .config import DEFAULT_MIN_ROW_LENGTH

try:
    from ramses_tx.logger import CONSOLE_COLS as _CONSOLE_COLS
except Exception:
    _CONSOLE_COLS = 200


import re

_first_cap_re = re.compile(r"(.)([A-Z][a-z]+)")
_all_cap_re = re.compile(r"([a-z0-9])([A-Z])")


def to_snake_case(name: str | None) -> str:
    if not name:
        return ""
    name = name.strip().replace("'", "").replace(" ", "_")
    s1 = _first_cap_re.sub(r"\1_\2", name)
    s2 = _all_cap_re.sub(r"\1_\2", s1).lower()
    return s2.replace("__", "_")


def truncate(s: str | None, length: int) -> str:
    if not s:
        return ""
    return (s[: max(0, length - 3)] + "...") if len(s) > length else s


def print_formatted_row(
    text: str,
    *,
    style_prefix: str = "",
    min_row_length: int = DEFAULT_MIN_ROW_LENGTH,
    src: str | None = None,
    dst: str | None = None,
    verb: str | None = None,
    cmd: str | None = None,
    rssi: str | None = None
) -> None:
    dtm = _dt.datetime.now().strftime("%Y-%m-%d %X")
    if src:
        row = (
            f"{dtm} |{(rssi or '   ')}| {truncate(src, 21):<21} -> "
            f"{truncate(dst or '', 21):<21} |{(verb or ''):<2}| {truncate(cmd or '', 15):<15} | {text}"
        )
    else:
        row = f"{dtm} |{text}"
    row = f"{row[:_CONSOLE_COLS]}"
    row = f"{row:<{min_row_length}}"
    print(f"{style_prefix}{row.strip()}{Style.RESET_ALL}")


def clean_display_text(msg: Any, display_text: Any) -> str:
    try:
        if isinstance(display_text, dict):
            if getattr(msg, "code_name", None) in display_text:
                filtered = display_text[msg.code_name]
                if msg.code_name in ("temperature", "setpoint") and filtered is not None:
                    try:
                        return f"{float(filtered):>05.2f}°C"
                    except Exception:
                        return str(filtered)
                if "_demand" in msg.code_name and filtered is not None:
                    try:
                        return f"{float(filtered) * 100:> 5.0f}%"
                    except Exception:
                        return str(filtered)
                if "setpoint_bounds" in msg.code_name and isinstance(filtered, (list, tuple)) and len(filtered) == 2:
                    try:
                        return f"Min: {float(filtered[0]):>05.2f}°C, Max: {float(filtered[1]):>05.2f}°C"
                    except Exception:
                        pass
                return str(filtered)
            else:
                filtered_text = dict(display_text)
                for key in ["zone_idx", "parent_idx", "msg_id", "msg_type"] + [
                    k for k in list(filtered_text.keys()) if isinstance(k, str) and "unknown" in k
                ]:
                    filtered_text.pop(key, None)

                def _format_percent(v: Any) -> str:
                    try:
                        return f"{float(v) * 100:.0f}%"
                    except Exception:
                        return str(v)

                if "temperature" in filtered_text and filtered_text.get("value") is not None:
                    try:
                        filtered_text["value"] = f"{float(filtered_text['value']):.1f}°C"
                    except Exception:
                        pass
                for fld in ("heat_demand", "relay_demand", "modulation_level"):
                    if fld in filtered_text and filtered_text[fld] is not None:
                        filtered_text[fld] = _format_percent(filtered_text[fld])

                s = json.dumps(filtered_text, sort_keys=True)
                s = s[1:-1].replace('"', "").strip()
                if getattr(msg, "verb", None) == "RQ":
                    s = f"REQUEST: {'' if s else getattr(msg, 'code_name', '')}{s}"
                return s
        else:
            return str(display_text)
    except Exception:
        return str(display_text)

def zone_group(dev) -> str:
    """
    Classify devices into:
      - Znn  : numeric heating zones (hex-based, but sorted as decimal)
      - DHW  : DHW-related devices
      - SYS  : system devices (controller, relays, etc)
    """
    zone = getattr(dev, "zone", None)
    if not zone or not getattr(zone, "id", None):
        return "SYS"

    zid = zone.id.upper()

    # DHW handling
    if "DHW" in zid:
        return "DHW"

    # Look for the suffix "_XX" where XX is a hex zone number
    if "_" in zid:
        suffix = zid.split("_", 1)[1]  # e.g. "0A", "0B", "07"
        try:
            # Convert hex to int, then format back as 2-digit decimal zone
            zone_number = int(suffix, 16)
            return f"Z{zone_number:02d}"
        except ValueError:
            pass  # Not a hex number → fall through to SYS

    return "SYS"

def apply_address_aliases(msg: Any, device_alias, device_type) -> str:
    """
    Replace SRC and DST addresses in the ramses_rf string output
    while preserving column alignment.

    Ramses message format template (ramses_rf/src/ramses_tx/message.py):
        MSG_FORMAT_10 = "|| {:10s} | {:10s} | {:2s} | {:16s} | {:^4s} || {}"
    """

    try:
        # expected structure:
        # "|| SRC | DST | verb | code | zone || payload"
        parts = str(msg).split("|")

        # parts will be:
        # 0: ""     (before first ||)
        # 1: ""     (space)
        # 2: SRC
        # 3: DST
        # 4: verb
        # 5: code_name
        # 6: zone_idx
        # 7: ""    (space)
        # 8: payload (after ||)

        MAX_WIDTH = 20

        src = parts[2].strip()
        dst = parts[3].strip()

        # look up device alias names and types
        try:
            src_alias = device_alias(src) or src
        except Exception:
            src_alias = src

        try:
            dst_alias = device_alias(dst) or dst
        except Exception:
            dst_alias = dst

        src_type = device_type(src) or ""
        dst_type = device_type(dst) or ""

        # ensure string/add padding
        src_alias = f" {str(src_alias)} "
        dst_alias = f" {str(dst_alias)} "

        # truncate if needed and pad to fixed width
        src_fixed = src_alias[:MAX_WIDTH].ljust(MAX_WIDTH)
        dst_fixed = dst_alias[:MAX_WIDTH].ljust(MAX_WIDTH)

        parts[1] = f"{src_type:3}"  
        parts[2] = src_fixed
        parts[3] = dst_fixed

        # reconstruct message
        return "|".join(parts)

    except Exception as ex:
        # in worst case, return original message unmodified
        print(f"apply_address_aliases error: {ex}")
        return str(msg)

def make_fake_ramses_message(code_name, src_id, src_type, zone, payload, verb="RP"):
    class _Addr:
        def __init__(self, dev_id, dev_type, zone):
            self.id = dev_id
            self.type = dev_type
            self.zone = zone

    class _FakeMsg:
        def __init__(self):
            self.code_name = code_name
            self.src = _Addr(src_id, src_type, zone)
            self.dst = _Addr(src_id, src_type, zone)
            self.payload = payload
            self.verb = verb

        def __str__(self):
            z = getattr(zone, "idx", "?")
            return f"|  |{self.src.type} | {self.src.id:<18} -> {self.dst.id:<18} | {self.verb} | {self.code_name:<16} | {z:<4} || {self.payload}"

    return _FakeMsg()

def mqtt_safe_value(value):
    """
    Convert any Python value into a valid MQTT payload type:
      - str, int, float, bytes, bytearray → unchanged
      - None → None
      - dict/list/tuple/set/custom → JSON string
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bytes, bytearray)):
        return value
    return json.dumps(value, separators=(",", ":"))

