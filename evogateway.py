#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
import asyncio
import json
from platform import platform
import sys
import traceback
import re
import glob
from typing import Tuple
from signal import SIGINT, SIGTERM
import os
import inspect
import configparser
import paho.mqtt.client as mqtt
import time
import datetime
from colorama import init as colorama_init, Fore, Style, Back
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta as td

from ramses_rf import Gateway, GracefulExit
from ramses_rf.discovery import GET_SCHED, SET_SCHED
from ramses_rf.protocol import schedule
from ramses_rf.protocol import command
from ramses_rf.version import VERSION as RAMSES_RF_VERSION
from ramses_rf.protocol.command import Command
from ramses_rf.protocol.const import HGI_DEVICE_ID, NON_DEVICE_ID, _OUT_DEVICE_TABLE
from ramses_rf.protocol.logger import CONSOLE_COLS
from ramses_rf.schema import (
    KNOWN_LIST,
    CONFIG,
    DISABLE_DISCOVERY,
    DISABLE_SENDING,
    DONT_CREATE_MESSAGES,
    ENFORCE_KNOWNLIST,
    ENABLE_EAVESDROP,
    EVOFW_FLAG,
    INPUT_FILE,
    MAX_ZONES,
    PACKET_LOG,
    PACKET_LOG_SCHEMA,
    REDUCE_PROCESSING,
    SERIAL_PORT,
    SERIAL_CONFIG,
    USE_ALIASES,
    USE_SCHEMA,
    SZ_ALIAS,
    SZ_UFH_SYSTEM
)
from ramses_rf.protocol.schema import (
    LOG_FILE_NAME,
    LOG_ROTATE_BYTES,
    LOG_ROTATE_COUNT
)
from ramses_rf.protocol.exceptions import EvohomeError
from ramses_rf.message import CODE_NAMES as CODE_NAMES
# from ramses_rf.discovery import spawn_execute_cmd

LIB_KEYS = (
    INPUT_FILE,
    SERIAL_PORT,
    EVOFW_FLAG,
    PACKET_LOG,
    # "process_level",  # TODO
    REDUCE_PROCESSING,
)

DEFAULT_COLORS = {" I": f"{Fore.WHITE}", "RP": f"{Fore.LIGHTWHITE_EX}", "RQ": f"{Fore.BLACK}",
          " W": f"{Fore.MAGENTA}", "temperature": f"{Fore.YELLOW}","ERROR": f"{Back.RED}{Fore.YELLOW}", "mqtt_command": f"{Fore.LIGHTCYAN_EX}" }


if  os.path.isdir(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "3.9-0.18.6"

CONFIG_FILE     = "evogateway.cfg"

config = configparser.RawConfigParser()
config.read(CONFIG_FILE)

def get_display_colorscheme(reload_config=False):

    if reload_config:
        global CONFIG_FILE
        global config
        config.read(CONFIG_FILE)

    colours_string = config.get("MISC", "DISPLAY_COLOURS", fallback=None)
    try: # TODO! Get rid of eval and tidy up!
        scheme = eval(colours_string) if colours_string else None
    except:
        pass
    if not scheme:
        global DEFAULT_COLORS
        scheme = DEFAULT_COLORS

    if not " I" in scheme:  scheme[" I"] = DEFAULT_COLORS[" I"]
    if not "RQ" in scheme:  scheme["RQ"] = DEFAULT_COLORS["RQ"]
    if not "RP" in scheme:  scheme["RP"] = DEFAULT_COLORS["RP"]
    if not " W" in scheme:  scheme[" W"] = DEFAULT_COLORS[" W"]
    if not "ERROR" in scheme:  scheme["ERROR"] = DEFAULT_COLORS["ERROR"]
    if not "mqtt_command" in scheme:  scheme["mqtt_command"] = DEFAULT_COLORS["mqtt_command"]

    return scheme

COM_PORT                = config.get("Serial Port","COM_PORT", fallback="/dev/ttyUSB0")
COM_BAUD                = config.get("Serial Port","COM_BAUD", fallback=115200)

EVENTS_FILE             = config.get("Files", "EVENTS_FILE", fallback="events.log")
PACKET_LOG_FILE         = config.get("Files", "PACKET_LOG_FILE", fallback="packet.log")
LOG_FILE_ROTATE_COUNT   = config.getint("Files", "LOG_FILE_ROTATE_COUNT", fallback=9)
LOG_FILE_ROTATE_BYTES   = config.getint("Files", "LOG_FILE_ROTATE_BYTES", fallback=1000000)

DEVICES_FILE            = config.get("Files", "DEVICES_FILE", fallback="devices.json")
ZONES_FILE              = config.get("Files", "ZONES_FILE", fallback="zones.json")
LOAD_ZONES_FROM_FILE    = config.getboolean("Files", "LOAD_ZONES_FROM_FILE", fallback=True)
SCHEMA_FILE             = config.get("Files", "SCHEMA_FILE", fallback="ramsesrf_schema.json")
MAX_SAVE_FILE_COUNT     = config.getint("Files", "MAX_SAVE_FILE_COUNT", fallback=9)

MQTT_SERVER             = config.get("MQTT", "MQTT_SERVER", fallback="")
MQTT_USER               = config.get("MQTT", "MQTT_USER", fallback="")
MQTT_PW                 = config.get("MQTT", "MQTT_PW", fallback="")
MQTT_CLIENTID           = config.get("MQTT", "MQTT_SERVER", fallback="evoGateway")

MQTT_PUB_AS_JSON        = config.getboolean("MQTT", "MQTT_PUB_AS_JSON", fallback=False)
MQTT_GROUP_BY_ZONE      = config.getboolean("MQTT", "MQTT_GROUP_BY_ZONE", fallback=True)
MQTT_REQUIRE_ZONE_NAMES = config.getboolean("MQTT", "MQTT_REQUIRE_ZONE_NAMES", fallback=True)

MQTT_SUB_TOPIC          = config.get("MQTT", "MQTT_SUB_TOPIC", fallback="")
MQTT_PUB_TOPIC          = config.get("MQTT", "MQTT_PUB_TOPIC", fallback="")
MQTT_ZONE_IND_TOPIC     = config.get("MQTT", "MQTT_ZONE_INDEP_TOPIC", fallback="_zone_independent")
MQTT_ZONE_UNKNOWN       = config.get("MQTT", "MQTT_ZONE_UNKNOWN", fallback="_zone_unknown")

THIS_GATEWAY_NAME       = config.get("MISC", "THIS_GATEWAY_NAME", fallback="EvoGateway")
GATEWAY_DISABLE_SENDING = config.getboolean("MISC", "DISABLE_SENDING", fallback=False)

DISPLAY_FULL_JSON       = config.getboolean("MISC", "DISPLAY_FULL_JSON", fallback=False)
SCHEMA_EAVESDROP        = config.getboolean("Misc", "SCHEMA_EAVESDROP", fallback=False)
FORCE_SINGLE_HGI        = config.getboolean("Misc", "FORCE_SINGLE_HGI", fallback=True)
DHW_ZONE_PREFIX         = config.get("Misc", "DHW_ZONE_PREFIX", fallback="_dhw")

RAMSESRF_DISABLE_DISCOVERY = config.getboolean("Ramses_rf", "DISABLE_DISCOVERY", fallback=False)
RAMSESRF_ALLOW_EAVESDROP   = config.getboolean("Ramses_rf", "ALLOW_EAVESDROP", fallback=False)
RAMSESRF_KNOWN_LIST        = config.getboolean("Ramses_rf", "KNOWN_LIST", fallback=True)

MIN_ROW_LENGTH          = config.get("MISC", "MIN_ROW_LENGTH", fallback=160)

DISPLAY_COLOURS         = get_display_colorscheme()

MQTT_STATUS_SUBTOPIC    = "status"
MQTT_OFFLINE            = "Offline"
MQTT_ONLINE             = "Online"
SYS_CONFIG_COMMAND      = "sys_config"
SYSTEM_MSG_TAG          = "*"
SEND_STATUS_TRANSMITTED = "Transmitted"
SEND_STATUS_FAILED      = "Failed"
SEND_STATUS_SUCCESS     = "Successful"

SZ_ZONE_NAME            = "name"
SZ_UFH_CIRCUITS         = "circuits"

# -----------------------------------
DEVICES = {}
ZONES = {}
UFH_CIRCUITS = {}
MQTT_CLIENT = None
GWY = None
GWY_MODE = None
LAST_SEND_MSG = None

# -----------------------------------

log = logging.getLogger(f"evogateway_log")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(lineno)s] %(message)s')
# %(funcName)20s() [%(levelname)s]

# Log file handler
file_handler = RotatingFileHandler(EVENTS_FILE, maxBytes=LOG_FILE_ROTATE_BYTES, backupCount=LOG_FILE_ROTATE_COUNT)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# Log console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')
def to_snake(name):
    if name:
        name=name.strip().replace("'","").replace(" ","_")
        s1 = _first_cap_re.sub(r'\1_\2', name)
        s2 = _all_cap_re.sub(r'\1_\2', s1).lower()
        return s2.replace("__","_")


def truncate_str(str, length):
    if str:
        return (str[:length - 3] + '...') if len(str) > length else str
    else:
        return None


def _proc_kwargs(obj, kwargs) -> Tuple[dict, dict]:
    lib_kwargs, cli_kwargs = obj
    lib_kwargs[CONFIG].update({k: v for k, v in kwargs.items() if k in LIB_KEYS})
    cli_kwargs.update({k: v for k, v in kwargs.items() if k not in LIB_KEYS})
    return lib_kwargs, cli_kwargs


def get_parent_keys(d, value):
    for k,v in d.items():
        if isinstance(v, dict):
            p = get_parent_keys(v, value)
            if p:
                return [k] + p
        elif v == value:
            return [k]


def get_device_name(device_address):
    try:
        if device_address.id == HGI_DEVICE_ID or (FORCE_SINGLE_HGI and device_address.type in "18"):
            name = THIS_GATEWAY_NAME
        elif device_address.type in "01":
            name = "Controller"
        elif device_address.type in "63":
            name = "UNBOUND"
        else:
            name = DEVICES[device_address.id][SZ_ALIAS] if device_address.id in DEVICES else device_address.id
        if name == NON_DEVICE_ID:
            name = ""
        dev_type = _OUT_DEVICE_TABLE[device_address.type]["type"].replace("---", "").replace("NUL", "")
        name = "{} {}".format(dev_type, name).strip()
        return name

    except Exception as ex:
        log.error(f"{Style.BRIGHT}{DISPLAY_COLOURS.get('ERROR')}Exception occured for device_address '{device_address}': {ex}{Style.RESET_ALL}", exc_info=True)
        traceback.print_stack()


def get_msg_zone_name(src, target_zone_id=None):
    """ Use any 'target' zone name given in the payload, otherwise fall back to zone name of the sending device"""

    # If target of the message is a zone, use that unless the device type is a BDR or OTB etc.
    if src.type not in "13 10" and target_zone_id and int(target_zone_id, 16) >= 0:
        #
        # zone = GWY.evo.zone_by_idx[target_zone_id] if GWY.evo else None
        # zone_name = zone.name if zone else "_zone_{}".format(target_zone_id)

        if target_zone_id not in ZONES:
            update_zones_from_gwy()
        zone_name = ZONES[target_zone_id] if target_zone_id in ZONES else "_zone_{}".format(target_zone_id)
    else:
        if src.id not in DEVICES or "zone_id" not in DEVICES[src.id]:
            update_devices_from_gwy()

        src_zone_id = DEVICES[src.id]["zone_id"] if src.id in DEVICES and "zone_id" in DEVICES[src.id] else None
        if src_zone_id and not isinstance(src_zone_id, str):
            print(f"{Style.BRIGHT}{Fore.RED}[DEBUG] -----------> src_zone_id ({src_zone_id}) is not string: type = {type(src_zone_id)}. src.id: {src.id}, target_zone_id: {target_zone_id}. {Style.RESET_ALL}")
            traceback.print_stack()

        if src.type in "01 18" or target_zone_id == "-1":
            # Controllers and HGI
            zone_name = MQTT_ZONE_IND_TOPIC
        elif (src_zone_id and int(src_zone_id, 16) > 11) or src.type in "02 10 13":
            # Relay types, e.g. BDR, OTB, UFC
            zone_name = f"{MQTT_ZONE_IND_TOPIC}/relays"
        elif src_zone_id and int(src_zone_id, 16) >= 0 and src_zone_id in ZONES:
            # Normal 'zones'
            zone_name = ZONES[src_zone_id]
        else:
            log.error(f"----> Unknown zone for src: '{src} {DEVICES[src.id] if src.id in DEVICES else ''}'")
            zone_name = MQTT_ZONE_UNKNOWN

    return zone_name


def get_opentherm_msg(msg):
    if msg.code_name == "opentherm_msg":
        name = msg.payload.get("msg_name", None)
        if name:
            key = name if isinstance(name, str) else "OpenTherm" # some msg_name are unhashable/dict/have multiple data elements
            # return the whole payload dict as we don't know which specific message component is of interest
            return key, {key: msg.payload}
    else:
        log.error(f"Invalid opentherm_msg. msg.code_name: {msg.code_name}")
    return None, None


def spawn_schedule_task(action, **kwargs):
    ctl_id = GWY.evo.id
    if action == GET_SCHED:
        if not "zone_idx" in kwargs:
            log.error("get_schedules requires 'zone_idx'")
            return

        zone_idx = kwargs["zone_idx"]
        force_refresh = kwargs["force_refresh"] if "force_refresh" in kwargs else None
        kwargs = {"ctl_id": ctl_id, "zone_idx": zone_idx, "force_refresh": force_refresh}
        tasks = [GWY._loop.create_task(get_schedule_async(GWY, **kwargs))]

    elif action == SET_SCHED:
        if not "schedule" in kwargs:
            log.error("'set_schedule' requires 'schedule' json")
            return
        schedule = kwargs["schedule"]
        if not "zone_idx" in schedule:
            log.error("'zone_idx' must be defined in 'schedule' json")
            return
        zone_idx = schedule["zone_idx"]
        kwargs = {"ctl_id": ctl_id, "schedule": schedule}
        tasks = [GWY._loop.create_task(set_schedule_async(GWY, **kwargs))]

    GWY._tasks.extend(tasks)
    print_formatted_row(SYSTEM_MSG_TAG, text=f"{tasks}")
    log.info(f"'{action}' for zone {zone_idx} task spawned : {tasks}")


async def set_schedule_async(gwy, ctl_id: str, schedule: str) -> None:
    zone_idx = schedule["zone_idx"]
    zone = gwy._get_device(ctl_id, ctl_id=ctl_id)._evo._get_zone(zone_idx)
    try:
        await zone.set_schedule(schedule["schedule"])
        print_formatted_row(
                            SYSTEM_MSG_TAG,
                            text="Schedule updated for zone {zone_idx} ({zone.name})")
    except Exception as e:
        log.error(f"set_schedule_async(): Error: {e}", exc_info=True)
        # traceback.print_stack()


async def get_schedule_async(gwy, ctl_id: str, zone_idx: str, force_refresh: bool = None) -> None:
    # zone = gwy._get_device(ctl_id, ctl_id=ctl_id)._evo._get_zone(zone_idx)
    zone = gwy.evo._get_zone(zone_idx)
    try:
        await zone.get_schedule(force_refresh)
    except Exception as e:
        log.error("get_schedule_async(): Error: %s", e)


def process_schedule_message(msg):
    try:
        # Only process if we have received the last fragment
        if msg.payload["frag_index"] == msg.payload["frag_total"]:
            zone_idx = msg.payload["zone_idx"]
            if zone_idx == "HW":
                zone = GWY.system_by_id[GWY.evo.id].dhw
            else:
                zone = GWY.system_by_id[GWY.evo.id].zone_by_idx[zone_idx]

            # zone = GWY.system_by_id[GWY.evo.id].zone_by_idx[zone_idx]
            schedule = zone.schedule

            dtm = f"{msg.dtm:%H:%M:%S.%f}"[:-3]
            if DISPLAY_FULL_JSON:
                print(f"{DISPLAY_COLOURS.get(msg.verb)}{dtm} "
                    "Schedule for '{zone.name}' [{zone_idx}]: {schedule}"[:CONSOLE_COLS])
            else:
                print_formatted_row(SYSTEM_MSG_TAG,
                    text=f"Schedule for '{zone.name}' ({zone_idx}): {schedule}")

            msg.payload["topic_idx"] = "schedule"
            mqtt_publish_received_msg(msg, {"schedule": schedule, "zone_idx": zone_idx})

    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg: {msg}")


def cleanup_display_text(msg, display_text):
    """ Clean up/Simplify the displayed text for given message. display_text must be a dict """
    try:
        if type(display_text) == dict:
            if msg.code_name in display_text:
                # remove the command name (dict key) from the displayed text
                filtered_text = display_text[msg.code_name]

                # Formatting for temperature/demand numbers
                if msg.code_name in "temperature setpoint" and filtered_text is not None:
                    filtered_text = "{:>05.2f}°C".format(float(filtered_text))
                elif "_demand" in msg.code_name and filtered_text is not None:
                    filtered_text = "{:> 5.0f}%".format(float(filtered_text) * 100)

            else:
                filtered_text = display_text

                # Remove extra detail, not required for 'simple/clean' display
                for key in ["zone_idx", "parent_idx", "msg_id", "msg_type"] + [k for k in filtered_text if "unknown" in k]:
                    if key in filtered_text:
                        del filtered_text[key]

                if "value" in filtered_text and "temperature" in str(filtered_text.keys()) and filtered_text["keys]"]:
                    filtered_text["value"] = "{:.1f}°C".format(float(filtered_text))
                if "heat_demand" in filtered_text and filtered_text["heat_demand]"] is not None:
                    filtered_text["heat_demand"] = "{:.0f}%".format(float(filtered_text["heat_demand"]) * 100)
                if "relay_demand" in filtered_text and filtered_text["relay_demand]"] is not None:
                    filtered_text["relay_demand"] = "{:.0f}%".format(float(filtered_text["relay_demand"]) * 100)
                if "modulation_level" in filtered_text and filtered_text["modulation_level"] is not None:
                    filtered_text["modulation_level"] = "{:.0f}%".format(float(filtered_text["modulation_level"]) * 100)

                filtered_text = json.dumps(filtered_text, sort_keys=True)[1:-1]
                filtered_text = filtered_text.replace('"', '').strip()
                if msg.verb == "RQ":
                    filtered_text = "REQUEST: {}{}".format("" if filtered_text else msg.code_name, filtered_text)
            return filtered_text
        else:
            return display_text
    except Exception as ex:
        log.error(f"Exception occured: {ex}", exc_info=True)
        log.error(f"msg.payload: {msg.payload}, display_text: {display_text}")


def process_gwy_message(msg, prev_msg=None) -> None:
    """ Process received ramses_rf message from Gateway """

    log.debug("") # spacer, as we have other debug entries for a given received msg
    log.info(msg)  # Log event to file

    # Message class in ramses_rf lib does not seem to have the code name, so add it
    msg.code_name = CODE_NAMES[msg.code]

    if DISPLAY_FULL_JSON:
        display_full_msg(msg)

    # As some payloads are arrays, and others not, make consistent
    payload = [msg.payload] if not isinstance(msg.payload, list) else msg.payload

    for item in payload:
        # ramses_rf library seems to send each item as a dict
        try:
            if type(item) != dict:
                # Convert to a dict...
                item = {msg.code_name: str(item) }
            if not DISPLAY_FULL_JSON:
                zone_id = item["zone_idx"] if "zone_idx" in item else None
                display_simple_msg(msg, item, zone_id, "")
            mqtt_publish_received_msg(msg, item)

        except Exception as e:
            log.error(f"Exception occured: {e}", exc_info=True)
            log.error(f"item: {item}, payload: {payload} ")
            log.error(f"msg: {msg}")

    if msg.code == "0404" and msg.verb == "RP":
        process_schedule_message(msg)


def print_ramsesrf_gwy_schema(gwy):
    if gwy.evo is None:
        print("'GWY.evo' is None. Defaulting to GWY.schema: ")
        print(f"Schema[gateway] = {json.dumps(gwy.schema, indent=4)}\r\n")
        print(f"Params[gateway] = {json.dumps(gwy.params)}\r\n")
        print(f"Status[gateway] = {json.dumps(gwy.status)}")
        return

    print(f"Schema[{repr(gwy.evo)}] = {json.dumps(gwy.evo.schema, indent=4)}\r\n")
    print(f"Params[{repr(gwy.evo)}] = {json.dumps(gwy.evo.params, indent=4)}\r\n")
    print(f"Status[{repr(gwy.evo)}] = {json.dumps(gwy.evo.status, indent=4)}\r\n")

    orphans = [d for d in sorted(gwy.devices) if d not in gwy.evo.devices]
    devices = {d.id: d.schema for d in orphans}
    print(f"Schema[orphans] = {json.dumps({'schema': devices}, indent=4)}\r\n")
    devices = {d.id: d.params for d in orphans}
    print(f"Params[orphans] = {json.dumps({'params': devices}, indent=4)}\r\n")
    devices = {d.id: d.status for d in orphans}
    print(f"Status[orphans] = {json.dumps({'status': devices}, indent=4)}\r\n")

    update_devices_from_gwy()

    if DEVICES:
        devices = {str(k) : {SZ_ALIAS : DEVICES[k][SZ_ALIAS]} for k in DEVICES if k is not None}
    print(f"DEVICES = {json.dumps(devices, indent=4)}")


def display_full_msg(msg):
    """ Show the full json payload (as in the ramses_rf cli client) """
    dtm = f"{msg.dtm:%H:%M:%S.%f}"[:-3]
    if msg.src.type == "18":
        print(f"{Style.BRIGHT}{DISPLAY_COLOURS.get(msg.verb)}{dtm} {msg}"[:CONSOLE_COLS])
    elif msg.verb:
        print(f"{DISPLAY_COLOURS.get(msg.verb)}{dtm} {msg}"[:CONSOLE_COLS])
    else:
        print(f"{Style.RESET_ALL}{dtm} {msg}"[:CONSOLE_COLS])


def display_simple_msg(msg, payload_dict, target_zone_id, suffix_text=""):
    src = get_device_name(msg.src)
    dst = get_device_name(msg.dst) if msg.src.id != msg.dst.id else ""

    # Make a copy as we are deleting elements from the displayed text
    display_text = payload_dict.copy() if isinstance(payload_dict, dict) else payload_dict
    filtered_text = cleanup_display_text(msg, display_text)
    try:
        zone_name = "@ {:<20}".format(truncate_str(ZONES[target_zone_id], 20)) if target_zone_id and int(target_zone_id, 16) >= 0 and target_zone_id in ZONES else ""
        zone_id = "[Zone {:<3}]".format(target_zone_id) if target_zone_id and int(target_zone_id, 16) >= 0 else ""

        if msg.src.type == "18": # Messages from the HGI device
            style_prefix = f"{Fore.LIGHTBLACK_EX}"
        elif msg.code_name.lower() in DISPLAY_COLOURS :
            style_prefix = f"{DISPLAY_COLOURS.get(msg.code_name)}"
        elif msg.verb:
            style_prefix = f"{DISPLAY_COLOURS.get(msg.verb)}"
        else:
            style_prefix = f"{Style.RESET_ALL}"

        main_txt = f"{filtered_text if filtered_text else '-': <45} {zone_name:<25}"
        print_formatted_row(src, dst, msg.verb, msg.code_name, f"{main_txt: <75} {zone_id} {suffix_text}", msg._pkt.rssi, style_prefix)

    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg: {msg}, payload_dict: {payload_dict}, target_zone_id: {target_zone_id}, suffix_text: {suffix_text}")
        log.error(f"type(display_text): {type(display_text)}")
        log.error(f"filtered_text: {filtered_text}" if filtered_text else "filtered_text is None")
        log.error(f"Display row: {msg.code_name}: {msg.verb}| {src} -> {dst} | {display_text} {zone_name} [Zone {target_zone_id}] {suffix_text}")
        log.error(f"|rssi '{msg._pkt.rssi}'| src '{src}' -> dst '{dst}' | verb '{msg.verb}'| cmd '{msg.code_name}'")

def print_formatted_row(src="", dst="", verb="", cmd="", text="", rssi="   ", style_prefix=""):
    dtm = datetime.datetime.now().strftime("%Y-%m-%d %X")
    if src:
        row = f"{dtm} |{rssi}| {truncate_str(src, 21) if src else '':<21} -> {truncate_str(dst, 21) if dst else '':<21} |{verb:<2}| {cmd:<15} | {text}"
    else:
        row = f"{dtm} | {text}"
    row = "{:<{min_row_width}}".format(row, min_row_width=MIN_ROW_LENGTH)
    print(f"{Style.RESET_ALL}{style_prefix}{row.strip()}{Style.RESET_ALL}")


def send_command_callback(msg) -> None:
    """ Callback receives msg object on success, and False on failure """
    status=SEND_STATUS_SUCCESS if msg else SEND_STATUS_FAILED
    mqtt_publish_send_status(None, status)

    if msg:
        # print(f"code_name: {msg.code_name}, code: {msg.code}, is_expired: {msg.is_expired}")
        display_text = f"COMMAND SEND SUCCESS: '{msg.code_name}'"
    else:
        if "code" in LAST_SEND_MSG:
            cmd = LAST_SEND_MSG["code"]
        elif "command" in LAST_SEND_MSG:
            cmd = LAST_SEND_MSG["command"]
        else:
            cmd = "UNKNOWN"
        display_text = f"COMMAND SEND FAILED for '{LAST_SEND_MSG}'"

    print_formatted_row(THIS_GATEWAY_NAME, text=display_text, style_prefix=f"{DISPLAY_COLOURS['mqtt_command']}")
    log.info(display_text)


def save_schema_and_devices():
    if not GWY:
        log.error("Schema cannot be saved as GWY is none")
        return

    try:
        # Save the new discovered/'eavesdropped' ramses_rf schema
        schema = GWY.schema if GWY.evo is None else GWY.evo.schema
        schema = {"schema" : schema}
        save_json_to_file(schema, SCHEMA_FILE, False)

        update_zones_from_gwy()
        update_devices_from_gwy()

        if DEVICES:
            devices_simple = {str(k) : {SZ_ALIAS : DEVICES[k][SZ_ALIAS]} for k in DEVICES if k is not None}
            save_json_to_file(devices_simple, DEVICES_FILE, True)

        if ZONES:
            save_json_to_file(ZONES, ZONES_FILE, False)

        print(f"Updated '{DEVICES_FILE}' and ramses_rf schema files generated")
    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        traceback.print_stack()


def save_zones():
    update_zones_from_gwy()
    if ZONES:
        save_json_to_file(ZONES, ZONES_FILE, False)


def get_existing_device_name(device_id):
    return DEVICES[device_id][SZ_ALIAS] if device_id in DEVICES and SZ_ALIAS in DEVICES[device_id] else None


def update_devices_from_gwy(ignore_unnamed_zones=False):
    """ Refresh the local DEVICES collection with the devices that GWY has found """
    schema = GWY.evo.schema if GWY.evo else  GWY.schema
    global DEVICES

    controller_id = GWY.evo.id if GWY and GWY.evo else (GWY.schema["controller"] if "controller" in GWY.schema else None)
    if not controller_id is None and not controller_id in DEVICES:
        DEVICES[controller_id] = {SZ_ALIAS: f"Controller"}

    if "system" in schema and schema["system"] and "heating_control" in schema["system"]:
        device_id = schema["system"]["heating_control"]
        org_name = get_existing_device_name(device_id)
        DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else get_device_type_and_id(device_id)}

    if "zones" in schema:
        for zone_id, zone_items in schema["zones"].items():
            if "sensor" in zone_items:
                sensor_id = zone_items["sensor"]
                org_name = get_existing_device_name(sensor_id)
                DEVICES[sensor_id] = {SZ_ALIAS: org_name if org_name else f"{get_device_type_and_id(sensor_id)}", "zone_id": zone_id}

            if "devices" in zone_items:
                if zone_id in ZONES:
                    zone_name = ZONES[zone_id]
                elif not ignore_unnamed_zones:
                    zone_name = f"Zone_{zone_id}"
                else:
                    zone_name = None

                for device_id in zone_items["devices"]:
                    if device_id is not None:
                        org_name = get_existing_device_name(device_id)
                        DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else f"{zone_name} {get_device_type_and_id(device_id)}", "zone_id": zone_id}

    if "stored_hotwater" in schema:
        for dhw_device_type in schema["stored_hotwater"]:
            device_id = schema["stored_hotwater"][dhw_device_type]
            if device_id:
                DEVICES[device_id] = {SZ_ALIAS: dhw_device_type.replace("_"," ").title()}

    if SZ_UFH_SYSTEM in schema:
        ufc_ids = list(schema[SZ_UFH_SYSTEM].keys())
        for ufc_id in ufc_ids:
            org_name = get_existing_device_name(ufc_id)
            DEVICES[ufc_id] =  {SZ_ALIAS: org_name if org_name else f"UFH Controller {get_device_type_and_id(ufc_id)}"}

    if "orphans" in schema and schema["orphans"]:
        for device_id in schema["orphans"]:
            org_name = get_existing_device_name(device_id)
            DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else get_device_type_and_id(device_id)}

    mqtt_publish_schema()


def update_zones_from_gwy(schema={}, params={}):
    """ Refresh local ZONES with zones detected by GWY and has got zone names """
    
    if GWY:
        if not schema:            
            schema = GWY.evo.schema if GWY.evo else GWY.schema
        if not params:
            params = GWY.evo.params if GWY.evo else GWY.params

    global ZONES
    global UFH_CIRCUITS

    # GWY.evo.zones contains list of zone
    # GWY.evo.zone_by_idx['00'] gets zone object (e.g GWY.evo.zone_by_idx['00'].name)

    # ZONES = {}
    if "zones" in schema and params:
        for zone_id in schema["zones"]:
            if "zones" in params and SZ_ZONE_NAME in params["zones"][zone_id] and params["zones"][zone_id][SZ_ZONE_NAME]:
                ZONES[zone_id] = params["zones"][zone_id][SZ_ZONE_NAME]

    if schema and SZ_UFH_SYSTEM in schema:
        ufc_ids = list(schema[SZ_UFH_SYSTEM].keys())
        for ufc_id in ufc_ids:
            #TODO! If there are multiple ufh controllers, circuit numbers in ufh_circuits will have to be dependent on controller ID - is this available in messages?
            if SZ_UFH_CIRCUITS in schema[SZ_UFH_SYSTEM][ufc_id] and len(schema[SZ_UFH_SYSTEM][ufc_id][SZ_UFH_CIRCUITS]) > 0:
                for c in schema[SZ_UFH_SYSTEM][ufc_id][SZ_UFH_CIRCUITS]:
                    UFH_CIRCUITS[c] = schema[SZ_UFH_SYSTEM][ufc_id][SZ_UFH_CIRCUITS][c]

    # Only publish if GWY initialised
    if GWY:
        mqtt_publish_schema()


def get_device_type_and_id(device_id):
    if device_id and ":" in device_id and len(device_id) == 9:
        id_parts = device_id.split(":")
        dev_type = _OUT_DEVICE_TABLE[id_parts[0]]["type"]
        return f"{dev_type}:{id_parts[1]}"
    else:
        log.debug(f"get_device_type_and_id: Ignorning invalid device_id of '{device_id}'")
        log.debug(traceback.format_exc())


def get_sys_status_dict(status):
    return {"status": status, "status_ts": datetime.datetime.now().strftime("%Y-%m-%dT%X")}


def mqtt_initialise():
    if not MQTT_SERVER:
        log.error("MQTT Server details not found. Exiting...")
        raise SystemExit

    global MQTT_CLIENT
    MQTT_CLIENT = mqtt.Client()
    MQTT_CLIENT.on_connect = mqtt_on_connect
    MQTT_CLIENT.on_message = mqtt_on_message
    MQTT_CLIENT.will_set(f"{MQTT_PUB_TOPIC}/{MQTT_STATUS_SUBTOPIC}",
        payload=json.dumps(get_sys_status_dict(MQTT_OFFLINE), indent=4), qos=0, retain=True)

    if MQTT_USER:
        MQTT_CLIENT.username_pw_set(MQTT_USER, MQTT_PW)
    MQTT_CLIENT.connect(MQTT_SERVER)

    return MQTT_CLIENT


def mqtt_on_connect(client, *_):
    log.info(f"Connected to MQTT broker. Subscribing to topic {MQTT_SUB_TOPIC} for commands")
    client.subscribe(MQTT_SUB_TOPIC)
    client.publish(f"{MQTT_PUB_TOPIC}/{MQTT_STATUS_SUBTOPIC}", MQTT_ONLINE)
    mqtt_publish_status(MQTT_ONLINE)


def mqtt_on_message(client, _, msg):
    payload = str(msg.payload.decode("utf-8"))
    print_formatted_row("MQTT", text=f"Received MQTT message: {payload}", style_prefix=f"{DISPLAY_COLOURS['mqtt_command']}")
    log.info(f"MQTT message received: {payload}")
    mqtt_process_msg(payload)


def mqtt_publish_status(status):
    MQTT_CLIENT.publish(f"{MQTT_PUB_TOPIC}/{MQTT_STATUS_SUBTOPIC}", json.dumps(get_sys_status_dict(status), indent=4), 0, True)


def mqtt_publish_received_msg(msg, payload, no_unpack=False):
    """ We explicitly receive the payload instead of just using msg.payload, so that any pre-processing of the payload is assumed to be already done
        Payloads are assumed to always be dict
    """

    if not (MQTT_CLIENT and msg and (not MQTT_PUB_AS_JSON or payload)):
        return


    if not MQTT_CLIENT.is_connected:
        print_formatted_row(SYSTEM_MSG_TAG, text="[WARN] MQTT publish failed as client is not connected to broker")
        return

    if not isinstance(payload, dict):
        log.error(f"Payload in mqtt_publish_received_msg is not of type dict. type(payload): {type(payload)}, payload arg: {payload}, msg.payload: {msg.payload}")

    try:
        target_zone_id = None

        if "parent_idx" in payload and msg.src.type not in "10 13":
            # Ignore parent_idx if device type is OTB or BDR
            target_zone_id = payload["parent_idx"]
        elif "zone_idx" in payload:
            target_zone_id = payload["zone_idx"]
        elif "ufh_idx" in str(payload):
            if not UFH_CIRCUITS: # May just need an update
                update_zones_from_gwy()
            if UFH_CIRCUITS and payload["ufh_idx"] in UFH_CIRCUITS:
                target_zone_id = UFH_CIRCUITS[payload["ufh_idx"]]["zone_idx"]

        if msg.src.id not in DEVICES: # Refresh zones/devices list
            update_zones_from_gwy()
            update_devices_from_gwy()

        if hasattr(msg.src, "zone") and msg.src.zone and hasattr(msg.src.zone, "idx") and msg.src.zone.idx and not "HW" in msg.src.zone.idx:
            src_zone_id = msg.src.zone.idx
        elif hasattr(msg.src, "_domain_id") and msg.src._domain_id and int(msg.src._domain_id, 16) >= 0:
            src_zone_id = msg.src._domain_id
        else:
            src_zone_id = None

        if (target_zone_id and 0 <= int(target_zone_id, 16) < 12) or (src_zone_id and 0 <= int(src_zone_id, 16) < 12):
            if MQTT_GROUP_BY_ZONE and MQTT_REQUIRE_ZONE_NAMES and (not ZONES or (target_zone_id not in ZONES and src_zone_id not in ZONES)):
                # MQTT topic requires zone name...
                update_zones_from_gwy()
                if target_zone_id and target_zone_id not in ZONES and src_zone_id not in ZONES:
                    log.error(f"Both 'target_zone_id' and 'src_zone_id' not found in ZONES")
                    return # Return unless we have the zone name, as otherwise cannot build topic

        src_zone = to_snake(get_msg_zone_name(msg.src, target_zone_id)) #if not target_zone_id or target_zone_id <1 else get_device_zone_name(target_zone_id)
        src_device = to_snake(get_device_name(msg.src))

        if ("dhw_" in msg.code_name or "dhw_" in src_device or (src_zone_id and "HW" in src_zone_id)) and DHW_ZONE_PREFIX:
            # treat DHW as a zone if we are grouping by zone, otherwise as a device prefix
            if MQTT_GROUP_BY_ZONE:
                src_zone = f"{DHW_ZONE_PREFIX}"
            else:
                src_device = f"{DHW_ZONE_PREFIX}/{src_device}"

        if not MQTT_PUB_AS_JSON and "until" in payload and payload["until"] and " " in payload["until"]:
            # Patch with T separator
            try:
                d, t = payload["until"].split(" ")
                payload["until"] = f"{d}T{t}"
            except Exception as ex:
                log.error(f"Exception occured in patching 'until' value '{payload['until']}': {ex}", exc_info=True)

        # Need separate topics for certain payloads under CTL or HGI, such as fault log entries
        if "topic_idx" in payload:
            # topic_idx is not currently sent in ramses_rf payloads. Use here for custom topics, e.g. schedules
            topic_idx = f"/{payload['topic_idx']}"
        elif "log_idx" in payload:
            topic_idx = f"/{payload['log_idx']}"
        elif src_zone == MQTT_ZONE_IND_TOPIC and (src_device.startswith("hgi_") or src_device.startswith("ctl_")) and "zone_idx" in payload:
            topic_idx = f"/{payload['zone_idx']}"
        else:
            topic_idx = ""

        if MQTT_GROUP_BY_ZONE and src_zone:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_zone}/{src_device}/{msg.code_name}{topic_idx}"
        else:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_device}/{msg.code_name}{topic_idx}"

        subtopic = topic_base
        if not MQTT_PUB_AS_JSON and not no_unpack:
            if msg.code_name == "opentherm_msg":
                # This is an opentherm_msg. Extract msg item and updated_payload as new dict, with msg_name as key
                new_key, updated_payload = get_opentherm_msg(msg)
            else:
                updated_payload = payload
                new_key = None
            subtopic = f"{topic_base}/{to_snake(new_key)}" if new_key else topic_base

            # As some payloads are received as lists, others not, convert everything to a list so we can process in same way
            if updated_payload and not isinstance(updated_payload, list):
                updated_payload = [updated_payload]

            # Iterate through the list. payload_item should be a dict as updated_payload should now be a list of dict [{...}]
            if updated_payload:
                for payload_item in updated_payload:
                    try:
                        if isinstance(payload_item, dict): # we may have a further dict in the updated_payload - e.g. opentherm msg, system_fault etc
                            for k in payload_item:
                                MQTT_CLIENT.publish(f"{subtopic}/{to_snake(k)}", str(payload_item[k]), 0, True)
                                log.debug(f"        -> mqtt_publish_received_msg: 2. Posted subtopic: {subtopic}/{to_snake(k)}, value: {payload_item[k]}")
                        else:
                            MQTT_CLIENT.publish(subtopic, str(payload_item), 0, True)
                            log.info(f"        -> mqtt_publish_received_msg: 3. item is not a dict. Posted subtopic: {subtopic}, value: {payload_item}, type(playload_item): {type(payload_item)}")
                    except Exception as e:
                        log.error(f"Exception occured: {e}", exc_info=True)
                        log.error(f"------------> payload_item: \"{payload_item}\", type(payload_item): \"{type(payload_item)}\", updated_payload: \"{updated_payload}\"")
                        log.error(f"------------> msg: {msg}")
        else:
            MQTT_CLIENT.publish(subtopic, json.dumps(msg.payload), 0, True)

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%X")
        MQTT_CLIENT.publish(f"{topic_base}/{msg.code_name}_ts", timestamp, 0, True)
        # print("published to mqtt topic {}: {}".format(topic, msg))
    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg.src.id: {msg.src.id}, command: {msg.code_name}, payload: {payload}, pub_json: {MQTT_PUB_AS_JSON}")
        log.error(f"msg: {msg}")

        traceback.print_exc()
        pass


def mqtt_publish_send_status(cmd, status):
    if not cmd and not status:
        log.error("mqtt_publish_send_status: Both 'cmd' and 'status' cannot be None")
        return

    topic = f"{MQTT_SUB_TOPIC}/_last_command"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%X")
    if cmd:
        MQTT_CLIENT.publish(f"{topic}/command", cmd, 0, True)
        MQTT_CLIENT.publish(f"{topic}/command_ts", timestamp, 0, True)

    MQTT_CLIENT.publish(f"{topic}/status", status, 0, True)
    MQTT_CLIENT.publish(f"{topic}/status_ts", timestamp, 0, True)


def mqtt_publish_schema():
    topic = f"{MQTT_PUB_TOPIC}/{MQTT_ZONE_IND_TOPIC}/_gateway_config"

    MQTT_CLIENT.publish(f"{topic}/gwy_mode", "eavesdrop" if SCHEMA_EAVESDROP else "monitor", 0, True)
    MQTT_CLIENT.publish(f"{topic}/schema", json.dumps(GWY.schema if GWY.evo is None else GWY.evo.schema, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/params", json.dumps(GWY.params if GWY.evo is None else GWY.evo.params, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/status", json.dumps(GWY.status if GWY.evo is None else GWY.evo.status, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/config", json.dumps(vars(GWY.config), sort_keys=True), 0, True)

    MQTT_CLIENT.publish(f"{topic}/devices", json.dumps({str(k):  v for k, v in DEVICES.items()}, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/zones", json.dumps(ZONES), 0, True)
    MQTT_CLIENT.publish(f"{topic}/uhf_circuits", json.dumps(UFH_CIRCUITS, sort_keys=True), 0, True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%X")
    MQTT_CLIENT.publish(f"{topic}/_gateway_config_ts", timestamp, 0, True)


def mqtt_process_msg(msg):
    log.debug(f"MQTT message received: {msg}")

    try:
        json_data = json.loads(msg)
    except:
        log.error(f"mqtt message is not in JSON format: '{msg}'")
        return

    try:
        if SYS_CONFIG_COMMAND in json_data:
            if json_data[SYS_CONFIG_COMMAND].upper().strip() == "DISPLAY_FULL_JSON":
                global DISPLAY_FULL_JSON
                DISPLAY_FULL_JSON = json_data["value"] if "value" in json_data else False
            elif json_data[SYS_CONFIG_COMMAND].upper().strip() == "RELOAD_DISPLAY_COLOURS":
                global DISPLAY_COLOURS
                DISPLAY_COLOURS = get_display_colorscheme(True)
            elif json_data[SYS_CONFIG_COMMAND].upper().strip() == "POST_SCHEMA":
                update_zones_from_gwy()
                update_devices_from_gwy()
            elif json_data[SYS_CONFIG_COMMAND].upper().strip() == "SAVE_SCHEMA":
                update_zones_from_gwy()
                update_devices_from_gwy()
                save_schema_and_devices()
            else:
                print_formatted_row(SYSTEM_MSG_TAG,  text="System configuration command '{}' not recognised".format(json_data[SYS_CONFIG_COMMAND]))
                return
        else:
            if "code" in json_data:
                command_code = json_data["code"]
                if type(command_code) is int:
                    command_code = hex(command_code)
                    command_code = command_code.upper().replace("0X","")

                if not ("verb" in json_data and "payload" in json_data):
                    log.error(f"Failed to send command '{command_code}'. Both 'verb' and 'payload' must be provided when 'code' is used instead of 'command'")
                    return

                verb = json_data["verb"]
                payload = json_data["payload"]
                dest_id = json_data["dest_id"] if "dest_id" in json_data else GWY.evo.id
                gw_cmd = Command(verb, command_code, payload, dest_id)
                log.debug(f"--------> MQTT message converted to Command: '{gw_cmd}'")

            elif "command" in json_data:
                command_name = json_data["command"]
                if command_name in GET_SCHED:
                    zone_idx = json_data["zone_idx"] if "zone_idx" in json_data else None
                    force_refresh = json_data["force_refresh"] if "force_refresh" in json_data else None
                    spawn_schedule_task(GET_SCHED, zone_idx=zone_idx, force_refresh=force_refresh)
                    return
                elif command_name in SET_SCHED:
                    if "schedule" in json_data:
                        spawn_schedule_task(action=SET_SCHED, schedule=json_data["schedule"])
                    elif "schedule_json_file" in json_data:
                        with open(json_data["schedule_json_file"], 'r') as fp:
                            schedule = json.load(fp)
                        spawn_schedule_task(action=SET_SCHED, schedule=schedule)
                    else:
                        log.error("'set_schedule' command requires a 'schedule' json")
                    return
                elif command_name and command_name == "ping":
                    command_name = "get_system_time"

                cmd_method = getattr(Command, command_name)
                cmd_kwargs = sorted(list(inspect.signature(cmd_method).parameters.keys()))
                kwargs = {x: json_data[x] for x in json_data if x not in "command"}
                if not "dst_id" in kwargs:
                    kwargs["dst_id"] = GWY.evo.id

                # !TODO - not sure why just 'setpoint' requires this, and not others, e.g. datetime
                if command_name == "set_zone_mode" and not "ctl_id" in kwargs:
                    kwargs["ctl_id"] = GWY.evo.id

                try:
                    gw_cmd = cmd_method(**kwargs)
                except Exception as ex:
                    log.error(f"Error in sending command '{msg}': {ex}")
                    log.error(f"Command keywords: {cmd_kwargs}")
                    log.error(f"kwargs: {kwargs}")
                    print(traceback.format_exc())

            else:
                log.error(f"Invalid mqtt payload received: '{json.dumps(json_data)}'. Either 'command' or 'code' must be specified")
                return

            global LAST_SEND_MSG
            LAST_SEND_MSG = json_data
            log.debug(f"Sending command: {gw_cmd}")
            GWY.send_cmd(gw_cmd, callback=send_command_callback)
            mqtt_publish_send_status(msg, SEND_STATUS_TRANSMITTED)

    except TimeoutError:
        log.warning(f"Command '{gw_cmd if gw_cmd else msg}' failed due to time out")

    except Exception as ex:
        log.error(f"Error in sending command '{msg}': {ex}")
        print(traceback.format_exc())


def normalise_config_schema(config) -> Tuple[str, dict]:
    """Convert a HA config dict into ramses_rf format."""

    serial_port = config[CONFIG].pop(SERIAL_PORT, COM_PORT)

    if config[CONFIG].get(PACKET_LOG) and isinstance(config[CONFIG][PACKET_LOG], dict):
        return serial_port, config
    else:
        config[CONFIG][PACKET_LOG] = PACKET_LOG_SCHEMA(
            {
                LOG_FILE_NAME: config[CONFIG][PACKET_LOG] or PACKET_LOG_FILE,
                LOG_ROTATE_BYTES: config[CONFIG][LOG_ROTATE_BYTES] if LOG_ROTATE_BYTES in config[CONFIG] and
                    config[CONFIG][LOG_ROTATE_BYTES] else LOG_FILE_ROTATE_BYTES,
                LOG_ROTATE_COUNT: config[CONFIG][LOG_ROTATE_COUNT] if LOG_FILE_ROTATE_COUNT in config[CONFIG] and
                    config[CONFIG][LOG_FILE_ROTATE_COUNT] else LOG_FILE_ROTATE_COUNT
            }
        )

    return serial_port, config


def save_json_to_file(file_content, file_name, sorted=False):
    try:
        if os.path.isfile(file_name):

            if os.path.isfile(f"{file_name}.{MAX_SAVE_FILE_COUNT}"):
                # Remove any files with extension over and above MAX_SAVE_FILE_COUNT
                files = glob.glob(f"{file_name}.*")
                for f in files:
                    ext = f.split(".")[-1]
                    if ext.isnumeric():
                        if int(ext) > MAX_SAVE_FILE_COUNT:
                            os.remove(f)
                if os.path.isfile(f"{file_name}.1"):
                    os.remove(f"{file_name}.1")
                for j in range(2, MAX_SAVE_FILE_COUNT + 1):
                    if os.path.isfile(f"{file_name}.{j}"):
                        os.rename(f"{file_name}.{j}", f"{file_name}.{j-1}")

            # If we are already at max count. Delete .1, and take away 1 from all others.
            i = 1
            while os.path.exists(f"{file_name}.{i}"):
                i += 1
            os.rename(file_name, f"{file_name}.{i}")

        with open(file_name,'w') as fp:
            fp.write(json.dumps(file_content, sort_keys=sorted, indent=4))
        fp.close()
    except Exception as e:
        log.error(f"Exception occured saving file '{file_name}': {e}", exc_info=True)
        log.error(f"{json.dumps(file_content)}")


def load_json_from_file(file_path):
    items = {}
    try:
        if os.path.isfile(file_path):
            with open(file_path, 'r') as fp:
                items = json.load(fp)
    except Exception as ex:
        log.error(f"{Style.BRIGHT}{DISPLAY_COLOURS.get('ERROR')}Exception occured in loading file '{file_path}': {ex}{Style.RESET_ALL}", exc_info=True)

    return items


def initialise_sys(kwargs):

    mqtt_initialise()

    global DEVICES
    global ZONES
    global SCHEMA_EAVESDROP
    global SCHEMA_FILE

    BASIC_CONFIG = {CONFIG: { DISABLE_SENDING: False, DISABLE_DISCOVERY: RAMSESRF_DISABLE_DISCOVERY, "enforce_knownlist": RAMSESRF_KNOWN_LIST and not SCHEMA_EAVESDROP, ENFORCE_KNOWNLIST: RAMSESRF_KNOWN_LIST and not SCHEMA_EAVESDROP,
        EVOFW_FLAG: None, MAX_ZONES: 12, LOG_ROTATE_BYTES: LOG_FILE_ROTATE_BYTES,  LOG_ROTATE_COUNT: LOG_FILE_ROTATE_COUNT, PACKET_LOG: PACKET_LOG_FILE, SERIAL_PORT: COM_PORT, USE_ALIASES: True, USE_SCHEMA: True}}

    lib_kwargs, _ = _proc_kwargs((BASIC_CONFIG, {}), kwargs)

    schema_loaded_from_file = False
    if not SCHEMA_EAVESDROP and SCHEMA_FILE is not None:
        # If we have a ramses_rf schema file (and we are not in eavesdrop mode), use the schema

        if os.path.isfile(SCHEMA_FILE):
            log.info(f"Loading schema from file '{SCHEMA_FILE}'")
            with open(SCHEMA_FILE) as config_schema:
                schema = json.load(config_schema)
                if "schema" in schema and "controller" in schema["schema"] and schema["schema"]["controller"] is None:
                    schema_loaded_from_file = False
                    log.warning(f"The existing schema file '{SCHEMA_FILE}' appears to be invalid. Ignoring...")
                    SCHEMA_EAVESDROP = True
                else:
                    lib_kwargs.update(schema)
                    if COM_PORT: # override with the one in the main config file
                        lib_kwargs[CONFIG][SERIAL_PORT] = COM_PORT
                    log.debug(f"Schema loaded. Updated lib_kwargs: {lib_kwargs}")
                    schema_loaded_from_file = True
        else:
            log.warning(f"The schema file '{SCHEMA_FILE}' was not found'")
            SCHEMA_EAVESDROP = True

    if SCHEMA_EAVESDROP or not schema_loaded_from_file:
        # Initially enable 'eavesdropping' mode to discover devices. Save these to a schema file for subsequent use
        # https://github.com/zxdavb/ramses_rf/issues/15?_pjax=%23js-repo-pjax-container#issuecomment-846774151

        SCHEMA_EAVESDROP = True
        # Disable known_list, so that we get everything
        if KNOWN_LIST in lib_kwargs[CONFIG]:
            del lib_kwargs[CONFIG][KNOWN_LIST]
        lib_kwargs[CONFIG][ENFORCE_KNOWNLIST] = False

        log.warning(f"Schema file missing or the 'known_list' section is missing. Defaulting to ramses_rf 'eavesdropping' mode")
        log.debug(f"Using temporary config schema: {json.dumps(lib_kwargs)}")

    lib_kwargs[CONFIG][ENABLE_EAVESDROP] = SCHEMA_EAVESDROP
    lib_kwargs[CONFIG][DISABLE_SENDING] = GATEWAY_DISABLE_SENDING

    # Load local devices file if available. This forms the 'known_list' and also allows for custom naming of devices
    DEVICES = load_json_from_file(DEVICES_FILE)

    # Add this server/gateway as a known device
    DEVICES[HGI_DEVICE_ID] = { SZ_ALIAS : THIS_GATEWAY_NAME}
    SCHEMA_EAVESDROP = len(DEVICES) <= 1

    if not SCHEMA_EAVESDROP and not KNOWN_LIST in lib_kwargs and DEVICES:
        # Create 'known_list' from DEVICES
        known_list = {KNOWN_LIST: {HGI_DEVICE_ID: { SZ_ALIAS : THIS_GATEWAY_NAME}}}
        # allowed_list = [{d: {"name": DEVICES[d]["name"]}} for d in DEVICES]
        for d in DEVICES:
            known_list[KNOWN_LIST][d] = {SZ_ALIAS : DEVICES[d][SZ_ALIAS]}
        lib_kwargs.update(known_list)

    if LOAD_ZONES_FROM_FILE:
        ZONES = load_json_from_file(ZONES_FILE)

    import re
    device_regex = r"^01:[0-9]{6}$"
    for ctl_id, schema in lib_kwargs.items():
        if re.match(device_regex, ctl_id):
            update_zones_from_gwy(schema, {})

    if DEVICES and len(DEVICES) >1:
        print_formatted_row("", text="")
        print_formatted_row("", text="------------------------------------------------------------------------------------------")
        print_formatted_row("", text=f"{Style.BRIGHT}{Fore.YELLOW}Devices loaded from '{DEVICES_FILE}' file:")

        for key in sorted(DEVICES):
            dev_type = _OUT_DEVICE_TABLE[key.split(":")[0]]["type"]
            if "schema" in lib_kwargs and "zones" in lib_kwargs["schema"]:
                zone_ids = get_parent_keys(lib_kwargs["schema"]["zones"], key)
                zone_id = zone_ids[0] if zone_ids else None
                zone_details = f"- Zone {zone_id:<3}" if zone_id else ""
            else:
                zone_details =""
            print_formatted_row("", text=f"{Style.BRIGHT}{Fore.BLUE}   {dev_type} {key} - {DEVICES[key][SZ_ALIAS]:<23} {zone_details}")

        print_formatted_row("", text="------------------------------------------------------------------------------------------")
        print_formatted_row("", text="")

    else:
        print_formatted_row("", text="Existing devices file not found. Defaulting to 'eavesdropping' mode")

    log.info(f"# evogateway {VERSION} (using 'ramses_rf' library {RAMSES_RF_VERSION})")
    print_formatted_row('',  text=f"{Style.BRIGHT}{Fore.YELLOW}# evogateway {VERSION} (using 'ramses_rf' library {RAMSES_RF_VERSION})")

    return lib_kwargs


async def main(**kwargs):

    lib_kwargs = initialise_sys(kwargs)
    global GWY
    serial_port, lib_kwargs = normalise_config_schema(lib_kwargs)
    GWY = Gateway(serial_port, **lib_kwargs)
    GWY.create_client(process_gwy_message)
    update_devices_from_gwy()
    update_zones_from_gwy()

    mqtt_publish_schema()

    try:
        MQTT_CLIENT.loop_start()
        tasks = asyncio.create_task(GWY.start())

        await tasks

    except Exception as ex:
        msg = f" - ended via: Exception: {ex}"
    else:  # if no Exceptions raised, e.g. EOF when parsing
        msg = " - ended without error (e.g. EOF)"

    mqtt_publish_schema()


    MQTT_CLIENT.loop_stop()



if __name__ == "__main__":

    try:
        asyncio.run(main())

    except asyncio.CancelledError:
        msg = " - ended via: CancelledError (e.g. SIGINT)"
    except KeyboardInterrupt:
        msg = " - ended via: KeyboardInterrupt"
    except EvohomeError as err:
        msg = f" - ended via: EvohomeError: {err}"
    else:  # if no Exceptions raised, e.g. EOF when parsing
        msg = " - ended without error (e.g. EOF)"

    if GWY:
        # Always update the zones file on exit
        save_zones()

        if SCHEMA_EAVESDROP:
            print_ramsesrf_gwy_schema(GWY)
            save_schema_and_devices()

    print(msg)
