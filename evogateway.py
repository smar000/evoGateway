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
from threading import Timer
from datetime import timedelta as td
from types import SimpleNamespace
from colorama import init as colorama_init, Fore, Style, Back
import logging
from logging.handlers import RotatingFileHandler

from ramses_rf import Gateway, GracefulExit
from ramses_rf.const import SZ_DOMAIN_ID, SZ_SCHEDULE, SZ_UFH_IDX
from ramses_rf.discovery import GET_SCHED, SET_SCHED, spawn_scripts
from ramses_rf.version import VERSION as RAMSES_RF_VERSION
from ramses_rf.protocol.command import Command
from ramses_rf.protocol.address import HGI_DEVICE_ID, NON_DEVICE_ID, DEV_TYPE_MAP
from ramses_rf.protocol.logger import CONSOLE_COLS
from ramses_rf.protocol.exceptions import EvohomeError
from ramses_rf.protocol.message import CODE_NAMES
from ramses_rf.protocol.schemas import (
    SZ_DISABLE_SENDING,
    SZ_ENFORCE_KNOWN_LIST,
    SZ_KNOWN_LIST,
    SZ_EVOFW_FLAG,
    SZ_SERIAL_PORT,
    SZ_FILE_NAME,
    SZ_ROTATE_BYTES,
    SZ_ROTATE_BACKUPS,
)

from ramses_rf.schemas import (
    SCH_GLOBAL_CONFIG,
    SZ_SCHEMA,
    SZ_MAIN_TCS,
    SZ_CONFIG,
    SZ_DISABLE_DISCOVERY,
    SZ_ENABLE_EAVESDROP,
    SZ_REDUCE_PROCESSING,
    SZ_SYSTEM,
    SZ_ORPHANS,
    SZ_ORPHANS_HEAT,
    SZ_DHW_SYSTEM,
    SZ_UFH_SYSTEM,
    SZ_APPLIANCE_CONTROL,
    SZ_SENSOR,
    SZ_DEVICES,
    SZ_ZONES,
    SZ_ZONE_IDX,
    SZ_MAX_ZONES,
    SZ_CIRCUITS,
    SZ_PACKET_LOG,
    SZ_USE_ALIASES,
    SZ_ALIAS,
    SZ_NAME
)

LIB_KEYS = tuple(SCH_GLOBAL_CONFIG({}).keys()) + (SZ_SERIAL_PORT,)

DEFAULT_COLORS = {" I": f"{Fore.WHITE}", "RP": f"{Fore.LIGHTWHITE_EX}", "RQ": f"{Fore.BLACK}",
    " W": f"{Fore.MAGENTA}", "temperature": f"{Fore.YELLOW}", "ERROR": f"{Back.RED}{Fore.YELLOW}",
    "mqtt_command": f"{Fore.LIGHTCYAN_EX}" }


if  os.path.isdir(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "3.13-0.22.40"

CONFIG_FILE     = "evogateway.cfg"

config = configparser.RawConfigParser()
config.read(CONFIG_FILE)

def get_display_colorscheme(reload_config=False):
    if reload_config:
        config.read(CONFIG_FILE)

    colours_string = config.get("MISC", "DISPLAY_COLOURS", fallback=None)
    try: # TODO! Get rid of eval and tidy up!
        scheme = eval(colours_string) if colours_string else None
    except:
        pass
    if not scheme:
        scheme = DEFAULT_COLORS

    if " I" not in scheme:
        scheme[" I"] = DEFAULT_COLORS[" I"]
    if "RQ" not in scheme:
        scheme["RQ"] = DEFAULT_COLORS["RQ"]
    if "RP" not in scheme:
        scheme["RP"] = DEFAULT_COLORS["RP"]
    if " W" not in scheme:
        scheme[" W"] = DEFAULT_COLORS[" W"]
    if "ERROR" not in scheme:
        scheme["ERROR"] = DEFAULT_COLORS["ERROR"]
    if "mqtt_command" not in scheme:
        scheme["mqtt_command"] = DEFAULT_COLORS["mqtt_command"]

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
MQTT_CLIENTID           = config.get("MQTT", "MQTT_CLIENTID", fallback="evoGateway")

MQTT_PUB_JSON_ONLY      = config.getboolean("MQTT", "MQTT_PUB_AS_JSON", fallback=False)
MQTT_PUB_KV_WITH_JSON   = config.getboolean("MQTT", "MQTT_PUB_KV_WITH_JSON", fallback=False)
if MQTT_PUB_KV_WITH_JSON:
    MQTT_PUB_JSON_ONLY = False

MQTT_GROUP_BY_ZONE      = config.getboolean("MQTT", "MQTT_GROUP_BY_ZONE", fallback=True)
MQTT_REQUIRE_ZONE_NAMES = config.getboolean("MQTT", "MQTT_REQUIRE_ZONE_NAMES", fallback=True)

MQTT_SUB_TOPIC          = config.get("MQTT", "MQTT_SUB_TOPIC", fallback="")
MQTT_PUB_TOPIC          = config.get("MQTT", "MQTT_PUB_TOPIC", fallback="")
MQTT_ZONE_IND_TOPIC     = config.get("MQTT", "MQTT_ZONE_INDEP_TOPIC", fallback="_zone_independent")
MQTT_ZONE_UNKNOWN       = config.get("MQTT", "MQTT_ZONE_UNKNOWN", fallback="_zone_unknown")

THIS_GATEWAY_NAME       = config.get("MISC", "THIS_GATEWAY_NAME", fallback="EvoGateway")
RAMSESRF_DISABLE_SENDING = config.getboolean("MISC", "DISABLE_SENDING", fallback=False)

DISPLAY_FULL_JSON       = config.getboolean("MISC", "DISPLAY_FULL_JSON", fallback=False)
FORCE_SINGLE_HGI        = config.getboolean("Misc", "FORCE_SINGLE_HGI", fallback=True)
DHW_ZONE_PREFIX         = config.get("Misc", "DHW_ZONE_PREFIX", fallback="_dhw")

RAMSESRF_DISABLE_DISCOVERY = config.getboolean("Ramses_rf", SZ_DISABLE_DISCOVERY, fallback=False)
RAMSESRF_ALLOW_EAVESDROP   = config.getboolean("Ramses_rf", SZ_ENABLE_EAVESDROP, fallback=False)
RAMSESRF_KNOWN_LIST        = config.getboolean("Ramses_rf", SZ_KNOWN_LIST, fallback=True)

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

RELAYS                  = {"f9": "Radiators", "fa": "DHW", "fc": "Appliance Controller"}

SZ_TOPIC_IDX            = "topic_idx"
SZ_LOG_IDX              = "log_idx"
SZ_FRAG_NUMBER          = "frag_number"
SZ_FORCE_IO             = "force_io"

GET_SCHED_WAIT_PERIOD   = 5

# -----------------------------------
DEVICES = {}
ZONES = {}
UFH_CIRCUITS = {}
MQTT_CLIENT = None
GWY = None
GWY_MODE = None
LAST_SEND_MSG = None

# -----------------------------------

log = logging.getLogger("evogateway_log")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(lineno)s] %(message)s')
# %(funcName)20s() [%(levelname)s]

# Log file handler
file_handler = RotatingFileHandler(EVENTS_FILE, maxBytes=LOG_FILE_ROTATE_BYTES,
    backupCount=LOG_FILE_ROTATE_COUNT)
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


def _proc_kwargs(obj, kwargs) -> Tuple[dict, dict]:
    lib_kwargs, cli_kwargs = obj
    lib_kwargs[SZ_CONFIG].update({k: v for k, v in kwargs.items() if k in LIB_KEYS})
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
            name = DEVICES[device_address.id][SZ_ALIAS] if device_address.id in DEVICES \
                else device_address.id
        if name == NON_DEVICE_ID:
            name = ""

        try:
            dev_type = DEV_TYPE_MAP[device_address.type]
        except:
            dev_type = ""
        name = "{} {}".format(dev_type, name).strip()
        return name

    except Exception as ex:
        log.error(f"{Style.BRIGHT}{DISPLAY_COLOURS.get('ERROR')}Exception occured for "
            "device_address '{device_address}': {ex}{Style.RESET_ALL}", exc_info=True)
        traceback.print_stack()


def get_msg_zone_name(src, target_zone_id=None):
    """ Use any 'target' zone name given in the payload, otherwise fall back
        to zone name of the sending device
    """

    # Use the standard zone names if target_zone_id available (unless source type is BDR or OTB)
    if src.type not in "13 10" and target_zone_id and int(target_zone_id, 16) >= 0:
        if target_zone_id not in ZONES:
            update_zones_from_gwy()
        if target_zone_id.strip().lower() in "f9 fa fc":
            # These are BDRs or UFH relays.  F9 = DHW, FA = Radiators and FC = UFH
            if src.type == "01":
                zone_name = f"{MQTT_ZONE_IND_TOPIC}"
            else:
                # Default to placing these under relays as they are not directly from controller
                zone_name = f"{MQTT_ZONE_IND_TOPIC}/relays"
        else:
            zone_name = ZONES[target_zone_id] if target_zone_id in ZONES \
                else "_zone_{}".format(target_zone_id)
    else:
        # i.e. device source type is BDR/OTB _or_ (not BDR/OTB but target_zone_id < 0)
        try:
            device = GWY.get_device(src.id)
            src_zone_id = device.zone.zone_idx if hasattr(device, "zone") and hasattr(device.zone, SZ_ZONE_IDX) else None
            if src_zone_id and src_zone_id not in 'FF HW' and src_zone_id in ZONES:
                zone_name = ZONES[src_zone_id]
            elif src.type in "01 18" or target_zone_id == "-1":
                # Controllers and HGI
                zone_name = MQTT_ZONE_IND_TOPIC
            elif src.type in "07":
                # DHW Wireless sender
                zone_name = DHW_ZONE_PREFIX
            elif src.type in "02 10 13" or (src_zone_id and src_zone_id !="HW" and int(src_zone_id, 16) > 11) :
                # Relay types, e.g. BDR, OTB, UFC
                zone_name = f"{MQTT_ZONE_IND_TOPIC}/relays"
            elif src_zone_id and int(src_zone_id, 16) >= 0 and src_zone_id in ZONES:
                # Normal 'zones'
                zone_name = ZONES[src_zone_id]
            else:
                log.error(f"----> Unknown zone for src: '{src} {DEVICES[src.id] if src.id in DEVICES else ''}'")
                zone_name = MQTT_ZONE_UNKNOWN
        except Exception as e:
            log.error(f"Error: {e}", exc_info=True)
            zone_name = MQTT_ZONE_UNKNOWN

    return zone_name


def get_opentherm_msg(msg):
    if msg.code_name == "opentherm_msg":
        name = msg.payload.get("msg_name", None)
        if name:
            # some msg_name are unhashable/dict/have multiple data elements
            key = name if isinstance(name, str) else "OpenTherm"
            # return the whole payload dict as we don't know which message component is of interest
            return key, {key: msg.payload}
    else:
        log.error(f"Invalid opentherm_msg. msg.code_name: {msg.code_name}")
    return None, None


def spawn_schedule_task(action, **kwargs):
    """ WIP......... """

    ctl_id = GWY.tcs.id
    if action == GET_SCHED:
        if not SZ_ZONE_IDX in kwargs:
            log.error("get_schedules requires 'zone_idx'")
            return

        zone_idx = kwargs[SZ_ZONE_IDX]
        force_io = kwargs[SZ_FORCE_IO] if SZ_FORCE_IO in kwargs else None
        zone = GWY.tcs.zone_by_idx[zone_idx]

        if zone.schedule and not force_io:
            # Schedule already available, so no need for any further io unless we are forcing
            display_schedule_for_zone(zone_idx)
            return

        asyncio.ensure_future(zone.get_schedule(force_io=force_io), loop=GWY._loop)

    elif action == SET_SCHED:
        if not SZ_ZONE_IDX in kwargs:
            log.error("'set_schedule' requires 'zone_idx' to be specified")
            return
        if not SZ_SCHEDULE in kwargs and type(kwargs[SZ_SCHEDULE] is not list):
            log.error("'set_schedule' requires 'schedule' json")
            return

        zone_idx = kwargs[SZ_ZONE_IDX]
        zone = GWY.tcs.zone_by_idx[zone_idx]
        schedule = kwargs[SZ_SCHEDULE]
        force_refresh = kwargs["force_refresh"] if "force_refresh" in kwargs else None
        asyncio.ensure_future(zone.set_schedule(schedule), loop=GWY._loop)

    # Create timer to display schedule data after GET_SCHED_WAIT_PERIOD seconds
    timer = Timer(GET_SCHED_WAIT_PERIOD, display_schedule_for_zone, [zone_idx])
    timer.start()


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
                for key in [SZ_ZONE_IDX, "parent_idx", "msg_id", "msg_type"] + [k for k in filtered_text if "unknown" in k]:
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
                zone_id = item[SZ_ZONE_IDX] if SZ_ZONE_IDX in item else None
                display_simple_msg(msg, item, zone_id, "")
            mqtt_publish_received_msg(msg, item)

        except Exception as e:
            log.error(f"Exception occured: {e}", exc_info=True)
            log.error(f"item: {item}, payload: {payload} ")
            log.error(f"msg: {msg}")


def print_ramsesrf_gwy_schema(gwy):

    schema = get_current_schema(gwy)
    print(f"Schema[gateway] = {json.dumps(schema, indent=4)}\r\n")
    print(f"Params[gateway] = {json.dumps(gwy.params)}\r\n")
    print(f"Status[gateway] = {json.dumps(gwy.status)}")

    orphans = [d for d in sorted(gwy.schema[SZ_ORPHANS_HEAT])]
    print(f"Schema[{SZ_ORPHANS_HEAT}] = {json.dumps({'schema': orphans}, indent=4)}\r\n")

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
        print_formatted_row(src, dst, msg.verb, msg.code_name, f"{main_txt: <75} {zone_id} {suffix_text}", msg._pkt._rssi, style_prefix)

    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg: {msg}, payload_dict: {payload_dict}, target_zone_id: {target_zone_id}, suffix_text: {suffix_text}")
        log.error(f"type(display_text): {type(display_text)}")
        log.error(f"filtered_text: {filtered_text}" if filtered_text else "filtered_text is None")
        log.error(f"Display row: {msg.code_name}: {msg.verb}| {src} -> {dst} | {display_text} {zone_name} [Zone {target_zone_id}] {suffix_text}")
        log.error(f"|rssi '{msg._pkt._rssi}'| src '{src}' -> dst '{dst}' | verb '{msg.verb}'| cmd '{msg.code_name}'")


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


def get_current_schema(gwy):
    config = {SZ_CONFIG: vars(gwy.config)}
    known_list = { SZ_KNOWN_LIST: gwy.known_list}
    schema = {**config, **gwy.schema, **known_list}

    return schema


def save_schema_and_devices():
    if not GWY:
        log.error("Schema cannot be saved as GWY is none")
        return

    try:
        # Save the new discovered/'eavesdropped' ramses_rf schema
        schema = schema = get_current_schema(GWY)
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
    schema = GWY.tcs.schema if GWY.tcs else  GWY.schema
    global DEVICES

    controller_id = GWY.tcs.id if GWY and GWY.tcs else (GWY.schema[SZ_MAIN_TCS] if SZ_MAIN_TCS in GWY.schema else None)
    if controller_id is not None and controller_id not in DEVICES:
        DEVICES[controller_id] = {SZ_ALIAS: f"Controller"}

    if SZ_SYSTEM in schema and schema[SZ_SYSTEM] and SZ_APPLIANCE_CONTROL in schema[SZ_SYSTEM]:
        device_id = schema[SZ_SYSTEM][SZ_APPLIANCE_CONTROL]
        org_name = get_existing_device_name(device_id)
        DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else get_device_type_and_id(device_id)}

    if SZ_ZONES in schema:
        for zone_id, zone_items in schema[SZ_ZONES].items():
            if SZ_SENSOR in zone_items:
                sensor_id = zone_items[SZ_SENSOR]
                org_name = get_existing_device_name(sensor_id)
                DEVICES[sensor_id] = {SZ_ALIAS: org_name if org_name else f"{get_device_type_and_id(sensor_id)}", "zone_id": zone_id}

            if SZ_DEVICES in zone_items:
                if zone_id in ZONES:
                    zone_name = ZONES[zone_id]
                elif not ignore_unnamed_zones:
                    zone_name = f"Zone_{zone_id}"
                else:
                    zone_name = None

                for device_id in zone_items[SZ_DEVICES]:
                    if device_id is not None:
                        org_name = get_existing_device_name(device_id)
                        DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else f"{zone_name} {get_device_type_and_id(device_id)}", "zone_id": zone_id}

    if SZ_DHW_SYSTEM in schema:
        for dhw_device_type in schema[SZ_DHW_SYSTEM]:
            device_id = schema[SZ_DHW_SYSTEM][dhw_device_type]
            if device_id:
                DEVICES[device_id] = {SZ_ALIAS: dhw_device_type.replace("_"," ").title()}

    if SZ_UFH_SYSTEM in schema:
        ufc_ids = list(schema[SZ_UFH_SYSTEM].keys())
        for ufc_id in ufc_ids:
            org_name = get_existing_device_name(ufc_id)
            DEVICES[ufc_id] =  {SZ_ALIAS: org_name if org_name else f"UFH Controller {get_device_type_and_id(ufc_id)}"}

    if SZ_ORPHANS in schema and schema[SZ_ORPHANS]:
        for device_id in schema[SZ_ORPHANS]:
            org_name = get_existing_device_name(device_id)
            DEVICES[device_id] = {SZ_ALIAS: org_name if org_name else get_device_type_and_id(device_id)}

    mqtt_publish_schema()


def update_zones_from_gwy(schema={}, params={}):
    """ Refresh local ZONES with zones detected by GWY and has got zone names """

    if GWY:
        if not schema:
            schema = GWY.tcs.schema if GWY.tcs else GWY.schema
        if not params:
            params = GWY.tcs.params if GWY.tcs else GWY.params

    global ZONES
    global UFH_CIRCUITS

    # GWY.tcs.zones contains list of zone
    # GWY.tcs.zone_by_idx['00'] gets zone object (e.g GWY.tcs.zone_by_idx['00'].name)

    # ZONES = {}
    if SZ_ZONES in schema and params:
        for zone_id in schema[SZ_ZONES]:
            if SZ_ZONES in params and SZ_NAME in params[SZ_ZONES][zone_id] and params[SZ_ZONES][zone_id][SZ_NAME]:
                ZONES[zone_id] = params[SZ_ZONES][zone_id][SZ_NAME]

    if schema and SZ_UFH_SYSTEM in schema:
        ufc_ids = list(schema[SZ_UFH_SYSTEM].keys())
        for ufc_id in ufc_ids:
            #TODO! If there are multiple ufh controllers, circuit numbers in ufh_circuits will have to be dependent on controller ID - is this available in messages?
            if SZ_CIRCUITS in schema[SZ_UFH_SYSTEM][ufc_id] and len(schema[SZ_UFH_SYSTEM][ufc_id][SZ_CIRCUITS]) > 0:
                for c in schema[SZ_UFH_SYSTEM][ufc_id][SZ_CIRCUITS]:
                    if c not in UFH_CIRCUITS.keys():
                        UFH_CIRCUITS[c] = schema[SZ_UFH_SYSTEM][ufc_id][SZ_CIRCUITS][c]

    # Only publish if GWY initialised
    if GWY:
        mqtt_publish_schema()


def get_device_type_and_id(device_id):
    if device_id and ":" in device_id and len(device_id) == 9:
        id_parts = device_id.split(":")
        dev_type = DEV_TYPE_MAP[id_parts[0]]
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

    if not (MQTT_CLIENT and msg and (not MQTT_PUB_JSON_ONLY or payload)):
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
        elif SZ_ZONE_IDX in payload:
            target_zone_id = payload[SZ_ZONE_IDX]
        elif SZ_DOMAIN_ID in payload:
            target_zone_id = payload[SZ_DOMAIN_ID]
        elif SZ_UFH_IDX in str(payload):
            if not UFH_CIRCUITS: # May just need an update
                update_zones_from_gwy()
            if UFH_CIRCUITS and payload[SZ_UFH_IDX] in UFH_CIRCUITS and SZ_ZONE_IDX in UFH_CIRCUITS[payload[SZ_UFH_IDX]]:
                target_zone_id = UFH_CIRCUITS[payload[SZ_UFH_IDX]][SZ_ZONE_IDX]

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

        if not MQTT_PUB_JSON_ONLY and "until" in payload and payload["until"] and " " in payload["until"]:
            # Patch with T separator
            try:
                d, t = payload["until"].split(" ")
                payload["until"] = f"{d}T{t}"
            except Exception as ex:
                log.error(f"Exception occured in patching 'until' value '{payload['until']}': {ex}", exc_info=True)

        # Need separate sub-topics for certain payloads under CTL, HGI or UFH controller, such as fault log entries
        if "topic_idx" in payload:
            # topic_idx is not currently sent in ramses_rf payloads. Use here for custom topics, e.g. schedules
            topic_idx = f"/{payload['topic_idx']}"
        elif "log_idx" in payload:
            topic_idx = f"/{payload['log_idx']}"
        elif SZ_FRAG_NUMBER in payload:
            topic_idx = f"/fragment_{payload['frag_number']}"
        elif src_zone.endswith("/relays") and "ufx_idx" in payload:
            topic_idx = f"/_ufx_idx_{payload['ufx_idx']}"
        elif src_zone.startswith(MQTT_ZONE_IND_TOPIC) and (src_device.startswith("hgi_") or src_device.startswith("ctl_") or src_device.startswith("ufc_")) and (SZ_ZONE_IDX in payload or SZ_DOMAIN_ID in payload):
            if SZ_ZONE_IDX in payload:
                topic_idx = f"/{payload[SZ_ZONE_IDX]}"
            elif payload[SZ_DOMAIN_ID].lower() in RELAYS:
                topic_idx =  f"/_domain_{payload['domain_id'].upper()}_{to_snake(RELAYS[payload['domain_id'].lower()])}"
            else:
                topic_idx = payload[SZ_DOMAIN_ID].lower()
        else:
            topic_idx = ""

        if MQTT_GROUP_BY_ZONE and src_zone:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_zone}/{src_device}/{msg.code_name}{topic_idx}"
        else:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_device}/{msg.code_name}{topic_idx}"

        subtopic = topic_base

        # if msg.code_name == "relay_demand" or SZ_DOMAIN_ID in payload:
        #     log.info(f"[DEBUG] ----->                          : payload: {payload}, target_zone_id: {target_zone_id}, msg: {msg}")
        #     log.info(f"[DEBUG] ----->                          : subtopic: '{subtopic}', topic_idx: '{topic_idx}', src_zone: {src_zone}, src_device: {src_device}")

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%X%Z")
        if not MQTT_PUB_JSON_ONLY and not no_unpack:
            #Unpack the JSON and publish the individual key/value pairs

            if MQTT_PUB_KV_WITH_JSON:
                # Publish the payload JSON into the subtopic key
                payload["timestamp"] = timestamp
                MQTT_CLIENT.publish(subtopic, json.dumps(payload), 0, True)

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
            # Publish the JSON
            MQTT_CLIENT.publish(subtopic, json.dumps(msg.payload), 0, True)

        MQTT_CLIENT.publish(f"{topic_base}/{msg.code_name}_ts", timestamp, 0, True)
        # print("published to mqtt topic {}: {}".format(topic, msg))
    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg.src.id: {msg.src.id}, command: {msg.code_name}, payload: {payload}, pub_json: {MQTT_PUB_JSON_ONLY}")
        log.error(f"msg: {msg}")

        traceback.print_exc()
        pass


def mqtt_publish_zone_schedules(with_display=False):
    """ Publish all avialable zone schedules"""

    for zone in GWY.tcs.zones:
        if zone.schedule:
            # Fake a Message object for publishing...
            msg = SimpleNamespace(**{"code_name":"zone_schedule", SZ_ZONE_IDX: zone.idx, "src": SimpleNamespace(**{"id": GWY.tcs.id, "type": GWY.get_device(GWY.tcs.id).type, "zone": zone})})
            mqtt_publish_received_msg(msg, {SZ_SCHEDULE: zone.schedule, SZ_ZONE_IDX: zone.idx})
            if with_display:
                display_schedule_for_zone(zone)


def display_schedule_for_zone(zone_idx):
    """ Display schedule for given zone and post to mqtt"""

    zone = GWY.tcs.zone_by_idx[zone_idx]
    if zone and zone.schedule:
        schedule = json.dumps(zone.schedule)
        dtm = f"{datetime.datetime.now():%H:%M:%S.%f}"[:-3]
        zone_name = f"{zone.name} [{zone.idx}]" if zone.name else f"{zone.idx}"
        if DISPLAY_FULL_JSON:
            print(f"{DISPLAY_COLOURS.get('RP')}{dtm} "
                f"Schedule for zone {zone_name}: {schedule}"[:CONSOLE_COLS])
        else:
            print_formatted_row(SYSTEM_MSG_TAG,
                text=f"Schedule for zone '{zone_name}': {schedule}")

        # Fake a Message object for publishing...
        msg = SimpleNamespace(**{"code_name":"zone_schedule", SZ_ZONE_IDX: zone.idx, "src": SimpleNamespace(**{"id": GWY.tcs.id, "type": GWY.get_device(GWY.tcs.id).type, "zone": zone})})
        mqtt_publish_received_msg(msg, {SZ_SCHEDULE: zone.schedule, SZ_ZONE_IDX: zone.idx})



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

    MQTT_CLIENT.publish(f"{topic}/gwy_mode", "eavesdrop" if RAMSESRF_ALLOW_EAVESDROP else "monitor", 0, True)
    MQTT_CLIENT.publish(f"{topic}/schema", json.dumps(GWY.schema if GWY.tcs is None else GWY.tcs.schema, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/params", json.dumps(GWY.params if GWY.tcs is None else GWY.tcs.params, sort_keys=True), 0, True)
    MQTT_CLIENT.publish(f"{topic}/status", json.dumps(GWY.status if GWY.tcs is None else GWY.tcs.status, sort_keys=True), 0, True)
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

                if "verb" not in json_data or "payload" not in json_data:
                    log.error(f"Failed to send command '{command_code}'. Both 'verb' and 'payload' must be provided when 'code' is used instead of 'command'")
                    return

                verb = json_data["verb"]
                payload = json_data["payload"]
                dest_id = json_data["dest_id"] if "dest_id" in json_data else GWY.tcs.id
                gw_cmd = GWY.create_cmd(verb, dest_id, command_code, payload)                 # Command.from_attrs()
                log.debug(f"--------> MQTT message converted to Command: '{gw_cmd}'")

            elif "command" in json_data:
                command_name = json_data["command"]
                if command_name in GET_SCHED:
                    zone_idx = json_data[SZ_ZONE_IDX] if SZ_ZONE_IDX in json_data else None
                    force_refresh = json_data["force_refresh"] if "force_refresh" in json_data else None
                    spawn_schedule_task(GET_SCHED, zone_idx=zone_idx, force_refresh=force_refresh)
                    return
                elif command_name in SET_SCHED:
                    if SZ_SCHEDULE in json_data:
                        spawn_schedule_task(action=SET_SCHED, zone_idx=json_data[SZ_ZONE_IDX],schedule=json_data[SZ_SCHEDULE])
                    elif "schedule_json_file" in json_data:
                        with open(json_data["schedule_json_file"], 'r') as fp:
                            schedule = json.load(fp)
                        spawn_schedule_task(action=SET_SCHED, schedule=schedule)
                    else:
                        log.error("'set_schedule' command requires a 'schedule' json")
                    return
                elif command_name and command_name == "ping":
                    command_name = "get_system_time"

                ramses_cmd_constructor = getattr(Command, command_name)


                # ramses_cmd_kwargs = sorted(list(inspect.signature(ramses_cmd_constructor).parameters.keys()))
                # inspect.signature not able to get args through the command constructor decorators. Try wrapper attributes
                ramses_cmd_kwargs = sorted(list(ramses_cmd_constructor.__closure__[0].cell_contents.__annotations__))
                kwargs = {x: json_data[x] for x in json_data if x not in "command"}
                if ramses_cmd_kwargs and "dst_id" in ramses_cmd_kwargs and "dst_id" not in kwargs:
                    kwargs["dst_id"] = GWY.tcs.id

                # !TODO - not sure why just 'setpoint' requires this, and not others, e.g. datetime
                if command_name == "set_zone_mode" and not "ctl_id" in kwargs:
                    kwargs["ctl_id"] = GWY.tcs.id

                try:
                    gw_cmd = ramses_cmd_constructor(**kwargs)
                except Exception as ex:
                    log.error(f"Error in sending command '{msg}': {ex}")
                    log.error(f"Command keywords: {ramses_cmd_kwargs}")
                    log.error(f"kwargs: {kwargs}")
                    print(traceback.format_exc())
                    return
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
    global RAMSESRF_ALLOW_EAVESDROP
    global RAMSESRF_DISABLE_DISCOVERY
    global SCHEMA_FILE

    BASIC_CONFIG = { SZ_CONFIG: {
        SZ_DISABLE_SENDING: False,
        SZ_DISABLE_DISCOVERY: RAMSESRF_DISABLE_DISCOVERY,
        SZ_ENABLE_EAVESDROP: RAMSESRF_ALLOW_EAVESDROP,
        SZ_ENFORCE_KNOWN_LIST: RAMSESRF_KNOWN_LIST and RAMSESRF_DISABLE_DISCOVERY,
        SZ_EVOFW_FLAG: None,
        SZ_MAX_ZONES: 12,
        SZ_USE_ALIASES: True }
    }

    # SZ_SERIAL_PORT: COM_PORT,
    # SZ_ROTATE_BYTES: LOG_FILE_ROTATE_BYTES,  SZ_ROTATE_BACKUPS: LOG_FILE_ROTATE_COUNT,

    lib_kwargs, _ = _proc_kwargs((BASIC_CONFIG, {}), kwargs)

    schema_loaded_from_file = False
    if RAMSESRF_DISABLE_DISCOVERY and SCHEMA_FILE is not None:
        # If we have a ramses_rf schema file (and we are not in eavesdrop mode), use the schema

        if os.path.isfile(SCHEMA_FILE):
            log.info(f"Loading schema from file '{SCHEMA_FILE}'")
            with open(SCHEMA_FILE) as config_schema:
                schema = json.load(config_schema)
                if SZ_SCHEMA in schema and SZ_MAIN_TCS in schema[SZ_SCHEMA] and schema[SZ_SCHEMA][SZ_MAIN_TCS] is None:
                    schema_loaded_from_file = False
                    log.warning(f"The existing schema file '{SCHEMA_FILE}' appears to be invalid. Ignoring...")
                    RAMSESRF_DISABLE_DISCOVERY = False
                else:
                    lib_kwargs.update(schema)
                    if COM_PORT: # override with the one in the main config file
                        lib_kwargs[SZ_CONFIG][SZ_SERIAL_PORT] = COM_PORT
                    log.debug(f"Schema loaded. Updated lib_kwargs: {lib_kwargs}")
                    schema_loaded_from_file = True
        else:
            log.warning(f"The schema file '{SCHEMA_FILE}' was not found'")
            RAMSESRF_DISABLE_DISCOVERY = False

    # If we don't have a schema file, set 'discover' mode (discovered schema saved on exit)
    if not RAMSESRF_DISABLE_DISCOVERY or not schema_loaded_from_file:
        RAMSESRF_DISABLE_DISCOVERY = False
        # Disable known_list, so that we get everything
        if SZ_KNOWN_LIST in lib_kwargs[SZ_CONFIG]:
            del lib_kwargs[SZ_CONFIG][SZ_KNOWN_LIST]
        lib_kwargs[SZ_CONFIG][SZ_ENFORCE_KNOWN_LIST] = False

        log.warning(f"Schema file missing or the 'known_list' section is missing. Defaulting to ramses_rf 'eavesdropping' mode")
        log.debug(f"Using temporary config schema: {json.dumps(lib_kwargs)}")


    # Load local devices file if available. This forms the 'known_list' and also allows for custom naming of devices
    DEVICES = load_json_from_file(DEVICES_FILE)

    # Add this server/gateway as a known device
    DEVICES[HGI_DEVICE_ID] = { SZ_ALIAS : THIS_GATEWAY_NAME}

    # Force discover if we don't have any devices
    if len(DEVICES) <= 1:
        RAMSESRF_DISABLE_DISCOVERY = False

    lib_kwargs[SZ_CONFIG][SZ_DISABLE_DISCOVERY] = RAMSESRF_DISABLE_DISCOVERY
    lib_kwargs[SZ_CONFIG][SZ_DISABLE_SENDING] = RAMSESRF_DISABLE_SENDING

    if RAMSESRF_DISABLE_DISCOVERY and not SZ_KNOWN_LIST in lib_kwargs and DEVICES:
        # Create 'known_list' from DEVICES
        known_list = {SZ_KNOWN_LIST: {HGI_DEVICE_ID: { SZ_ALIAS : THIS_GATEWAY_NAME}}}
        # allowed_list = [{d: {"name": DEVICES[d]["name"]}} for d in DEVICES]
        for d in DEVICES:
            known_list[SZ_KNOWN_LIST][d] = {SZ_ALIAS : DEVICES[d][SZ_ALIAS]}
        lib_kwargs.update(known_list)

    if LOAD_ZONES_FROM_FILE:
        ZONES = load_json_from_file(ZONES_FILE)

    import re
    device_regex = r"^01:[0-9]{6}$"
    for ctl_id, schema in lib_kwargs.items():
        if re.match(device_regex, ctl_id):
            update_zones_from_gwy(schema, {})

    serial_port = lib_kwargs[SZ_CONFIG].pop(SZ_SERIAL_PORT, COM_PORT)

    if lib_kwargs.get(SZ_PACKET_LOG) and isinstance(lib_kwargs[SZ_CONFIG][SZ_PACKET_LOG], dict):
        #f If packet log requirements already in schema, use these
        return serial_port, lib_kwargs
    else:
        # Otherwise set values from config file
        if SZ_PACKET_LOG not in lib_kwargs:
            lib_kwargs[SZ_PACKET_LOG] = {}
        packet_log_dict = lib_kwargs[SZ_PACKET_LOG]

        if PACKET_LOG_FILE:
            packet_log_dict[SZ_FILE_NAME] = PACKET_LOG_FILE

        if LOG_FILE_ROTATE_BYTES and LOG_FILE_ROTATE_BYTES > 0:
            packet_log_dict[SZ_ROTATE_BYTES] = LOG_FILE_ROTATE_BYTES

        if LOG_FILE_ROTATE_COUNT and LOG_FILE_ROTATE_COUNT > 0:
            packet_log_dict[SZ_ROTATE_BACKUPS] = LOG_FILE_ROTATE_COUNT

    return serial_port, lib_kwargs


def show_startup_info(lib_kwargs):
    if DEVICES and len(DEVICES) >1:
        print_formatted_row("", text="")
        print_formatted_row("", text="------------------------------------------------------------------------------------------")
        print_formatted_row("", text=f"{Style.BRIGHT}{Fore.YELLOW}Devices loaded from '{DEVICES_FILE}' file:")

        for key in sorted(DEVICES, key=lambda x: (x is None, x)):
            if key is not None:
                dev_type = DEV_TYPE_MAP[key.split(":")[0]]
                device = GWY.get_device(key) if not "18:" in key else None
                if device:
                    zone_id = device.zone.zone_idx if hasattr(device, "zone") and hasattr(device.zone, SZ_ZONE_IDX) else None
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


async def main(**kwargs):
    serial_port, lib_kwargs = initialise_sys(kwargs)

    global GWY
    GWY = Gateway(serial_port, **lib_kwargs)
    GWY.create_client(process_gwy_message)

    update_devices_from_gwy()
    update_zones_from_gwy()
    mqtt_publish_schema()
    show_startup_info(lib_kwargs)

    try:
        MQTT_CLIENT.loop_start()
        await GWY.start()
        await GWY.pkt_source
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

        if RAMSESRF_ALLOW_EAVESDROP or not RAMSESRF_DISABLE_DISCOVERY:
            print_ramsesrf_gwy_schema(GWY)
            save_schema_and_devices()

    print(msg)
