#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
import asyncio
import json
import sys
import traceback
import re
from typing import Tuple
from signal import SIGINT, SIGTERM
import os,sys
import traceback
import configparser
import paho.mqtt.client as mqtt
import time, datetime
from colorama import init as colorama_init, Fore, Style, Back
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta as td

from evohome_rf import Gateway, GracefulExit
from evohome_rf.command import Command
from evohome_rf.discovery import (
    EXECUTE_CMD,
    GET_FAULTS,
    GET_SCHED,
    SET_SCHED,
    SCAN_DISC,
    SCAN_FULL,
    SCAN_HARD,
    SCAN_XXXX,
    spawn_execute_scripts,
    spawn_monitor_scripts,
)
from evohome_rf.exceptions import EvohomeError
from evohome_rf.helpers import is_valid_dev_id
from evohome_rf.packet import CONSOLE_COLS
from evohome_rf.message import CODE_NAMES as CODE_NAMES
from evohome_rf.discovery import spawn_execute_cmd
from evohome_rf.const import NON_DEVICE_ID, DEVICE_TABLE
from evohome_rf.schema import (
    ALLOW_LIST,
    CONFIG,
    DISABLE_DISCOVERY,
    DISABLE_SENDING,
    DONT_CREATE_MESSAGES,
    ENFORCE_ALLOWLIST,
    EVOFW_FLAG,
    INPUT_FILE,
    PACKET_LOG,
    REDUCE_PROCESSING,
    SERIAL_PORT,
)

LIB_KEYS = (
    INPUT_FILE,
    SERIAL_PORT,
    EVOFW_FLAG,
    PACKET_LOG,
    # "process_level",  # TODO
    REDUCE_PROCESSING,
)

COLORS = {" I": f"{Fore.WHITE}", "RP": f"{Style.BRIGHT}{Fore.CYAN}", "RQ": f"{Fore.CYAN}", 
          " W": f"{Fore.MAGENTA}", "temperature": f"{Fore.YELLOW}","ERROR": f"{Back.RED}{Fore.YELLOW}"}


if  os.path.isdir(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "3.0.0_alpha6"


CONFIG_FILE     = "evogateway.cfg"

config = configparser.RawConfigParser()
config.read(CONFIG_FILE)

# Use json config for multiple com ports if available
COM_PORTS         = config.get("Serial Port", "COM_PORTS", fallback=None)
if COM_PORTS and type(COM_PORTS) == str:
    COM_PORTS = json.loads(COM_PORTS.replace("'", "\""))

# otherwise default to single port
if COM_PORTS is None:
    COM_PORT          = config.get("Serial Port","COM_PORT", fallback="/dev/ttyUSB0")
    COM_BAUD          = config.get("Serial Port","COM_BAUD", fallback=115200)
    COM_RETRY_LIMIT   = config.get("Serial Port","COM_RETRY_LIMIT", fallback=10)
    COM_PORTS = {COM_PORT: {"baud" : COM_BAUD, "retry_limit": COM_RETRY_LIMIT, "is_send_port": True}}

EVENTS_FILE       = config.get("Files", "EVENTS_FILE", fallback="events.log")
PACKET_LOG_FILE   = config.get("Files", "PACKET_LOG_FILE", fallback="packet.log")
DEVICES_FILE      = config.get("Files", "DEVICES_FILE", fallback="devices.json")
NEW_DEVICES_FILE  = config.get("Files", "NEW_DEVICES_FILE", fallback="devices_new.json")
SCHEMA_FILE       = config.get("Files", "SCHEMA_FILE", fallback= None)

MQTT_SERVER       = config.get("MQTT", "MQTT_SERVER", fallback="")                  # Leave blank to disable MQTT publishing. Messages will still be saved in the various files
MQTT_SUB_TOPIC    = config.get("MQTT", "MQTT_SUB_TOPIC", fallback="")               # Note to exclude any trailing '/'
MQTT_PUB_TOPIC    = config.get("MQTT", "MQTT_PUB_TOPIC", fallback="")
MQTT_ZONE_IND_TOPIC= config.get("MQTT", "MQTT_ZONE_INDEP_TOPIC", fallback="_zone_independent") 

MQTT_USER         = config.get("MQTT", "MQTT_USER", fallback="")
MQTT_PW           = config.get("MQTT", "MQTT_PW", fallback="")
MQTT_CLIENTID     = config.get("MQTT", "MQTT_SERVER", fallback="evoGateway")
MQTT_PUB_AS_JSON  = config.getboolean("MQTT", "MQTT_PUB_AS_JSON", fallback=False)
MQTT_GROUP_BY_ZONE= config.getboolean("MQTT", "MQTT_GROUP_BY_ZONE", fallback=True)
CONTROLLER_ID     = config.get("SENDER", "CONTROLLER_ID", fallback="01:139901")

MAX_HISTORY_STACK_LENGTH = config.get("MISC", "MAX_HISTORY_STACK_LENGTH", fallback=10)

THIS_GATEWAY_ID   = config.get("SENDER", "THIS_GATEWAY_ID", fallback="18:000730") # TODO! This is now hardcoded into evofw3, so no point in making configurable???
THIS_GATEWAY_NAME = config.get("SENDER", "THIS_GATEWAY_NAME", fallback="EvoGateway")
THIS_GATEWAY_TYPE_ID = THIS_GATEWAY_ID.split(":")[0]

GATEWAY_DISABLE_SENDING = config.getboolean("SENDER", "DISABLE_SENDING", fallback=False)

# COMMAND_RESEND_TIMEOUT_SECS = float(getConfig(config,"SENDER", "COMMAND_RESEND_TIMEOUT_SECS", 60.0))
# COMMAND_RESEND_ATTEMPTS= int(getConfig(config,"SENDER", "COMMAND_RESEND_ATTEMPTS", 3))    # A value of zero also disables waiting for sent command acknowledgements
# AUTO_RESET_PORTS_ON_FAILURE = getConfig(config,"SENDER", "AUTO_RESET_PORTS_ON_FAILURE", "False").lower() == "true"

DISPLAY_FULL_JSON = config.getboolean("MISC", "DISPLAY_FULL_JSON", fallback=False)
MAX_LOG_HISTORY   = config.get("SENDER", "MAX_LOG_HISTORY", fallback=3)
MIN_ROW_LENGTH    = config.get("MISC", "MIN_ROW_LENGTH", fallback=160)


EMPTY_DEVICE_ID   = "--:------"

SYS_CONFIG_COMMAND = "sys_config"
# RESET_COM_PORTS   = "reset_com_ports"
# CANCEL_SEND_COMMANDS ="cancel_commands"

SYSTEM_MSG_TAG = "*"

CONTROLLER_MODES = {0: "Auto", 1: "Heating Off", 2: "Eco-Auto", 3: "Away", 4: "Day Off", 7:"Custom"} # 0=auto, 1= off, 2=eco, 4 = day off, 7 = custom


# -----------------------------------
DEVICES = {}
ZONES = {}
MQTT_CLIENT = None
GWY = None
# -----------------------------------

log = logging.getLogger(f"evogateway_log")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(lineno)s] %(message)s')
# %(funcName)20s() [%(levelname)s]

# Log file handler
file_handler = RotatingFileHandler(EVENTS_FILE, maxBytes=1000000, backupCount=MAX_LOG_HISTORY)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# Log console handler 
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)




class MQTTCommand():
    ''' Object to hold details of command received via MQTT to be sent to evohome controller.'''
    def __init__(self, command_code=None, command_name=None, destination=None, args=None, serial_port=-1, send_mode="I", instruction=None):
        self.command_code = command_code
        self.command_name = command_name
        self.destination = destination
        self.args = args
        self.arg_desc = "[]"
        self.send_mode = send_mode
        self.send_string = None
        self.send_dtm = None
        self.retry_dtm = None
        self.retries = 0
        self.send_failed = False
        self.wait_for_ack = False
        self.reset_ports_on_fail = False
        self.send_acknowledged = False
        self.send_acknowledged_dtm = None
        self.dev1 = None
        self.dev2 = None
        self.dev3 = None
        self.payload = ""
        self.command_instruction = instruction


    def payload_length(self):
        if self.payload:
            return int(len(self.payload)/2)
        else:
            return 0


_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')
def to_snake(name):
  name=name.strip().replace("'","").replace(" ","_")
  s1 = _first_cap_re.sub(r'\1_\2', name)
  s2 = _all_cap_re.sub(r'\1_\2', s1).lower()
  return s2.replace("__","_")


def _proc_kwargs(obj, kwargs) -> Tuple[dict, dict]:
    lib_kwargs, cli_kwargs = obj
    lib_kwargs[CONFIG].update({k: v for k, v in kwargs.items() if k in LIB_KEYS})
    cli_kwargs.update({k: v for k, v in kwargs.items() if k not in LIB_KEYS})
    return lib_kwargs, cli_kwargs


def get_device_name(device_address):
    try:
        if device_address.id == THIS_GATEWAY_ID:
            name = THIS_GATEWAY_NAME
        elif device_address.type in "01":
            name = "Controller"
        elif device_address.type in "63":
            name = "UNBOUND"
        else:
            name = DEVICES[device_address.id]["name"] if device_address.id in DEVICES else device_address.id            
        if name == NON_DEVICE_ID: 
            name = ""
        dev_type = DEVICE_TABLE[device_address.type]["type"].replace("---", "").replace("NUL", "")
        name = "{} {}".format(dev_type, name).strip()
        return name

    except Exception as ex:
        log.error("{Style.BRIGHT}{COLORS.get('ERROR')}Exception occured", exc_info=True)


def get_msg_zone_name(device_id, target_zone=None):
    """ Use any 'target' zone name given in the payload, otherwise fall back to sending device zone"""    

    if target_zone and target_zone > 0:
        # Target of the message is a zone, so use that
        zone_name = ZONES[target_zone] if target_zone in ZONES else "_zone_{}".format(target_zone)
    else:
        src_zone_id = DEVICES[device_id]["zoneId"] if device_id in DEVICES else -1
        if device_id[:2] == "01":
            zone_name = MQTT_ZONE_IND_TOPIC
        elif src_zone_id > 12:
            zone_name = f"{MQTT_ZONE_IND_TOPIC}/relays"
        elif src_zone_id > 0 and src_zone_id in ZONES:            
            zone_name = ZONES[src_zone_id] 
        else:
            zone_name = MQTT_ZONE_IND_TOPIC
    return zone_name


def get_msg_target_zone_id(data):
    # data must be a dict
    zone_id = -1
    if "ufh_idx" in str(data):
        log.debug(f"        ->1. Found ufh_idx in data: {data}")

    if isinstance(data, dict) or isinstance(data, list):
        if "ufh_idx" in data:
            zone_ids = [DEVICES[d]["zoneId"] for d in DEVICES if "ufh_zoneId" in DEVICES[d] and DEVICES[d]["ufh_zoneId"] == int(data["ufh_idx"])]
            log.debug(f"        -> 2. Processed ufh_idx : data[ufh_idx]: {data['ufh_idx']}; zone_ids: {zone_ids}")
            zone_id = zone_ids[0] if len(zone_ids) > 0 else -1
        elif "zone_idx" in data:
            zone_id = int(data["zone_idx"], 16) + 1 if "zone_idx" in data else -1
    else:
        raise TypeError(f"argument must be a dict but instead a {type(data)} was sent with value: {data}").with_traceback(tracebackobj)

    return zone_id
        

def get_opentherm_msg(msg):
    if msg.code_name == "opentherm_msg":       
        name = msg.payload.get("msg_name", None)
        if name:
            # return the whole payload dict as we don't know which specific message component is of interest
            return name, {name: msg.payload}
    else:
        log.error(f"Invalid opentherm_msg. msg.code_name: {msg.code_name}")
    return None, None


def get_system_fault_msg(msg):
    if msg.code_name == "system_fault":       
        name = msg.payload.get("log_idx", None)
        if name:
            return name, {name: msg.payload}
    else:
        log.error(f"Not a system_fault msg. msg.code_name: {msg.code_name}")
    return None, None


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


def process_gwy_message(msg) -> None:
    """ Process received message from Gateway """

    log.debug("") # spacer, as we have other debug entries for a given received msg
    log.info(msg)  # Log event to file

    # Message class in evohome_rf lib does not have the code name, so add it
    msg.code_name = CODE_NAMES[msg.code]
    
    if DISPLAY_FULL_JSON: 
        display_full_msg(msg)        
        
    # As some payloads are arrays, and others not, make consistent
    payload = [msg.payload] if not isinstance(msg.payload, list) else msg.payload               

    for item in payload:
        # evohome_rf library sends each item as a dict
        try:
            if type(item) != dict: 
                log.warn(f"        -> process_gwy_message: 0. item is not dict. type(item): {type(item)}, item: {item}, type(payload): {type(payload)}")
                log.warn(f"        -> process_gwy_message:    msg.payload: {payload}")
            if not DISPLAY_FULL_JSON: 
                zone_id = get_msg_target_zone_id(item)
                display_simple_msg(msg, item, zone_id, "")
                log.debug(f"        -> process_gwy_message: 1. type(item): {type(item)}, item: {item} ")
            mqtt_publish(msg, item)

        except Exception as e:
            log.error(f"Exception occured: {e}", exc_info=True)            
            log.error(f"item: {item}, payload: {payload} ")    
            log.error(f"msg: {msg}")        


def display_full_msg(msg):
    """ Show the full json payload (as in the evohome_rf cli client) """
    dtm = f"{msg.dtm:%H:%M:%S.%f}"[:-3]
    if msg.src.type == "18":
        print(f"{Style.BRIGHT}{COLORS.get(msg.verb)}{dtm} {msg}"[:CONSOLE_COLS])
    elif msg.verb:
        print(f"{COLORS.get(msg.verb)}{dtm} {msg}"[:CONSOLE_COLS])
    else:
        print(f"{Style.RESET_ALL}{dtm} {msg}"[:CONSOLE_COLS])


def display_simple_msg(msg, payload_dict, target_zone=-1, suffix_text=""):    
    src = get_device_name(msg.src)
    dst = get_device_name(msg.dst) if msg.src.id != msg.dst.id else ""

    # Make a copy as we are deleting elements from the displayed text
    display_text = payload_dict.copy() if isinstance(payload_dict, dict) else payload_dict 
    filtered_text = cleanup_display_text(msg, display_text)
    try:        
        zone_name = "@ {:<20}".format(ZONES[target_zone]) if target_zone > 0 and target_zone in ZONES else ""
        zone_id = "[Zone {:<3}]".format(target_zone) if target_zone > 0 else ""

        # display_row = f"{msg.verb.strip():<2}| {src:<22} -> {dst:<22} | {filtered_text} {zone_name:<25} {zone_id} {suffix_text}{Style.RESET_ALL} "
        display_row = f"{msg.verb.strip():<2}| {src:<22} -> {dst:<22} | {filtered_text} {zone_name:<30} {zone_id} {suffix_text}{Style.RESET_ALL} "
        display_row = display_row.replace('\n', ' ').replace('\r', '') # carriage returns appear to slip in for some messages

        if msg.src.type == "18":
            style_prefix = f"{Style.BRIGHT}{Fore.MAGENTA}"
        elif msg.code_name in "temperature dhw_temp" :
            style_prefix = f"{COLORS.get('temperature')}"
        elif msg.verb:
            style_prefix = f"{COLORS.get(msg.verb)}"
        else:
            style_prefix = f"{Style.RESET_ALL}"
        
        print_formatted_row(src, dst, msg.verb, msg.code_name, f"{filtered_text} {zone_name:<25} {zone_id} {suffix_text}", msg.rssi, style_prefix)          

    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"type(display_text): {type(display_text)}")
        log.error(f"Display row: {msg.verb}| {src} -> {dst} | {display_text} {zone_name} [Zone {target_zone}] {suffix_text}")


def print_formatted_row(src="", dst="", verb="", cmd="", text="", rssi="   ", style_prefix=""):
    dtm = datetime.datetime.now().strftime("%Y-%m-%d %X")
    formatted_dst = f"{dst}" if dst else ""
    if src:
        row = f"{dtm} |{rssi}| {src:<21} -> {dst:<21} |{verb:<2}| {cmd:<15} | {text}"
    else:
        row = f"{dtm} | {text}"
    row = "{:<{min_row_width}}".format(row, min_row_width=MIN_ROW_LENGTH)        
    print(f"{Style.RESET_ALL}{style_prefix}{row.strip()}{Style.RESET_ALL}")
    

def rq_callback(msg) -> None:
    print(f"-----------------> rq_callback: {msg}")

def rp_callback(msg) -> None:
    print(f"=================> rp_callback: {msg}")



def mqtt_initialise():
    if not MQTT_SERVER:
        log.error("MQTT Server details not found. Exiting...")
        raise SystemExit
    
    global MQTT_CLIENT
    MQTT_CLIENT = mqtt.Client()
    MQTT_CLIENT.on_connect = mqtt_on_connect
    MQTT_CLIENT.on_message = mqtt_on_message

    if MQTT_USER:
        MQTT_CLIENT.username_pw_set(MQTT_USER, MQTT_PW)
    MQTT_CLIENT.connect(MQTT_SERVER)
    
    return MQTT_CLIENT


def mqtt_on_connect(client, *_):
    log.info(f"Connected to MQTT broker. Subscribing to topic {MQTT_SUB_TOPIC} for commands")
    client.subscribe(MQTT_SUB_TOPIC)


def mqtt_on_message(client, _, msg):
    payload = str(msg.payload.decode("utf-8"))
    print_formatted_row("MQTT", text=f"Received MQTT message: {payload}", style_prefix=f"{Fore.GREEN}")        
    mqtt_process_msg(payload)


def mqtt_publish(msg, payload):
    """ We explicitly receive the payload instead of just using msg.payload, so that any pre-processing of the payload is assumed to be already done
        Payloads are assumed to always be dict
    """

    if not (MQTT_CLIENT and msg and (not MQTT_PUB_AS_JSON or payload)):
        return

    if not MQTT_CLIENT.is_connected:
        print_formatted_row(SYSTEM_MSG_TAG, text="[WARN] MQTT publish failed as client is not connected to broker")
        return

    if not isinstance(payload, dict):
        log.error(f"Payload in mqtt_publish is not of type dict. type(payload): {type(payload)}, payload arg: {payload}, msg.payload: {msg.payload}")

    try:
        target_zone_id = get_msg_target_zone_id(payload)        
        src_zone = to_snake(get_msg_zone_name(msg.src.id, target_zone_id)) #if not target_zone_id or target_zone_id <1 else get_device_zone_name(target_zone_id)
        src_device = to_snake(get_device_name(msg.src))

        if MQTT_GROUP_BY_ZONE and src_zone:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_zone}/{src_device}/{msg.code_name}"
        else:
            topic_base = f"{MQTT_PUB_TOPIC}/{src_device}/{msg.code_name}"
        
        subtopic = topic_base        
        if not MQTT_PUB_AS_JSON:
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

            log.debug(f"        -> mqtt_publish: 0. updated_payload: {updated_payload}, type(updated_payload): {type(updated_payload)}, new_key: {new_key}")
            log.debug(f"        -> mqtt_publish:    payload: {payload}")
            
            # Iterate through the list. payload_item should be a dict as updated_payload should now be a list of dict [{...}]
            for payload_item in updated_payload:                
                log.debug(f"        -> mqtt_publish: 1. payload_item: {payload_item}, type: {type(payload_item)}")
                log.debug(f"        -> mqtt_publish:    updated_payload: {updated_payload}")
                try:
                    if isinstance(payload_item, dict): # we may have a further dict in the updated_payload - e.g. opentherm msg, system_fault etc
                        for k in payload_item:
                            MQTT_CLIENT.publish(f"{subtopic}/{to_snake(k)}", str(payload_item[k]), 0, True)                
                            log.debug(f"        -> mqtt_publish: 2. Posted subtopic: {subtopic}/{to_snake(k)}, value: {payload_item[k]}")
                    else:
                        MQTT_CLIENT.publish(subtopic, str(payload_item), 0, True)        
                        log.info(f"        -> mqtt_publish: 3. item is not a dict. Posted subtopic: {subtopic}, value: {payload_item}, type(playload_item): {type(payload_item)}")
                except Exception as e:
                    log.error(f"Exception occured: {e}", exc_info=True)
                    log.error(f"------------> payload_item: \"{payload_item}\", type(payload_item): \"{type(payload_item)}\", updated_payload: \"{updated_payload}\"")
                    log.error(f"------------> msg: {msg}")                
        else:
            MQTT_CLIENT.publish(subtopic, json.dumps(msg.payload), 0, True)
                
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%XZ")        
        MQTT_CLIENT.publish(f"{topic_base}/{msg.code_name}_ts", timestamp, 0, True)
        # print("published to mqtt topic {}: {}".format(topic, msg))
    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"msg.src.id: {msg.src.id}, command: {msg.code_name}, payload: {payload}, pub_json: {MQTT_PUB_AS_JSON}")
        log.error(f"msg: {msg}")
        
        traceback.print_exc()
        pass


def mqtt_publish_schema():
    topic = f"{MQTT_PUB_TOPIC}/{MQTT_ZONE_IND_TOPIC}/_gateway_config"
    MQTT_CLIENT.publish(f"{topic}/schema", json.dumps(GWY.schema), 0, True)
    MQTT_CLIENT.publish(f"{topic}/params", json.dumps(GWY.params), 0, True)
    MQTT_CLIENT.publish(f"{topic}/status", json.dumps(GWY.status), 0, True)
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%XZ")        
    MQTT_CLIENT.publish(f"{topic}/_gateway_config_ts", timestamp, 0, True)


def mqtt_process_msg(payload):
    try:
        log.debug(f"MQTT message received: {payload}")
        json_data = json.loads(payload)        

        if SYS_CONFIG_COMMAND in json_data:
            if json_data[SYS_CONFIG_COMMAND].upper().strip() == "DISPLAY_FULL_JSON":
                global DISPLAY_FULL_JSON
                DISPLAY_FULL_JSON = json_data["value"] if "value" in json_data else False
                
            elif json_data[SYS_CONFIG_COMMAND].upper().strip() == "POST_SCHEMA":
                mqtt_publish_schema()
            elif json_data[SYS_CONFIG_COMMAND] == CANCEL_SEND_COMMANDS:
                send_queue = []
                last_sent_command = None
                print_formatted_row(SYSTEM_MSG_TAG,  text="Cancelled all queued outbound commands")
                return
            else:
                print_formatted_row(SYSTEM_MSG_TAG,  text="System configuration command '{}' not recognised".format(json_data[SYS_CONFIG_COMMAND]))
                return
        else:
            command_name = json_data["command"] if "command" in json_data else None
            command_code = json_data["command_code"] if "command_code" in json_data else None
            if command_code:
                if type(command_code) is int:
                    command_code = hex(command_code)
                    command_code = command_code.upper().replace("0X","")
            if command_name or command_code:
                args = json_data["arguments"] if "arguments" in json_data else {}
                send_mode = json_data["send_mode"] if "send_mode" in json_data else None

            new_command = MQTTCommand(command_code=command_code, command_name=command_name, args=args, send_mode=send_mode, instruction=json.dumps(json_data))    
            # new_command.wait_for_ack = json_data["wait_for_ack"] if "wait_for_ack" in json_data else COMMAND_RESEND_ATTEMPTS > 0
            # new_command.reset_ports_on_fail = json_data["reset_ports_on_fail"] if "reset_ports_on_fail" in json_data else AUTO_RESET_PORTS_ON_FAILURE

            updated_command = build_send_string(new_command)
            log.debug(f"MQTT message received: {payload}. Converted to command string: {updated_command.send_string}")
            
            cmd_args = {EXECUTE_CMD : updated_command.send_string }                    
            cb_rq = {"func": rq_callback, "timeout": 10}
            gw_cmd = Command(updated_command.send_mode, updated_command.dev2, updated_command.command_code, updated_command.payload, callback=cb_rq)
            # print(f"-----------> gw_cmd: {gw_cmd}")

            cb_rp = {"func": rp_callback, "timeout": 10}   
            try:         
                # kwargs = {"timeout": 8}
                kwargs = {}
                resp = asyncio.run(GWY.async_send_cmd(gw_cmd, **kwargs))
                print(f"-----------> async resp: {resp}")

            except TimeoutError:
                log.warning(f"-----------> command failed due to timed out")
            
            # GWY.send_cmd(gw_cmd, callback=rp_callback)
            # GWY.send_data(Command("RQ", "01:139901", "313F", "00"))
            # spawn_execute_cmd(GWY, **cmd_args)
    except Exception as e:
        log.error(f"Exception occurred in process mqtt msg:{payload}\n{e}", exc_info=True)
        return


def build_send_string(command):
    ''' Build the send string for GWY'''

    if not command.command_code and not command.command_name:
        print_formatted_row("ERROR", text="Cannot process command without valid command_code ({}) or command_name ({}) [args: '{}']".format(
            command.command_code, command.command_name, args))
        return

    # Check and do system config commands first
    if command.command_code == SYS_CONFIG_COMMAND:
        # if command.command_name == RESET_COM_PORTS:
        #     reset_com_ports()
        # else:
        print_formatted_row(SYSTEM_MSG_TAG,  text="System configuration command '{}' not recognised".format(command.command_name))
        return # Either way, we return. Rest of the fn is processing actual evohome commands
    elif command.command_name == "ping":
        command.command_name = "datetime"
        
    if not command.command_name: # command_name takes priority over command_code        
        if command.command_code in CODE_NAMES:
            command.command_name = CODE_NAMES[command.command_code]
        else:
            print_formatted_row("DEBUG",  text="Command name not found for code '{}'".format(command.command_code))
    
    if command.command_name and not command.command_code:
        # Command must be an evohome one. Process and send.    
        codes_array = [k for k in CODE_NAMES if CODE_NAMES[k] == command.command_name]
        command.command_code = codes_array[0] if len(codes_array) > 0 else None       

    if not command.command_code or command.command_code == "0000":
        print_formatted_row("ERROR",  text="Unrecognised command.command_name '{}'".format(command.command_name))
        return

    send_string = ""
    try:
        if "payload" not in command.args or not command.command_code:
            if (command.command_name and command.command_name == "dhw_mode") or (command.command_code and command.command_code == "1F41"):
                # 1F41: Change dhw mode
                state_id = command.args["state_id"]
                until = command.args["until"] if "until" in command.args else None
                mode_id = command.args["mode_id"] if "mode_id" in command.args else -1
                command.payload = get_dhw_state_payload(state_id, until, mode_id)
                if command.send_mode is None:
                    command.send_mode = "W"
                if until:
                    command.arg_desc ="[{} until {}]".format("ON" if state_id == 1 else "OFF", until)
                else:
                    command.arg_desc ="[{}]".format("ON" if state_id == 1 else "OFF")

            elif (command.command_name and command.command_name in "datetime ping") or (command.command_code and command.command_code == "313F"):
                # 0x313F: Send a datetime update request, i.e. like a ping
                command.payload = "00"
                if command.send_mode is None:
                    command.send_mode = "RQ"

            elif command.command_name and command.command_name in "system_fault":
                # Default to getting last log entry           
                command.payload = "000000"
                command.send_mode = "RQ"

            elif (command.command_name and command.command_name == "system_mode") or (command.command_code and command.command_code == "2E04"):
                # 0x2E04: Set controller mode
                mode = command.args["mode"]
                until = command.args["until"] if "until" in command.args else None
                command.payload = get_controller_mode_payload(mode, until)

                # Send mode needs to be 'W' to set the controller to the new controller mode
                if command.send_mode is None:
                    command.send_mode = "W"
                if until:
                    command.arg_desc = "[{} until {}]".format(mode, until)
                else:
                    command.arg_desc = mode

            elif (command.command_name and command.command_name == "zone_mode") or (command.command_code and command.command_code == "2349"):
                # 0x2349: Setpoint override
                zone_id = command.args["zone_id"]
                setpoint = command.args["setpoint"]
                until = command.args["until"] if "until" in command.args else None
                mode = command.args["mode"] if "mode" in command.args else None
                command.payload = get_setpoint_override_payload(zone_id, setpoint, until, mode)
                if command.send_mode is None:
                    command.send_mode = "W"
                if until:
                    command.arg_desc = "['{}': {} degC until {}]".format(zones[zone_id] if zone_id in zones else zone_id, setpoint, until)
                else:
                    command.arg_desc = command.arg_desc = "['{}': {} deg C]".format(zones[zone_id] if zone_id in zones else zone_id, setpoint)
            else:
                if not command.send_mode: # default to RQ
                    command.send_mode = "RQ"
        else:
            command.payload = command.args["payload"]
            if command.send_mode is None:
                command.send_mode = "I"

        command.dev1 = command.args["dev1"] if "dev1" in command.args else THIS_GATEWAY_ID
        command.dev2 = command.args["dev2"] if "dev2" in command.args else CONTROLLER_ID
        command.dev3 = command.args["dev3"] if "dev3" in command.args else EMPTY_DEVICE_ID

        command.destination = command.dev2
        # command.send_string = "{} --- {} {} {} {:<4} {:03d} {}".format(command.send_mode, command.dev1,
        #                       command.dev2, command.dev3, command.command_code, command.payload_length(), command.payload)

        command.send_string = f"{command.send_mode} {command.dev2} {command.command_code:<4} {command.payload}"
        log.debug("Send command string: {}".format(command.send_string))

        return command

    except Exception as e:
        log.error(f"Exception occured: {e}", exc_info=True)
        log.error(f"command: {command}, command_code: {command.command_code}, command_name: {command.command_name}")
        traceback.print_exc()


def initialise_sys(kwargs):

    mqtt_initialise()

    global DEVICES
    global ZONES

    if os.path.isfile(DEVICES_FILE):
        with open(DEVICES_FILE, 'r') as fp:
            DEVICES = json.load(fp)             # Get a list of known devices, ideally with their zone details etc
    
    
    # Add this server/gateway as a device, but using dummy zone ID for now
    DEVICES[THIS_GATEWAY_ID] = { "name" : THIS_GATEWAY_NAME, "zoneId": -1, "zoneMaster": True }

    send_queue = []
    send_queue_size_displayed = 0         # Used to track if we've shown the queue size recently or not

    for d in DEVICES:
        if DEVICES[d]['zoneMaster']:
            ZONES[DEVICES[d]["zoneId"]] = DEVICES[d]["name"]
        # generate the mqtt topic for the device (using Homie convention)

    print_formatted_row('', text='')
    print_formatted_row('', text='-----------------------------------------------------------')
    print_formatted_row('', text=f"{Style.BRIGHT}{Fore.BLUE}Devices loaded from '{DEVICES_FILE}' file:")
    for key in sorted(DEVICES):
        zm = " [Master]" if DEVICES[key]['zoneMaster'] else ""
        # print_formatted_row('','   ' + key + " - " + '{0: <22}'.format(DEVICES[key]['name']) + " - Zone " + '{0: <3}'.format(DEVICES[key]["zoneId"]) + zm )
        print_formatted_row('', text=f'{Style.BRIGHT}{Fore.BLUE}   {key} - {DEVICES[key]["name"]:<23} - Zone {DEVICES[key]["zoneId"]:<3}{zm}')


    print_formatted_row('', text='-----------------------------------------------------------')
    print_formatted_row('', text='')

    lib_kwargs, _ = _proc_kwargs(({CONFIG: {}}, {}), kwargs)
    
    if SCHEMA_FILE is not None:
        with open(SCHEMA_FILE) as config_schema:
            lib_kwargs.update(json.load(config_schema))
        if COM_PORT: # override with the one in the main config file
            lib_kwargs[CONFIG][SERIAL_PORT] = COM_PORT
    else:        
        # evohome_rf schema file not found. Build a skeleton schema from evogateway config file
        schema = {"config": { "disable_sending": False, "disable_discovery": False,"enforce_allowlist": None,"enforce_blocklist": None,
                "evofw_flag": None, "max_zones": 12, "packet_log": PACKET_LOG_FILE,"serial_port": COM_PORT, "use_names": True, "use_schema": True},
                "schema" : { "controller": CONTROLLER_ID}}
        lib_kwargs.update(schema)

        if DEVICES:
            allowlist = {"allowlist": {}}
            # allowed_list = [{d: {"name": DEVICES[d]["name"]}} for d in DEVICES]
            for d in DEVICES:
                allowlist["allowlist"][d] = {"name" : DEVICES[d]["name"]}    
            lib_kwargs.update(allowlist)

        log.debug(f"Auto generated config schema: {json.dumps(lib_kwargs)}")

    lib_kwargs[CONFIG][DISABLE_SENDING] = GATEWAY_DISABLE_SENDING

    return lib_kwargs
    

async def main(**kwargs):    

    lib_kwargs = initialise_sys(kwargs)
    
    global GWY
    GWY = Gateway(lib_kwargs[CONFIG].pop(SERIAL_PORT, COM_PORT), **lib_kwargs)
    protocol, _ = GWY.create_client(process_gwy_message)    
    mqtt_publish_schema()
    
    try:  
        MQTT_CLIENT.loop_start()
        tasks = asyncio.create_task(GWY.start())

        await tasks

    except asyncio.CancelledError:
        msg = " - ended via: CancelledError (e.g. SIGINT)"
    except GracefulExit:
        msg = " - ended via: GracefulExit"
    except KeyboardInterrupt:
        msg = " - ended via: KeyboardInterrupt"
    except EvohomeError as err:
        msg = f" - ended via: EvohomeError: {err}"
    else:  # if no Exceptions raised, e.g. EOF when parsing
        msg = " - ended without error (e.g. EOF)"
    
    MQTT_CLIENT.loop_stop()
    
   

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except asyncio.CancelledError:
        msg = " - ended via: CancelledError (e.g. SIGINT)"
    except GracefulExit:
        msg = " - ended via: GracefulExit"
    except KeyboardInterrupt:
        msg = " - ended via: KeyboardInterrupt"
    except EvohomeError as err:
        msg = f" - ended via: EvohomeError: {err}"
    else:  # if no Exceptions raised, e.g. EOF when parsing
        msg = " - ended without error (e.g. EOF)"
