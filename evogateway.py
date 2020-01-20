# -*- coding: utf-8 -*-
#!/usr/bin/python
# evohome Listener/Sender
# Copyright (c) 2019 SMAR info@smar.co.uk
#
# Tested with Python 2.7.12. Requires:
# - pyserial (python -m pip install pyserial)
# - paho (pip install paho-mqtt)
#
# Simple evohome 'listener' and 'sender', for listening in and sending messages between evohome devices using an arudino + CC1101 radio receiver
# (other hardware options also possible - see credits below).
# Messages are interpreted and then posted to an mqtt broker if an MQTT broker is defined in the configuration. Similary, sending commands over the
# radio network are initiated via an mqtt 'send' topic, and 'send' status updates posted back to an mqtt topic.
#
# CREDITS:
# Code here is significntly based on the Domitcz source, specifically the EvohomeRadio.cpp file, by
# fulltalgoRythm - https://github.com/domoticz/domoticz/blob/development/hardware/EvohomeRadio.cpp
# Also see http://www.automatedhome.co.uk/vbulletin/showthread.php?5085-My-HGI80-equivalent-Domoticz-setup-without-HGI80
# for info and discussions on homebrew hardware options.
#
# Details on the evohome protocol can be found here: https://github.com/Evsdd/The-Evohome-Protocol/wiki
#
# The arduino nano I am using is running a firmware modded by ghoti57 available
# from https://github.com/ghoti57/evofw2, who had forked it from
# codeaholics, https://github.com/Evsdd, who in turn had forked it  from
# fulltalgoRythm's orignal firmware, https://github.com/fullTalgoRythm/EvohomeWirelessFW.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import os,sys
import traceback
import ConfigParser
import paho.mqtt.client as mqtt
import re
import serial
import time, datetime
import signal
import json
import re
from collections import namedtuple, deque
from enum import Enum, IntEnum

if  os.path.isdir(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "1.9.6"
CONFIG_FILE     = "evogateway.cfg"

# --- Configs/Default
def getConfig(config,section,name,default):
    if config.has_option(section,name):
        return config.get(section,name)
    else:
        return default


# Get any configuration overrides that may be defined in  CONFIG_FILE
# If override not specified, then use the defaults here

config = ConfigParser.RawConfigParser()
config.read(CONFIG_FILE)

# Use json config for multiple com ports if available
COM_PORTS         = getConfig(config,"Serial Port", "COM_PORTS", None)
if COM_PORTS and type(COM_PORTS) == str:
  COM_PORTS = json.loads(COM_PORTS.replace("'", "\""))

# otherwise default to single port
if COM_PORTS is None:
  COM_PORT          = getConfig(config,"Serial Port","COM_PORT","/dev/ttyUSB0")
  COM_BAUD          = int(getConfig(config,"Serial Port","COM_BAUD",115200))
  COM_RETRY_LIMIT   = int(getConfig(config,"Serial Port","COM_RETRY_LIMIT",10))
  COM_PORTS = {COM_PORT: {"baud" : COM_BAUD, "retry_limit": COM_RETRY_LIMIT, "is_send_port": True}}

EVENTS_FILE       = getConfig(config,"Files", "EVENTS_FILE", "events.log")
LOG_FILE          = getConfig(config,"Files", "LOG_FILE", "evogateway.log")
DEVICES_FILE      = getConfig(config,"Files", "DEVICES_FILE", "devices.json")
NEW_DEVICES_FILE  = getConfig(config,"Files", "NEW_DEVICES_FILE", "devices_new.json")

LOG_DROPPED_PACKETS = getConfig(config,"Other", "LOG_DROPPED_PACKETS", False)

MQTT_SERVER       = getConfig(config,"MQTT", "MQTT_SERVER", "")                  # Leave blank to disable MQTT publishing. Messages will still be saved in the various files
MQTT_SUB_TOPIC    = getConfig(config,"MQTT", "MQTT_SUB_TOPIC", "")               # Note to exclude any trailing '/'
MQTT_PUB_TOPIC    = getConfig(config,"MQTT", "MQTT_PUB_TOPIC", "")
MQTT_USER         = getConfig(config,"MQTT", "MQTT_USER", "")
MQTT_PW           = getConfig(config,"MQTT", "MQTT_PW", "")
MQTT_CLIENTID     = getConfig(config,"MQTT", "MQTT_SERVER", "evoGateway")

CONTROLLER_ID     = getConfig(config,"SENDER", "CONTROLLER_ID", "01:139901")
THIS_GATEWAY_ID   = getConfig(config,"SENDER", "THIS_GATEWAY_ID","30:999999")
THIS_GATEWAY_NAME = getConfig(config,"SENDER", "THIS_GATEWAY_NAME","EvoGateway")
THIS_GATEWAY_TYPE_ID = THIS_GATEWAY_ID.split(":")[0]

COMMAND_RESEND_TIMEOUT_SECS = float(getConfig(config,"SENDER", "COMMAND_RESEND_TIMEOUT_SECS",60.0))
COMMAND_RESEND_ATTEMPTS= int(getConfig(config,"SENDER", "COMMAND_RESEND_ATTEMPTS",3))
AUTO_RESET_PORTS_ON_FAILURE = getConfig(config,"SENDER", "AUTO_RESET_PORTS_ON_FAILURE", False)

MAX_LOG_HISTORY   = getConfig(config,"SENDER", "MAX_LOG_HISTORY",3)

MAX_HISTORY_STACK_LENGTH = 5
EMPTY_DEVICE_ID   = "--:------"

SYS_CONFIG_COMMAND = "sys_config"
RESET_COM_PORTS   = "reset_com_ports"
CANCEL_SEND_COMMANDS ="cancel_commands"

SYSTEM_MSG_TAG = "*"
#----------------------------------------
class TwoWayDict(dict):
    def __len__(self):
        return dict.__len__(self) / 2
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)

DEVICE_TYPE = TwoWayDict()
DEVICE_TYPE["01"] = "CTL"  # Main evohome touchscreen controller
DEVICE_TYPE["02"] = "UFH"  # Underfloor controller, HCC80R or HCE80
DEVICE_TYPE["03"] = "STAT" # Wireless thermostat -  HCW82
DEVICE_TYPE["04"] = "TRV"  # Radiator TRVs, e.g. HR92
DEVICE_TYPE["07"] = "DHW"  # Hotwater wireless Sender
DEVICE_TYPE["13"] = "BDR"  # BDR relays
DEVICE_TYPE["30"] = "GWAY" # Mobile Gateway such as RGS100
DEVICE_TYPE["34"] = "STAT" # Wireless round thermostats T87RF2033 or part of Y87RF2024 


CONTROLLER_MODES = {0: "Auto", 1: "Heating Off", 2: "Eco-Auto", 3: "Away", 4: "Day Off", 7:"Custom"} # 0=auto, 1= off, 2=eco, 4 = day off, 7 = custom

# --- Classes
class Message():
  ''' Object to hold details of interpreted (received) message. '''
  def __init__(self,rawmsg):
    self.rawmsg       = rawmsg.strip()
    self.source_id    = rawmsg[11:20]

    self.msg_type     = rawmsg[4:6].strip()
    self.source       = rawmsg[11:20]               # device 1 - This looks as if this is always the source; Note this is overwritten with name of device
    self.source_type  = rawmsg[11:13]               # the first 2 digits seem to be identifier for type of device
    self.source_name  = self.source
    self.device2      = rawmsg[21:30]               # device 2 - Requests (RQ), Responses (RP) and Write (W) seem to be sent to device 2 only
    self.device2_type = rawmsg[21:23]               # device 2 type
    self.device2_name = self.device2
    self.device3      = rawmsg[31:40]               # device 3 - Information (I) is to device 3. Broadcast messages have device 1 and 3 are the same
    self.device3_type = rawmsg[31:33]               # device 3 type
    self.device3_name = self.device3

    if self.device2 == EMPTY_DEVICE_ID:
        self.destination = self.device3
        self.destination_type = self.device3_type
    else:
        self.destination = self.device2
        self.destination_type = self.device2_type
    self.destination_name = self.destination
    self._initialise_device_names()

    self.command_code = rawmsg[41:45].upper()       # command code hex
    self.command_name = self.command_code           # needs to be assigned outside, as we are doing all the processing outside of this class/struct
    try:
        self.payload_length = int(rawmsg[46:49])          # Note this is not HEX...
    except Exception as e:
        print ("Error instantiating Message class on line '{}': {}. Raw msg: '{}'. length = {}".format(sys.exc_info()[-1].tb_lineno, str(e), rawmsg, len(rawmsg)))
        self.payload_length = 0

    self.payload      = rawmsg[50:]
    self.port         = None
    self.failed_decrypt= "_ENC" in rawmsg or "_BAD" in rawmsg or "BAD_" in rawmsg or "ERR" in rawmsg

  def _initialise_device_names(self):
    ''' Substitute device IDs for names if we have them or identify broadcast '''
    try:
        if self.source_type == DEVICE_TYPE['CTL'] and self.is_broadcast():
            self.destination_name = "CONTROLLER"
            self.source_name = "CONTROLLER"
        elif DEVICE_TYPE[self.source_type] and self.source in devices and devices[self.source]['name']:
            self.source_name = "{} {}".format(DEVICE_TYPE[self.source_type], devices[self.source]['name'])      # Get the device's actual name if we have it
        else:
            print("Could not find device type '{}' or name for device '{}'".format(self.source_type, self.source))
        if self.destination_name != "CONTROLLER" and self.destination in devices and devices[self.destination]['name']:
            if self.destination_type == DEVICE_TYPE['CTL']:
                self.destination_name = "CONTROLLER"
            else:
                device_name = devices[self.destination]['name'] if self.destination in devices else self.destination
                self.destination_name = "{} {}".format(DEVICE_TYPE[self.destination_type], devices[self.destination]['name'])      # Get the device's actual name if we have it
    except Exception as e:
        print ("Error initalising device names in Message class instantiation, on line '{}': {}. Raw msg: '{}'. length = {}".format(sys.exc_info()[-1].tb_lineno, str(e), self.rawmsg, len(self.rawmsg)))


  def is_broadcast(self):
    return self.source == self.destination

class Command():
  ''' Object to hold details of command sent to evohome controller.'''
  def __init__(self, command_code=None, command_name=None, destination=None, args=None, serial_port=-1, send_mode="I", instruction=None):
    self.command_code = command_code
    self.command_name = command_name
    self.destination = destination
    self.args = args
    self.arg_desc = "[]"
    self.serial_port = serial_port
    self.send_mode = send_mode
    self.send_string = None
    self.send_dtm = None
    self.retry_dtm = None
    self.retries = 0
    self.send_failed = False
    self.send_acknowledged = False
    self.send_acknowledged_dtm = None
    self.dev1 = None
    self.dev2 = None
    self.dev3 = None
    self.payload = ""
    self.command_instruction = instruction


  def payload_length(self):
    return len(self.payload)/2


# --- General Functions
def sig_handler(signum, frame):              # Trap Ctl C
    print("{} Tidying up and exiting...".format(datetime.datetime.now().strftime("%Y-%m-%d %X")))
    # display_and_log("Tidying up and exiting...")
    file.close(logfile)
    for port_id, port in serial_ports.items():
      if port["connection"].is_open:
        print("Closing port '{}'".format(port["connection"].port))
        port["connection"].close()


def rotate_files(baseFileName):
  if os.path.isfile(baseFileName + "." + str(MAX_LOG_HISTORY)):
    os.remove(baseFileName + "." + str(MAX_LOG_HISTORY))

  i = MAX_LOG_HISTORY - 1
  while i > 0:
    if i>1:
        orgFileExt = "." + str(i)
    else:
        orgFileExt =""
    if os.path.isfile(baseFileName + orgFileExt):
        os.rename(baseFileName + orgFileExt, baseFileName + "." + str(i + 1))
    i -= 1


_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')

def to_snake(name):
  name=name.strip().replace("'","").replace(" ","_")
  s1 = _first_cap_re.sub(r'\1_\2', name)
  s2 = _all_cap_re.sub(r'\1_\2', s1).lower()
  return s2.replace("__","_")


def to_camel_case(s):
  return re.sub(r'(?!^) ([a-zA-Z])', lambda m: m.group(1).upper(), s)


def get_dtm_from_packed_hex(dtm_hex):
  dtm_mins = int(dtm_hex[0:2],16)
  dtm_hours = int(dtm_hex[2:4],16)
  dtm_day = int(dtm_hex[4:6],16)
  dtm_month = int(dtm_hex[6:8],16)
  dtm_year = int(dtm_hex[8:12],16)
  return datetime.datetime(year=dtm_year,month=dtm_month, day=dtm_day,hour=dtm_hours,minute=dtm_mins)


def display_data_row(msg, display_text, ref_zone=-1, suffix_text=""):
  destination = "BROADCAST MESSAGE" if msg.is_broadcast() else msg.destination_name
  if ref_zone >-1:
    zone_name = "@ {:<20}".format(zones[ref_zone]) if ref_zone in zones else "                      "
    display_row = "{:<2}| {:<21} -> {:<21} | {:>5} {} [Zone {:<3}] {}".format(
        msg.msg_type, msg.source_name, destination, display_text, zone_name, ref_zone, suffix_text)
  else:
    display_row = "{:<2}| {:<21} -> {:<21} | {:>5} {}".format(msg.msg_type, msg.source_name, destination, display_text, suffix_text)
  display_and_log(msg.command_name, display_row, msg.port)


def display_and_log(source="-", display_message="", port_tag=" "):
  try:
    global eventfile
    if os.path.getsize(EVENTS_FILE) > 5000000:
        eventfile.close()
        rotate_files(EVENTS_FILE)
        eventfile = open(EVENTS_FILE,"a")
    row = "{} |{:>1}| {:<20}| {}".format(datetime.datetime.now().strftime("%Y-%m-%d %X"), port_tag, source, display_message)
    row = "{:<140}".format(row)
    print (row)
    # print   (datetime.datetime.now().strftime("%Y-%m-%d %X") + ": " + "{:<20}".format(str(source)) + ": " + str(display_message))
    eventfile.write(row + "\n")
    file.flush(eventfile)
  except Exception as e:
    print (str(e))
    pass


def log(logentry, port_tag="-"):
  global logfile
  if os.path.getsize(LOG_FILE) > 10000000:
        logfile.close()
        rotate_files(LOG_FILE)
        logfile = open(LOG_FILE,"a")

  logfile.write("{} |{}| {}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %X"), port_tag, logentry.rstrip()))
  file.flush(logfile)


# Init com ports
def init_com_ports():
  serial_ports = {}
  if len(COM_PORTS) > 0:
    count = 1
    for port, params in COM_PORTS.items():
      limit = params["retry_limit"] if "retry_limit" in params else 3
      serial_port = None
      while (limit > 0) and serial_port is None:
        try:
          serial_port = serial.Serial(port)
          serial_port.baudrate = params["baud"] if "baud" in params else 115200
          serial_port.bytesize = 8
          serial_port.parity   = 'N'
          serial_port.stopbits = 1
          serial_port.timeout = 1

          break
        except Exception as e:
          if limit > 1:
              display_and_log("COM_PORT ERROR",repr(e) + ". Retrying in 5 seconds")
              time.sleep(5)
              limit -= 1
          else:
              display_and_log("COM_PORT ERROR","Error connecting to COM port {}. Giving up...".format(params["com_port"]))

      if serial_port is not None:
        serial_ports[port] = {"connection": serial_port, "parameters" : params, "tag": count}
        display_and_log(SYSTEM_MSG_TAG,"{}: Connected to serial port {}".format(serial_ports[port]["tag"], port))
        count +=1
  return serial_ports

def reset_com_ports():
  if len(serial_ports) > 1:
    display_and_log(SYSTEM_MSG_TAG,"Resetting serial port connections")
  # if port is changed for a given serial_port, the serial_port is closed/reopened as per pySerial docs
  for port_id, port in serial_ports.items():
      if port["connection"]:
        display_and_log(SYSTEM_MSG_TAG,"Resetting port '{}'".format(port["connection"].port))
        # port["connection"].port = port["connection"].port
        port["connection"].close()
        time.sleep(2)
        port["connection"].open()
  display_and_log(SYSTEM_MSG_TAG,"Serial ports have been reset")

# --- MQTT Functions -
def initialise_mqtt_client(mqtt_client):
  ''' Initalise the mqtt client object '''
  if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PW)
  mqtt_client.on_connect = mqtt_on_connect
  mqtt_client.on_message = mqtt_on_message
  mqtt_client.on_log = mqtt_on_log
  mqtt_client.on_disconnect = mqtt_on_disconnect
  mqtt_client.is_connected = False # Custom attribute so that we can track connection status

  display_and_log (SYSTEM_MSG_TAG,"Connecting to mqtt server %s" % MQTT_SERVER)
  mqtt_client.connect(MQTT_SERVER, port=1883, keepalive=60, bind_address="")

  display_and_log (SYSTEM_MSG_TAG,"Subscribing to mqtt topic '%s'" % MQTT_SUB_TOPIC)
  mqtt_client.subscribe(MQTT_SUB_TOPIC)
  mqtt_client.loop_start()


def mqtt_on_connect(client, userdata, flags, rc):
  ''' mqtt connection event processing '''
  if rc == 0:
      client.is_connected = True #set flag
      display_and_log (SYSTEM_MSG_TAG,"MQTT connection established with broker")
  else:
      client.is_connected = False
      display_and_log (SYSTEM_MSG_TAG,"MQTT connection failed (code {})".format(rc))
      if DEBUG:
          display_and_log(SYSTEM_MSG_TAG, "[DEBUG] mqtt userdata: {}, flags: {}, client: {}".format(userdata, flags, client))


def mqtt_on_disconnect(client, userdata, rc):
    ''' mqtt disconnection event processing '''
    client.loop_stop()
    client.is_connected = False
    if rc != 0:
        display_and_log(SYSTEM_MSG_TAG, "[WARN] Unexpected disconnection.")
        if DEBUG:
            display_and_log(SYSTEM_MSG_TAG, "[DEBUG] mqtt rc: {}, userdata: {}, client: {}".format(rc, userdata, client))


def mqtt_on_log(client, obj, level, string):
    ''' mqtt log event received '''
    if DEBUG:
        display_and_log(SYSTEM_MSG_TAG, "[DEBUG] MQTT log message received. Client: {}, obj: {}, level: {}".format(client, obj, level))
    display_and_log(SYSTEM_MSG_TAG, "[DEBUG] MQTT log msg: {}".format(string))


def mqtt_on_message(client, userdata, msg):
  ''' mqtt message received on subscribed topic '''
  # print(msg.payload)
  global send_queue
  global last_sent_command

  json_data = json.loads(str(msg.payload))
  # print(json_data)
  log("{: <18} {}".format("MQTT_SUB", json_data))

  if SYS_CONFIG_COMMAND in json_data:
    if json_data[SYS_CONFIG_COMMAND] in RESET_COM_PORTS:
      new_command = get_reset_serialports_command()
      new_command.instruction = json.dumps(json_data)
    elif json_data[SYS_CONFIG_COMMAND] == CANCEL_SEND_COMMANDS:
      send_queue = []
      last_sent_command = None
      display_and_log(SYSTEM_MSG_TAG, "Cancelled all queued outbound commands")
      return
    else:
      display_and_log(SYSTEM_MSG_TAG, "System configuration command '{}' not recognised".format(json_data[SYS_CONFIG_COMMAND]))
      return
  else:
    command_name = json_data["command"] if "command" in json_data else None
    command_code = json_data["command_code"] if "command_code" in json_data else None
    if command_code:
      if type(command_code) is int:
        command_code = hex(command_code)
      command_code = command_code.upper().replace("0X","")
    if command_name or command_code:
        args = json_data["arguments"] if "arguments" in json_data else ""
        send_mode = json_data["send_mode"] if "send_mode" in json_data else None
    new_command = Command(command_code=command_code, command_name=command_name, args=args, send_mode=send_mode, instruction=json.dumps(json_data))

  send_queue.append(new_command)


def mqtt_publish(device, command, msg, topic=None):
  if not mqtt_client.is_connected:
    display_and_log(SYSTEM_MSG_TAG,"[WARN] MQTT publish failed as client is not connected to broker")
    return

  try:
      if not topic:
        topic = "{}/{}/{}".format(MQTT_PUB_TOPIC, to_snake(device), command.strip())
      timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%XZ")
      mqtt_client.publish(topic, msg, 0, True)
      mqtt_client.publish("{}{}".format(topic,"_ts"), timestamp, 0, True)
      # print("published to mqtt topic {}: {}".format(topic, msg))
  except Exception as e:
      print(str(e))
      pass



def mqtt_init_homeassistant():
    # WIP....
    # Treat each zone as a HA 'device' with unique_id = zone number, and device name = zone name
    # HA component structure:
    # 1. Heating/DHW zone:
    #   - hvac 
    #     |- action_topic: 'heating' or 'off' (possibly 'idle') (heat demand > 0?)
    #     |- modes: current evohome schedule mode; allowed HA options "auto", "off", "heat"
    #     |- current_temperature_topic: evohome zone temperature
    #     |- temperature_command_topic: zone setpoint 
    #     |- temperature_state_topic: this monitors zone setpoint target as reported by the controller i.e. our setpoint_CTL temperatures
    #     |- away_mode_state_topic: as we can't set away mode in modes, may need to use this
    #     |- min_temp, max_temp, temp_step: min/max/step for the zone
    #     |- device, unique_id: use this for the evohome zone; only one hvac device allowed per unique_id
    #   - sensor (non relays, e.g. HR91 TRVs, Thermostats etc):
    #     |- zone level heat demand 
    #     |- <zone_individual_device>_temperature (e.g. TRV)
    #     |- <zone_individual_device>_heat_demand
    #     |- <zone_individual_device>_window_status
    #     |- <zone_individual_device>_battery
    #     |- <zone_individual_device>_setpoint_override
    # 2. BDR Relays, UFH controller:
    #     - sensor
    #     |- actuator_status
    #     |- actuator_status_ts
    #     |- heat_demand
    #     |- heat_demand_ts   
    # 3. Controller:
    #     - sensor
    #     |- command
    #     |- sent_command
    #     |- sent_command_ts
    #     |- sent_command_ack
    #     |- sent_command_ack_ts
    #     |- sent_command_failed
    #     |- sent_command_ack_ts
    #     |- send_command_last_retry_ts
    pass

def init_homie():
    # WIP....

    # This server is the Gateway device - i.e. the homie device
    # All other devices on the evohome network, including controllers etc, are treated as nodes of this gateway device

    device_type_properties = {
        "CTL" : ["temperature","setpoint","until","heat-demand"],
        "UFH" : [],
        "TRV" : ["temperature", "setpoint", "until", "heat-demand", "window"],
        "DHW" : ["state", "temperature", "dhw-mode", "until"],
        "BDR" : ["temperature", "heat-demand"],
        "GWAY": [],
        "STAT": ["temperature", "setpoint", "until"]
        }

    msgs =[]
    topic_base = "homie/evohome-gateway-{}".format(THIS_GATEWAY_ID.replace(":","-"))
    msgs.append (("{}/$homie".format(topic_base), 3.0, 0, True))
    msgs.append (("{}/$name".format(topic_base), "evohome Listener/Sender Gateway {}".format(THIS_GATEWAY_ID), 0, True))
    msgs.append (("{}/$state".format(topic_base), "ready", 0, True))
    msgs.append (("{}/$extensions".format(topic_base), "", 0, True))

    nodes_list = []
    node_msgs = []
    nodes_topic = "{}/$nodes".format(topic_base)

    for device_id, device in devices.items():
        device_type = device_id.split(":")[0]
        device_name = to_snake(device["name"]).lower().replace("_","-")

        zone_id = device["zoneId"]
        zone_name = zones[zone_id]         # if 1 <= zone_id <= 12 else str(zone_id)

        node_name = "{}-{}-{}".format(DEVICE_TYPE[device_type].lower(), device_name, device_id.replace(":","-"))
        nodes_list.append(node_name)

        node_topic = "{}/{}".format(topic_base, node_name)

        # Add to device object, for reuse later
        device['topic'] = node_topic
        device['node_name'] = node_name

        node_msgs.append (("{}/$name".format(node_topic), "[{:<4}] {}".format(DEVICE_TYPE[device_type], device["name"]),0,True))
        node_msgs.append (("{}/$type".format(node_topic), "[{:<4}]".format(DEVICE_TYPE[device_type]),0,True))

        node_properties = ",".join(device_type_properties[DEVICE_TYPE[device_type]])
        node_msgs.append (("{}/$properties".format(node_topic), node_properties, 0, True))

        for device_property in device_type_properties[DEVICE_TYPE[device_type]]:
            node_msgs += get_homie_node_topics(node_topic, device_property)

    msgs.append (("{}/$nodes".format(topic_base), ",".join(nodes_list), 0, True))
    msgs += node_msgs
    return msgs

def get_homie_node_topics(node_topic, node_property):
    property_params = namedtuple("property_params", "property_name unit datatype format settable default")

    if node_property in "temperature":
        params = property_params("Temperature", "°C", "float", "25:100", "false", 5.0)
    elif node_property in "setpoint":
        params = property_params("Setpoint", "°C", "float", "25:100", "false", 5.0)
    elif node_property in "until":
        params = property_params("Temporary Until", "°C", "string", "", "false", "")
    elif node_property in "heat-demand":
        params = property_params("Heat Demand", "%", "float", "0:100", "false", 0.0)
    elif node_property in "window":
        params = property_params("Room Window Status", "", "enum", "OPEN,CLOSED", "false", "CLOSED")
    elif node_property in "actuator-status":
        params = property_params("Actuator Status", "", "enum", "ON:OFF", "false", "OFF")
    elif node_property in "dhw-mode":
        params = property_params("DHW Mode", "", "enum", "AUTO,ON,OFF,TIMED", "false", "AUTO")
    elif node_property in "state":
        params = property_params("State", "", "enum", "ON,OFF", "false", "OFF")

    topic_base = "{}/{}".format(node_topic, node_property)
    msgs = []
    msgs.append ((topic_base, params.default, 0, True))
    msgs.append (("{}/$name".format(topic_base), params.property_name, 0, True))
    msgs.append (("{}/$unit".format(topic_base), params.unit, 0, True))
    msgs.append (("{}/$datatype".format(topic_base), params.datatype, 0, True))
    msgs.append (("{}/$format".format(topic_base), params.format, 0, True))
    msgs.append (("{}/$settable".format(topic_base), params.settable, 0, True))
    return msgs

# --- evohome received message command processing functions
def process_received_message(data, port_tag=None):
  ''' Process the raw message received. Also check if it is a reponse to previous sent command '''
  if not ("_ENC" in data or "_BAD" in data or "BAD_" in data or "ERR" in data) and len(data) > 40:          #Make sure no obvious errors in getting the data....
    if not data.startswith("---"):
        # Echos of commands sent by us come back without the --- prefix. Noticed on the fifo firmware that sometimes the request type prefix seems to be messed up. Workaround for this...
        if data.strip().startswith("W---"):
            data = data[1:]
        else:
            data = "---  {}".format(data) if len(data.split(" ",1)[0]) <2 else "--- {}".format(data)

    if data_pattern.match(data):
        msg = Message(data)
        msg.port = port_tag
        # Check if device is known...
        if not msg.source in devices:
            display_and_log("NEW DEVICE FOUND", msg.source)
            devices.update({msg.source : {"name" : msg.source, "zone_id" : -1, "zoneMaster" : False  }})
            with open(NEW_DEVICES_FILE,'w') as fp:
                fp.write(json.dumps(devices, sort_keys=True, indent=4))
            fp.close()

        if msg.command_code in COMMANDS:
            try:
                msg.command_name = COMMANDS[msg.command_code].__name__.upper() # Get the name of the command from our list of commands
                COMMANDS[msg.command_code](msg)
                log('{: <18} {}'.format(msg.command_name, data), port_tag)
            except Exception as e:
                display_and_log ("ERROR", "'{}' on line {} [Command {}, data: '{}', port: {}]".format(str(e), sys.exc_info()[-1].tb_lineno, msg.command_name, data, port_tag))
                traceback.format_exc()
                # display_and_log("ERROR",msg.command_name + ": " + repr(e) + ": " + data)
            return msg
        else:
            msg.command_name = "UNKNOWN COMMAND"
            display_data_row(msg, "Command code: {}, Payload: {}".format(msg.command_code, msg.payload))
            # display_and_log("UNKNOWN COMMAND","Command code: {}, Payload: {}".format(msg.command_code, msg.payload), port_tag)
            # log("UNKNOWN COMMAND: {}".format(data), port_tag)
    else:
        display_and_log("ERROR","Pattern match failed on received data: '{}' (port: {})".format(data, port_tag))
        return None
  else:
    return None


def get_zone_details(payload, source_type=None):
  zone_id = int(payload[0:2],16)
  if zone_id < 12:
    zone_id += 1
    zone_name = zones[zone_id] if zone_id in zones else "Zone {}".format(zone_id)
    topic = zone_name
  else:
    # if zone_id == DEVICE_TYPE['UFH']:
    #   topic="UFH Controller"
    if zone_id == 0xfa:  # Depends on whether it is main controller sending message, or UFH controller
        if source_type and str(source_type) == DEVICE_TYPE["UFH"]:
            zone_name ="UFH Controller"
        else:
            zone_name ="BDR DHW Relay"
    elif zone_id == 0xfc:    # Boiler relay or possibly broadcast
        zone_name  = "BDR Boiler Relay"
    elif zone_id == 0xf9:  # Radiator circuit zone valve relay
        zone_name ="BDR Radiators Relay"
    elif zone_id == 0xc: # Electric underfloor relay
        zone_name ="UFH Electric Relay"
    else:
        device_type = str(hex(msg.source_type))
        zone_name  = "RLY {}".format(device_type)
    topic = "Relays/{}".format(zone_name)
  return zone_id, zone_name, topic


def bind(msg):
  display_data_row(msg, "Payload: {}".format(msg.payload), -1, "Payload length: {}".format(msg.payload_length))
  pass


def sync(msg):
  # https://www.domoticaforum.eu/viewtopic.php?f=7&t=5806&start=120#p73918
  # Basically the controller sends a broadcast write 1f09 with f8 in the first byte
  # and the last 2 bytes giving the time in 10ths of a second to the next broadcast message.
  # If the TRVs don't hear the controller for a while they start sending a 1f09 request presumably looking for a 1f09 reply
  # giving the time to the next broadcast


  if msg.payload[0:2] == "FF":
    timeout = int(msg.payload[2:6],16) / 10
    timeout_time = (datetime.datetime.now() + datetime.timedelta(seconds = timeout)).strftime("%H:%M:%S")
    display_data_row(msg, "Next sync at {} (in {} secs)".format(timeout_time, timeout))
  else:
    display_data_row(msg, "Payload: {}".format(msg.payload))
  pass


def schedule_sync(msg):
  display_data_row(msg, "Payload: {}".format(msg.payload), -1, "Payload length: {}".format(msg.payload_length))
  pass
  # The 0x0006 command is a schedule sync message sent between the gateway to controller to check whether there have been any changes since the last exchange of schedule information
  # https://www.automatedhome.co.uk/vbulletin/showthread.php?5085-My-HGI80-equivalent-Domoticz-setup-without-HGI80/page13


def zone_name(msg):
  display_data_row(msg, "Payload: {}".format(msg.payload), -1, "Payload length: {}".format(msg.payload_length))
  pass


def setpoint_ufh(msg):
  # Not 100% sure what this command is. First 2 digits seem to be ufh controller zone, followed by 4 digits which appear to be for the
  # zone's setpoint, then 0A2801.
  # Pattern is repeated with any other zones that the ufh controller may have.

  i = 0
  while (i < (msg.payload_length *2)):
    zone_id, zone_name, topic = get_zone_details(msg.payload[0+i:2+i])
    # We add 900 to identify UFH zone numbers
    zone_setpoint = float(int(msg.payload[2+i:6+i],16))/100
    # zone_name =""
    for d in devices:
        if devices[d].get('ufh_zoneId') and zone_id == devices[d]['ufh_zoneId']:
            zone_id = devices[d]["zoneId"]
            zone_name = devices[d]["name"]
            break
    if zone_name == "":
        display_and_log("DEBUG","UFH Setpoint Zone '{}' name not found".format(zone_id), msg.port)
        zone_name = "UFH Zone {}".format(zone_id)
    display_data_row(msg, "{:5.2f}°C".format(zone_setpoint), zone_id)
    # display_and_log(msg.command_name,'{0: <22}{:>5}  [Zone UFH {}]".format(zone_name, zone_setpoint, zone_id))
    mqtt_publish(zone_name, "setpoint",zone_setpoint)

    i += 12


def setpoint(msg):
  if msg.payload_length % 3 != 0:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be multiple of 3). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    command_name_suffix =""
    if msg.payload_length > 3:
        command_name_suffix = "_CTL"
        msg.command_name = "{}_CTL".format(msg.command_name)
    i = 0
    payload_string_length = msg.payload_length * 2
    while (i < payload_string_length):
        zone_data = msg.payload[i:i+6]
        zone_id = int(zone_data[0:2],16) + 1 #Zone number
        zone_name = zones[zone_id] if zone_id in zones else "Zone {}".format(zone_id)

        # display_and_log("DEBUG","Setpoint: zone not found for zone_id " + str(zone_id) + ", MSG: " + msg.rawmsg)
        zone_setpoint = float(int(zone_data[2:4],16) << 8 | int(zone_data [4:6],16))/100
        if (zone_setpoint >= 300.0): # Setpoint of 325 seems to be the number sent when TRV manually switched to OFF. Use >300 to avoid rounding errors etc
            zone_setpoint = 0
            flag = " *(Heating is OFF)"
        else:
            flag = ""

        display_data_row(msg, "{:5.2f}°C{}".format(zone_setpoint,flag), zone_id)
        mqtt_publish(zone_name, "setpoint" + command_name_suffix,zone_setpoint)
        i += 6


def setpoint_override(msg):
  if msg.payload_length != 7 and msg.payload_length != 13:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 7 or 13). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    zone_id, zone_name, topic = get_zone_details(msg.payload[0:2])
    new_setpoint = float(int(msg.payload[2:4],16) << 8 | int(msg.payload [4:6],16))/100

    #!!TODO!! Trap for 0x7FFF - this means setpoint not set
    if msg.payload_length == 13: # We have an 'until' date
        dtm_hex=msg.payload[14:]
        dtm = get_dtm_from_packed_hex(dtm_hex)
        until = " - Until " + str(dtm)
        mqtt_publish(topic, "mode", "Temporary")
        mqtt_publish(topic, "mode_until", dtm.strftime("%Y-%m-%dT%XZ"))
    else:
        until =""
        mqtt_publish(topic, "mode", "Scheduled")
        mqtt_publish(topic, "mode_until", "")
    display_data_row(msg, "{:5.2f}°C".format(new_setpoint), zone_id, until)
    mqtt_publish(topic, "setpointOverride",new_setpoint)


def zone_temperature(msg):
  temperature = float(int(msg.payload[2:6],16))/100
  zone_id=0
  if devices.get(msg.source_id):
    zone_id = devices[msg.source_id]["zoneId"]
  else:
    display_and_log("DEBUG","Device not found for source " + msg.source_id)
  if zone_id >0:
    zoneDesc = " [Zone " + str(zone_id) + "]"
  else:
    zoneDesc = ""
  display_data_row(msg, "{:5.2f}°C".format(temperature), zone_id)
  mqtt_publish("{}/{}".format(zones[zone_id], msg.source_name), "temperature",temperature)


def window_status(msg):
  if msg.payload_length < 3:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be less than 3). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    # Zone is first 2, then window status. 3rd pair seems to be always zero apparently
    zone_id, zone_name, _ = get_zone_details(msg.payload[0:2])
    statusId = int(msg.payload[2:4],16)
    misc = int(msg.payload[4:6],16)

    if statusId == 0:
        status = "CLOSED"
    elif statusId == 0xC8:
        status = "OPEN"
    else:
        status = "Unknown (" + str(statusId) + ")"

  if misc >0:
        miscDesc = " (Misc: " + str(misc) + ")"
  else:
        miscDesc = ""
  display_data_row(msg, "{:>7}".format(status), zone_id)
  mqtt_publish("{}/{}".format(zones[zone_id], msg.source_name),"window_status",status)


def other_command(msg):
  display_and_log(msg.command_name, msg.rawmsg)


def date_request(msg):
  display_data_row(msg, "Ping/Datetime Sync")


def relay_heat_demand(msg):
  # Heat demand sent by the controller for CH / DHW / Boiler  (F9/FA/FC)
  if msg.payload_length != 2:
    display_and_log(msg.command_name,"Invalid payload length of {} (should be 2). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    type_id = int(msg.payload[0:2],16)
    demand = int(msg.payload[2:4],16)
    if type_id <12:
        type_id +=1
    zone_id, zone_name, topic = get_zone_details(msg.payload, msg.source_type)

    demand_percentage = float(demand)/200*100
    display_data_row(msg, "{:>6.1f}% @ {}".format(demand_percentage, zone_name, "(type id: {})".format(type_id)))
    mqtt_publish(topic,"heat_demand",demand_percentage)


def zone_heat_demand(msg):
  if msg.payload_length % 2 != 0 :
    display_and_log(msg.command_name, "Invalid payload length of {} (should be mod 2). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    topic = ""
    i = 0
    while (i < (msg.payload_length *2)):
        # try:
        zone_id, zone_name, topic = get_zone_details(msg.payload[i:2+i])
        demand = int(msg.payload[2+i:4+i],16)

        # We use zone combined with device name for topic, as demand can be from individual trv
        if zone_id <=12:
            topic = "{}/{}".format(topic, msg.source_name)

        if msg.source_type == DEVICE_TYPE['UFH'] and zone_id <= 8: # UFH zone controller only supports 5 (+3 with optional card) zones
            # if destination device is the main touch controller, then then zone Id is that of the matched zone in the main touch controller itself
            # Otherwise, if the destination device is the ufh controller (i.e. message is to itself/broadcast etc), then the zone Id is the ufh controller zone id (i.e. 1 to 8)
            # zone_id must therefore be the ufh controller zones, and valued 0 to 7.
            ufh_zone_id = zone_id - 1 # 1 was added in the get_zone_details fn above as ufh subzones zero based
            if msg.destination_type == DEVICE_TYPE['CTL']:
                device_type = "UFH " + zone_name.split(' ', 1)[1]
                topic = zone_name
            elif msg.is_broadcast(): # the zone_id refers to the UFH controller zone, and not the main evohome controller zone
                # zone_id in this refers to the ufh zone id, and so zone_name etc need to be corrected
                zone_name = "UFH Sub-Zone Id {}".format(ufh_zone_id)
                for d in devices:
                    if 'ufh_zoneId' in devices[d] and devices[d]['ufh_zoneId'] == ufh_zone_id:
                        # display_and_log("DEBUG","UFH Zone matched to " + devices[d]["name"])
                        zone_id = devices[d]["zoneId"] #
                        zone_name = devices[d]["name"]
                        zone_name_parts = zone_name.split(' ', 1)
                        device_type = "UFH {}".format(zone_name_parts[1]) if len(zone_name_parts) > 1 else "UFH {}".format(zone_name)
                        topic = "{}/{}".format(zone_name, msg.source_name)
                        break
            else:
                display_and_log("ERROR","UFH message received, but destination is neither main controller nor UFH controller. \
                  destination = {}, destination_type = {}. msg: {} ".format(msg.destination, msg.destination_type, msg.rawmsg))

        demand_percentage = float(demand)/200*100
        display_data_row(msg, "{:6.1f}%".format(demand_percentage), zone_id)

        if len(topic) > 0:
            mqtt_publish(topic,"heat_demand",demand_percentage)
        else:
            display_and_log("DEBUG", "ERROR: Could not post to MQTT as topic undefined")
        i += 4


def dhw_settings(msg):
  #  <1:DevNo><2(uint16_t):SetPoint><1:Overrun?><2:Differential>
  if msg.payload_length != 6:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 6). Raw msg: {}".format(msg.payload_length, msg.rawmsg))

  device_number = int(msg.payload[0:2], 16)
  setpoint = float(int(msg.payload[2:6], 16)) / 100
  overrun = int(msg.payload[6:8],16)
  differential = float(int(msg.payload[8:12], 16)) / 100
  reheat_trigger = setpoint - differential

  display_data_row(msg,"DHW Setpoint: {}°C; Re-heat triggered at {}°C. (Overrun state: {})".format(setpoint, reheat_trigger, overrun), -1, "(Device: {})".format(device_number))


def actuator_check_req(msg):
  # this is used to synchronise time periods for each relay bound to a controller
  # i.e. all relays get this message and use it to determine when to start each cycle (demand is just a % of the cycle length)
  # https://www.domoticaforum.eu/viewtopic.php?f=7&t=5806&start=105#p73681
  if msg.payload_length != 2:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 2). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    device_number = int(msg.payload[0:2],16)
    demand = int(msg.payload[2:4],16)
    # if device_number == 0xfc: # 252 - apparently some sort of broadcast?
    #   device_type = ": Status Update Request"
    # else:
    #   device_type =""
    status = "Actuator time period sync request: {}".format(device_number)

    display_data_row(msg, status)


def actuator_state(msg):
  if msg.payload_length == 1 and msg.msg_type == "RQ":
    display_data_row(msg, "State update request")
  elif msg.payload_length != 3:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 3). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    device_number = int(msg.payload[0:2],16) # Apparently this is always 0 and so invalid
    demand = int(msg.payload[2:4],16)   # (0 for off or 0xc8 i.e. 100% for on)

    if demand == 0xc8:
        status = "ON"
    elif demand == 0:
        status ="OFF"
    else:
        status = "Unknown: " + str(demand)
    display_data_row(msg, "{:>7}".format(status))
    mqtt_publish("relays/{}".format(msg.source_name),"actuator_status",status)


def dhw_state(msg):
  if msg.payload_length == 1 and (msg.msg_type == "RQ" or msg.msg_type == "I"):
    display_and_log(msg.command_name, "Request sent: {}".format(msg.payload))
    return

  if msg.payload_length != 6 and msg.payload_length != 12:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be multiple 6 or 12). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    zone_id, _, _ = get_zone_details(msg.payload[0:2])
    stateId = int(msg.payload[2:4],16)    # 0 or 1 for DHW on/off, or 0xFF if not installed
    modeId = int(msg.payload[4:6],16)     # 04 = timed??

    if stateId == 0xFF:
        state ="DHW not installed"
    elif stateId == 1:
        state = "On"
    elif stateId == 0:
        state = "Off"
    else:
        state ="Unknown state: {}".format(stateId)

    if modeId == 0:
        mode="Auto"
    elif modeId ==4:
        mode="Timed"
    else:
        mode=str(modeId)

    if msg.payload_length == 12:
        dtm_hex=msg.payload[12:]
        dtm = get_dtm_from_packed_hex(dtm_hex)
        until = " - Until {}".format(dtm)
    else:
        until =""

    if stateId == 0xFF:
        display_and_log(msg.command_name, "{}: DHW not installed".format(msg.source))
    else:
        display_data_row(msg, "State: {}, mode: {}".format(state, mode), -1, until)
        mqtt_publish("DHW","state",stateId)
        mqtt_publish("DHW","mode",mode)
        if until >"":
            mqtt_publish("DHW", "mode_until", dtm.strftime("%Y-%m-%dT%XZ"))
        else:
            mqtt_publish("DHW", "mode_until", "")


def dhw_temperature(msg):
  if msg.payload_length == 1:
    # This is most likely an outbound request
    return

  if msg.payload_length != 3:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 3). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
    return

  temperature = float(int(msg.payload[2:6],16))/100
  display_data_row(msg, "{:5.2f}°C".format(temperature))
  mqtt_publish("DHW", "temperature", temperature)


def zone_info(msg):
    if msg.payload_length % 6 != 0:
        display_and_log(msg.command_name, "Invalid payload length of {} (should be mod 6). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
        return

    i = 0
    payload_string_length = msg.payload_length * 2
    while (i < payload_string_length):
        zone_data = msg.payload[i:i+12]
        zone_id, zone_name, topic = get_zone_details(zone_data)

        zone_flags = int(zone_data[2:4],16)
        min_temperature = float(int(zone_data[4:8],16) / 100)
        max_temperature = float(int(zone_data[8:12],16) / 100)

        display_data_row(msg, "Temp. range: {}°C to {}°C".format(min_temperature, max_temperature), zone_id, "(Flags: {})".format(zone_flags))
        # mqtt_publish(zone_name, "setpoint" + command_name_suffix,zone_setpoint)
        i += 12


def device_info(msg):
    if msg.payload_length == 3:
        display_and_log(msg.command_name, "Device information requested: {}".format(msg.payload))
        return
    if msg.payload_length != 22:
        display_and_log(msg.command_name, "Invalid payload length of {} (should be mod 22). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
        return
    display_data_row(msg, msg.rawmsg)
    # display_and_log(msg.command_name, msg.rawmsg)


def battery_info(msg):
  if msg.payload_length != 3:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 3). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    device_id = int(msg.payload[0:2],16)
    battery = int(msg.payload[2:4],16)
    lowBattery = int(msg.payload[4:5],16)
    zone_id = devices[msg.source]["zoneId"]

    if battery == 0xFF:
        battery = 100 # recode full battery (0xFF) to 100 for consistency across device types
    else:
        battery = battery / 2  #recode battery level values to 0-100 from original 0-200 values

    if(lowBattery != 0):    #TODO... Need to check this to understand how it is used.
        warning = " - LOW BATTERY WARNING"
    else:
        warning = ""

    display_data_row(msg, "{:.1f}% (device ID {})".format(battery, device_id), zone_id, warning)
    topic = "dhw" if zone_id == 250 else zones[zone_id]
    mqtt_publish("{}/{}".format(topic, msg.source_name),"battery",battery)


def controller_mode(msg):
  if msg.payload_length != 8:
    display_and_log(msg.command_name, "Invalid payload length of {} (should be 8). Raw msg: {}".format(msg.payload_length, msg.rawmsg))
  else:
    modeId = int(msg.payload[0:2],16)   # controller mode
    try:
        mode = CONTROLLER_MODES[modeId]
    except:
        mode="Unknown (" + str(modeId) + ")"
    durationCode = int(msg.payload[14:16],16) # 0 = Permanent, 1 = temporary

    if durationCode == 1:
        dtm_hex=msg.payload[2:14]
        dtm = get_dtm_from_packed_hex(dtm_hex)
        until = " [Until {}]".format(dtm)
    else:
        if modeId != 0:
            until =" - PERMANENT"
        else:
            until =""
    display_data_row(msg, "{} mode".format(mode), -1, until)
    mqtt_publish(msg.source_name,"mode",mode)


def heartbeat(msg):
  display_and_log(msg.command_name, msg.rawmsg, msg.port)


def external_sensor(msg):
  display_and_log(msg.command_name, msg.rawmsg, msg.port)


def unknown_command(msg):
  display_and_log(msg.command_name, msg.rawmsg, msg.port)


def get_reset_serialports_command():
   return Command(SYS_CONFIG_COMMAND, RESET_COM_PORTS)

# --- evohome send command functions
def process_send_command(command):
  ''' Process system configuration command or send command to evohome gateway '''
  if not command.command_code and not command.command_name:
      display_and_log("ERROR","Cannot process command without valid command_code ({}) or command_name ({}) [args: '{}']".format(
        command.command_code, command.command_name, args))
      return

  # Check and do system config commands first
  if command.command_code == SYS_CONFIG_COMMAND:
    if command.command_name == RESET_COM_PORTS:
      reset_com_ports()
    else:
      display_and_log(SYSTEM_MSG_TAG, "System configuration command '{}' not recognised".format(command.command_name))
    return # Either way, we return. Rest of the fn is processing actual evohome commands

  # Command must be an evohome one. Process and send.
  if not command.command_code: # command_name takes priority over command_code
    command.command_code = COMMAND_CODES[command.command_name] if command.command_name in COMMAND_CODES else "0000"
  else:
    if command.command_code in COMMANDS:
      command.command_name = COMMANDS[command.command_code].__name__.upper()
    else:
      display_and_log("DEBUG", "Command name not found for code '{}'".format(command.command_code))

  if command.command_code == "0000":
      display_and_log("ERROR","Unrecognised command.command_name '{}'".format(command.command_name))
      return


  send_string = ""
  # print (command.command_name)
  if "payload" not in command.args:
      if (command.command_name and command.command_name == "dhw_state") or (command.command_code and command.command_code == "1F41"):
          # 1F41: Change dhw state
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

      elif (command.command_name and command.command_name in "date_request ping") or (command.command_code and command.command_code == "313F"):
          # 0x313F: Send a datetime update request, i.e. like a ping
          command.payload = "00"
          if command.send_mode is None:
              command.send_mode = "RQ"

      elif (command.command_name and command.command_name == "controller_mode") or (command.command_code and command.command_code == "2E04"):
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

      elif (command.command_name and command.command_name == "setpoint_override") or (command.command_code and command.command_code == "2349"):
          # 0x2349: Setpoint override
          zone_id = command.args["zone_id"]
          setpoint = command.args["setpoint"]
          until = command.args["until"] if "until" in command.args else None
          command.payload = get_setpoint_override_payload(zone_id, setpoint, until)
          if command.send_mode is None:
              command.send_mode = "W"
          if until:
            command.arg_desc = "['{}': {} degC until {}]".format(zones[zone_id] if zone_id in zones else zone_id, setpoint, until)
          else:
            command.arg_desc = command.arg_desc = "['{}': {} deg C]".format(zones[zone_id] if zone_id in zones else zone_id, setpoint)
  else:
      command.payload = command.args["payload"]
      if command.send_mode is None:
        command.send_mode = "I"

  command.dev1 = command.args["dev1"] if "dev1" in command.args else THIS_GATEWAY_ID
  command.dev2 = command.args["dev2"] if "dev2" in command.args else CONTROLLER_ID
  command.dev3 = command.args["dev3"] if "dev3" in command.args else EMPTY_DEVICE_ID
  command.destination = command.dev2

  if command.payload_length() > -1 and command.payload:
    sent_command = send_command_to_evohome(command)
    return sent_command

  else:
      display_and_log("ERROR","Invalid command.payload = '{}' or command.payload length = {}".format(command.payload, command.payload_length()))
      return None


def get_controller_mode_payload(mode_id, until_string=None):
    if until_string == None:
        duration_code = 0x0
        until = "FFFFFFFFFFFF"
    else:
        duration_code = 0x1
        until = dtm_string_to_payload(until_string)

    payload = "{:02X}{}{:02X}".format(mode_id, until ,duration_code)
    return payload


def get_dhw_state_payload(state_id, until_string=None, mode_id=-1):
    # state_id is 0 or 1 for DHW on/off
    if until_string == None:
        until = ""
        if mode_id == -1:
            mode_id = 0 # Revert to auto
    else:
        until = dtm_string_to_payload(until_string)
        mode_id = 4 # if we have an 'until', mode must be temporary
    zone_id = 0
    payload = "{:02X}{:02x}{:02X}FFFFFF{}".format(zone_id, state_id, mode_id, until)
    return payload


def get_setpoint_override_payload(zone_id, setpoint, until_string=""):
    #
    # modes:  [Auto, -1, Permanent, -1, Temporary] (zero based)
    #
    if until_string:
        until = dtm_string_to_payload(until_string)
        mode = 4
    elif setpoint >0:
        mode = 2
        until = ""
    else:
        # If setpoint is 0, we revert back to auto
        mode = 0
        until = ""

    payload = "{:02X}{:04X}{:02X}FFFFFF{}".format(zone_id - 1, int(setpoint * 100), mode, until)

    return payload


def dtm_string_to_payload(dtm_string):
    dtm = datetime.datetime.strptime(dtm_string, "%Y-%m-%dT%H:%M:%SZ")
    payload = "{:02X}{:02X}{:02X}{:02X}{:04X}".format(dtm.minute, dtm.hour, dtm.day, dtm.month, dtm.year)
    return payload


def send_command_to_evohome(command):
  if not command and not(command.command_code and command.payload):
    display_and_log("ERROR","Send to evohome failed as invalid send_command: {}".format(command))
    return

  command.send_string = "{} --- {} {} {} {:<4} {:03d} {}".format(command.send_mode, command.dev1,
    command.dev2, command.dev3, command.command_code, command.payload_length(), command.payload)
  log_row = "{}: Sending '{}'".format(command.command_name.upper() if command.command_name is not None else "-None-", command.send_string)
  log('{: <18} {}'.format("COMMAND_OUT", log_row))
  # display_and_log("COMMAND_OUT","{}: Sending '{}'".format(command.command_name.upper() if command.command_name is not None else "-None-", command.send_string))

  byte_command = bytearray(b'{}\r\n'.format(command.send_string))
  response = serial_port.write(byte_command)

  display_and_log("COMMAND_OUT","{} {} Command SENT".format(command.command_name.upper() if command.command_name is not None else "-None-",
    command.arg_desc if command.arg_desc !="[]" else ":"))

  if mqtt_client.is_connected:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%XZ")
    mqtt_client.publish("{}/command".format(SENT_COMMAND_TOPIC), "{} {}".format(command.command_name, command.args), 0, True)
    mqtt_client.publish("{}/evo_msg".format(SENT_COMMAND_TOPIC), command.send_string, 0, True)
    mqtt_client.publish("{}/ack".format(SENT_COMMAND_TOPIC),False, 0, True)
    mqtt_client.publish("{}/retries".format(SENT_COMMAND_TOPIC), command.retries, 0, True)
    mqtt_client.publish("{}/retry_ts".format(SENT_COMMAND_TOPIC), "", 0, True)
    mqtt_client.publish("{}/failed".format(SENT_COMMAND_TOPIC), False, 0, True)
    mqtt_client.publish("{}/org_instruction".format(SENT_COMMAND_TOPIC), command.command_instruction, 0, True)
    mqtt_client.publish(MQTT_SUB_TOPIC, "", 0, True)
    if command.retries == 0:
      mqtt_client.publish("{}/initial_sent_ts".format(SENT_COMMAND_TOPIC), timestamp, 0, True)
    else:
      mqtt_client.publish("{}/last_retry_ts".format(SENT_COMMAND_TOPIC), timestamp, 0, True)
  else:
    display_and_log(SYSTEM_MSG_TAG,"[WARN] Client not connected to MQTT broker. No command status messages posted")

  if command.retries ==  0:
    command.send_dtm = datetime.datetime.now()
  command.retry_dtm = datetime.datetime.now()
  command.retries += 1

  return command


def check_previous_command_sent(previous_command):
  ''' Resend previous command if ack not received in reasonable time '''
  if not previous_command or previous_command.send_acknowledged:
    return
  if previous_command.retries < COMMAND_RESEND_ATTEMPTS + 1:
    seconds_since_sent = (datetime.datetime.now() - previous_command.retry_dtm).total_seconds()

    if seconds_since_sent > COMMAND_RESEND_TIMEOUT_SECS:
      display_and_log("COMMAND_OUT","{} {} Command NOT acknowledged. Resending attempt {} of {}...".format(
        previous_command.command_name.upper(), previous_command.arg_desc if previous_command.arg_desc != "[]" else ":", previous_command.retries, COMMAND_RESEND_ATTEMPTS))
      previous_command = send_command_to_evohome(previous_command)
  else:
    if not previous_command.send_failed:
      previous_command.send_failed = True
      mqtt_publish("","command_sent_failed",True,SENT_COMMAND_TOPIC)
      display_and_log("COMMAND","ERROR: Previously sent command '{}' failed to send. No ack received from controller".format(previous_command.command_name))

      if AUTO_RESET_PORTS_ON_FAILURE:
        # command_code, command_name, args, send_mode = get_reset_serialports_command()
        send_queue.append(get_reset_serialports_command())


# --- evohome Commands Dict
COMMANDS = {
  '0002': external_sensor,
  '0004': zone_name,
  '0006': schedule_sync,
  '0008': relay_heat_demand,
  '000A': zone_info,
  '0100': other_command,
  '0418': device_info,
  '1060': battery_info,
  '10A0': dhw_settings,
  '10E0': heartbeat,
  '1260': dhw_temperature,
  '12B0': window_status,
  '1F09': sync,
  '1F41': dhw_state,
  '1FC9': bind,
  '22C9': setpoint_ufh,
  '2309': setpoint,
  '2349': setpoint_override,
  '2E04': controller_mode,
  '30C9': zone_temperature,
  '313F': date_request,
  '3150': zone_heat_demand,
  '3B00': actuator_check_req,
  '3EF0': actuator_state
}
# 10A0: DHW settings sent between controller and DHW sensor can also be requested by the gateway

COMMAND_CODES = {
  "actuator_check_req" : "3B00",
  "actuator_state" : "3EF0",
  "battery_info" : "1060",
  "bind" : "1FC9",
  "controller_mode" : "2E04",
  "date_request" : "313F",
  "device_info" : "0418",
  "dhw_state" : "1F41",
  "dhw_temperature" : "1260",
  "external_sensor": "0002",
  "heartbeat" : "10E0",
  "other_command" : "0100",
  "ping" : "313F",
  "relay_heat_demand" : "0008",
  "setpoint" : "2309",
  "setpoint_override" : "2349",
  "setpoint_ufh" : "22C9",
  "sync" : "1F09",
  "window_status" : "12B0",
  "zone_heat_demand" : "3150",
  "zone_info" : "000A",
  "zone_name": "0004",
  "zone_temperature" : "30C9"
}

THIS_GATEWAY_TYPE = DEVICE_TYPE[THIS_GATEWAY_TYPE_ID]

SENT_COMMAND_SUBTOPIC = "sent_command"
SENT_COMMAND_TOPIC_BASE = to_snake("{}/{}_{}".format(MQTT_PUB_TOPIC, THIS_GATEWAY_TYPE, THIS_GATEWAY_NAME))
SENT_COMMAND_TOPIC = "{}/{}".format(SENT_COMMAND_TOPIC_BASE, SENT_COMMAND_SUBTOPIC)

# --- Main
rotate_files(LOG_FILE)
rotate_files(EVENTS_FILE)
logfile = open(LOG_FILE, "a")
eventfile = open(EVENTS_FILE,"a")

signal.signal(signal.SIGINT, sig_handler)    # Trap CTL-C etc

# display_and_log("","\n")
display_and_log("","evohome Listener/Sender Gateway version " + VERSION )

serial_ports = init_com_ports() # global var serial_ports also changed in fn reset_com_ports
if len(serial_ports) == 0:
  print("Serial port(s) parameters not found. Exiting...")
  sys.exit()

# for port_id, port in serial_ports.items():
#   display_and_log("","{}: Connected to COM port {}".format(port["tag"], port_id))

logfile.write("")
logfile.write("-----------------------------------------------------------\n")

if os.path.isfile(DEVICES_FILE):
  with open(DEVICES_FILE, 'r') as fp:
    devices = json.load(fp)             # Get a list of known devices, ideally with their zone details etc
else:
  devices = {}
# Add this server/gateway as a device, but using dummy zone ID for now
devices[THIS_GATEWAY_ID] = { "name" : THIS_GATEWAY_NAME, "zoneId": 240, "zoneMaster": True }

zones = {}                            # Create a seperate collection of Zones, so that we can look up zone names quickly
send_queue = []
send_queue_size_displayed = 0         # Used to track if we've shown the queue size recently or not

for d in devices:
  if devices[d]['zoneMaster']:
    zones[devices[d]["zoneId"]] = devices[d]["name"]
  # generate the mqtt topic for the device (using Homie convention)

display_and_log('','')
display_and_log('','-----------------------------------------------------------')
display_and_log('',"Devices loaded from '" + DEVICES_FILE + "' file:")
for key in sorted(devices):
  zm = " [Master]" if devices[key]['zoneMaster'] else ""
  display_and_log('','   ' + key + " - " + '{0: <22}'.format(devices[key]['name']) + " - Zone " + '{0: <3}'.format(devices[key]["zoneId"]) + zm )

display_and_log('','-----------------------------------------------------------')
display_and_log('','')
display_and_log('','Listening...')

file.flush(logfile)

# init MQTT
mqtt_client = mqtt.Client()
initialise_mqtt_client(mqtt_client)

prev_data_had_errors = False
data_pattern = re.compile("^--- ( I| W|RQ|RP) --- \d{2}:\d{6} (--:------ |\d{2}:\d{6} ){2}[0-9a-fA-F]{4} \d{3}")
data_row_stack = deque()
last_sent_command = None
ports_open = any(port["connection"].is_open for port_id, port in serial_ports.items()) # ports_open var also updated in fn close_com_ports

# Main loop
while ports_open:
  try:
    for port_id, port in serial_ports.items():
      serial_port = port["connection"]
      if serial_port.is_open:

        # Check if last command needs to be resent
        if last_sent_command and not last_sent_command.send_failed and not last_sent_command.send_acknowledged:
          check_previous_command_sent(last_sent_command)

        # Process any unsent commands waiting to be sent only if we don't have any pending last_sent_command
        if send_queue and "is_send_port" in port["parameters"] and port["parameters"]["is_send_port"]:
          if not last_sent_command or last_sent_command.send_acknowledged or last_sent_command.send_failed:
            new_command = send_queue.pop()
            last_sent_command = process_send_command(new_command)
            if send_queue and len(send_queue) != send_queue_size_displayed:
              display_and_log("DEBUG","{} command(s) queued for sending to controller".format(len(send_queue)))
          elif len(send_queue) != send_queue_size_displayed:
            display_and_log("DEBUG","{} command(s) queued and held, pending acknowledgement of '{}' command previously sent".format(len(send_queue), last_sent_command.command_name))
          send_queue_size_displayed = len(send_queue)

        # Now check for incoming...
        if serial_port.inWaiting() > 0:
          data_row = serial_port.readline().strip()
          if data_row:
            stack_entry = "{}: {}".format(datetime.datetime.now().strftime("%Y-%m-%d %X"), data_row)

            # Make sure it is not a duplicate message (e.g. received through additional listener/gateway devices)
            if stack_entry not in data_row_stack:
              msg = process_received_message(data_row, port["tag"])
              if msg:
                # Check if the received message is acknowledgement of previously sent command
                if last_sent_command and msg.source == last_sent_command.destination and msg.destination == THIS_GATEWAY_ID:
                  # display_and_log("Previously sent command '{}' acknowledged".format(last_sent_command.command_name), msg.source)
                  mqtt_publish("", "", True, "{}/ack".format(SENT_COMMAND_TOPIC))
                  last_sent_command.send_acknowledged = True
                  last_sent_command.send_acknowledged_dtm = datetime.datetime.now()
                  display_and_log("COMMAND_OUT","{} {} Command ACKNOWLEDGED".format(last_sent_command.command_name.upper(),
                    last_sent_command.arg_desc if last_sent_command.arg_desc != "[]" else ":"))
                prev_data_had_errors = False
              else:
                  if not prev_data_had_errors and LOG_DROPPED_PACKETS:
                      prev_data_had_errors = True
                      display_and_log("ERROR","--- Message dropped: packet error from hardware/firmware", port["tag"])
                  log("{: <18} {}".format("", data_row), port["tag"])
              file.flush(logfile)
              data_row_stack.append(stack_entry)
              if len(data_row_stack) > MAX_HISTORY_STACK_LENGTH:
                data_row_stack.popleft()

      time.sleep(0.01)
    ports_open = any(port["connection"].is_open for port_id, port in serial_ports.items())
  except serial.SerialException:
    display_and_log("ERROR","Serial port exception occured")
  except KeyboardInterrupt:
    for port_id, port in serial_ports.items():
      if port["connection"].is_open:
        print("Closing port '{}'".format(port["connection"].port))
        port["connection"].close()

    # comConnected = False

mqtt_client.loop_stop()
print("Session ended\n")
