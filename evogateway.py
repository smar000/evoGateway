# -*- coding: utf-8 -*-
#!/usr/bin/python
# Evohome Listener/Sender
# Copyright (c) 2019 SMAR info@smar.co.uk
#  
# Tested with Python 2.7.12. Requires:
# - pyserial (python -m pip install pyserial)
# - paho (pip install paho-mqtt)
#
# Simple Evohome 'listener' and 'sender', for listening in and sending messages between evohome devices using an arudino + CC1101 radio receiver 
# (other hardware options also possible - see credits below).
# Messages are interpreted and then posted to an mqtt broker if an MQTT broker is defined in the configuration. Similary, sending commands over the
# radio network are initiated via an mqtt 'send' topic.
#
# CREDITS:  
# Code here is significntly based on the Domitcz source, specifically the EvohomeRadio.cpp file, by 
# fulltalgoRythm - https://github.com/domoticz/domoticz/blob/development/hardware/EvohomeRadio.cpp
# Also see http://www.automatedhome.co.uk/vbulletin/showthread.php?5085-My-HGI80-equivalent-Domoticz-setup-without-HGI80 
# for info and discussions on homebrew hardware options.
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
import ConfigParser
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import re
import serial                     
import time, datetime
import signal
import json
import re
from collections import namedtuple
from enum import Enum, IntEnum

if  os.path.isdir(sys.argv[0]):
    os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "1.9"
CONFIG_FILE     = "evolistener.cfg"

#------------------------------------- Configs/Default ---------------------------------------------# 
def getConfig(config,section,name,default):
    if config.has_option(section,name):
        return config.get(section,name)
    else:
        return default


# Get any configuration overrides that may be defined in  CONFIG_FILE
# If override not specified, then use the defaults here

config = ConfigParser.RawConfigParser()
config.read(CONFIG_FILE)

COM_PORT          = getConfig(config,"Serial Port","COM_PORT","/dev/ttyUSB0")
COM_BAUD          = int(getConfig(config,"Serial Port","COM_BAUD",115200))
COM_RETRY_LIMIT   = int(getConfig(config,"Serial Port","COM_RETRY_LIMIT",10))

EVENTS_FILE       = getConfig(config,"Files","EVENTS_FILE","events.log")
LOG_FILE          = getConfig(config,"Files","LOG_FILE","evolistener.log")
DEVICES_FILE      = getConfig(config,"Files","DEVICES_FILE","devices.json")
NEW_DEVICES_FILE  = getConfig(config,"Files","NEW_DEVICES_FILE","devices_new.json")

LOG_DROPPED_PACKETS = getConfig(config,"Other","LOG_DROPPED_PACKETS",False)

MQTT_SERVER       = getConfig(config,"MQTT","MQTT_SERVER","")                  # Leave blank to disable MQTT publishing. Messages will still be saved in the various files
MQTT_SUB_TOPIC    = getConfig(config,"MQTT","MQTT_SUB_TOPIC","evohome/listener/send")   # Note to exclude any trailing '/' 
MQTT_PUB_TOPIC    = getConfig(config,"MQTT","MQTT_PUB_TOPIC","evohome/listener") 
MQTT_USER         = getConfig(config,"MQTT","MQTT_USER","") 
MQTT_PW           = getConfig(config,"MQTT","MQTT_PW","") 
MQTT_CLIENTID     = getConfig(config,"MQTT","MQTT_SERVER","evoListener")

CONTROLLER_ID     = getConfig(config,"SENDER", "CONTROLLER_ID", "01:139901")
THIS_GATEWAY_ID   = getConfig(config,"SENDER", "THIS_GATEWAY_ID","30:999999")
THIS_GATEWAY_NAME = getConfig(config,"SENDER", "THIS_GATEWAY_NAME","EvoGateway")

MAX_LOG_HISTORY   = 5

EMPTY_DEVICE_ID   = "--:------"

#---------------------------------------- 
class TwoWayDict(dict):
    def __len__(self):
        return dict.__len__(self) / 2
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)

#---------------------------------------- 
DEVICE_TYPE = TwoWayDict()
DEVICE_TYPE["01"] = "CTL"
DEVICE_TYPE["02"] = "UFH"
DEVICE_TYPE["04"] = "TRV"
DEVICE_TYPE["07"] = "DHW"
DEVICE_TYPE["13"] = "BDR" 
DEVICE_TYPE["30"] = "GWAY"
DEVICE_TYPE["34"] = "STAT"
# Type 30 is a Mobile Gateway such as RGS100

CONTROLLER_MODES = {0: "Auto", 1: "Heating Off", 2: "Eco-Auto", 3: "Away", 4: "Day Off", 7:"Custom"} # 0=auto, 1= off, 2=eco, 4 = day off, 7 = custom

#-------------------------------------------- 

class Message(): 
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

    self.command      = rawmsg[41:45].upper()       # command code hex
    self.command_name = self.command                # needs to be assigned outside, as we are doing all the processing outside of this class/struct
    try:
      self.payload_length= int(rawmsg[46:49])          # Note this is not HEX...
    except Exception as e:
      print ("Error instantiating Message class on line '{}': {}. Raw msg: '{}'. length = {}".format(sys.exc_info()[-1].tb_lineno, str(e), rawmsg, len(rawmsg)))
      self.payload_length=0
    
    self.payload      = rawmsg[50:]
    self.failed_decrypt= "_ENC" in rawmsg or "_BAD" in rawmsg or "BAD_" in rawmsg or "ERR" in rawmsg

  def _initialise_device_names(self):
    if self.source_type == DEVICE_TYPE['CTL'] and self.is_broadcast(): 
      self.destination_name = "CONTROLLER"
      self.source_name = "CONTROLLER"
    elif DEVICE_TYPE[self.source_type] and devices[self.source]['name']:
      self.source_name = "{} {}".format(DEVICE_TYPE[self.source_type], devices[self.source]['name'])      # Get the device's actual name if we have it
    else:
        print("Could not find device type '{}' or name for device '{}'".format(self.source_type, self.source))
    if self.destination_name != "CONTROLLER" and devices[self.destination] and devices[self.destination]['name']:
      if self.destination_type == DEVICE_TYPE['CTL']:
        self.destination_name = "CONTROLLER"
      else:
        device_name = devices[self.destination]['name'] if devices[self.destination] else self.destination
        self.destination_name = "{} {}".format(DEVICE_TYPE[self.destination_type], devices[self.destination]['name'])      # Get the device's actual name if we have it


  def is_broadcast(self):
    return self.source == self.destination


#-------------------------------------------- General Functions  -----------------------------------
def sig_handler(signum, frame):              # Trap Ctl C
    display_and_log("Tidying up and exiting...")
    file.close(logfile)
    if serial_port.is_open:
      serial_port.close()                   

#--------------------------------------------
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

#--------------------------------------------
first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')

def to_snake(name):
  name=name.strip().replace("'","").replace(" ","_")
  s1 = first_cap_re.sub(r'\1_\2', name)
  s2 = all_cap_re.sub(r'\1_\2', s1).lower()
  return s2.replace("__","_")

#--------------------------------------------
def to_camel_case(s):
  return re.sub(r'(?!^) ([a-zA-Z])', lambda m: m.group(1).upper(), s)

#--------------------------------------------
def get_dtm_from_packed_hex(dtm_hex):
  dtm_mins = int(dtm_hex[0:2],16)
  dtm_hours = int(dtm_hex[2:4],16)
  dtm_day = int(dtm_hex[4:6],16)
  dtm_month = int(dtm_hex[6:8],16)
  dtm_year = int(dtm_hex[8:12],16)
  return datetime.datetime(year=dtm_year,month=dtm_month, day=dtm_day,hour=dtm_hours,minute=dtm_mins)


#--------------------------------------------
def display_data_row(msg, display_text, ref_zone=-1, suffix_text=""):
  destination = "BROADCAST MESSAGE" if msg.is_broadcast() else msg.destination_name
  if ref_zone >-1 and zones[ref_zone]:
    zone_name = "@ {:<20}".format(zones[ref_zone]) if ref_zone in zones else "                      "
    display_row = "{:<2}| {:<21} -> {:<21} | {:>5} {} [Zone {:<2}] {}".format(
      msg.msg_type, msg.source_name, destination, display_text, zone_name, ref_zone, suffix_text)
  else:
    display_row = "{:<2}| {:<21} -> {:<21} | {:>5} {}".format(msg.msg_type, msg.source_name, destination, display_text, suffix_text)
  display_and_log(msg.command_name, display_row)

#--------------------------------------------
def display_and_log(source="-", displayMessage=""):
  try:
    global eventfile
    if os.path.getsize(EVENTS_FILE) > 5000000:
      eventfile.close()
      rotate_files(EVENTS_FILE)
      eventfile = open(EVENTS_FILE,"a")
    row = "{} {:<20}| {}".format(datetime.datetime.now().strftime("%Y-%m-%d %X"), source, displayMessage)
    print (row)
    # print   (datetime.datetime.now().strftime("%Y-%m-%d %X") + ": " + "{:<20}".format(str(source)) + ": " + str(displayMessage))
    eventfile.write(row.strip() + "\n")
    file.flush(eventfile)
  except Exception as e:
    print (str(e))
    pass

#--------------------------------------------
def log(logentry):
  global logfile
  if os.path.getsize(LOG_FILE) > 10000000:
      logfile.close()
      rotate_files(LOG_FILE)
      logfile = open(LOG_FILE,"a")

  logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %X") + ": " + logentry.strip() + "\n")
  file.flush(logfile)

# MQTT Functions ---------------------------------
def initialise_mqtt_client(mqtt_client):
  mqtt_client.username_pw_set(MQTT_USER, MQTT_PW)
  mqtt_client.on_connect = mqtt_on_connect
  mqtt_client.on_message = mqtt_on_message
  # mqtt_client.on_log = mqtt_on_log
  mqtt_client.on_disconnect = mqtt_on_disconnect

  print("[INFO ] Connecting to mqtt server %s" % MQTT_SERVER)
  mqtt_client.connect(MQTT_SERVER, port=1883, keepalive=0, bind_address="")

  print("[INFO ] Subscribing to mqtt topic '%s'" % MQTT_SUB_TOPIC)
  mqtt_client.subscribe(MQTT_SUB_TOPIC)
  mqtt_client.loop_start()

def mqtt_on_connect(client, userdata, flags, rc):
    ''' mqtt connection event processing '''

    if rc == 0:
        client.connected_flag = True #set flag
        print("[INFO ] MQTT connection established with broker")
    else:
        print("[INFO ] MQTT connection failed (code {})".format(rc))
        if DEBUG:
            print("[DEBUG] mqtt userdata: {}, flags: {}, client: {}".format(userdata, flags, client))


def mqtt_on_disconnect(client, userdata, rc):
    ''' mqtt disconnection event processing '''

    client.loop_stop()
    if rc != 0:
        print("Unexpected disconnection.")
        if DEBUG:
            print("[DEBUG] mqtt rc: {}, userdata: {}, client: {}".format(rc, userdata, client))


def mqtt_on_log(client, obj, level, string):
    ''' mqtt log event received '''
    if DEBUG:
        print("[DEBUG] MQTT log message received. Client: {}, obj: {}, level: {}".format(client, obj, level))
    print("[DEBUG] MQTT log msg: {}".format(string))


def mqtt_on_message(client, userdata, msg):
    ''' mqtt message received on subscribed topic '''
    json_data = json.loads(str(msg.payload))

    # display_and_log("MQTT_SUB", json_data)
    if "command" in json_data:
        command = json_data["command"]
        args = json_data["arguments"] if "arguments" in json_data else ""
        send_mode = json_data["send_mode"] if "send_mode" in json_data else None
        # display_and_log("MQTT_SUB","command: {}, args: {}, send mode: {}".format(command, args, send_mode))
        send_queue.append([command, args, send_mode])


def mqtt_publish(device,command,msg):
  if MQTT_SERVER > "":
    try:
      mqtt_auth={'username': MQTT_USER, 'password':MQTT_PW}
      topic = MQTT_PUB_TOPIC + "/" + to_snake(device) + "/" + command.strip()
      publish.single(topic, str(msg).strip(), hostname=MQTT_SERVER, auth=mqtt_auth,client_id=MQTT_CLIENTID,retain=True)
      # print("published to mqtt topic {}: {}".format(topic, msg))
    except Exception as e:
      print(str(e))
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
    msgs.append (("{}/$name".format(topic_base), "Evohome Listener/Sender Gateway {}".format(THIS_GATEWAY_ID), 0, True))
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
    # mqtt_auth={'username': MQTT_USER, 'password':MQTT_PW}
    # publish.multiple(msgs, hostname=MQTT_SERVER, port=1883, client_id="test",keepalive=60, auth=mqtt_auth)
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

#-------------------------------------------- Evohome Functions ------------------------------------
def extract_zone_details(payload):
  zone_id = int(msg.payload[0:2],16) 
  if zone_id < 12:
    zone_id += 1
  zone_name = zones[zone_id] if zone_id in zones else "Zone {}".format(zone_id)
  return zone_id, zone_name


def bind(msg):
  # display_and_log(msg.command_name, "BIND command received")
  pass

#--------------------------------------------
def sync(msg):
  # display_and_log(msg.command_name,"SYNC command received")
  pass

#--------------------------------------------
def schedule_sync(msg):
  pass
  # The 0x0006 command is a schedule sync message sent between the gateway to controller to check whether there have been any changes since the last exchange of schedule information
  # https://www.automatedhome.co.uk/vbulletin/showthread.php?5085-My-HGI80-equivalent-Domoticz-setup-without-HGI80/page13

#--------------------------------------------
def zone_name(msg):
  # display_and_log(msg.command_name,"NAME request received")
  pass

#--------------------------------------------
def setpoint_ufh(msg):
  # Not 100% sure what this command is. First 2 digits seem to be ufh controller zone, followed by 4 digits which appear to be for the 
  # zone's setpoint, then 0A2801. 
  # Pattern is repeated with any other zones that the ufh controller may have.
 
  i = 0
  while (i < (msg.payload_length *2)):
    zone_id, zone_name = extract_zone_details(msg.payload[0+i:2+i])
    # We add 900 to identify UFH zone numbers
    zone_setpoint = float(int(msg.payload[2+i:6+i],16))/100
    # zone_name =""
    for d in devices:
      if devices[d].get('ufh_zoneId') and zone_id == devices[d]['ufh_zoneId']:
        zone_id = devices[d]["zoneId"]
        zone_name = devices[d]["name"]
        break
    if zone_name == "":
      display_and_log("DEBUG","UFH Setpoint Zone " + str(zone_id) + " name not found")    
      zone_name = "UFH Zone " + str(zone_id)

    display_and_log(msg.command_name,'{0: <22}'.format(zone_name) + '{:>5}'.format(str(zone_setpoint)) + "  [Zone UFH " + str(zone_id) + "]")
    mqtt_publish(zone_name, "setpoint",zone_setpoint)

    i += 12

#--------------------------------------------
def setpoint(msg):
  if msg.payload_length % 3 != 0:
    display_and_log(msg.command_name, msg.source + " - command error - invalid length: " + msg.rawmsg)
  else:
    command_name_suffix =""
    if msg.payload_length > 3:
        command_name_suffix = "_CTL"
        msg.command_name = "{}_CTL".format(msg.command_name)
    i = 0
    while (i < msg.payload_length):
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

#--------------------------------------------
def setpoint_override(msg):
  if msg.payload_length != 7 and msg.payload_length != 13:
    display_and_log(msg.command_name, msg.source + ": ERROR - invalid length: " + msg.rawmsg)
  else:
    zone_id, zone_name = extract_zone_details(msg.payload[0:2])
    new_setpoint = float(int(msg.payload[2:4],16) << 8 | int(msg.payload [4:6],16))/100

    #!!TODO!! Trap for 0x7FFF - this means setpoint not set
    if msg.payload_length == 13: # We have an 'until' date
      dtm_hex=msg.payload[14:]
      dtm = get_dtm_from_packed_hex(dtm_hex)
      until = " - Until " + str(dtm)
      mqtt_publish(zone_name, "scheduleMode", "Temporary")
      mqtt_publish(zone_name, "scheduleModeUntil", dtm.strftime("%Y-%m-%dT%XZ"))
    else:
      until =""
      mqtt_publish(zone_name, "scheduleMode", "Scheduled")
      mqtt_publish(zone_name, "scheduleModeUntil", "")
    display_data_row(msg, "{:5.2f}°C{}".format(new_setpoint), zone_id, until)
    mqtt_publish(zone_name, "setpointOverride",new_setpoint)

#--------------------------------------------
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
  
#--------------------------------------------
def window_status(msg):
  if msg.payload_length < 3:
    display_and_log(msg.command_name, msg.source + ": Error - invalid msg.payload length (" + msg.payload_length + "): " + msg.rawmsg)
  else:
    # Zone is first 2, then window status. 3rd pair seems to be always zero apparently
    zone_id, zone_name = extract_zone_details(msg.payload[0:2])
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

#--------------------------------------------
def other_command(msg):
  display_and_log(msg.command_name, msg.source + " - " + ". MSG: " + msg.rawmsg)

#--------------------------------------------
def date_request(msg):
  display_data_row(msg, "Ping/Datetime Sync", zone_id)
  # display_and_log(msg.command_name, msg.source + " - " + ". MSG: " + msg.rawmsg)

#--------------------------------------------
def relay_heat_demand(msg):
  # Heat demand sent by the controller for CH / DHW / Boiler  (F9/FA/FC)
  if msg.payload_length != 2:
    display_and_log(msg.command_name, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
  else:
    typeId = int(msg.payload[0:2],16)
    demand = int(msg.payload[2:4],16)
    if typeId <12:
      typeId +=1

    if msg.source_type == DEVICE_TYPE['UFH']:
      device_type = "UFH"
      topic="RLY UFH"      
    elif typeId == 0xfa:  # Depends on whether it is main controller sending message, or UFH controller
      device_type = "BDR DHW Relay"
      topic="DHW"      
    elif typeId == 0xfc:    # Boiler relay or possibly broadcast
      device_type = "BDR Boiler Relay"
      topic = "BDR Boiler Relay"
    elif typeId == 0xf9:  # Radiator circuit zone valve relay
      device_type = "BDR Radiators Relay"
      topic="BDR Radiators Relay"
    elif typeId == 0xc: # Electric underfloor relay
        device_type = "RLY Elec UFH Relay"
        topic="RLY UFH"
    else:
      device_type = str(hex(typeId)) 
      topic = "RLY " + device_type

    demand_percentage = float(demand)/200*100
    display_data_row(msg, "{:<6.1f}% @ {}".format(demand_percentage, device_type))
    mqtt_publish(topic,"heat_demand",demand_percentage)
    
#--------------------------------------------
def zone_heat_demand(msg):
  if msg.payload_length % 2 != 0 :
    display_and_log(msg.command_name, msg.source + ": ERROR - invalid msg.payload length: " + str(msg.rawmsg))
  else:
    topic = ""
    i = 0
    while (i < (msg.payload_length *2)):
      # try:
        zone_id, zone_name = extract_zone_details(msg.payload[0:2])
        demand = int(msg.payload[2+i:4+i],16)
        
        zone_label = "Zone"
        if msg.source_type == DEVICE_TYPE['UFH']:
          if msg.source == msg.device3 or zone_id == 0xfc: # Messages from the UFH controller to itself seem always to have zone Id 252 (0xfc) - i.e. relay
            device_type = "RLY UFH"
            topic = "RLY UFH"
          elif zone_id <=8: # 
            # if destination device is the main touch controller, then then zone Id is that of the matched zone in the main touch controller itself
            # Otherwise, if the destination device is the ufh controller (i.e. message is to itself/broadcast etc), then the zone Id is the ufh controller zone id (i.e. 1 to 8)

            if msg.device3_type == DEVICE_TYPE['CTL']:
              device_type = "UFH " + zone_name.split(' ', 1)[1] 
              topic = zone_name

            elif msg.device3_type == msg.source_type: # destination should be the ufh controller itself (i.e. message to itlself). zone_id should be ufh zone
              for d in devices:
                if devices[d].get('ufh_zoneId') and zone_id == devices[d]['ufh_zoneId']:
                  # display_and_log("DEBUG","UFH Zone matched to " + devices[d]["name"])    
                  zone_id = devices[d]["zoneId"]
                  zone_name = devices[d]["name"]
                  device_type = "UFH " + zone_name.split(' ', 1)[1] 
                  topic = zone_name
                  break
            else:
              display_and_log("ERROR","UFH message received, but destination is neither main controller nor UFH controller. destination = ", msg.device3 + ", destination_type = " + 
                msg.device3_type + ", MSG: " + msg.rawmsg) 
          zone_label = "UFH Zone"
        elif zone_id == 0xfc:
              device_type = "Boiler Relay"
              topic = "BDR Boiler"
        elif zone_id == 0xfa:
          device_type = "DHW Relay"
          topic="DHW"
        elif zone_id == 0xf9:
          device_type = "Radiators Relay"  
          topic="BDR Radiators Relay"
        # elif msg.source_type == DEVICE_TYPE['UFH']: # TODO.... MAP zones....
        #   device_type = "UFH Controller"  
        #   topic="UFH Zone"   
        else:
          device_type = str(hex(zone_id))
          topic = "{}/{}".format(zone_name, msg.source_name)

        if "unknown" in zone_name.lower():
          display_and_log("ERROR", "Unknown zone name in zone_heat_deman with zone ID {}".format(zone_id))

        demand_percentage = float(demand)/200*100
        display_data_row(msg, "{:5.1f}% ".format(demand_percentage), zone_id)
        if len(topic) > 0:
          mqtt_publish(topic,"heat_demand",demand_percentage)
        else:
          display_and_log("DEBUG", "ERROR: Could not post to MQTT as topic undefined")
        i += 4

#--------------------------------------------
def dhw_settings(msg):
  #  <1:DevNo><2(uint16_t):SetPoint><1:Overrun?><2:Differential>
  display_and_log(msg.command_name, msg.rawmsg)

#--------------------------------------------
def actuator_check_req(msg):
  # this is used to synchronise time periods for each relay bound to a controller 
  # i.e. all relays get this message and use it to determine when to start each cycle (demand is just a % of the cycle length)
  if msg.payload_length != 2:
    display_and_log(msg.command_name, msg.source + ": Error decoding Actuator Check - invalid msg.payload length: " + msg.rawmsg)
  else:
    device_number = int(msg.payload[0:2],16) 
    demand = int(msg.payload[2:4],16)
    # if device_number == 0xfc: # 252 - apparently some sort of broadcast?
    #   device_type = ": Status Update Request"
    # else:
    #   device_type =""
    status = "Actuator time period sync request: {}".format(device_number)

    display_data_row(msg, status)
    
#--------------------------------------------
def actuator_state(msg):
  if msg.payload_length == 1 and msg.msg_type == "RQ":
    display_data_row(msg, "State update request")
  elif msg.payload_length != 3:    
    display_and_log(msg.command_name, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
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
    mqtt_publish(msg.source_name,"actuator_status",status)

#--------------------------------------------
def dhw_state(msg):
  if msg.payload_length != 6 and msg.payload_length != 12:
    display_and_log(msg.command_name, "{}: ERROR - invalid msg.payload length: {}".format(msg.source, msg.rawmsg))
  else:
    zone_id, _ = extract_zone_details(msg.payload[0:2])
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
        mqtt_publish("DHW","until",dtm)
      else:
        mqtt_publish("DHW","until","")

#--------------------------------------------
def dhw_temperature(msg):
  temperature = float(int(msg.payload[2:6],16))/100
  display_data_row(msg, "%6.2f" % temperature)
  mqtt_publish("DHW", "temperature", temperature)

#--------------------------------------------
def zone_info(msg):
  # display_and_log(msg.command_name, msg.source + " - MSG: " + msg.rawmsg)
  pass

#--------------------------------------------
def device_info(msg):
  #display_and_log(msg.command_name, msg.source + " - MSG: " + msg.rawmsg)
  pass

#--------------------------------------------
def battery_info(msg):
  if msg.payload_length != 3:
    display_and_log(msg.command_name, msg.source + ": Decoding error - invalid msg.payload length: " + msg.rawmsg)
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
    # display_and_log(msg.command_name, msg.source + '{:>5}'.format(str(battery)) + "%" + warning)
    mqtt_publish("{}/{}".format(zones[zone_id], msg.source_name),"battery",battery)

#--------------------------------------------
def controller_mode(msg):
  if msg.payload_length != 8:
    display_and_log(msg.command_name, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
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

#--------------------------------------------
def heartbeat(msg):
  display_and_log(msg.command_name, msg.source + " - MSG: " + msg.rawmsg)

#--------------------------------------------
def external_sensor(msg):
  display_and_log(msg.command_name, msg.source + " - MSG: " + msg.rawmsg)

#--------------------------------------------
def unknown_command(msg):
  display_and_log(msg.command_name, msg.source + " - MSG: " + msg.rawmsg)

#-------------------------------------------- Evohome Send Command Functions
def get_controller_mode_payload(mode_id, until_string=None):
    payload_length = 8
    if until_string == None:
        duration_code = 0x0
        until = "FFFFFFFFFFFF"
    else:
        duration_code = 0x1
        until = dtm_string_to_payload(until_string)

    payload = "{:02X}{}{:02X}".format(mode_id, until ,duration_code)
    return payload, payload_length

#--------------------------------------------
def get_dhw_state_payload(state_id, until_string=None, mode_id=-1):
    # state_id is 0 or 1 for DHW on/off
    if until_string == None:
        payload_length = 6
        until = ""
        if mode_id == -1: 
            mode_id = 0 # Revert to auto
    else:
        payload_length = 12
        until = dtm_string_to_payload(until_string)
        mode_id = 4 # if we have an 'until', mode must be temporary
    zone_id = 0
    payload = "{:02X}{:02x}{:02X}FFFFFF{}".format(zone_id, state_id, mode_id, until)
    return payload, payload_length

def get_setpoint_override_payload(zone_id, setpoint, until_string=""):
    #  
    # modes:  [Auto, -1, Permanent, -1, Temporary] (zero based)
    #  
    if until_string:
        payload_length = 13        
        until = dtm_string_to_payload(until_string)
        mode = 4 
    elif setpoint >0:
        payload_length = 7
        mode = 2
        until = ""
    else:
        # If setpoint is 0, we revert back to auto
        payload_length = 7
        mode = 0
        until = ""

    payload = "{:02X}{:04X}{:02X}FFFFFF{}".format(zone_id - 1, int(setpoint * 100), mode, until)

    return payload, payload_length

def dtm_string_to_payload(dtm_string):
    dtm = datetime.datetime.strptime(dtm_string, "%Y-%m-%dT%H:%M:%SZ")
    payload = "{:02X}{:02X}{:02X}{:02X}{:04X}".format(dtm.minute, dtm.hour, dtm.day, dtm.month, dtm.year)
    return payload

def process_command(command, args, serial_port, send_mode = "I"):
    command_code = COMMAND_CODES[command] if command in COMMAND_CODES else 0x0     
    if command_code == 0x0:
        display_and_log("ERROR","Unrecognised command '{}'".format(command))
        return

    send_string = ""
    
    payload_length = -1
    payload = ""

    if command == "dhw_state":
        # 1F41: Change dhw state
        state_id = args["state_id"]
        until = args["until"] if "until" in args else None
        mode_id = args["mode_id"] if "mode_id" in args else -1
        payload, payload_length = get_dhw_state_payload(state_id, until, mode_id)
        if send_mode is None:
            send_mode = "W"     
    elif command in "date_request ping":
        # 0x313F: Send a datetime update request, i.e. like a ping
        payload_length = 1
        payload = "00"
        if send_mode is None:
          send_mode = "RQ"
    elif command == "controller_mode":
        # 0x2E04: Set controller mode 
        mode = args["mode"]
        until = args["until"] if "until" in args else None
        payload, payload_length = get_controller_mode_payload(mode, until)
        
        # Send mode needs to be 'W' to set the controller to the new controller mode
        if send_mode is None:
            send_mode = "W"     
    elif command == "setpoint_override":
        # 0x2349: Setpoint override
        zone_id = args["zone_id"]
        setpoint = args["setpoint"]
        until = args["until"] if "until" in args else None
        payload, payload_length = get_setpoint_override_payload(zone_id, setpoint, until)
        if send_mode is None:
            send_mode = "W"     

    if payload_length > -1 and payload:
        send_string = "{} --- {} {} {} {:04X} {:03d} {}".format(send_mode, THIS_GATEWAY_ID, CONTROLLER_ID, EMPTY_DEVICE_ID, command_code, payload_length, payload)
        display_and_log("COMMAND_MSG", send_string)
        byte_command = bytearray(b'{}\r\n'.format(send_string))
        response = serial_port.write(byte_command)
        
    else:
        display_and_log("ERROR","Invalid payload '{}'/payload length '{}'".format(payload, payload_length))


#-------------------------------------------- Evohome Commands Dict
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
  '10e0': heartbeat,
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
  "actuator_check_req" : 0x3B00,
  "actuator_state" : 0x3EF0,
  "battery_info" : 0x1060,
  "bind" : 0x1FC9,
  "controller_mode" : 0x2E04,
  "date_request" : 0x313F,
  "device_info" : 0x0418,
  "dhw_state" : 0x1F41,
  "dhw_temperature" : 0x1260,
  "external_sensor": 0x0002,
  "heartbeat" : 0x10e0,
  "other_command" : 0x0100,  
  "ping" : 0x313F,
  "relay_heat_demand" : 0x0008,
  "setpoint" : 0x2309,
  "setpoint_override" : 0x2349,
  "setpoint_ufh" : 0x22C9,
  "sync" : 0x1F09,
  "window_status" : 0x12B0,
  "zone_heat_demand" : 0x3150,
  "zone_info" : 0x000A,
  "zone_name": 0x0004, 
  "zone_temperature" : 0x30C9
}

#-------------------------------- Main ----------------------------------------
rotate_files(LOG_FILE)
rotate_files(EVENTS_FILE)
logfile = open(LOG_FILE, "a")
eventfile = open(EVENTS_FILE,"a")

signal.signal(signal.SIGINT, sig_handler)    # Trap CTL-C etc


#-------------------------------------------------------------------------------

comConnected = False 
while (COM_RETRY_LIMIT > 0 and not comConnected):
  try:
    serial_port = serial.Serial(COM_PORT)  
    serial_port.baudrate = COM_BAUD                
    serial_port.bytesize = 8              
    serial_port.parity   = 'N'            
    serial_port.stopbits = 1              
    serial_port.timeout = 1               

    comConnected = True
  except Exception as e:
    if COM_RETRY_LIMIT >1:
      display_and_log("COM_PORT ERROR",repr(e) + ". Retrying in 5 seconds")
      time.sleep(5)
      COM_RETRY_LIMIT -= 1
    else:
      display_and_log("COM_PORT ERROR","Error connecting to COM port " + COM_PORT + ". Giving up...")

if not comConnected:
  sys.exit()

display_and_log("","\n")
display_and_log("","Evohome listener version " + VERSION )
display_and_log("","Connected to COM port " + COM_PORT)  

logfile.write("")
logfile.write("-----------------------------------------------------------\n")


if os.path.isfile(DEVICES_FILE):
  with open(DEVICES_FILE, 'r') as fp:
    devices = json.load(fp)             # Get a list of known devices, ideally with their zone details etc
else:
  devices = {}
# Add this server/gateway as a device, but using dummy zone ID for now
devices[THIS_GATEWAY_ID] = { "name" : THIS_GATEWAY_NAME, "zoneId": 249, "zoneMaster": True }

zones = {}                            # Create a seperate collection of Zones, so that we can look up zone names quickly
send_queue = []

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

#---------------- Main loop ---------------------
while serial_port.is_open:
  try:
    # if serial_port.is_open:
    if send_queue:
      command_code, args, send_mode = send_queue.pop()
      process_command(command_code, args, serial_port, send_mode)
      if send_queue:
        display_and_log("DEBUG", "---> send_queue remaining: {}".format(send_queue))

    data = serial_port.readline().strip()        # Wait and read data
    if data:                         # Only proceed if line read before timeout 
        log(data)
          
        # print(data)
        if not ("_ENC" in data or "_BAD" in data or "BAD_" in data or "ERR" in data) and len(data) > 40:          #Make sure no obvious errors in getting the data.... 
          prev_data_had_errors = False
          if not data.startswith("---"):
            # Echos of commands sent by us come back without the --- prefix. Noticed on the fifo firmware that sometimes the request type prefix seems to be messed up. Workaround for this...
            if data.strip().startswith("W---"):
              data = data[1:]
            else:
              data = "---  {}".format(data) if len(data.split(" ",1)[0]) <2 else "--- {}".format(data)
            

          if data_pattern.match(data):
              msg = Message(data)

              # Check if device is known...
              if not msg.source in devices:
                display_and_log("NEW DEVICE FOUND", msg.source)
                devices.update({msg.source : {"name" : msg.source, "zone_id" : -1, "zoneMaster" : False  }})
                with open(NEW_DEVICES_FILE,'w') as fp:
                  fp.write(json.dumps(devices, sort_keys=True, indent=4))
                fp.close()
              
              if msg.command in COMMANDS:
                try:
                  msg.command_name = COMMANDS[msg.command].__name__.upper() # Get the name of the command from our list of commands
                  COMMANDS[msg.command](msg)
                  log('{0: <18}'.format(msg.command_name) + " " + data) 
                except Exception as e:
                  display_and_log("ERROR",msg.command_name + ": " + repr(e) + ": " + data)

              else:
                display_and_log("UNKNOWN COMMAND", msg.source + ": Command code '" + msg.command + "'. MSG: " + data.strip())
                log("UNKNOWN COMMAND: " + data)
          else:
            display_and_log("ERROR","Pattern match failed on received data: '{}'".format(data))
        else:
          if not prev_data_had_errors and LOG_DROPPED_PACKETS:
            prev_data_had_errors = True
            display_and_log("ERROR","--- Message dropped: packet error from hardware/firmware")
          log(data)
          

        file.flush(logfile)

  except KeyboardInterrupt:
    if serial_port.is_open:
      serial_port.close()                   
    comConnected = False

mqtt_client.loop_stop()


  # except Exception as e:
  #   if serial_port.is_open:
  #     serial_port.close()
  #   print ("Evohome listener stopped")                   
  #   pass



