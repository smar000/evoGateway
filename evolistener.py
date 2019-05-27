# Evohome Listener
# Copyright (c) 2018 SMAR info@smar.co.uk
#  
# Tested with Python 2.7.12. Requires:
# - pyserial (python -m pip install pyserial)
# - paho (pip install paho-mqtt)
#
# 
#
# Simple Evohome 'listener', for listening in on the messages between evohome devices using an arudino + CC1101 radio receiver 
# (other hardware options also possible - see credits below).
# Messages are interpreted and then posted to an mqtt broker if an MQTT broker is defined in the configuration.
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
import re
import serial                     
import time, datetime
import signal
import json


os.chdir(os.path.dirname(sys.argv[0]))

#---------------------------------------------------------------------------------------------------
VERSION         = "0.8.5"
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

COM_PORT         = getConfig(config,"Serial Port","COM_PORT","/dev/ttyUSB0")
COM_BAUD         = int(getConfig(config,"Serial Port","COM_BAUD",115200))
COM_RETRY_LIMIT  = int(getConfig(config,"Serial Port","COM_RETRY_LIMIT",10))

EVENTS_FILE      = getConfig(config,"Files","EVENTS_FILE","events.log")
LOG_FILE         = getConfig(config,"Files","LOG_FILE","evolistener.log")
DEVICES_FILE     = getConfig(config,"Files","DEVICES_FILE","devices.json")
NEW_DEVICES_FILE = getConfig(config,"Files","NEW_DEVICES_FILE","devices_new.json")

MQTT_SERVER      = getConfig(config,"MQTT","MQTT_SERVER","")                  # Leave blank to disable MQTT publishing. Messages will still be saved in the various files
MQTT_TOPIC_BASE  = getConfig(config,"MQTT","MQTT_TOPIC","evohome/listener")   # Note to exclude any trailing '/' 
MQTT_USER        = getConfig(config,"MQTT","MQTT_USER","") 
MQTT_PW          = getConfig(config,"MQTT","MQTT_PW","") 
MQTT_CLIENTID    = getConfig(config,"MQTT","MQTT_SERVER","evoListener")

MAX_LOG_HISTORY  = 5

#---------------------------------------- 
CONTROLLER_MODES = {0: "Auto", 2: "Eco-Auto", 3: "Away", 4: "Day Off",7:"Custom", 1: "Heating Off"} # 0=auto, 1= off, 2=eco, 4 = day off, 7 = custom
DEV_TYPE_CONTROLLER = "01"
DEV_TYPE_UFH        = "02"
DEV_TYPE_TRV        = "04"
DEV_TYPE_DHWSENDER  = "07"
DEV_TYPE_RELAY      = "13"
DEV_TYPE_THERMOSTAT = "34"

#-------------------------------------------- Classes           -----------------------------------

class Message(): # Using this more to have a C type struct for passing the message around than anything else at this stage
  def __init__(self,rawmsg):
    self.rawmsg       = rawmsg.strip()
    self.sourceId     = rawmsg[11:20] 

    self.msgType      = rawmsg[4:6] 
    self.source       = rawmsg[11:20]               # device 1 - This looks as if this is always the source; Note this is overwritten with name of device 
    self.sourceType   = rawmsg[11:13]                # the first 2 digits seem to be identifier for type of device
    self.device2      = rawmsg[21:30]               # device 2 - seen this on actuactor check requests only so far (TODO!! Look into this further at some stage)
    self.device2Type  = rawmsg[21:23]               # device 2 type
    self.destination  = rawmsg[31:40]               # device 3 - Looks as if this is always the destination
    self.destinationType = rawmsg[31:33]
    self.command      = rawmsg[41:45].upper()       # command code hx
    self.commandName  = self.command                # needs to be assigned outside, as we are doing all the processing outside of this class/struct
    try:
      self.payloadLength= int(rawmsg[46:49])          # Note this is not HEX...
    except Exception as e:
      print (str(e))
      self.payloadLength=0
    self.payload      = rawmsg[50:]

    self.failedDecrypt= "_ENC" in rawmsg or "_BAD" in rawmsg or "BAD_" in rawmsg or "ERR" in rawmsg

#-------------------------------------------- General Functions  -----------------------------------
def sigHandler(signum, frame):              # Trap Ctl C
    display ("Tidying up and exiting...")
    file.close(logfile)
    if serialPort.is_open:
      serialPort.close()                   

#--------------------------------------------
def rotateFiles(baseFileName):
  # try:
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

    # if os.path.isfile(baseFileName + ".2"):
    #   os.remove(baseFileName + ".2")
    # if os.path.isfile(baseFileName + ".1"):
    #   os.rename(baseFileName + ".1", baseFileName + ".2")
    # if os.path.isfile(baseFileName):
    #   os.rename(baseFileName, baseFileName + ".1")
  # except Exception as e:
  #   print ("Error rotating base file '" + baseFileName + "'")
  #   print (str(e))

#--------------------------------------------
first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')

def toSnake(name):
  name=name.strip().replace("'","").replace(" ","_")
  s1 = first_cap_re.sub(r'\1_\2', name)
  s2 = all_cap_re.sub(r'\1_\2', s1).lower()
  return s2.replace("__","_")

#--------------------------------------------
def display(source="-", displayMessage=""):
  try:
    global eventfile
    if os.path.getsize(EVENTS_FILE) > 5000000:
      eventfile.close()
      rotateFiles(EVENTS_FILE)
      eventfile = open(EVENTS_FILE,"a")

    print (datetime.datetime.now().strftime("%Y-%m-%d %X") + ": " + "{:<20}".format(str(source)) + ": " + str(displayMessage))
    eventfile.write(datetime.datetime.now().strftime("%Y-%m-%d %X") +  " {:<20}".format(str(source)) + ": " + str(displayMessage).strip() + "\n")
    file.flush(eventfile)
  except Exception as e:
    print (str(e))
    pass

#--------------------------------------------
def log(logentry):
  global logfile
  if os.path.getsize(LOG_FILE) > 10000000:
      logfile.close()
      rotateFiles(LOG_FILE)
      logfile = open(LOG_FILE,"a")

  logfile.write(datetime.datetime.now().strftime("%Y-%m-%d %X") + ": " + logentry.strip() + "\n")
  file.flush(logfile)

#--------------------------------------------
def postToMqtt(device,command,msg):
  if MQTT_SERVER > "":
    try:
      mqtt_auth={'username': MQTT_USER, 'password':MQTT_PW}
      topic = MQTT_TOPIC_BASE + "/" + toSnake(device) + "/" + command.strip()
      publish.single(topic, str(msg).strip(), hostname=MQTT_SERVER, auth=mqtt_auth,client_id=MQTT_CLIENTID,retain=True)
    except Exception as e:
      print(str(e))
      pass  

#--------------------------------------------
def toCamelCase(s):
  return re.sub(r'(?!^) ([a-zA-Z])', lambda m: m.group(1).upper(), s)


#-------------------------------------------- Evohome Commands Functions ------------------------------------

def bind(msg):
  # display (msg.commandName, "BIND command received")
  pass

#--------------------------------------------
def sync(msg):
  # display(msg.commandName,"SYNC command received")
  pass

#--------------------------------------------
def zone_name(msg):
  # display(msg.commandName,"NAME request received")
  pass

#--------------------------------------------
def setpoint_ufh(msg):
  # Not 100% sure what this command is. First 2 digits seem to be ufh controller zone, followed by 4 digits which appear to be for the 
  # zone's setpoint, then 0A2801. 
  # Pattern is repeated with any other zones that the ufh controller may have.

 
  i = 0
  while (i < (msg.payloadLength *2)):
    # try:
      zoneId = int(msg.payload[0+i:2+i],16) # We add 900 to identify UFH zone numbers
      zoneSetpoint = float(int(msg.payload[2+i:6+i],16))/100
      zone_name =""
      for d in devices:
        if devices[d].get('ufh_zoneId') and zoneId == devices[d]['ufh_zoneId']:
          zoneId = devices[d]["zoneId"]
          zone_name = devices[d]["name"]
          break
      if zone_name == "":
        display("DEBUG","UFH Setpoint Zone " + str(zoneId) + " name not found")    
        zone_name = "UFH Zone " + str(zoneId)

      display(msg.commandName,'{0: <22}'.format(zone_name) + '{:>5}'.format(str(zoneSetpoint)) + "  [Zone UFH " + str(zoneId) + "]")
      postToMqtt(zone_name, "setpoint",zoneSetpoint)

      i += 12
    # except:
    #   display(msg.commandName, msg.source + "Error decoding UFH zone heat demand data for device " + msg.source + ". MSG: " + msg.rawmsg)        



#--------------------------------------------
def setpoint(msg):
  if msg.payloadLength % 3 != 0:
    display(msg.commandName, msg.source + " - command error - invalid length: " + msg.rawmsg)
  else:
    cmdNameSuffix =""
    if msg.payloadLength > 3:
        cmdNameSuffix = "_CTL"
    i = 0
    while (i < msg.payloadLength):
<<<<<<< HEAD
      zoneData = msg.payload[i:i+6]
      zoneId = int(zoneData[0:2],16) + 1 #Zone number
      if zoneId >= 0 and zoneId in zones:
        zone_name = zones[zoneId]
      else:
        zone_name = "Zone " + str(zoneId)
        display("DEBUG","Setpoint: zone not found for zoneId " + str(zoneId) + ", MSG: " + msg.rawmsg)
      zoneSetPoint = float(int(zoneData[2:4],16) << 8 | int(zoneData [4:6],16))/100
      if (zoneSetPoint == 325.11): # This seems to be the number sent when TRV manually switched to OFF
        zoneSetpoint = 0 
        flag = " *(Heating is OFF)"
      else:
        flag = ""

      display(msg.commandName + cmdNameSuffix,'{0: <22}'.format(zone_name) + '{:>5}'.format(str(zoneSetPoint)) + "  [Zone " + str(zoneId) + "]" + flag)
      # if cmdNameSuffix == "": # Use controller value as main setpoint as it controls the boiler... there is sometimes a difference between the controller and actual device
      postToMqtt(zone_name, "setpoint" + cmdNameSuffix,zoneSetPoint)
      # else:
      #   postToMqtt(zone_name, "setpoint_ondevice",zoneSetPoint)
=======
      try:
        zoneData = msg.payload[i:i+6]
        zoneId = int(zoneData[0:2],16) + 1 #Zone number
        if zoneId >= 0 and zoneId in zones:
          zone_name = zones[zoneId]
        else:
          zone_name = "Zone " + str(zoneId)
        zoneSetPoint = float(int(zoneData[2:4],16) << 8 | int(zoneData [4:6],16))/100
        if (zoneSetPoint == 325.11): # This seems to be the number sent when TRV manually switched to OFF
          zoneSetPoint = 0
          flag = " *(Heating is OFF)"
        else:
          flag = ""
>>>>>>> 16466a0ff4911c5213faf3be7df72c1e3134d4e0

      # log("SETPOINT_STATUS     : " + '{0: <22}'.format(zone_name) + str(zoneSetPoint) + " [Zone " + str(zoneId) + "]")
      i += 6                          


#--------------------------------------------
def setpoint_override(msg):
  if msg.payloadLength != 7 and msg.payloadLength != 13:
    display(msg.commandName, msg.source + ": ERROR - invalid length: " + msg.rawmsg)
  else:
    zoneId = int(msg.payload[0:2],16) +1
    zone_name = zones[zoneId]
    newSetPoint = float(int(msg.payload[2:4],16) << 8 | int(msg.payload [4:6],16))/100

    #!!TODO!! Trap for 0x7FFF - this means setpoint not set
    if msg.payloadLength == 13: # We have an 'until' date
      dtmHex=msg.payload[14:]
      dtmMins = int(dtmHex[0:2],16)
      dtmHours = int(dtmHex[2:4],16)
      dtmDay = int(dtmHex[4:6],16)
      dtmMonth = int(dtmHex[6:8],16)
      dtmYear = int(dtmHex[8:12],16)
      dtm = datetime.datetime(year=dtmYear,month=dtmMonth, day=dtmDay,hour=dtmHours,minute=dtmMins)
      until = " - Until " + str(dtm)
      postToMqtt(zone_name, "scheduleMode", "Temporary")
      postToMqtt(zone_name, "scheduleModeUntil", dtm.strftime("%Y-%m-%dT%XZ"))
    else:
      until =""
      postToMqtt(zone_name, "scheduleMode", "Scheduled")
      postToMqtt(zone_name, "scheduleModeUntil", "")
    display(msg.commandName, '{0: <22}'.format(msg.source) + '{:>5}'.format(str(newSetPoint)) + "  [Zone " + str(zoneId) + "] " + until)
    postToMqtt(zone_name, "setpointOverride",newSetPoint)


#--------------------------------------------
def zone_temperature(msg):
  temperature = float(int(msg.payload[2:6],16))/100
  zoneId=0
  if devices.get(msg.sourceId):
    zoneId = devices[msg.sourceId]['zoneId']
  else:
    display("DEBUG","Device not found for source " + msg.sourceId)
  if zoneId >0:
    zoneDesc = " [Zone " + str(zoneId) + "]"
  else:
    zoneDesc = ""
  display(msg.commandName, msg.source + ("%6.2f" % temperature) + zoneDesc)
  postToMqtt(msg.source, "temperature",temperature)
  
#--------------------------------------------
def window_status(msg):
  if msg.payloadLength < 3:
    display(msg.commandName, msg.source + ": Error - invalid msg.payload length (" + msg.payloadLength + "): " + msg.rawmsg)
  else:
    # Zone is first 2, then window status. 3rd pair seems to be always zero apparently
    zoneId = int(msg.payload[0:2],16) 
    statusId = int(msg.payload[2:4],16)
    misc = int(msg.payload[4:6],16)

    if zoneId <12:
      zoneId += 1
    
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
  display(msg.commandName, '{0: <22}'.format(msg.source) + '{:<6}'.format(status) + " [Zone " + str(zoneId) + "]" + miscDesc)
  postToMqtt(msg.source,"window_status",status)

#--------------------------------------------
def other_command(msg):
  display(msg.commandName, msg.source + " - " + ". MSG: " + msg.rawmsg)

#--------------------------------------------
def date_request(msg):
  display(msg.commandName, msg.source + " - " + ". MSG: " + msg.rawmsg)

#--------------------------------------------
def relay_heat_demand(msg):
  if msg.payloadLength != 2:
    display(msg.commandName, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
  else:
    typeId = int(msg.payload[0:2],16)
    demand = int(msg.payload[2:4],16)
    if typeId <12:
      typeId +=1

    if msg.sourceType == DEV_TYPE_UFH:
      deviceType = "UFH"
      topic="RLY UFH"      
    elif typeId == 0xfa:  # Depends on whether it is main controller sending message, or UFH controller
      deviceType = "DHW"
      topic="DHW"      
    elif typeId == 0xfc:    # Boiler relay or possibly broadcast
      deviceType = "Boiler"
      topic = "RLY Boiler"
    elif typeId == 0xf9:  # Radiator circuit zone valve relay
      deviceType = "Radiators"
      topic="RLY Heating"
    elif typeId == 0xc: # Electric underfloor relay
    	deviceType = "RLY ELEC UFH"
    	topic="RLY UFH"
    else:
      deviceType = str(hex(typeId)) 
      topic = "RLY " + deviceType

    demandPercentage = float(demand)/200*100
    display(msg.commandName, msg.source + "{0: >5}".format(str(demandPercentage)) + "%" + " [Relay: '" + deviceType +"']" )
    postToMqtt(topic,"heat_demand",demandPercentage)
    
#--------------------------------------------
def zone_heat_demand(msg):
  if msg.payloadLength % 2 != 0 :
    display(msg.commandName, msg.source + ": ERROR - invalid msg.payload length: " + str(msg.rawmsg))
  else:
    topic = ""
    i = 0
    sourceDevice = msg.source 
    while (i < (msg.payloadLength *2)):
      # try:
        zoneId = int(msg.payload[0+i:2+i],16) 
        demand = int(msg.payload[2+i:4+i],16)
        
        if zoneId <= 12:
          zoneId += 1     # Normal zones are plus 1

        zoneLabel = "Zone"
        if msg.sourceType == DEV_TYPE_UFH:
          if msg.source == msg.destination or zoneId == 0xfc: # Messages from the UFH controller to itself seem always to have zone Id 252 (0xfc) - i.e. relay
            deviceType = "RLY UFH"
            topic = "RLY UFH"
          elif zoneId <=8: # 
            # if destination device is the main touch controller, then then zone Id is that of the matched zone in the main touch controller itself
            # Otherwise, if the destination device is the ufh controller (i.e. message is to itself/broadcast etc), then the zone Id is the ufh controller zone id (i.e. 1 to 8)

            if msg.destinationType == DEV_TYPE_CONTROLLER:
              zone_name = zones[zoneId] # get zone name from main controller zones 
              deviceType = "UFH " + zone_name.split(' ', 1)[1] 
              sourceDevice = deviceType
              topic = zone_name

            elif msg.destinationType == msg.sourceType: # destination should be the ufh controller itself (i.e. message to itlself). zoneId should be ufh zone
              for d in devices:
                if devices[d].get('ufh_zoneId') and zoneId == devices[d]['ufh_zoneId']:
                  # display("DEBUG","UFH Zone matched to " + devices[d]["name"])    
                  zoneId = devices[d]["zoneId"]
                  zone_name = devices[d]["name"]
                  deviceType = "UFH " + zone_name.split(' ', 1)[1] 
                  topic = zone_name
                  break
            else:
              display("ERROR","UFH message received, but destination is neither main controller nor UFH controller. destination = ", msg.destination + ", destinationType = " + 
                msg.destinationType + ", MSG: " + msg.rawmsg) 
          zoneLabel = "UFH Zone"
        elif zoneId == 0xfc:
	          deviceType = "Boiler Relay"
	          topic = "RLY Boiler"
        elif zoneId == 0xfa:
          deviceType = "DHW Relay"
          topic="DHW"
        elif zoneId == 0xf9:
          deviceType = "Radiators Relay"  
          topic="RLY Heating"
        # elif msg.sourceType == DEV_TYPE_UFH: # TODO.... MAP zones....
        #   deviceType = "UFH Controller"  
        #   topic="UFH Zone"   
        else:
          deviceType = str(hex(zoneId))
          topic = msg.source

        demandPercentage = float(demand)/200*100
        display(msg.commandName, '{0: <22}'.format(sourceDevice) + "{0: >5}".format(str(demandPercentage)) + "% [" + zoneLabel + " " + str(zoneId) + "]")
        #[DEBUG: payload size: " + str(msg.payloadLength) + ", i=" + str(i) + "]"
        if len(topic) > 0:
          postToMqtt(topic,"heat_demand",demandPercentage)
        else:
          display("DEBUG", "ERROR: Could not post to MQTT as topic undefined")
        i += 4
      # except:
      #   display(msg.commandName, msg.source + "Error decoding zone heat demand data for device " + msg.source + ". MSG: " + msg.rawmsg)        


#--------------------------------------------
def actuator_check_req(msg):
  # this is used to synchronise time periods for each relay bound to a controller 
  # i.e. all relays get this message and use it to determine when to start each cycle (demand is just a % of the cycle length)
  if msg.payloadLength != 2:
    display(msg.commandName, msg.source + ": Error decoding Actuator Check - invalid msg.payload length: " + msg.rawmsg)
  else:
    deviceNo = int(msg.payload[0:2],16) 
    demand = int(msg.payload[2:4],16)
    if deviceNo == 0xfc: # 252 - apparently some sort of broadcast?
      deviceType = " (Status Update Request)"
    else:
      deviceType =""
    display(msg.commandName, msg.source + str(demand) + deviceType)

    
#--------------------------------------------
def actuator_state(msg):
  if msg.payloadLength == 1 and msg.msgType == "RQ":
    if msg.device2 in zones:
      dev2name = zones[msg.device2]
    else:
      dev2name = msg.device2
    display(msg.commandName, "Request from " + msg.source + " to " + dev2name)
  elif msg.payloadLength != 3:
    display(msg.commandName, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
  else:
    deviceNo = int(msg.payload[0:2],16) # Apparently this is always 0 and so invalid
    demand = int(msg.payload[2:4],16)   # (0 for off or 0xc8 i.e. 100% for on)

    if demand == 0xc8: 
      status = "ON"
    elif demand == 0:
      status ="OFF"
    else:
      status = "Unknown: " + str(demand)
    display(msg.commandName, msg.source + status)
    postToMqtt(msg.source,"actuator_status",status)

#--------------------------------------------
def dhw_status(msg):
  if not( msg.payloadLength ==6 or msg.payloadLength == 12) :
    display(msg.commandName, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
  else:
    zoneId = int(msg.payload[0:2],16)   # Apparently this is always 0 for controller?
    stateId = int(msg.payload[2:4],16)    # 0 or 1 for DHW on/off, or 0xFF if not installed
    modeId = int(msg.payload[4:6],16)     # 04 = timed??

    if stateId == 0xFF:
      state ="DHW not installed"
    elif stateId == 1:
      state = "On"
    elif stateId == 0:
      state = "Off"
    else:
      state ="Unknown state: " + str(stateId)

    if modeId == 0:
      mode="Auto"
    elif modeId ==4:
      mode="Timed"
    else:
      mode=str(modeId)

    if msg.payloadLength ==12 :
      dtmHex=msg.payload[12:]
      dtmMins = int(dtmHex[0:2],16)
      dtmHours = int(dtmHex[2:4],16)
      dtmDay = int(dtmHex[4:6],16)
      dtmMonth = int(dtmHex[6:8],16)
      dtmYear = int(dtmHex[8:12],16)
      dtm = datetime.datetime(year=dtmYear,month=dtmMonth, day=dtmDay,hour=dtmHours,minute=dtmMins)
      until = " [Until " + str(dtm) + "]"
    else:
      until =""

    if stateId == 0xFF:
      display(msg.commandName, msg.source + " DHW not installed")      
    else:   
      display(msg.commandName, msg.source + "[ZoneID " + str(zoneId) + "] State: " + state + ", mode: " + mode + until )
      postToMqtt("DHW","state",stateId)
      postToMqtt("DHW","mode",mode)
      if until >"":        
        postToMqtt("DHW","until",dtm)
      else:
        postToMqtt("DHW","until","")

#--------------------------------------------
def dhw_temperature(msg):
  temperature = float(int(msg.payload[2:6],16))/100
  display(msg.commandName, msg.source + ("%6.2f" % temperature))
  postToMqtt("DHW", "temperature", temperature)

#--------------------------------------------
def zone_info(msg):
  # display(msg.commandName, msg.source + " - MSG: " + msg.rawmsg)
  pass

#--------------------------------------------
def device_info(msg):
  #display(msg.commandName, msg.source + " - MSG: " + msg.rawmsg)
  pass

#--------------------------------------------
def battery_info(msg):
  if msg.payloadLength != 3:
    display(msg.commandName, msg.source + ": Decoding error - invalid msg.payload length: " + msg.rawmsg)
  else:
    deviceID = int(msg.payload[0:2],16)   
    battery = int(msg.payload[2:4],16)    
    lowBattery = int(msg.payload[4:5],16)   
    
    if battery == 0xFF:
      battery = 100 # recode full battery (0xFF) to 100 for consistency across device types
    else:
      battery = battery / 2  #recode battery level values to 0-100 from original 0-200 values
    
    if(lowBattery != 0):    #TODO... Need to check this to understand how it is used.
      warning = " - LOW BATTERY WARNING"
    else:
      warning = ""

    display(msg.commandName, msg.source + '{:>5}'.format(str(battery)) + "%" + warning)
    postToMqtt(msg.source,"battery",battery)

#--------------------------------------------
def controller_mode(msg):
  if msg.payloadLength != 8:
    display(msg.commandName, msg.source + ": ERROR - invalid msg.payload length: " + msg.rawmsg)
  else:
    modeId = int(msg.payload[0:2],16)   
    try:
      mode = CONTROLLER_MODES[modeId] 
    except:
      mode="Unknown (" + str(modeId) + ")"
    durationCode = int(msg.payload[14:16],16) # 0 = Permanent, 1 = temporary
    print("modeId " + str(modeId) + " duration code: " + str(durationCode))
    
    if durationCode == 1:
      dtmHex=msg.payload[2:14]
      dtmMins = int(dtmHex[0:2],16)
      dtmHours = int(dtmHex[2:4],16)
      dtmDay = int(dtmHex[4:6],16)
      dtmMonth = int(dtmHex[6:8],16)
      dtmYear = int(dtmHex[8:12],16)
      dtm = datetime.datetime(year=dtmYear,month=dtmMonth, day=dtmDay,hour=dtmHours,minute=dtmMins)
      until = " [Until " + str(dtm) + "]"
    else:
      if modeId != 0:
        until =" - PERMANENT" 
      else:
        until =""
    display(msg.commandName, msg.source + mode + " mode" + until)
    postToMqtt(msg.source,"mode",mode)


#--------------------------------------------
def heartbeat(msg):
  display(msg.commandName, msg.source + " - MSG: " + msg.rawmsg)

#--------------------------------------------
def external_sensor(msg):
  display(msg.commandName, msg.source + " - MSG: " + msg.rawmsg)

#--------------------------------------------
def unknown_command(msg):
  display(msg.commandName, msg.source + " - MSG: " + msg.rawmsg)

#-------------------------------------------- Evohome Commands Dict
COMMANDS = {
  '0002': external_sensor,
  '0004': zone_name,
  '0008': relay_heat_demand,
  '000A': zone_info,
  '0100': other_command,  
  '0418': device_info,
  '1060': battery_info,
  '10e0': heartbeat,
  '1260': dhw_temperature,
  '12B0': window_status,
  '1F09': sync,
  '1F41': dhw_status,
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


#-------------------------------- Main ----------------------------------------
rotateFiles(LOG_FILE)
rotateFiles(EVENTS_FILE)
logfile = open(LOG_FILE, "a")
eventfile = open(EVENTS_FILE,"a")

signal.signal(signal.SIGINT, sigHandler)    # Trap CTL-C etc


#-------------------------------------------------------------------------------

comConnected = False 
while (COM_RETRY_LIMIT > 0 and not comConnected):
  try:
    serialPort = serial.Serial(COM_PORT)  
    serialPort.baudrate = COM_BAUD                
    serialPort.bytesize = 8              
    serialPort.parity   = 'N'            
    serialPort.stopbits = 1              
    serialPort.timeout = 1               

    comConnected = True
  except Exception as e:
    if COM_RETRY_LIMIT >1:
      display("COM_PORT ERROR",repr(e) + ". Retrying in 5 seconds")
      time.sleep(5)
      COM_RETRY_LIMIT -= 1
    else:
      display("COM_PORT ERROR","Error connecting to COM port " + COM_PORT + ". Giving up...")

if not comConnected:
  sys.exit()


display("","\n")
display("","Evohome listener version " + VERSION )
display("","Connected to COM port " + COM_PORT)  

logfile.write("")
logfile.write("-----------------------------------------------------------\n")

if os.path.isfile(DEVICES_FILE):
  with open(DEVICES_FILE, 'r') as fp:
    devices = json.load(fp)             # Get a list of known devices, ideally with their zone details etc
else:
  devices = {}

zones = {}                            # Create a seperate collection of Zones, so that we can look up zone names quickly
for d in devices:
  if devices[d]['zoneMaster']:
    zones[devices[d]["zoneId"]] = devices[d]["name"]
# print (zones)

display('','')
display('','-----------------------------------------------------------')
display('',"Devices loaded from '" + DEVICES_FILE + "' file:")
for key in sorted(devices):
  zm = " [Master]" if devices[key]['zoneMaster'] else ""
  display('','   ' + key + " - " + '{0: <22}'.format(devices[key]['name']) + " - Zone " + '{0: <3}'.format(devices[key]['zoneId']) + zm )

display('','-----------------------------------------------------------')
display('','')
display('','Listening...')

file.flush(logfile)

#---------------- Main loop ---------------------
while serialPort.is_open:
  try:
    # if serialPort.is_open:
    data = serialPort.readline()        # Wait and read data
    if data:                         # Only proceed if line read before timeout 
        # print(data)
        if not ("_ENC" in data or "_BAD" in data or "BAD_" in data or "ERR" in data):          #Make sure no errors in getting the data.... 
          
          msg = Message(data)
          msg_type = data[4:6]             # Extract message type

          # Check if device is known...
          if not msg.source in devices:
            display("NEW DEVICE FOUND", msg.source)
            devices.update({msg.source : {"name" : msg.source, "zoneId" : -1, "zoneMaster" : False  }})
            with open(NEW_DEVICES_FILE,'w') as fp:
              fp.write(json.dumps(devices, sort_keys=True, indent=4))
            fp.close()
          else:
            if msg.sourceType == DEV_TYPE_CONTROLLER and msg.destinationType == DEV_TYPE_CONTROLLER: # Controller broadcast message I think 
              msg.source="CONTROLLER"
            if msg.source != "CONTROLLER" and devices[msg.source]['name'] > "":
              msg.source = devices[msg.source]['name']      # Get the device's actual name if we have it

          msg.source = '{0: <22}'.format(msg.source) #Pad out to 22 chars

          if msg.command in COMMANDS:
            try:
              msg.commandName = COMMANDS[msg.command].__name__.upper() # Get the name of the command from our list of commands
              COMMANDS[msg.command](msg)
              log('{0: <18}'.format(msg.commandName) + " " + data) 
            except Exception as e:
              display("ERROR",msg.commandName + ": " + repr(e) + ": " + data)

          else:
            display("UNKNOWN COMMAND", msg.source + ": Command code '" + msg.command + "'. MSG: " + data.strip())
            log("UNKNOWN COMMAND: " + data)
        else:
          display("ERROR","--- Message dropped: packet error from hardware/firmware")
          log(data)

        file.flush(logfile)

  except KeyboardInterrupt:
    if serialPort.is_open:
      serialPort.close()                   
    comConnected = False

  # except Exception as e:
  #   if serialPort.is_open:
  #     serialPort.close()
  #   print ("Evohome listener stopped")                   
  #   pass



