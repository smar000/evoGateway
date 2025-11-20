# **evoGateway**

*A local evohome ↔ MQTT integration service*

---
![alt text](./misc/images/evogateway_screenshot_v4.png "evoGateway Console")


---

## **Overview**

evoGateway provides a local, fully independent interface between Honeywell’s evohome RF network and any MQTT-based home automation system.

It receives and decodes RF traffic from evohome devices using the excellent `ramses_rf` library, publishes structured MQTT topics, and also accepts MQTT commands which are transmitted back onto the RF network as if they originated from an evohome Honeywell HGI80.

The gateway operates without relying on Honeywell’s cloud services, offering a robust and responsive integration path for platforms such as openHAB, Home Assistant, and any system capable of consuming or producing MQTT messages.

All RF communication, schema management, and MQTT publishing is performed locally, providing a reliable and transparent interface to the evohome system.



---

## **Upgrading from Older Versions**

There has been extensive refactoring and reorganisation of evoGateway since the previous version 3.x. Users upgrading from earlier releases should be aware of several important changes:

### **1. Project Structure**

The codebase is now organised into distinct functional components, moving away from the single large script file. This should make it easier to adapt to any future changes in the underlying `ramses_rf` library:

```
.
├── config/
│   ├── evogateway.cfg
│   ├── ramses_rf_schema.json
│
├── evogateway/
│   ├── app.py
│   ├── config.py
│   ├── logger.py
│   ├── router.py
│   ├── services.py
│   └── utils.py
│
├── logs/
│   ├── events.log
│   └── packet.log
│
├── README.md
└── evogateway.py
```

### **2. The Ramses schema file is now the single source of truth**

* The `ramses_rf_schema.json` file is now the master source for zones/devices.
* **`devices.json` and `zones.json` have been removed**.
  Device names, zone names, and relationships are now maintained entirely in the schema file.

### **3. MQTT topic structure has been updated**

* A more consistent and hierarchical MQTT layout is now used by default.
* The previous flat/legacy layout is still available by enabling:

  ```
  MQTT_LEGACY_TOPIC_STRUCTURE = True
  ```
* Device and zone categorisation defaults have changed slightly, especially around controllers, HGI devices, and DHW classification.

### **4. Some configuration file options have changed**

* Several older parameters have been renamed or removed.
* A complete sample configuration file is provided (`config/evogateway.cfg.sample`).

### **5. Code modularisation**

* The old single-file architecture has been replaced with a module-based structure.
* Logging, MQTT handling, routing, serial handling, and formatting logic now live in dedicated modules.

While core functionality should hopefully remain consistent with previous versions, some behaviour will almost certainly differ slightly as compared to previous versions. 

---

## **Installation**

### **Requirements**

* Python **3.12.3**
* Python dependencies can be installed via:

```bash
pip install -r requirements.txt
```

Includes:

* `ramses_rf`
* `paho-mqtt`
* `colorama`

---
### **Hardware**
**NOTE** The hardware can be purchased in component form from ebay/Ali Express etc or, fully assembled with a proper PCB, from ebay (search for `nanoCUL FTDI 868MHz`).

If assembling yourself, you will need:

* **1 x Arduino nano** (clone should be fine), preferably with FTDI usb chipset, though the cheaper CH341 chipset also worked. The only issue I had with the CH341 was that the USB port was not always cleanly released when the python script exited. My FTDI based build is much more reliable in this respect.

* **1 x CC1101 radio, 868MHz**, e.g. something like [this](https://quadmeup.com/cc1101-868mhz-wireless-transciever-pinout). However, note that there has been some report of issues with the radio crystal's accuracy on some of these boards ([discussion on evofw3 board](https://github.com/ghoti57/evofw3/issues/11#issuecomment-823484744)).

* **A breadboard** or **8 x Dupont fly leads**. If using fly leads you need to ensure that sure that you have the correct male/female combination for your arduino and CC1101 card. 

![alt text](./misc/images/arduino-cc1101.jpeg "Arduino/CC1101 Hardware")


Wiring pin connections will depend on the specific CC1101 board. In my case, I used the following:
```
WIRE COLOUR			CC1101 PIN 	NANO PIN
Red 				Vcc	        3.3V pin
Black 				GND 		GND
Orange 				MOSI		15
Yellow 				SCLK		17
Blue				MISO		16
Dark Red 			GDO2		32
Grey				GDO0 		1
White 				CSN 		14

ANT 				Antenna coil
```

Although arduinos with the FTDI FT232L chipset are recommended, clone nanos with the CH341 usb chip have also worked reasonably. In my case the radio board is connected directly to the nano using just male/female dupont wires, with the male side soldered directly onto the radio board. Note that actual pin connection points should always follow the requirements of evofw3 (e.g. see the [atm328_pins.h](https://github.com/ghoti57/evofw3/blob/36b7a9ca1d97fc0f81ffaea1e8e8b3c780024cd6/atm328_pins.h) file), otherwise corresponding changes may need to be made in in this file before compiling and flashing the firmware.

### Arduino Firmware
evoGateway requires the **[evofw3](https://github.com/ghoti57/evofw3)** arduino firmware by @ghoti57, to decode/encode the radio signals and make the message packets available to/from the ramses_rf framework. 


---

## Configuration

evoGateway uses two distinct configuration sources, each serving a different purpose:

1. **The evoGateway configuration file** (`evogateway.cfg`)
   * Contains all runtime settings for the gateway itself (serial port, MQTT behaviour, logging, file paths, etc.).   

2. **The Ramses RF schema file** (`ramses_rf_schema.json`)
   * Stores the discovered evohome network structure (devices, zones, controller relationships, metadata, etc.).
   * Automatically generated by evoGateway on first run, and can be modified by hand as required.

Both files live under the `config/` directory.

_(For those upgrading from an earlier version, the previous `devices.json` and `zones.json` files have been retired; the schema file is now the single authoritative source.)_

---

### 1. evoGateway Configuration File (`evogateway.cfg`)

This file defines the operational parameters for evoGateway. A sample configuration with all available parameters is provided in:

```
config/evogateway_sample.cfg
```

Users should copy this to `evogateway.cfg` and adjust as needed. The key parameters to include are the serial port settings and the mqtt broker details. The rest are optional and can be omitted.

#### **Serial Port Settings**

Controls communication with the evofw3 Arduino interface:

* `COM_PORT` (mandatory)
* `COM_BAUD`

The `COM_PORT`parameter can take any format that `ramses_rf` supports, such as:
* /dev/ttyUSB0
* /dev/serial/by-id/usb-1a86_USB2.0-Serial-if00-port0 
* mqtt://username:password@172.16.1.200:1883


#### **MQTT Settings**

Broker credentials, topic definitions, and publishing behaviour:

* MQTT server, username, password
* Topic roots and subtopics
* JSON-only vs. JSON+key/value publishing
* Zone grouping or device-flat layout
* Legacy topic compatibility mode



---

### 2. Ramses RF Schema (`ramses_rf_schema.json`)

The ramses schema file is the **authoritative record of the evohome system** as 'discovered' by the `ramses_rf` framework by listening in on the RF network.

It contains structured data on:

* All devices observed on the network
* Device types and capabilities
* Zones and zone data
* Controller identity
* Temperature ranges, supported features, flow configuration
* A `known_list` of devices that are permitted to operate via the ramses framework

#### **Schema Creation and Updates**

* On first run (or if there is no valid schema file, or if the `ENABLE_DISCOVERY` config parameter is explicitly set to `True`), evoGateway begins in both **discovery mode + eavesdrop mode**, to build the schema incrementally from RF traffic.

* During discovery, the evolving schema is published regularly to MQTT.

* Once sufficient information is collected (this may may take some time to build up, especially if there are any ufh controllers or DHW senders on the network), the schema can be saved either manually via an MQTT command or automatically at system shutdown. The default save location is `config/ramses_rf_schema.json`. 
  
* On subsequent runs, evoGateway loads this file if available, avoiding repeated discovery (which can be very resource intensive). 
* Note that if evoGateway is started with a valid schema file, it will NOT normally re-save the current schema at system shutdown, in order to avoid overwriting any custom modifications in the startup schema. This behaviour can be changed by setting the configuration parameter `ALWAYS_SAVE_SCHEMA_ON_SHUTDOWN` to `True`.


#### Schema Support Commands

Two administrative commands (sent via the MQTT command topic) control schema publishing and persistence:

| Command       | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `POST_SCHEMA` | Publishes the current schema over MQTT to the `_gateway_config` subtree. |
| `SAVE_SCHEMA` | Publishes the schema *and* writes it back to the JSON file on disk.      |



#### Editing the Schema

Although it is not normally necessary to modify this file manually, initially it may be easier to add device aliases directly in this file, and/or amend the `known_list`, as these do not seem to always come through from the automatic discovery. 

The schema file can subsequently be regenerated from scratch at any time by either:
1. Deleting the current schema and then starting evoGateway
2. Setting the `ENABLE_DISCOVERY` to `True` in the `Ramses_rf` section of the evoGateway config file.

---

## **Operation**

Once installed and configured, evoGateway:

1. Loads the existing `ramses_rf` schema
2. Opens the serial connection to the hardware
3. Starts listening to all evohome RF traffic
4. Publishes decoded messages to MQTT
5. Subscribes to a command topic and transmits valid commands onto the RF network
6. Maintains logs of all events and packets

### **Schema Handling**

* When operating in eavesdropping or discovery mode, the current schema is saved on clean shutdown.
* The schema can also be manually posted to MQTT or saved to file at any time via system MQTT commands.

---

## **MQTT Topic Structure**

evoGateway's MQTT topic structure has been slightly changed from earlier versions, to provide a slightly cleaner and more consistent hierarchy. The key changes are:

* Devices that previously appeared under `"_zone_independent"` now appear under a dedicated **`system/`** topic.
* All zones are now grouped under a single **`zones/`** root topic.
* Hot water (DHW) is treated as just another zone under `zones`.
* Device categories such as **controllers**, **HGI**, **relays**, **UFH**, **OTB** etc remain available and behave largely as before, but are now placed cleanly within the new hierarchy.

### **Legacy Structure**

Users upgrading from older evoGateway installations who prefer the original topic layout for continuity can try the configuration setting `MQTT_LEGACY_TOPIC_STRUCTURE = True`. In this mode:

* The old `"_zone_independent"` root is restored.
* Zones appear directly under the root topic. No structural `zones/` grouping is applied.
* Controllers and HGIs are *not* separated into their own buckets.


## **MQTT Command Interface**

evoGateway listens for **commands** on a designated MQTT topic and sends them onto the RF network (via the ramses library) as if they originated from a Honeywell HGI80.

The default command topic is `evohome/evogateway/system/_command` but this can be changed as required in the config file:

```ini
[MQTT]
MQTT_CMD_TOPIC = evohome/evogateway/system/_command
```

### 1. Command Message Format

Commands are sent as **JSON objects** to the command topic.

There are two supported styles:

1. **High-level “command” calls** (preferred) – map directly onto `ramses_rf` command constructors.
2. **Low-level “code” packets** – send raw RAMSES frames (for advanced use only).

Both use a single JSON document, for example:

```json
{"command": "set_zone_setpoint", "zone_idx": "01", "setpoint": 21.0}
```

or

```json
{"code": "2309", "verb": "W", "payload": "00072100"}
```

---

### 2. High-Level ramses_rf Commands (Recommended)

The preferred approach is to use the high-level `command` interface, which wraps the `ramses_rf` `Command` constructors. These can be found in the `ramses_rf` repo.

#### Structure

```json
{
  "command": "<method_name>",
  "<keyword_1>": "<value>",
  "<keyword_2>": "<value>",
  ...
}
```

* `command`
  Name of the `ramses_rf` command method (e.g. `set_zone_setpoint`, `get_system_time`, `set_dhw_mode`, etc.) 

* Additional keys
  Passed through as keyword arguments to that method (e.g. `zone_idx`, `setpoint`, `until`, `ctl_id`).

If `ctl_id` (controller ID) is omitted, evoGateway will attempt to automatically insert the ID of the controller discovered from the Ramses schema.

#### Examples

**a) Get the system time**

```json
{"command": "get_system_time"}
```

**b) Set a zone setpoint**

Set *Zone 01* to 21.0 °C indefinitely:

```json
{"command": "set_zone_setpoint", "zone_idx": "01", "setpoint": 21.0}
```

**c) Set DHW mode until a given time**

```json
{
  "command": "set_dhw_mode",
  "active": true,
  "until": "2025-05-31T17:40:00"
}
```

Date/time values should be in ISO-8601 format (`YYYY-MM-DDTHH:MM:SS`).

**Note:** The available `command` names and their parameters are defined by `ramses_rf` within its [Command class](https://github.com/zxdavb/ramses_rf/blob/aa5f55ae63adb2750b783308a8690b82657a5f35/ramses_rf/command.py#L291). The gateway does not add or remove arguments; it simply passes them through as given. 
  
As of November 2025, the following constructor methods are available, along with their respective list of keyword arguments (note that not all the arguments are mandatory):

```
Code 1F41, command method:  get_dhw_mode(ctl_id)
Code 0404, command method:  get_dhw_schedule_fragment(ctl_id, frag_idx, frag_cnt)
Code 3220, command method:  get_opentherm_data(dev_id, msg_id)
Code 0418, command method:  get_system_log_entry(ctl_id, log_idx)
Code 2E04, command method:  get_system_mode(ctl_id)
Code 313F, command method:  get_system_time(ctl_id)
Code 1100, command method:  get_tpi_params(ctl_id)
Code 000A, command method:  get_zone_config(ctl_id, zone_idx)
Code 2349, command method:  get_zone_mode(ctl_id, zone_idx)
Code 0004, command method:  get_zone_name(ctl_id, zone_idx)
Code 0404, command method:  get_zone_schedule_fragment(ctl_id, zone_idx, frag_idx, frag_cnt)
Code 1F41, command method:  set_dhw_mode(ctl_id, mode, active, until)
Code 10A0, command method:  set_dhw_params(ctl_id, setpoint, overrun, differential)
Code 1030, command method:  set_mix_valve_params(ctl_id, zone_idx, max_flow_setpoint, min_flow_setpoint, valve_run_time, pump_run_time)
Code 2E04, command method:  set_system_mode(ctl_id, system_mode, until)
Code 313F, command method:  set_system_time(ctl_id, datetime)
Code 1100, command method:  set_tpi_params(ctl_id, domain_id, cycle_rate, min_on_time, min_off_time, proportional_band_width)
Code 000A, command method:  set_zone_config(ctl_id, zone_idx, min_temp, max_temp, local_override, openwindow_function, multiroom_mode)
Code 2349, command method:  set_zone_mode(ctl_id, zone_idx, mode, setpoint, until)
Code 0004, command method:  set_zone_name(ctl_id, zone_idx, name)
Code 2309, command method:  set_zone_setpoint(ctl_id, zone_idx, setpoint)
Code 0404, command method:  get_zone_schedule_fragment  ctl_id, zone_idx, frag_idx, frag_cnt
```

---

### 3. Low-Level Code / Verb / Payload Commands (Advanced)

For advanced scenarios or when experimenting with undocumented features, you can send raw RAMSES frames.

#### Structure

```json
{
  "code": "XXXX",
  "verb": "RQ|RP|W|I",
  "payload": "AABBCC...",
  "dest_id": "01:234567"    // optional
}
```

* `code` – 4-character RAMSES message code (e.g. `0418`, `2309`).
* `verb` – RAMSES verb (`RQ`, `RP`, `W`, ` I`, etc.).
* `payload` – **hex string** payload (exactly as required by that code).
* `dest_id` – optional destination device ID. If omitted, the controller is usually assumed.

**Example – get first system log entry using raw code:**

```json
{"code": "0418", "verb": "RQ", "payload": "000000"}
```

Use this mode only if you are familiar with the RAMSES protocol; the gateway performs minimal validation.

---

### 4. Schedule Commands

evoGateway exposes a simple schedule interface over the same command topic for complete local control of the evohome schedule, via the following commands:

* `get_schedule`
* `set_schedule`

#### Get Schedule

```json
{
  "command": "get_schedule",
  "zone_idx": "01",
  "force_refresh": true
}
```

* `zone_idx` – required; the zone index as a hexadecimal string (e.g. `"01"`, `"0A"`, `"HW"`).
* `force_refresh` – optional; if `true`, retrieves a fresh schedule from the controller.

Schedules are returned via normal RF messages and published to MQTT as `zone_schedule` payloads under the relevant zone/device topics.

#### Set Schedule

```json
{
  "command": "set_schedule",
  "zone_idx": "01",
  "schedule": { ... }
}
```

* `schedule` must match the structure expected by `ramses_rf` for zone schedules. Initially downloading a schedule via the `get_schedule` command provides an easy template to work from.
* Schedules should be sent as complete schedules for the whole work. Uploading a schedule will overwrite the whole of the existing schedule stored in the evohome controller

If required parameters (e.g. `zone_idx` or `schedule`) are missing, the gateway will log an error and ignore the command.

---

### 5. Command Status & Diagnostics

Each command sent via MQTT is tracked and a status is published to the `_last_command` subtopic beneath the command root.

By default:

* Commands are sent to:

  ```text
  <MQTT_CMD_TOPIC>/_commands
  ```

* Status is published under:

  ```text
  <MQTT_CMD_TOPIC>/_last_command/status
  ```

The status payload typically includes:

* Whether the command was **transmitted** onto the RF network.
* Whether an expected **acknowledgement / response** was successfully received.
* Any relevant error or timeout information.

This allows for automations to:

* Wait for confirmation that a command was accepted.
* Detect failed or timed-out operations.
* etc.

---

### 6. System Commands

In addition to “normal” `ramses_rf` commands, evoGateway also accepts a few **gateway maintenance commands** via the same MQTT command topic.

These use a `sys_config` key instead of `command`:

```json
{"sys_config": "POST_SCHEMA"}
```

Currently supported (subject to change as the gateway evolves):

* `POST_SCHEMA`
  Publishes the current schema, parameters, and status from `ramses_rf` to the `_gateway_config` subtree under the system topic.

* `SAVE_SCHEMA`
  As above, and additionally saves the current schema back to the `ramses_rf_schema.json` file.

These operations are of course local to evoGateway and are not forwarded on the RF network.

---

## **Credits**

evoGateway builds upon two major open-source contributions:

* **`ramses_rf`** by David Bonnes (@zxdavb)
  [https://github.com/zxdavb/ramses_rf](https://github.com/zxdavb/ramses_rf)

* **`evofw3`** by Peter Price (@ghoti57)
  [https://github.com/ghoti57/evofw3](https://github.com/ghoti57/evofw3)

Their work provides the foundation for all decoding, encoding, and RF communication.
