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
│   ├── ha_discovery.py
│   ├── logger.py
│   ├── models.py
│   ├── registry.py
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


#### **Home Assistant MQTT Discovery**

evoGateway can publish [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) payloads in the Home Assistant MQTT discovery format, for automatic recognition of the devices by compatible home automation systems such as openHAB and, of course, Home Assistant. 

Enable it by adding to the `[MQTT]` section of `evogateway.cfg`:

```ini
HA_DISCOVERY_ENABLED = True
HA_DISCOVERY_PREFIX  = homeassistant   ; default — match HA's discovery prefix setting
```

##### Configuration options

| Parameter | Default | Description |
|---|---|---|
| `HA_DISCOVERY_ENABLED` | `False` | Enable/disable HA MQTT discovery |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | Must match HA's own discovery prefix |
| `HA_DISCOVERY_ID_PREFIX` | *(snake_case of `MQTT_CLIENTID`)* | Prefix for all device/entity unique IDs. Set this explicitly when running dev and production gateways against the same HA instance to avoid ID collisions. Send `REMOVE_HA_DISCOVERY` before changing this value. |
| `HA_ZONE_CLIMATE_PREFIX` | *(empty)* | Optional text prepended to zone climate entity names — e.g. `Zone` → *"Zone Living Room Heating"* |
| `HA_ZONE_CLIMATE_SUFFIX` | `Heating` | Optional text appended to zone climate entity names — e.g. *"Living Room Heating"*. Set to empty to use the bare zone name. |

When enabled, evoGateway publishes retained discovery messages using **HA device-mode discovery** — one topic per logical device containing all its entities:

```
<HA_DISCOVERY_PREFIX>/device/<device_id>/config
```

Discovery is published at startup and after every MQTT broker reconnect. When set to `False`, evoGateway automatically removes any previously-published discovery entries from the broker on the next startup.

##### What gets discovered

| Device | Entities bundled inside |
|---|---|
| Gateway | System mode select (auto, away, heating off, etc.); DHW, Radiators and UFH relay demand % sensors (from controller) |
| Heating zone (one per zone) | Climate (temperature + setpoint control), heat demand % sensor |
| Hot water (DHW) | Climate (heat / auto / off modes), relay active binary sensor, DHW wireless sender battery % |
| Physical device — TRV, thermostat, DHW sensor (one per device) | Temperature, battery %, window open, heat demand — varies by device type |
| Physical device — BDR91 relay (system relays, e.g. radiators) | Actuator state binary sensor (derived from modulation level), heat demand % |
| Physical device — OpenTherm Bridge | Flame, CH active, DHW active, modulation %; boiler setpoint; boiler/return water temps, CH pressure, modulation level, fault description and OEM fault code (from OpenTherm protocol messages) |

##### HA device grouping and naming

* The **gateway** is a top-level HA device.
* Each **heating zone** is a child device of the gateway, containing the zone climate and heat demand entities.
* Each **physical device** (TRV, relay, etc.) is a child of its zone, named `"{Zone Name} {Type} ({alias})"` — e.g. *"Living Room TRV (Middle)"*. All of its sensors appear together on one HA device card.
* The hierarchy in HA is: `evoGateway → Zone → Physical Device`.

##### Data sources

Zone climate entities read temperature and setpoint from the controller's per-zone topics (e.g. `zones/living_room/ctl_controller/temperature`) rather than the aggregated state, so each zone always shows the authoritative controller-reported value.

evoGateway also maintains two aggregated JSON topics used by other entities:

| Topic | Contents |
|---|---|
| `<root>/zones/<zone_slug>/state` | `temperature`, `setpoint`, `mode`, `heat_demand`, `timestamp` |
| `<root>/system/state` | `system_mode`, `heat_demand`, `flame`, `boiler_status`, `timestamp` |

These are updated in real time as RF messages arrive. All other existing per-device topics are unaffected.

##### Availability

All discovered entities use the gateway's `<root>/status` topic as their availability source. When evoGateway shuts down, the MQTT Last Will sets this to `Offline` and all entities show as unavailable in HA. They recover automatically on the next evoGateway startup — no HA restart needed.

##### Removing discovery entries

If `HA_DISCOVERY_ENABLED` is set to `False`, evoGateway will **automatically remove all previously-published discovery entries** on the next startup. No manual action is needed.

To remove entries immediately without restarting (e.g. before renaming zones), send:

```json
{"sys_config": "REMOVE_HA_DISCOVERY"}
```

This publishes empty retained payloads to every discovery topic, which instructs HA to remove the entities immediately. Re-enabling discovery on the next start will recreate them from scratch.

> **Note:** If you rename zones and disable discovery in the same restart, automatic cleanup may miss entries published under the old zone names. Use the `REMOVE_HA_DISCOVERY` command *before* renaming as a workaround.
>
> Neither operation affects evoGateway's normal MQTT publishing.

---

#### **Watchdog Settings**

evoGateway includes two watchdog mechanisms:

* **MQTT heartbeat** — periodically republishes the `Online` status so consumers (e.g. Home Assistant) can detect a stale-but-connected gateway by comparing the `status_ts` field against wall clock.
* **RF silence detector** — monitors the time since the last RF message was received and escalates through up to four stages if the radio goes quiet:
  1. **Warn** — log a warning and publish status `RF Timeout` to MQTT.
  2. **Restart RF** — restart the ramses_rf layer only, without exiting the process.
  3. **Restart process** — restart the whole Python process via `os.execv` (no external process manager needed).
  4. **Exit** — raise `SystemExit` so the process manager (e.g. systemd) can restart the service.

Each stage is individually optional (set its timeout to `0` to skip it). The RF restart and process restart stages can also be triggered manually via MQTT — see [System Commands](#6-system-commands).

> **systemd and Docker behaviour**
> The **restart process** stage replaces the running Python process in-place via `os.execv` — the container or service wrapper never sees an exit, so no external restart is needed. The **exit** stage raises `SystemExit`, which *does* cause the process to terminate; both systemd (`Restart=on-failure`) and Docker (`restart: unless-stopped`) will automatically bring it back up. When running under Docker the provided `docker-compose.yml` already has the correct restart policy set.

All thresholds are configured in the `[Watchdog]` section of `evogateway.cfg`:

| Parameter | Default | Description |
|---|---|---|
| `MQTT_HEARTBEAT_INTERVAL` | `300` | Seconds between heartbeat re-publishes of `Online` to the status topic. Set to `0` to disable the heartbeat entirely. |
| `RF_WARN_TIMEOUT` | `900` | Seconds of RF silence before **warning**: logs a warning and publishes status `RF Timeout`. Set to `0` to disable. |
| `RF_RESTART_TIMEOUT` | `1800` | Seconds of RF silence before **restarting the RF layer** (ramses_rf only, no process exit). Fires independently — does not require the warn stage to be enabled. Set to `0` to disable (also disables the process restart and exit stages, which depend on this one). |
| `RF_PROCESS_RESTART_TIMEOUT` | `900` | Seconds after the RF restart before **restarting the whole process** via `os.execv`. Set to `0` to skip this stage and go straight to the exit stage. |
| `RF_EXIT_TIMEOUT` | `1800` | Seconds after the RF restart before **exiting** so the process manager can restart the service. Only reached if the process restart stage is disabled, or if `RF_EXIT_TIMEOUT` is set lower than `RF_PROCESS_RESTART_TIMEOUT`. Set to `0` to disable. |

#### **Misc / Logging Settings**

* `LOG_LEVEL` — verbosity of the event log (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
* `EVENTS_CONSOLE_OUTPUT` — whether to echo events to the console
* `USE_LOCAL_TIME` — when `True`, event log and MQTT status timestamps use the local OS timezone (DST-aware). Defaults to `False` (system default, typically UTC in server environments).



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
  
As of November 2025, the following constructor methods appear to be available, along with their respective list of keyword arguments (note that not all the arguments are mandatory):
<details>
<summary><strong>Click to expand list of commands</strong></summary>

```
Method                  |Verb| Code    | Arguments
------------------------| ---| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
put_weather_temp        | I  | 0002    | dev_id, temperature
get_zone_name           | RQ | 0004    | ctl_id, zone_idx
set_zone_name           | W  | 0004    | ctl_id, zone_idx, name
get_schedule_version    | RQ | 0006    | ctl_id
get_relay_demand        | RQ | 0008    | dev_id, zone_idx|None
get_zone_config         | RQ | 000A    | ctl_id, zone_idx
set_zone_config         | W  | 000A    | ctl_id, zone_idx, min_temp = 5, max_temp = 35, local_override = False, openwindow_function = False, multiroom_mode = False
get_system_language     | RQ | 0100    | ctl_id 
get_schedule_fragment   | RQ | 0404    | ctl_id, zone_idx, frag_number, total_frags|None
set_schedule_fragment   | W  | 0404    | ctl_id, zone_idx, frag_num, frag_cnt, fragment
get_system_log_entry    | RQ | 0418    | ctl_id, log_idx
get_mix_valve_params    | RQ | 1030    | ctl_id, zone_idx
set_mix_valve_params    | W  | 1030    | ctl_id, zone_idx, max_flow_setpoint, min_flow_setpoint, valve_run_time, pump_run_time
get_dhw_params          | RQ | 10A0    | ctl_id
set_dhw_params          | W  | 10A0    | ctl_id, setpoint|None, overrun|None, differential|None
get_tpi_params          | RQ | 1100    | dev_id, domain_id|None
set_tpi_params          | W  | 1100    | ctl_id, domain_id|None, cycle_rate = 3, min_on_time = 5, min_off_time = 5, proportional_band_width|None
get_dhw_temp            | RQ | 1260    | ctl_id
put_dhw_temp            | I  | 1260    | dev_id, temperature|None
put_outdoor_temp        | I  | 1290    | dev_id, temperature|None
put_co2_level           | I  | 1298    | dev_id, co2_level|None
put_indoor_humidity     | I  | 12A0    | dev_id, indoor_humidity|None
get_zone_window_state   | RQ | 12B0    | ctl_id, zone_idx
get_dhw_mode            | RQ | 1F41    | ctl_id
set_dhw_mode            | W  | 1F41    | ctl_id, mode|None, active|None, until|None, duration|None
put_bind                | I/W| 1FC9    | verb: VerbT, src_id, codes: Code|Iterable[Code]|None, dst_id|None
set_fan_mode            | I  | 22F1    | fan_id, fan_mode|None, seqn|None, src_id|None, idx = "00"
set_bypass_position     | I/W| 22F7    | fan_id, bypass_position|None, src_id|None, **kwargs (bypass_mode: 'auto'|'on'|'off' optional)
get_zone_setpoint       | W  | 2309    | ctl_id, zone_idx
set_zone_setpoint       | W  | 2309    | ctl_id, zone_idx, setpoint
get_zone_mode           | RQ | 2349    | ctl_id, zone_idx
set_zone_mode           | W  | 2349    | ctl_id, zone_idx, mode|None, setpoint|None, until|None, duration|None
set_fan_param           | W  | 2411    | fan_id, param_id, value|int|float|bool, src_id|None
get_fan_param           | RQ | 2411    | fan_id, param_id, src_id
get_system_mode         | RQ | 2E04    | ctl_id
set_system_mode         | W  | 2E04    | ctl_id, system_mode|None, until|None
put_presence_detected   | I  | 2E10    | dev_id, presence_detected|None
get_zone_temp           | RQ | 30C9    | ctl_id, zone_idx
put_sensor_temp         | I  | 30C9    | dev_id, temperature|None
get_system_time         | RQ | 313F    | ctl_id
set_system_time         | W  | 313F    | ctl_id, datetime, is_dst = False
get_hvac_fan_31da       | I  | 31DA    | dev_id, hvac_id, bypass_position|None, air_quality|None, co2_level|None, indoor_humidity|None, outdoor_humidity|None, exhaust_temp|None, supply_temp|None, indoor_temp|None, outdoor_temp|None, speed_capabilities: list[str], fan_info, _unknown_fan_info_flags: list[int], exhaust_fan_speed|None, supply_fan_speed|None, remaining_mins|None, post_heat|None, pre_heat|None, supply_flow|None, exhaust_flow|None, **kwargs (air_quality_basis, _extra)
get_opentherm_data      | RQ | 3220    | otb_id, msg_id
put_actuator_state      | I  | 3EF0    | dev_id, modulation_level|None
put_actuator_cycle      | RP | 3EF1    | src_id, dst_id, modulation_level, actuator_countdown, cycle_countdown|None

```
</details>


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

| Command | Description |
|---|---|
| `POST_SCHEMA` | Publishes the current schema, parameters, and status from `ramses_rf` to the `_gateway_config` subtree under the system topic. |
| `SAVE_SCHEMA` | As above, and additionally saves the current schema back to the `ramses_rf_schema.json` file. |
| `REMOVE_HA_DISCOVERY` | Removes all evoGateway MQTT discovery entries from Home Assistant by publishing empty retained payloads to every discovery topic. Use this before a reconfiguration (e.g. renaming zones) or when uninstalling. See the [Home Assistant MQTT Discovery](#home-assistant-mqtt-discovery) section for details. |
| `RESTART_RF` | Restarts the RF layer (ramses_rf Gateway) without exiting the process. Equivalent to watchdog Stage 2. Published status `RF Restarting` while in progress, then `Online` on success. Use this if the evohome radio appears stuck. |
| `RESTART_GATEWAY` | Performs a clean shutdown and raises `SystemExit`, allowing the process manager (e.g. systemd) to restart the service. Equivalent to watchdog Stage 3. |
| `RESTART_PROCESS` | Performs a clean shutdown and then restarts the Python process directly via `os.execv`, without relying on an external process manager. Use this on systems without a service supervisor. |

These operations are local to evoGateway and are not forwarded on the RF network.

**Example — manually restart the RF layer:**

```json
{"sys_config": "RESTART_RF"}
```

**Example — restart the whole gateway process (no systemd required):**

```json
{"sys_config": "RESTART_PROCESS"}
```

---

## **Credits**

evoGateway builds upon two major open-source contributions:

* **`ramses_rf`** by David Bonnes (@zxdavb)
  [https://github.com/zxdavb/ramses_rf](https://github.com/zxdavb/ramses_rf)

* **`evofw3`** by Peter Price (@ghoti57)
  [https://github.com/ghoti57/evofw3](https://github.com/ghoti57/evofw3)

Their work provides the foundation for all decoding, encoding, and RF communication.
