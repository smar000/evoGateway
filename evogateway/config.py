"""Configuration Management for evoGateway.

This module defines the structured configuration schema using dataclasses (e.g., AppConfig, 
FilesConfig) and contains the logic for parsing settings from the external configuration 
file (evogateway.cfg). It also houses global constants related to file paths, logging, 
and application defaults.
"""

from __future__ import annotations
import ast
import configparser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Final
import paho.mqtt.client as mqtt  # Only needed for DEFAULT_COLOURS if moved here
from colorama import Fore, Back, Style # For DEFAULT_COLOURS only
import logging

# Constants & defaults
GATEWAY_VERSION = "4.6.2"

CONFIG_DIR_NAME: Final[Path] = Path("config")
LOGS_DIR_NAME: Final[Path] = Path("logs")
DEFAULT_EVOGW_CONFIG_FILE: Final[Path] = CONFIG_DIR_NAME / "evogateway.cfg"

GET_SCHED: Final[str] = "get_schedule"
SET_SCHED: Final[str] = "set_schedule"

MQTT_STATUS_SUBTOPIC: Final[str] = "status"
MQTT_OFFLINE: Final[str] = "Offline"
MQTT_ONLINE: Final[str] = "Online"

SEND_STATUS_TRANSMITTED: Final[str] = "Transmitted"
SEND_STATUS_FAILED: Final[str] = "Failed"
SEND_STATUS_SUCCESS: Final[str] = "Successful"

# Unified colour palette used across ANSI printing and colorlog
COLOUR_PALETTE = {
    "RQ": {
        "ansi": Fore.LIGHTBLACK_EX,
        "logger": "light_black",
    },
    "RP": {
        "ansi": Fore.LIGHTWHITE_EX,
        "logger": "white",
    },
    " W": {
        "ansi": Fore.LIGHTMAGENTA_EX,
        "logger": "light_magenta",
    },
    " I": {
        "ansi": Fore.CYAN,
        "logger": "cyan",
    },
    "MQTT": {
        "ansi": Fore.LIGHTGREEN_EX,
        "logger": "light_green",
    },
    "ERROR": {
        "ansi": Fore.LIGHTRED_EX,
        "logger": "light_red",
    },
    "INFO": {
        "ansi": Fore.LIGHTBLUE_EX,
        "logger": "light_blue",
    },
    "temperature": {
        "ansi": Fore.LIGHTYELLOW_EX,
        "logger": "light_yellow",
    },
}

DEFAULT_COLOURS = {
    key: value["ansi"] for key, value in COLOUR_PALETTE.items()
}

DEFAULT_MIN_ROW_LENGTH = 160
GET_SCHED_WAIT_PERIOD = 5

LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL, # 50
    "ERROR": logging.ERROR,       # 40
    "WARNING": logging.WARNING,   # 30
    "INFO": logging.INFO,         # 20
    "DEBUG": logging.DEBUG,       # 10
}

# Config dataclasses & loader
@dataclass
class FilesConfig:
    events_file: Path = Path("events.log")
    packet_log_file: Path = Path("packet.log")
    packet_log_retention_days: int = 7
    rotate_count: int = 9
    rotate_bytes: int = 1_000_000
    schema_file: Path = Path("ramses_rf_schema.json")
    ot_sensors_cache_file: Path = Path("ot_sensors_cache.json")
    save_schema_on_shutdown: bool = False
    max_save_file_count: int = 9


@dataclass
class SerialConfig:
    port: str = "/dev/ttyUSB0"
    baud: int = 115200

from dataclasses import dataclass, field

@dataclass
class MqttConfig:
    """
    MQTT configuration including:
      - broker connection settings
      - publishing behaviour
      - topic structure settings
      - a nested TopicLayout object (`self.topics`)
    """

    # Broker connection settings
    server: str = ""
    user: str = ""
    password: str = ""
    client_id: str = "evoGateway"

    # Publishing options
    pub_json_only: bool = False
    pub_kv_with_json: bool = False
    group_by_zone: bool = True

    # Legacy compatibility toggle
    use_legacy_topic_structure: bool = False

    # Root publishing and command topics
    cmd_topic: str = ""
    root_topic: str = ""

    # Legacy topic structure (for backwards compatiblity)
    system_subtopic: str = "_zone_independent"
    zone_unknown_subtopic: str = "_zone_unknown"
    zones_subtopic: str = ""
    controllers_subtopic: str = ""
    relays_subtopic: str = "relays"

    dhw_is_zone: bool = True
    dhw_zone_subtopic: str = "_dhw"

    # Home Assistant MQTT discovery
    ha_discovery_enabled: bool = False
    ha_discovery_prefix: str = "homeassistant"

    # Prefix for all HA device/entity unique IDs (defaults to snake_case of MQTT_CLIENTID).
    # Change this if running dev and prod gateways against the same HA instance.
    ha_discovery_id_prefix: str = ""

    # Optional text prepended/appended to zone climate entity names.
    # e.g. prefix="" suffix="Heating" → "Living Room Heating"
    ha_zone_climate_prefix: str = ""
    ha_zone_climate_suffix: str = "Heating"

    # Advanced: override DHW topic sub-paths (relative to {root}/zones/{dhw_slug}/).
    # Leave empty to auto-detect from device registry.
    ha_dhw_temp_subtopic: str = ""
    ha_dhw_params_subtopic: str = ""
    ha_dhw_mode_subtopic: str = ""

    # Filled automatically in __post_init__
    topics: "MqttConfig.TopicLayout" = field(init=False)

    @dataclass(frozen=True)
    class TopicLayout:
        root: str
        cmd: str

        # structural buckets
        zones: str
        system: str
        zone_unknown: str

        # device-type buckets
        controller: str
        hgi: str
        relays: str
        ufh: str
        boiler: str
        dhw: str

        # behaviour flags
        dhw_is_zone: bool
        use_legacy: bool

    def __post_init__(self) -> None:
        """
        Build the TopicLayout object (`self.topics`).
        This is the SINGLE source of MQTT topic structure everywhere.
        """

        if self.use_legacy_topic_structure:
            self.topics = self.TopicLayout(
                root=self.root_topic,
                cmd=self.cmd_topic,

                zones=self.zones_subtopic or "",
                system=self.system_subtopic,
                zone_unknown=self.zone_unknown_subtopic,

                controller=self.controllers_subtopic or "controller",
                hgi="",
                relays=self.relays_subtopic or "relays",
                ufh="",
                boiler="",
                dhw=self.dhw_zone_subtopic or "_dhw",

                dhw_is_zone=self.dhw_is_zone,
                use_legacy=True,
            )
        else:
            # New hierarchical topic layout
            self.topics = self.TopicLayout(
                root=self.root_topic,
                cmd=self.cmd_topic,

                zones=self.zones_subtopic or "zones",
                system="system",
                zone_unknown=self.zone_unknown_subtopic or "unknown",

                controller="controller",
                hgi="hgi",
                relays="relays",
                ufh="ufh",
                boiler="boiler",
                dhw=self.dhw_zone_subtopic or "_dhw",

                dhw_is_zone=self.dhw_is_zone,
                use_legacy=False,
            )

    def build_topic_layout(self) -> "MqttConfig.TopicLayout":
        """Return an immutable TopicLayout depending on legacy vs new structure."""
        if self.use_legacy_topic_structure:
            return MqttConfig.TopicLayout(
                root=self.root_topic or "evogateway",
                cmd=self.cmd_topic or "_zone_indepdendent/_command",
                system=self.system_subtopic or "_zone_independent",
                zone_unknown=self.zone_unknown_subtopic or "_zone_unknown",
                zones=self.zones_subtopic or "",                
                controller="",   # legacy doesn't separate controllers
                hgi="",          # or HGI
                relays=self.relays_subtopic or "relays",
                ufh="",          # no separation in legacy layout
                boiler="",       # same
                dhw=self.dhw_zone_subtopic or "_dhw",
                dhw_is_zone=self.dhw_is_zone,
                use_legacy=True,
            )

        # New structured topic layout
        return MqttConfig.TopicLayout(
            root=self.root_topic or "evogateway",
            cmd=self.cmd_topic or "system/_command",
            system="system",
            zone_unknown="unknown",
            zones="zones",
            controller="controllers",
            hgi="hgi",
            relays="relays",
            ufh="ufh",
            boiler="boiler",
            dhw=self.dhw_zone_subtopic or "_dhw",
            dhw_is_zone=self.dhw_is_zone,
            use_legacy=False,
        )
   
@dataclass
class RamsesConfig:
    disable_sending: bool = False
    disable_discovery: bool = True
    enable_eavesdrop: bool = False
    known_list: bool = True


@dataclass
class WatchdogConfig:
    # Poll interval (seconds) for the heartbeat/watchdog loop.
    # All RF timeout thresholds are accurate to within ±this value.
    # MQTT_HEARTBEAT_INTERVAL should be >= this value for accurate timing.
    # Set to 0 to disable both the MQTT heartbeat and all watchdog stages entirely.
    watchdog_check_interval: int = 60       # 1 min

    # How often (seconds) to re-publish the MQTT Online heartbeat.
    # Set to 0 to disable the heartbeat entirely.
    mqtt_heartbeat_interval: int = 300      # 5 min

    # RF silence thresholds. Set any value to 0 to disable that stage.
    # Stage 2 is independent of Stage 1 (does not require Stage 1 to have fired).
    # Stages 3 and 4 are measured from when Stage 2 (RF restart) was attempted.
    rf_warn_timeout: int = 900              # 15 min from last RF msg  → Stage 1: warn
    rf_restart_timeout: int = 1800          # 30 min from last RF msg  → Stage 2: restart RF layer
    rf_process_restart_timeout: int = 900   # 15 min after Stage 2     → Stage 3: restart process
    rf_exit_timeout: int = 1800             # 30 min after Stage 2     → Stage 4: raise SystemExit

@dataclass
class MiscConfig:
    this_gateway_name: str = "evoGateway"
    min_row_length: int = DEFAULT_MIN_ROW_LENGTH
    merge_unknown_otb: bool = True
    merge_unknown_hgi: bool = True
    merge_unknown_ctl: bool = False
    display_colours: Dict[str, str] = field(default_factory=lambda: DEFAULT_COLOURS.copy())
    log_level_int: int = logging.INFO # default to INFO (20)
    log_events_to_console: bool = False
    log_events_with_device_names: bool = True
    use_local_time: bool = False

@dataclass
class AppConfig:
    serial: SerialConfig
    files: FilesConfig
    mqtt: MqttConfig
    ramses: RamsesConfig
    misc: MiscConfig
    watchdog: WatchdogConfig = field(default_factory=WatchdogConfig)

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "AppConfig":
        cfg_path = Path(path or "evogateway.cfg")
        parser = configparser.RawConfigParser()
        parser.read(cfg_path)

        serial = SerialConfig(
            port=parser.get("Serial Port", "COM_PORT", fallback="/dev/ttyUSB0"),
            baud=parser.getint("Serial Port", "COM_BAUD", fallback=115200),
        )
        
        config_dir = Path(CONFIG_DIR_NAME)
        logs_dir = Path(LOGS_DIR_NAME)

        events_filename = parser.get("Files", "EVENTS_FILE", fallback="events.log")
        packet_filename = parser.get("Files", "PACKET_LOG_FILE", fallback="packet.log")
        schema_filename = parser.get("Files", "SCHEMA_FILE", fallback="ramses_rf_schema.json")

        files = FilesConfig(
            events_file=logs_dir.joinpath(events_filename),
            packet_log_file=logs_dir.joinpath(packet_filename),
            packet_log_retention_days=parser.getint("Files", "PACKET_LOG_RETENTION_DAYS", fallback=7),
            rotate_count=parser.getint("Files", "LOG_FILE_ROTATE_COUNT", fallback=9),
            rotate_bytes=parser.getint("Files", "LOG_FILE_ROTATE_BYTES", fallback=1_000_000),
            schema_file=config_dir.joinpath(schema_filename),
            ot_sensors_cache_file=config_dir.joinpath("ot_sensors_cache.json"),
            save_schema_on_shutdown=parser.getboolean("Files", "ALWAYS_SAVE_SCHEMA_ON_SHUTDOWN", fallback=False),
            max_save_file_count=parser.getint("Files", "MAX_SAVE_FILE_COUNT", fallback=9),
        )        

        mqtt_pub_json_only = parser.getboolean("MQTT", "MQTT_PUB_AS_JSON", fallback=False)
        mqtt_pub_kv_with_json = parser.getboolean("MQTT", "MQTT_PUB_KV_WITH_JSON", fallback=True)
        if mqtt_pub_kv_with_json:
            mqtt_pub_json_only = False

        mqtt = MqttConfig(
            server=parser.get("MQTT", "MQTT_SERVER", fallback=""),
            user=parser.get("MQTT", "MQTT_USER", fallback=""),
            password=parser.get("MQTT", "MQTT_PW", fallback=""),
            client_id=parser.get("MQTT", "MQTT_CLIENTID", fallback="evoGateway"),

            pub_json_only=mqtt_pub_json_only,
            pub_kv_with_json=mqtt_pub_kv_with_json,
            group_by_zone=parser.getboolean("MQTT", "MQTT_GROUP_BY_ZONE", fallback=True),

            use_legacy_topic_structure=parser.getboolean("MQTT", "MQTT_LEGACY_TOPIC_STRUCTURE", fallback=False),

            root_topic=parser.get("MQTT", "MQTT_ROOT_TOPIC", fallback="evogateway"),
            cmd_topic=parser.get("MQTT", "MQTT_CMD_TOPIC", fallback="system/_command"),
            system_subtopic=parser.get("MQTT", "MQTT_SYSTEM_SUBTOPIC", fallback="system"),            
            zone_unknown_subtopic=parser.get("MQTT", "MQTT_ZONE_UNKNOWN_SUBTOPIC", fallback="_zone_unknown"),
            zones_subtopic=parser.get("MQTT", "MQTT_ZONES_SUBTOPIC", fallback=""),
            controllers_subtopic=parser.get("MQTT", "MQTT_CONTROLLERS_SUBTOPIC", fallback=""),
            relays_subtopic=parser.get("MQTT", "MQTT_RELAYS_SUBTOPIC", fallback=""),
            dhw_zone_subtopic=parser.get("MQTT", "MQTT_DHW_ZONE_SUBTOPIC", fallback="_dhw"),
            
            dhw_is_zone=parser.getboolean("MQTT", "MQTT_DHW_IS_ZONE", fallback=True),

            ha_discovery_enabled=parser.getboolean("MQTT", "HA_DISCOVERY_ENABLED", fallback=False),
            ha_discovery_prefix=parser.get("MQTT", "HA_DISCOVERY_PREFIX", fallback="homeassistant"),
            ha_discovery_id_prefix=parser.get("MQTT", "HA_DISCOVERY_ID_PREFIX", fallback=""),
            ha_zone_climate_prefix=parser.get("MQTT", "HA_ZONE_CLIMATE_PREFIX", fallback=""),
            ha_zone_climate_suffix=parser.get("MQTT", "HA_ZONE_CLIMATE_SUFFIX", fallback="Heating"),

            ha_dhw_temp_subtopic=parser.get("MQTT", "HA_DHW_TEMP_SUBTOPIC", fallback=""),
            ha_dhw_params_subtopic=parser.get("MQTT", "HA_DHW_PARAMS_SUBTOPIC", fallback=""),
            ha_dhw_mode_subtopic=parser.get("MQTT", "HA_DHW_MODE_SUBTOPIC", fallback=""),
        )

        ramses = RamsesConfig(
            disable_sending=parser.getboolean("Ramses_rf", "DISABLE_SENDING", fallback=False),
            disable_discovery=not parser.getboolean("Ramses_rf", "ENABLE_DISCOVERY", fallback=False),
            enable_eavesdrop=parser.getboolean("Ramses_rf", "ENABLE_EAVESDROP", fallback=False),
            known_list=parser.getboolean("Ramses_rf", "KNOWN_LIST", fallback=True),
        )


        log_level_str = parser.get("Misc", "LOG_LEVEL", fallback="INFO").upper()        
        log_level_int = LOG_LEVELS.get(log_level_str, logging.INFO)
        if log_level_int == logging.INFO and log_level_str != "INFO":            
            print(f"Warning: Invalid LOG_LEVEL '{log_level_str}' found in config. Defaulting to INFO.")
        

        misc = MiscConfig(
            this_gateway_name=parser.get("Misc", "THIS_GATEWAY_NAME", fallback="evoGateway"),
            min_row_length=parser.getint("Misc", "MIN_ROW_LENGTH", fallback=DEFAULT_MIN_ROW_LENGTH),
            merge_unknown_otb=parser.getboolean("Misc", "MERGE_UNKNOWN_OTB", fallback=True),
            merge_unknown_hgi=parser.getboolean("Misc", "MERGE_UNKNOWN_HGI", fallback=True),
            merge_unknown_ctl=parser.getboolean("Misc", "MERGE_UNKNOWN_CTL", fallback=False),

            display_colours=_ensure_colour_scheme(parser.get("Misc", "DISPLAY_COLOURS", fallback=None)),
            log_level_int=log_level_int, # <- Pass the integer value
            log_events_to_console = parser.getboolean("Misc", "EVENTS_CONSOLE_OUTPUT", fallback=False),
            log_events_with_device_names = parser.getboolean("Misc", "EVENTS_LOG_WITH_DEVICE_NAMES", fallback=True),
            use_local_time=parser.getboolean("Misc", "USE_LOCAL_TIME", fallback=False),
        )

        watchdog = WatchdogConfig(
            watchdog_check_interval=parser.getint("Watchdog", "WATCHDOG_CHECK_INTERVAL", fallback=60),
            mqtt_heartbeat_interval=parser.getint("Watchdog", "MQTT_HEARTBEAT_INTERVAL", fallback=300),
            rf_warn_timeout=parser.getint("Watchdog", "RF_WARN_TIMEOUT", fallback=900),
            rf_restart_timeout=parser.getint("Watchdog", "RF_RESTART_TIMEOUT", fallback=1800),
            rf_process_restart_timeout=parser.getint("Watchdog", "RF_PROCESS_RESTART_TIMEOUT", fallback=900),
            rf_exit_timeout=parser.getint("Watchdog", "RF_EXIT_TIMEOUT", fallback=1800),
        )

        return cls(serial=serial, files=files, mqtt=mqtt, ramses=ramses, misc=misc, watchdog=watchdog)

def _ensure_colour_scheme(raw_value: str | None) -> dict[str, str]:
    """
    Validate and normalise the console colour scheme using DEFAULT_COLOURS
    as the source of valid keys. Ensures all keys exist and values are valid
    ANSI strings; user-supplied overrides only replace existing defaults.
    """
    result = DEFAULT_COLOURS.copy()

    if not raw_value:
        return result

    try:
        user_dict = ast.literal_eval(raw_value)
        if isinstance(user_dict, dict):
            for key in DEFAULT_COLOURS:
                val = user_dict.get(key)
                if isinstance(val, str):
                    result[key] = val
    except Exception:
        # Invalid user config → fall back to defaults
        pass

    return result
