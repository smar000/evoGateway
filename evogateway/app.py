"""The evoGateway Application Orchestrator.

This module defines the EvoGatewayApp class, which acts as the application's central 
dependency injector and lifecycle manager. It initializes all core components (Logger, 
Configuration, Persistence, MQTT, RamsesRF Service, Router, and Schedule Handler) 
and manages the main application loop, including graceful startup and shutdown procedures.
"""

from __future__ import annotations
import asyncio
import datetime as _dt
from typing import TYPE_CHECKING, Any
import json

from ramses_rf.version import VERSION as RAMSES_RF_VERSION
from colorama import init as colorama_init, Fore, Style

# Internal dependencies
from .config import GATEWAY_VERSION, MQTT_STATUS_SUBTOPIC, GET_SCHED, SET_SCHED
from .config import AppConfig # Import the entire config object
from .logger import init_logging
from .router import MessageRouter
from .services import RamsesService, MQTTService, ScheduleHandler, PersistenceService
from .registry import DeviceRegistry
from .utils import print_formatted_row


class EvoGatewayApp:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.log = init_logging(
            events_file=cfg.files.events_file,
            rotate_bytes=cfg.files.rotate_bytes,
            rotate_count=cfg.files.rotate_count,
            console_level=self.cfg.misc.log_level_int,
            log_events_to_console=self.cfg.misc.log_events_to_console,
        )
        colorama_init()

        self.loop: asyncio.AbstractEventLoop | None = None
        self.mqtt: MQTTService | None = None
        self.router: MessageRouter | None = None
        self.ramses: RamsesService | None = None
        self.schedule: ScheduleHandler | None = None

        self.registry = DeviceRegistry()

        self._loaded_schema: dict[str, Any] | None = None        
        self.persistence = PersistenceService(            
            schema_file=self.cfg.files.schema_file,
            logger = self.log,
            max_backups=self.cfg.files.max_save_file_count,
        )

        # Intialise mqtt topic structure to use legacy or new structure
        self.mqtt_topics = self.cfg.mqtt.build_topic_layout()

        self.router = MessageRouter(
            mqtt=self.mqtt,
            color_scheme=self.cfg.misc.display_colours,
            min_row_length=self.cfg.misc.min_row_length,
            registry=self.registry,
            group_by_zone=self.cfg.mqtt.group_by_zone,
            mqtt_topics=self.mqtt_topics,
            pub_json_only=self.cfg.mqtt.pub_json_only, 
            pub_kv_with_json=self.cfg.mqtt.pub_kv_with_json,
            log_events_with_device_names=True,
            logger=self.log,
        )
        self.schedule = ScheduleHandler(
            ramses=self.ramses, 
            router=self.router, 
            logger=self.log
        ) # Injecting the router and callbacks

    def _build_ramses_config(self) -> tuple[str, dict[str, Any]]:
        """
        Build kwargs for ramses_rf.Gateway, pulling schema from PersistenceService, with fallback to discovery.
        """

        # Force discovery if we don't have a schema file or if disable discovery overridden in config file 
        discovery_disabled = not self.persistence.schema_exists() or self.cfg.ramses.disable_discovery

        # Root library kwargs structure
        lib_cfg: dict[str, Any] = {
            "config": {
                "disable_sending": self.cfg.ramses.disable_sending,
                "disable_discovery": discovery_disabled,
                "enable_eavesdrop": not discovery_disabled or self.cfg.ramses.enable_eavesdrop,
                "enforce_known_list": False,
                "max_zones": 12,
                "use_aliases": True,
            }
        }
        
        if discovery_disabled:
            # Try to load from schema file
            schema = self.persistence.load_schema()
            has_schema = bool(schema)
        
            if has_schema:
                lib_cfg.update(schema)                
                # Config file flags take priority 
                lib_cfg["config"]["disable_discovery"] = self.cfg.ramses.disable_discovery
                lib_cfg["config"]["enable_eavesdrop"] = not self.cfg.ramses.disable_discovery or self.cfg.ramses.enable_eavesdrop
            else:
                msg = "No valid schema available. Falling back to discovery"
                self.log.info(msg)
                print_formatted_row(msg)
                lib_cfg["config"]["disable_discovery"] = False
                lib_cfg["config"]["enforce_known_list"] = False
                lib_cfg["config"]["enable_eavesdrop"] = True
        else:
            print_formatted_row("Discovery mode enabled")

        # Packet log settings
        lib_cfg["packet_log"] = {}
        if self.cfg.files.packet_log_file:
            lib_cfg["packet_log"]["file_name"] = str(self.cfg.files.packet_log_file)
        if self.cfg.files.rotate_bytes:
            lib_cfg["packet_log"]["rotate_bytes"] = self.cfg.files.rotate_bytes
        if self.cfg.files.rotate_count:
            lib_cfg["packet_log"]["rotate_backups"] = self.cfg.files.rotate_count

        return self.cfg.serial.port, lib_cfg

    # MQTT inbound
    async def _on_mqtt_message(self, payload: dict) -> None:
        self.log.info(f"MQTT message received: {payload}")
        style = self.cfg.misc.display_colours.get("MQTT", "")
        print_formatted_row(
            f"MQTT message received: {payload}",
            style_prefix=style,
            min_row_length=self.cfg.misc.min_row_length,
        )
        
        try:
            if payload.get("command") in (GET_SCHED, SET_SCHED):
                if self.schedule:
                    await self.schedule.handle_command(payload, self._publish_command_status)
                return
            if self.ramses:
                await self.ramses.process_command(payload, self._publish_command_status)
        except Exception as ex:
            pass

    def _publish_command_status(
        self,
        cmd: str | None,
        status: str,
        error: str | None = None,
    ) -> None:
        """Publish command status via MQTT, log file, and console."""

        ts = _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        msg_parts: list[str] = [f"Command Send Status: {status}"]
        if cmd:
            msg_parts.append(f"(command={cmd})")
        if error:
            msg_parts.append(f"- {error}")
        msg = " ".join(msg_parts)

        # Log to events log 
        try:
            if status.lower() == "failed":
                self.log.error(msg)
            else:
                self.log.info(msg)
        except Exception:
            # Don't let logging failure stop us
            pass

        # Print to console in the usual formatted style 
        try:
            # Simple mapping: failed -> ERROR colour, else -> INFO colour
            style_key = "ERROR" if status.lower() == "failed" else "INFO"
            style = self.cfg.misc.display_colours.get(style_key, "")

            print_formatted_row(
                msg,
                style_prefix=style,
                min_row_length=self.cfg.misc.min_row_length,
            )
        except Exception:
            # Again, don't let console issues kill status publishing
            self.log.exception("Failed to print command status row")

        # Publish to MQTT as JSON 
        if not (self.mqtt and self.cfg.mqtt.cmd_topic):
            return

        topic = f"{self.cfg.mqtt.root_topic}/{self.cfg.mqtt.cmd_topic}/_last_command"

        payload: dict[str, Any] = {
            "status": status,
            "ts": ts,
        }
        if cmd is not None:
            payload["command"] = cmd
        if error:
            payload["error"] = error

        try:
            # Publish both the json and the individual items
            self.mqtt._client.publish(topic, json.dumps(payload), qos=1, retain=True)
            self.mqtt._client.publish(f"{topic}/command", cmd, qos=1, retain=True)
            self.mqtt._client.publish(f"{topic}/status", status, qos=1, retain=True)
            self.mqtt._client.publish(f"{topic}/error", error or "", qos=1, retain=True)
            self.mqtt._client.publish(f"{topic}/timestamp", ts, qos=1, retain=True)
        except Exception:
            self.log.exception("Failed to publish command status to MQTT")

    def _publish_schema_snapshot(self) -> None:
        if not (self.mqtt and self.ramses and self.ramses.gwy):
            return

        try:
            gwy = self.ramses.gwy
            topics = self.mqtt_topics  
            gateway_name = self.cfg.misc.this_gateway_name.lower()

            base = f"{topics.system}/_{gateway_name.lower()}"

            # Mode (monitor/eavesdrop)
            using_discovery = not self.ramses.gwy.config.disable_discovery
            using_eavesdrop = self.ramses.gwy.config.enable_eavesdrop
            mode = "eavesdrop" if using_discovery or using_eavesdrop else "monitor"
            self.mqtt.publish(f"{base}/_{gateway_name}_mode", mode)

            # Config, schema,  params and status
            config = vars(gwy.config)
            full_schema = self.current_schema_snapshot()
            tcs_schema = gwy.schema if gwy.tcs is None else gwy.tcs.schema
            params = gwy.params if gwy.tcs is None else gwy.tcs.params
            status = gwy.status if gwy.tcs is None else gwy.tcs.status
            known_list  = gwy.known_list

            self.mqtt.publish(f"{base}/config", json.dumps(config, sort_keys=True))
            self.mqtt.publish(f"{base}/schema_full", json.dumps(full_schema, sort_keys=True))
            self.mqtt.publish(f"{base}/schema_tcs", json.dumps(tcs_schema, sort_keys=True))
            self.mqtt.publish(f"{base}/params", json.dumps(params, sort_keys=True))
            self.mqtt.publish(f"{base}/status", json.dumps(status, sort_keys=True))
            self.mqtt.publish(f"{base}/known_list", json.dumps(known_list, sort_keys=True))

            # Devices (id → alias + zone_id)
            devices = {
                str(k): {"alias": getattr(v,"alias",""), "zone_id": getattr(v,"zone_id","")} for k, v in gwy.device_by_id.items()
            }
            self.mqtt.publish(f"{base}/devices", json.dumps(devices, sort_keys=True))
            self.mqtt.publish(f"{base}/devices_count", len(devices))

            # Zones            
            zones = {               
                str(k): v.name if v.name else str(v)
                for k, v in gwy.tcs.zone_by_idx.items()
            } if gwy.tcs else {}
            self.mqtt.publish(f"{base}/zones", json.dumps(zones, sort_keys=True))
            self.mqtt.publish(f"{base}/zones_count", len(zones))

            # Underfloor heating circuits
            self.mqtt.publish(f"{base}/ufh_circuits", json.dumps(self.ramses.ufh_circuits, sort_keys=True))

            # Timestamp
            self.mqtt.publish(
                f"{base}/_{gateway_name}_ts",
                _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            )
        except Exception as ex:
            print_formatted_row(f"Exception: {ex}")
        
    # Print schema/devices to console 
    def _print_gateway_schema(self) -> None:
        if not (self.ramses and self.ramses.gwy):
            return
        gwy = self.ramses.gwy
        schema = self.current_schema_snapshot()
        print(f"Schema: {json.dumps(schema, indent=4)}\r\n")
        try:
            print(f"Params: {json.dumps(gwy.params)}\r\n")
        except Exception:
            pass
        try:
            print(f"Status: {json.dumps(gwy.status)}")
        except Exception:
            pass
        try:
            orphans = [d for d in sorted(gwy.schema.get('orphans_heat', []))]
            print(f"Schema[orphans_heat]: {json.dumps({'orphans_heat': orphans}, indent=4)}\r\n")
        except Exception:
            pass

        # Devices
        devices = {
            str(k): {"alias": getattr(v,"alias",""), "zone_id": getattr(v,"zone_id","")} for k, v in gwy.device_by_id.items()
        }
        print(f"Devices: {json.dumps(devices, indent=4)}")


    def current_schema_snapshot(self) -> dict:
        gwy = self.ramses.gwy
        config = {"config": vars(gwy.config)}
        known_list = {"known_list": getattr(gwy, "known_list", {})}
        schema = {**config, **(gwy.schema or {}), **known_list}
        return schema

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()

        # MQTT (threaded)
        self.mqtt = MQTTService(
            server=self.cfg.mqtt.server,
            user=self.cfg.mqtt.user,
            password=self.cfg.mqtt.password,
            client_id=self.cfg.mqtt.client_id,
            cmd_topic=self.cfg.mqtt.cmd_topic,
            root_topic=self.cfg.mqtt.root_topic,
            status_subtopic=MQTT_STATUS_SUBTOPIC,
            on_message_async=self._on_mqtt_message,
            loop=self.loop,
            logger=self.log,
        )
        self.mqtt.start()  # do not await

        # Router
        self.router = MessageRouter(
            mqtt=self.mqtt,
            color_scheme=self.cfg.misc.display_colours,
            min_row_length=self.cfg.misc.min_row_length,
            registry=self.registry,  
            group_by_zone=self.cfg.mqtt.group_by_zone,
            mqtt_topics=self.mqtt_topics,
            pub_json_only=self.cfg.mqtt.pub_json_only,
            pub_kv_with_json=self.cfg.mqtt.pub_kv_with_json,
            log_events_with_device_names=self.cfg.misc.log_events_with_device_names,
            logger=self.log,
        )

        # Ramses
        serial_port, self._loaded_schema = self._build_ramses_config()
        self.ramses = RamsesService(
            serial_port=serial_port,
            lib_kwargs=self._loaded_schema,
            logger=self.log,
            registry=self.registry,  
            on_message=self.router.handle_message,
            publish_schema=self._publish_schema_snapshot,
            on_sys_config=self._handle_sys_config,
            colors=self.cfg.misc.display_colours,
            min_row_length=self.cfg.misc.min_row_length,
        )

        # Schedule
        self.schedule = ScheduleHandler(
            ramses=self.ramses, 
            router=self.router, 
            logger=self.log
        )

        # Banner
        print_formatted_row(
            text=f"# evogateway {GATEWAY_VERSION} (ramses_rf {RAMSES_RF_VERSION})",
            style_prefix="",
            min_row_length=self.cfg.misc.min_row_length,
        )

        # Start RF gateway
        await self.ramses.start()

        # Update registry with the connected HGI friendly name
        hgi_id = self.registry.hgi_id
        if hgi_id and self.cfg.misc.this_gateway_name:
            self.registry.update_alias(hgi_id, self.cfg.misc.this_gateway_name)
      
        # Show known devices on startup 
        self.router.display_device_list(self.ramses)

        try:
            while True:
                await asyncio.sleep(3600)

        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            print("Shutting down…")

        finally:
            await self.shutdown()
            raise SystemExit

    async def shutdown(self) -> None:
        # If eavesdrop OR discovery active -> print/save schema
        try:
            if self.ramses and self.ramses.gwy:
                using_discovery = not self.ramses.gwy.config.disable_discovery
                using_eavesdrop = self.ramses.gwy.config.enable_eavesdrop

                self._print_gateway_schema()
                self._publish_schema_snapshot()

                if self.cfg.files.save_schema_on_shutdown or using_eavesdrop or using_discovery: 
                    # Save schema file
                    self._handle_sys_config("SAVE_SCHEMA")
                    print("Schema has been saved to file")

        except Exception:
            self.log.exception("Failed saving schema on shutdown")

        # Stop MQTT
        try:
            if self.mqtt:
                self.mqtt.stop()
        except Exception:
            pass

        # Stop RF (no-op placeholder)
        try:
            if self.ramses:
                await self.ramses.stop()
        except Exception:
            pass

    # Sys-config persistence hook (invoked from RamsesService on SAVE_SCHEMA/POST_SCHEMA)
    def _handle_sys_config(self, cmd: str) -> None:
        try:
            self._publish_schema_snapshot()
            if cmd in "SAVE_SCHEMA":
                schema = self.current_schema_snapshot()
                self.persistence.save_schema(schema)
        except Exception:
            self.log.exception("Failed persisting schema")
