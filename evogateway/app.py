"""The evoGateway Application Orchestrator.

This module defines the EvoGatewayApp class, which acts as the application's central 
dependency injector and lifecycle manager. It initializes all core components (Logger, 
Configuration, Persistence, MQTT, RamsesRF Service, Router, and Schedule Handler) 
and manages the main application loop, including graceful startup and shutdown procedures.
"""

from __future__ import annotations
import asyncio
import datetime as _dt
import os
import sys
from typing import TYPE_CHECKING, Any
import json

from ramses_rf.version import VERSION as RAMSES_RF_VERSION
from ramses_rf.config import GatewayConfig
from ramses_tx.config import EngineConfig
from colorama import init as colorama_init, Fore, Style

# Internal dependencies
from .config import GATEWAY_VERSION, MQTT_STATUS_SUBTOPIC, MQTT_ONLINE, GET_SCHED, SET_SCHED
from .config import AppConfig # Import the entire config object
from .logger import init_logging
from .router import MessageRouter
from .services import RamsesService, MQTTService, ScheduleHandler, PersistenceService
from .registry import DeviceRegistry
from .utils import print_formatted_row, local_now


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
        self._heartbeat_task: asyncio.Task | None = None
        self._restart_process: bool = False
        self._shutdown_reason: str = ""

        self.registry = DeviceRegistry(
            merge_unknown_otb=cfg.misc.merge_unknown_otb,
            merge_unknown_hgi=cfg.misc.merge_unknown_hgi,
            merge_unknown_ctl=cfg.misc.merge_unknown_ctl,
        )

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

    def _build_ramses_config(self) -> GatewayConfig:
        """Build a GatewayConfig for ramses_rf.Gateway, loading schema with fallback to discovery."""
        discovery_disabled = not self.persistence.schema_exists() or self.cfg.ramses.disable_discovery
        disable_discovery = discovery_disabled
        enable_eavesdrop = not discovery_disabled or self.cfg.ramses.enable_eavesdrop
        enforce_known_list = False

        schema: dict[str, Any] = {}
        known_list: dict[str, Any] = {}
        block_list: dict[str, Any] = {}

        if discovery_disabled:
            loaded_schema = self.persistence.load_schema()
            if loaded_schema:
                # The schema file stores a flat dict: TCS id keys + metadata keys.
                # Strip non-schema metadata before passing to GatewayConfig.
                _meta_keys = {"config", "known_list", "block_list"}
                schema = {k: v for k, v in loaded_schema.items() if k not in _meta_keys}
                known_list = loaded_schema.get("known_list", {})
                block_list = loaded_schema.get("block_list", {})
                disable_discovery = self.cfg.ramses.disable_discovery
                enable_eavesdrop = not self.cfg.ramses.disable_discovery or self.cfg.ramses.enable_eavesdrop
            else:
                msg = "No valid schema available. Falling back to discovery"
                self.log.info(msg)
                print_formatted_row(msg)
                disable_discovery = False
                enforce_known_list = False
                enable_eavesdrop = True
        else:
            print_formatted_row("Discovery mode enabled")

        # TODO: packet_log format changed in ramses_rf 0.57.0 — field names changed
        # (file_name → packet_log_path, rotate_backups count → packet_log_retention_days
        # in days). Restore packet logging once the new format is mapped.
        engine = EngineConfig(
            port_name=self.cfg.serial.port,
            disable_sending=self.cfg.ramses.disable_sending,
            enforce_known_list=enforce_known_list,
        )

        return GatewayConfig(
            disable_discovery=disable_discovery,
            enable_eavesdrop=enable_eavesdrop,
            max_zones=12,
            schema=schema,
            known_list=known_list,
            block_list=block_list,
            engine=engine,
        )

    # MQTT inbound
    async def _on_mqtt_message(self, payload: dict, is_retained: bool = False) -> None:
        self.log.info(f"MQTT message received: {payload}")
        style = self.cfg.misc.display_colours.get("MQTT", "")
        print_formatted_row(
            f"MQTT message received: {payload}",
            style_prefix=style,
            min_row_length=self.cfg.misc.min_row_length,
        )

        try:
            sys_cmd = str(payload.get("sys_config", "")).upper().strip()
            if sys_cmd in ("RESTART_RF", "RESTART_GATEWAY", "RESTART_PROCESS"):
                if is_retained:
                    self.log.warning(f"Clearing stale retained gateway command '{sys_cmd}' from broker — command will not be executed")
                    self.mqtt.publish(self.cfg.mqtt.cmd_topic, "", retain=True)
                    return
                await self._handle_gateway_command(sys_cmd)
                return
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

        ts = local_now(self.cfg.misc.use_local_time).strftime("%Y-%m-%dT%H:%M:%S")

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

    async def _publish_schema_snapshot(self) -> None:
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

            # Config, schema, params and status
            full_schema = await self.current_schema_snapshot()
            tcs_schema = await gwy.schema() if gwy.tcs is None else await gwy.tcs.schema()
            params = await gwy.params() if gwy.tcs is None else await gwy.tcs.params()
            status = await gwy.status() if gwy.tcs is None else await gwy.tcs.status()
            known_list = gwy.config.known_list

            self.mqtt.publish(f"{base}/schema_full", json.dumps(full_schema, default=str, sort_keys=True))
            self.mqtt.publish(f"{base}/schema_tcs", json.dumps(tcs_schema, default=str, sort_keys=True))
            self.mqtt.publish(f"{base}/params", json.dumps(params, default=str, sort_keys=True))
            self.mqtt.publish(f"{base}/status", json.dumps(status, default=str, sort_keys=True))
            self.mqtt.publish(f"{base}/known_list", json.dumps(known_list, default=str, sort_keys=True))

            # Devices (id → alias + zone_id)
            devices = {
                str(k): {"alias": getattr(v, "alias", ""), "zone_id": getattr(v, "zone_id", "")}
                for k, v in gwy.device_registry.device_by_id.items()
            }
            self.mqtt.publish(f"{base}/devices", json.dumps(devices, sort_keys=True))
            self.mqtt.publish(f"{base}/devices_count", len(devices))

            # Zones (use _name attr — .name() is async in 0.57.0)
            zones = {
                str(k): (getattr(v, "_name", None) or str(v))
                for k, v in gwy.tcs.zone_by_idx.items()
            } if gwy.tcs else {}
            self.mqtt.publish(f"{base}/zones", json.dumps(zones, sort_keys=True))
            self.mqtt.publish(f"{base}/zones_count", len(zones))

            # Underfloor heating circuits
            self.mqtt.publish(f"{base}/ufh_circuits", json.dumps(self.ramses.ufh_circuits, sort_keys=True))

            # Timestamp
            self.mqtt.publish(
                f"{base}/_{gateway_name}_ts",
                local_now(self.cfg.misc.use_local_time).strftime("%Y-%m-%dT%H:%M:%S"),
            )
        except Exception as ex:
            self.log.warning(f"Schema publish error: {ex}")

    # Print schema/devices to console
    async def _print_gateway_schema(self) -> None:
        if not (self.ramses and self.ramses.gwy):
            return
        gwy = self.ramses.gwy
        schema = await self.current_schema_snapshot()
        print(f"Schema: {json.dumps(schema, indent=4)}\r\n")
        try:
            print(f"Params: {json.dumps(await gwy.params())}\r\n")
        except Exception:
            pass
        try:
            print(f"Status: {json.dumps(await gwy.status())}")
        except Exception:
            pass
        try:
            full_schema = await gwy.schema()
            orphans = [d for d in sorted(full_schema.get('orphans_heat', []))]
            print(f"Schema[orphans_heat]: {json.dumps({'orphans_heat': orphans}, indent=4)}\r\n")
        except Exception:
            pass

        # Devices
        devices = {
            str(k): {"alias": getattr(v, "alias", ""), "zone_id": getattr(v, "zone_id", "")}
            for k, v in gwy.device_registry.device_by_id.items()
        }
        print(f"Devices: {json.dumps(devices, indent=4)}")


    async def current_schema_snapshot(self) -> dict:
        gwy = self.ramses.gwy
        known_list = {"known_list": gwy.config.known_list}
        live_schema = await gwy.schema() or {}
        schema = {**live_schema, **known_list}
        return schema

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()

        # ── ramses_rf 0.57.0 compatibility workaround ────────────────────────────
        # When we send a W command (e.g. set_dhw_mode), evofw3 echoes the packet
        # back to us.  ramses_rf's internal _msg_handler processes that echo and
        # routes it through Controller._handle_msg → TCS._handle_msg →
        # DhwZone._handle_msg.  The DHW zone handler asserts:
        #
        #   assert (msg.src == self.ctl and msg.code in (..., Code._1F41, ...)
        #           or msg.payload.get("domain_id") in ("F9", "FA")
        #           or msg.payload.get("zone_idx") == "HW")
        #
        # The echo has src=18:xxx (our HGI, not the controller), and the 1F41
        # parser in 0.57.0 does not include domain_id/zone_idx in its output, so
        # all three conditions fail → AssertionError.
        #
        # This is a bug in ramses_rf 0.57.0 (reported upstream).  The W command IS
        # transmitted and processed correctly by the controller; the crash is only
        # in the library's internal echo bookkeeping and has no effect on our
        # routing layer (our _handle_gwy_message runs independently).
        #
        # The only symptom visible to us is a full "Exception in callback" traceback
        # printed to stderr on every W send.  We suppress it here by installing a
        # narrow custom exception handler that intercepts only AssertionErrors whose
        # message contains "inappropriately routed" (the exact text in zones.py).
        # All other exceptions pass through to the default handler unchanged.
        _prev_exc_handler = self.loop.get_exception_handler()
        def _exc_handler(loop: asyncio.AbstractEventLoop, ctx: dict) -> None:
            exc = ctx.get("exception")
            if isinstance(exc, AssertionError) and "inappropriately routed" in str(exc):
                self.log.debug(f"Suppressed ramses_rf 0.57.0 routing assertion: {exc}")
                return
            if _prev_exc_handler is not None:
                _prev_exc_handler(loop, ctx)
            else:
                loop.default_exception_handler(ctx)
        self.loop.set_exception_handler(_exc_handler)
        # ─────────────────────────────────────────────────────────────────────────

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
            use_local_time=self.cfg.misc.use_local_time,
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
            use_local_time=self.cfg.misc.use_local_time,
        )

        # Ramses
        gwy_config = self._build_ramses_config()
        self.ramses = RamsesService(
            gwy_config=gwy_config,
            logger=self.log,
            registry=self.registry,
            on_message=self.router.handle_message,
            publish_schema=self._publish_schema_snapshot,
            on_sys_config=self._handle_sys_config,
            colors=self.cfg.misc.display_colours,
            min_row_length=self.cfg.misc.min_row_length,
            gateway_name=self.cfg.misc.this_gateway_name,
        )

        # Schedule
        self.schedule = ScheduleHandler(
            ramses=self.ramses, 
            router=self.router, 
            logger=self.log
        )

        # Startup separator and banner — visible in both console and log file
        _sep = "=" * 80
        _banner = f"evoGateway {GATEWAY_VERSION} (ramses_rf {RAMSES_RF_VERSION}) starting"
        print("")
        print_formatted_row(text=_sep, style_prefix="", min_row_length=self.cfg.misc.min_row_length)
        print_formatted_row(text=f"# {_banner}", style_prefix="", min_row_length=self.cfg.misc.min_row_length)
        self.log.info(_sep)
        self.log.info(_banner)

        # Start RF gateway
        await self.ramses.start()

        # Update registry with the connected HGI friendly name
        hgi_id = self.registry.hgi_id
        if hgi_id and self.cfg.misc.this_gateway_name:
            self.registry.update_alias(hgi_id, self.cfg.misc.this_gateway_name)
      
        # Show known devices on startup
        self.router.display_device_list(self.ramses)

        # Home Assistant MQTT discovery
        from .ha_discovery import HADiscovery
        from .utils import to_snake_case
        id_prefix = to_snake_case(
            self.cfg.mqtt.ha_discovery_id_prefix or self.cfg.mqtt.client_id
        )
        self._ha_discovery = HADiscovery(
            registry=self.registry,
            mqtt_topics=self.mqtt_topics,
            mqtt_service=self.mqtt,
            ha_prefix=self.cfg.mqtt.ha_discovery_prefix,
            gateway_name=self.cfg.misc.this_gateway_name,
            id_prefix=id_prefix,
            zone_climate_prefix=self.cfg.mqtt.ha_zone_climate_prefix,
            zone_climate_suffix=self.cfg.mqtt.ha_zone_climate_suffix,
            dhw_temp_subtopic=self.cfg.mqtt.ha_dhw_temp_subtopic,
            dhw_params_subtopic=self.cfg.mqtt.ha_dhw_params_subtopic,
            dhw_mode_subtopic=self.cfg.mqtt.ha_dhw_mode_subtopic,
            ot_cache_file=self.cfg.files.ot_sensors_cache_file,
        )
        if self.cfg.mqtt.ha_discovery_enabled:
            self._ha_discovery.publish_all()
            # Re-publish on every MQTT reconnect so HA picks up after broker restart
            self.mqtt._on_connect_extra = self._ha_discovery.publish_all
            # Wire lazy OT sensor discovery: router notifies HA discovery on first sight
            self.router._ot_sensor_callback = self._ha_discovery.on_new_ot_sensor
        else:
            # Discovery disabled — remove any retained entries left from a previous run
            await self._ha_discovery.remove_all()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._heartbeat_task

        except KeyboardInterrupt:
            self._shutdown_reason = self._shutdown_reason or "keyboard interrupt"
        except (SystemExit, asyncio.CancelledError):
            pass  # reason already logged/set by the triggering code path

        finally:
            _sep = "=" * 80
            _stop_label = "restarting" if self._restart_process else "stopping"
            _stop_reason = f" — {self._shutdown_reason}" if self._shutdown_reason else ""
            _stop_msg = f"evoGateway {_stop_label}{_stop_reason}"
            print("")
            print_formatted_row(text=_sep, style_prefix="", min_row_length=self.cfg.misc.min_row_length)
            print_formatted_row(text=f"# {_stop_msg}", style_prefix="", min_row_length=self.cfg.misc.min_row_length)
            self.log.info(_sep)
            self.log.info(_stop_msg)
            await self.shutdown()
            if self._restart_process:
                os.execv(sys.executable, [sys.executable] + sys.argv)
            raise SystemExit

    async def _handle_gateway_command(self, cmd: str) -> None:
        """Handle MQTT-triggered gateway management commands."""
        self.log.info(f"Gateway management command: {cmd}")
        self._publish_command_status(cmd, "Transmitted")

        if cmd == "RESTART_RF":
            if not self.ramses:
                self._publish_command_status(cmd, "Failed", error="Ramses not running")
                return
            try:
                if self.mqtt:
                    self.mqtt.publish_status("RF Restarting")
                await self.ramses.restart()
                self._publish_command_status(cmd, "Successful")
                if self.mqtt:
                    self.mqtt.publish_status(MQTT_ONLINE)
            except Exception as ex:
                self.log.exception("RF restart failed")
                self._publish_command_status(cmd, "Failed", error=str(ex))

        elif cmd in ("RESTART_GATEWAY", "RESTART_PROCESS"):
            if cmd == "RESTART_PROCESS":
                self._restart_process = True
            self._publish_command_status(cmd, "Successful")
            self._shutdown_reason = f"MQTT command: {cmd}"
            self.log.warning(
                f"{'Full process' if cmd == 'RESTART_PROCESS' else 'Gateway service'} restart requested via MQTT"
            )
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            else:
                raise SystemExit(1)

    async def _heartbeat_loop(self) -> None:
        """Periodic MQTT heartbeat and three-stage RF watchdog.

        Each stage is independently optional: set its timeout to 0 to disable it.
        Stages are also independent of each other — Stage 2 does not require Stage 1
        to have fired first, so users can skip straight to restart without a warn.

        The poll interval (WATCHDOG_CHECK_INTERVAL) controls how often this loop
        wakes up. All timeout thresholds are accurate to within ±that interval.
        Set WATCHDOG_CHECK_INTERVAL = 0 to disable both the heartbeat and watchdog.
        """
        cfg = self.cfg.watchdog

        if cfg.watchdog_check_interval <= 0:
            return

        last_heartbeat_at: _dt.datetime | None = None
        rf_warned = False
        rf_restarted = False
        restart_at: _dt.datetime | None = None

        # Lowest non-zero RF threshold; used to detect recovery (real messages arriving)
        _recovery_threshold = next(
            (t for t in sorted([cfg.rf_warn_timeout, cfg.rf_restart_timeout]) if t > 0), 0
        )

        while True:
            await asyncio.sleep(cfg.watchdog_check_interval)
            now = local_now(self.cfg.misc.use_local_time)

            # --- MQTT heartbeat (disabled when interval == 0) ---
            if cfg.mqtt_heartbeat_interval > 0 and self.mqtt and (
                last_heartbeat_at is None
                or (now - last_heartbeat_at).total_seconds() >= cfg.mqtt_heartbeat_interval
            ):
                self.mqtt.publish_status(MQTT_ONLINE)
                last_heartbeat_at = now

            # --- RF watchdog ---
            if not (self.ramses and self.ramses.last_rf_message_ts):
                continue

            silence = (now - self.ramses.last_rf_message_ts).total_seconds()

            # Recovery: real RF messages arrived (silence dropped below the first threshold)
            if (rf_warned or rf_restarted) and _recovery_threshold > 0 and silence < _recovery_threshold:
                msg = f"RF communication restored (was silent for {silence:.0f}s)"
                self.log.info(msg)
                print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("INFO", ""), min_row_length=self.cfg.misc.min_row_length)
                if self.mqtt:
                    self.mqtt.publish_status(MQTT_ONLINE)
                rf_warned = False
                rf_restarted = False
                restart_at = None
                continue

            # Stages 3 & 4: both measured from when Stage 2 (RF restart) was attempted
            if rf_restarted and restart_at:
                elapsed = (now - restart_at).total_seconds()

                # Stage 3: restart the whole process (disabled when rf_process_restart_timeout == 0)
                if cfg.rf_process_restart_timeout > 0 and elapsed >= cfg.rf_process_restart_timeout:
                    msg = f"RF watchdog: still silent {elapsed:.0f}s after RF restart - attempting full process restart"
                    self.log.critical(msg)
                    print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("ERROR", ""), min_row_length=self.cfg.misc.min_row_length)
                    if self.mqtt:
                        self.mqtt.publish_status("Restarting")
                    self._restart_process = True
                    self._shutdown_reason = f"RF watchdog stage 3 — {msg}"
                    raise SystemExit(1)

                # Stage 4: give up and let the process manager restart us
                # (disabled when rf_exit_timeout == 0; only reachable if Stage 3 is disabled
                # or rf_exit_timeout < rf_process_restart_timeout)
                if cfg.rf_exit_timeout > 0 and elapsed >= cfg.rf_exit_timeout:
                    msg = f"RF watchdog: still silent {elapsed:.0f}s after RF restart - exiting for systemd restart"
                    self.log.critical(msg)
                    print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("ERROR", ""), min_row_length=self.cfg.misc.min_row_length)
                    if self.mqtt:
                        self.mqtt.publish_status("Offline")
                    self._shutdown_reason = f"RF watchdog stage 4 — {msg}"
                    raise SystemExit(1)

                continue  # still within post-restart wait window

            # Stage 2: restart RF layer (independent of Stage 1; disabled when rf_restart_timeout == 0)
            if cfg.rf_restart_timeout > 0 and not rf_restarted and silence >= cfg.rf_restart_timeout:
                msg = f"RF watchdog: silent for {silence/60:.1f} min - attempting RF layer restart"
                self.log.error(msg)
                print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("ERROR", ""), min_row_length=self.cfg.misc.min_row_length)
                if self.mqtt:
                    self.mqtt.publish_status("RF Restarting")
                try:
                    await self.ramses.restart()
                    rf_restarted = True
                    restart_at = local_now(self.cfg.misc.use_local_time)
                    msg = "RF watchdog: RF layer restart completed"
                    self.log.info(msg)
                    print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("INFO", ""), min_row_length=self.cfg.misc.min_row_length)
                    if self.mqtt:
                        self.mqtt.publish_status("RF Restarted")
                except Exception:
                    self.log.exception("RF watchdog: RF layer restart failed")
                    if self.mqtt:
                        self.mqtt.publish_status("RF Restart Failed")
                continue

            # Stage 1: warn (disabled when rf_warn_timeout == 0)
            if cfg.rf_warn_timeout > 0 and not rf_warned and silence >= cfg.rf_warn_timeout:
                msg = f"RF watchdog: silent for {silence/60:.1f} min - no messages received"
                self.log.warning(msg)
                print_formatted_row(msg, style_prefix=self.cfg.misc.display_colours.get("INFO", ""), min_row_length=self.cfg.misc.min_row_length)
                if self.mqtt:
                    self.mqtt.publish_status("RF Timeout")
                rf_warned = True

    async def shutdown(self) -> None:
        # If eavesdrop OR discovery active -> print/save schema
        try:
            if self.ramses and self.ramses.gwy:
                using_discovery = not self.ramses.gwy.config.disable_discovery
                using_eavesdrop = self.ramses.gwy.config.enable_eavesdrop

                await self._print_gateway_schema()
                await self._publish_schema_snapshot()

                if self.cfg.files.save_schema_on_shutdown or using_eavesdrop or using_discovery:
                    # Save schema file
                    await self._handle_sys_config("SAVE_SCHEMA")
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
    async def _handle_sys_config(self, cmd: str) -> None:
        try:
            if cmd in ("POST_SCHEMA", "SAVE_SCHEMA"):
                await self._publish_schema_snapshot()
            if cmd == "SAVE_SCHEMA":
                schema = await self.current_schema_snapshot()
                self.persistence.save_schema(schema)
            if cmd == "REMOVE_HA_DISCOVERY":
                if self.mqtt:
                    self.mqtt._on_connect_extra = None
                if hasattr(self, "_ha_discovery") and self._ha_discovery and self.loop:
                    self.loop.create_task(self._ha_discovery.remove_all())
        except Exception:
            self.log.exception("Failed handling sys_config command")
