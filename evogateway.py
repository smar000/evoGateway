#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""evoGateway Application Runner

This script serves as the entry point for the evoGateway. 

### Usage:
To run the application, provide the configuration file path:

    $ python evogateway.py --config config/evogateway.cfg
"""

from __future__ import annotations
import argparse
import asyncio
from pathlib import Path
from typing import Any, Optional

from evogateway.app import EvoGatewayApp 
from evogateway.config import AppConfig, DEFAULT_EVOGW_CONFIG_FILE
from evogateway.utils import print_formatted_row


async def _main_async(config_path: Optional[str]) -> None:
    cfg = AppConfig.load(Path(config_path) if config_path else None)
    app = EvoGatewayApp(cfg)
    await app.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_EVOGW_CONFIG_FILE, help="Path to config file")
    args = parser.parse_args()
    try:
        asyncio.run(_main_async(args.config))
    except KeyboardInterrupt:
        print("\nEvoGateway shutting down...")


if __name__ == "__main__":
    main()
