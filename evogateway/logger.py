"""Application Logging Setup.

This module provides the init_logging function responsible for configuring the
primary application logger. It sets up file rotation handlers for persistent
logging and a StreamHandler for console output, ensuring a structured and
manageable logging environment.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from colorlog import ColoredFormatter

from .config import Style, COLOUR_PALETTE, DEFAULT_COLOURS


# Map Python logging levels to colourlog colours
# These values come from the unified COLOUR_PALETTE in config.py
LOG_COLORS = {
    # DEBUG should behave like low-importance RF chatter (RQ)
    "DEBUG": COLOUR_PALETTE["RQ"]["logger"],

    # INFO should match protocol " I" (general informational)
    "INFO": COLOUR_PALETTE[" I"]["logger"],

    # WARNING should match RF " W" (write commands)
    "WARNING": COLOUR_PALETTE[" W"]["logger"],

    # ERROR uses the unified error colour
    "ERROR": COLOUR_PALETTE["ERROR"]["logger"],

    # CRITICAL is allowed to be stronger than ERROR
    "CRITICAL": "red",
}


def init_logging(
    events_file: Path,
    rotate_bytes: int = 1_000_000,
    rotate_count: int = 9,
    log_events_to_console: bool = False,
    console_level: int = logging.WARNING,
    logger_name: str = "evogateway_log",
) -> logging.Logger:

    # Ensure log directory exists
    events_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Avoid attaching handlers twice
    if logger.handlers:
        return logger

    # File handler (event log)
    file_fmt = logging.Formatter(
        "%(asctime)s |   | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(
        str(events_file),
        maxBytes=int(rotate_bytes),
        backupCount=int(rotate_count),
    )
    fh.setLevel(console_level)
    fh.setFormatter(file_fmt)
    logger.addHandler(fh)

    # Console Handler
    if log_events_to_console:

        class ColourFormatter(logging.Formatter):
            """
            A formatter that applies ANSI colour codes based on evohome RF verbs.

            It inspects the final message and applies the colour defined in
            DEFAULT_COLOURS for the matching RF verb (RQ, RP, W, I).
            """

            def __init__(self, fmt, color_scheme, datefmt=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.color_scheme = color_scheme

            def format(self, record):
                msg = super().format(record)

                # Look for protocol verbs inside the message
                # e.g. " | RQ | ", " | RP | ", " |  I | "
                colour = ""
                for key, col in self.color_scheme.items():
                    if f" {key} " in msg:
                        colour = col
                        break

                if colour:
                    return f"{colour}{msg}{Style.RESET_ALL}"

                return msg

        # Our ColourFormatter (for protocol-specific colours)
        console_fmt = ColourFormatter(
            "%(asctime)s |   | %(message)s",
            color_scheme=DEFAULT_COLOURS,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # ColourLog formatter (for Python log levels)
        colorlog_fmt = ColoredFormatter(
            "%(log_color)s%(levelname)-8s%(reset)s %(message)s",
            log_colors=LOG_COLORS,
        )

        ch = logging.StreamHandler()
        ch.setLevel(console_level)
        ch.setFormatter(console_fmt)

        logger.addHandler(ch)

    return logger
