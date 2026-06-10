# src/core/logging.py
from __future__ import annotations

import logging
import os
import sys
from typing import Optional


def setup_logging(level: Optional[str] = None) -> None:
    """
    Configura logging para local y Lambda:
    - escribe a stdout (CloudWatch lo recoge)
    - formato consistente
    - idempotente (no duplica handlers)
    """
    lvl = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, lvl, logging.INFO)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Evitar duplicar handlers si Lambda reutiliza el runtime
    if root.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    fmt = (
        "%(asctime)s | %(levelname)s | %(name)s | "
        "%(message)s"
    )
    handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(handler)


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    setup_logging(level=level)
    return logging.getLogger(name)