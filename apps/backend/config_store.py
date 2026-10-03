"""Runtime, user-editable application config, persisted to a JSON file.

Distinct from ``config.py`` (environment-derived settings). Holds a single
global flag controlling whether the service emits Kafka events on changes.
Single-process assumption: an in-memory cache backed by one JSON file.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import BaseModel

from config import settings

logger = logging.getLogger("c360.config_store")

_DEFAULT_PATH = Path(__file__).parent / "runtime_config.json"

_cache: "AppConfig | None" = None


class AppConfig(BaseModel):
    kafka_produce_enabled: bool = False


def _path() -> Path:
    return Path(settings.RUNTIME_CONFIG_FILE) if settings.RUNTIME_CONFIG_FILE else _DEFAULT_PATH


def _load() -> AppConfig:
    path = _path()
    try:
        if path.exists():
            return AppConfig.model_validate_json(path.read_text())
    except Exception:
        logger.warning("Could not read runtime config at %s; using defaults", path, exc_info=True)
    return AppConfig()


def get_config() -> AppConfig:
    global _cache
    if _cache is None:
        _cache = _load()
    return _cache


def set_config(config: AppConfig) -> AppConfig:
    global _cache
    _path().write_text(config.model_dump_json())
    _cache = config
    return _cache


def reset_cache() -> None:
    """Testing helper: drop the in-memory cache so the next read reloads."""
    global _cache
    _cache = None
