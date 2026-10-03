"""FastAPI router for the /config endpoints (runtime app config)."""

from __future__ import annotations

from fastapi import APIRouter

import config_store
from config_store import AppConfig

router = APIRouter(prefix="/config", tags=["config"])


@router.get("", response_model=AppConfig, status_code=200)
def get_config() -> AppConfig:
    return config_store.get_config()


@router.put("", response_model=AppConfig, status_code=200)
def update_config(data: AppConfig) -> AppConfig:
    return config_store.set_config(data)
