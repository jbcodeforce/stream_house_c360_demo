"""Unit tests for the runtime config store (no app, no infra)."""

from __future__ import annotations

import config_store
from config import settings
from config_store import AppConfig


def test_default_when_file_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(tmp_path / "rc.json"))
    config_store.reset_cache()
    assert config_store.get_config().kafka_produce_enabled is False


def test_set_then_get_roundtrips_through_file(tmp_path, monkeypatch):
    f = tmp_path / "rc.json"
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(f))
    config_store.reset_cache()

    config_store.set_config(AppConfig(kafka_produce_enabled=True))
    assert f.exists()

    config_store.reset_cache()  # force a reload from disk
    assert config_store.get_config().kafka_produce_enabled is True


def test_corrupt_file_falls_back_to_default(tmp_path, monkeypatch):
    f = tmp_path / "rc.json"
    f.write_text("{ not valid json")
    monkeypatch.setattr(settings, "RUNTIME_CONFIG_FILE", str(f))
    config_store.reset_cache()
    assert config_store.get_config().kafka_produce_enabled is False
