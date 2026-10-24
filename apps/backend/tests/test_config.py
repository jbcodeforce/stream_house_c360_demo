import logging
import os
from unittest.mock import MagicMock, patch

from config import Settings, _mask_dsn, _resolve_env_file


def test_mask_dsn():
    assert _mask_dsn(None) == "None"
    assert _mask_dsn("") == "None"
    assert _mask_dsn("postgresql://user:secret@localhost:5432/db") == "postgresql://user:****@localhost:5432/db"
    assert _mask_dsn("postgresql://localhost:5432/db") == "postgresql://localhost:5432/db"


def test_resolve_env_file_default():
    with patch.dict(os.environ, {}, clear=True):
        res = _resolve_env_file()
        assert res.endswith(".env")


def test_resolve_env_file_custom(tmp_path):
    custom_env = tmp_path / "custom.env"
    custom_env.write_text("SINK=kafka\n")
    with patch.dict(os.environ, {"ENV_CONFIG_FILE": str(custom_env)}):
        res = _resolve_env_file()
        assert res == str(custom_env)


def test_settings_summary_and_log():
    settings = Settings(
        SINK="postgres",
        DATABASE_URL="postgresql://user:secret@localhost:5432/mydb",
        CDC_CONNECTOR_ENABLED=False,
    )
    summary = settings.summary()
    assert summary["SINK"] == "postgres"
    assert summary["DATABASE_URL"] == "postgresql://user:****@localhost:5432/mydb"
    assert summary["CDC_CONNECTOR_ENABLED"] == "False"

    mock_logger = MagicMock(spec=logging.Logger)
    settings.log_summary(mock_logger)
    assert mock_logger.info.called
