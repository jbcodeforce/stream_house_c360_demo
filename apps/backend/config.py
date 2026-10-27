from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _resolve_env_file() -> str:
    env_file = os.environ.get("ENV_CONFIG_FILE")
    if env_file:
        if os.path.isabs(env_file) or os.path.exists(env_file):
            return env_file
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.abspath(os.path.join(backend_dir, "..", ".."))
        for candidate in [
            os.path.join(repo_root, env_file),
            os.path.join(backend_dir, env_file),
            os.path.abspath(env_file),
        ]:
            if os.path.exists(candidate):
                return candidate
        return env_file
    backend_default = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    return backend_default if os.path.exists(backend_default) else ".env"


def _mask_dsn(dsn: str | None) -> str:
    if not dsn:
        return "None"
    try:
        parsed = urlparse(dsn)
        if parsed.password:
            netloc = f"{parsed.username or ''}:****@{parsed.hostname or ''}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return parsed._replace(netloc=netloc).geturl()
        return dsn
    except Exception:
        return "****"


def _host_owns_cdc(database_url: str | None) -> bool:
    """Whether a managed CDC connector should own Kafka for this DB host.

    Local PostgreSQL (localhost / 127.0.0.1) has no connector, so the backend
    is the only producer → False. Any remote host — RDS included — is assumed
    to have a Debezium/CDC connector publishing cdc.public.*, so the backend
    must stay silent → True. A wrong True merely goes quiet; a wrong False
    double-publishes, so anything that is not explicitly local defaults to the
    safe True. Mirrors the inference in scripts/run_dev.sh.
    """
    if not database_url:
        return False
    return (urlparse(database_url).hostname or "") not in _LOCAL_HOSTS


class Settings(BaseSettings):
    # The dotenv file path itself comes from the ENV_CONFIG_FILE env var
    # (default ".env"); real environment variables still take precedence.
    model_config = SettingsConfigDict(
        env_file=_resolve_env_file(), extra="ignore"
    )

    SINK: str = "postgres"

    # When True, a managed CDC connector (Debezium on RDS / remote PostgreSQL)
    # owns publishing to the cdc.public.* topics, so the backend must NOT also
    # dual-write Kafka events — doing so would double-publish. This hard guard
    # overrides ``KAFKA_PRODUCE_ENABLED``.
    #
    # Leave unset (None) to auto-resolve from the DATABASE_URL host on startup:
    # local PostgreSQL ⇒ False (the app produces), any remote host incl. RDS ⇒
    # True (the connector owns Kafka). Set the env var explicitly to override
    # the inference either way.
    CDC_CONNECTOR_ENABLED: bool | None = None

    # When True (and no CDC connector owns Kafka), the backend dual-writes a
    # Debezium-shaped event to cdc.public.* after each successful DB change.
    KAFKA_PRODUCE_ENABLED: bool = False

    # When True, on startup the backend loads the seed CSVs and publishes every
    # record as a Debezium "create" event to the cdc.public.* topics
    # (customers → accounts → transactions). Skipped when a CDC connector owns
    # Kafka (CDC_CONNECTOR_ENABLED) to avoid double-publishing.
    KAFKA_SEED_ON_START: bool = False

    DATABASE_URL: str | None = None
    KAFKA_BOOTSTRAP_SERVERS: str | None = None
    KAFKA_API_KEY: str | None = None
    KAFKA_API_SECRET: str | None = None
    SCHEMA_REGISTRY_URL: str | None = None
    SCHEMA_REGISTRY_API_KEY: str | None = None
    SCHEMA_REGISTRY_API_SECRET: str | None = None
    KAFKA_TOPIC_CUSTOMERS: str = "cdc.public.customers"
    KAFKA_TOPIC_ACCOUNTS: str = "cdc.public.accounts"
    KAFKA_TOPIC_TRANSACTIONS: str = "cdc.public.transactions"

    # Data Products graph page: selects the status/metrics provider.
    # "mock" / "mock-lag" use deterministic fixtures; "live" talks to
    # Confluent Cloud + Flink (Task 3).
    GRAPH_DATA_MODE: str = "mock"

    # C360 analytics dashboard: selects the snapshot-row provider.
    # "mock" uses a deterministic fixture; "live" runs a Flink SQL snapshot
    # query against fct_c360_profile (needs FLINK_CATALOG / FLINK_DATABASE).
    ANALYTICS_DATA_MODE: str = "mock"
    # Row cap for the live snapshot SELECT (the table is one row per customer).
    ANALYTICS_SNAPSHOT_LIMIT: int = 50_000

    # Live-mode credentials (unused until Task 3's LiveProvider).
    CONFLUENT_CLOUD_API_KEY: str | None = None
    CONFLUENT_CLOUD_API_SECRET: str | None = None
    FLINK_API_KEY: str | None = None
    FLINK_API_SECRET: str | None = None
    FLINK_ORG_ID: str | None = None
    FLINK_ENV_ID: str | None = None
    FLINK_COMPUTE_POOL_ID: str | None = None
    FLINK_REST_ENDPOINT: str | None = None
    # Catalog/database for Flink SQL snapshot queries — the Confluent Cloud
    # environment and Kafka cluster *display names* (not their IDs).
    FLINK_CATALOG: str | None = None
    FLINK_DATABASE: str | None = None

    # RTCE / Confluent Lightning Query API (point-lookup serving layer). Used by
    # the analytics "Customer lookup via RTCE" section in live mode. The endpoint
    # is https://sql.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud/query/v1alpha1;
    # the request is scoped by catalog_name = env *id* (FLINK_ENV_ID) and
    # database_name = Kafka cluster *id* (KAFKA_CLUSTER_ID) — IDs, not display
    # names. Authenticates with the Confluent Cloud API key/secret
    # (CONFLUENT_CLOUD_API_KEY/SECRET above); the Lightning API has no key of its
    # own. That key must authenticate the whole organization, or it returns 401.
    CLOUD_REGION: str | None = None
    CLOUD_PROVIDER: str | None = None
    KAFKA_CLUSTER_ID: str | None = None

    @model_validator(mode="after")
    def _resolve_cdc_connector(self) -> "Settings":
        """Infer CDC ownership from the DB host when not set explicitly."""
        if self.CDC_CONNECTOR_ENABLED is None:
            self.CDC_CONNECTOR_ENABLED = _host_owns_cdc(self.DATABASE_URL)
        return self

    def summary(self) -> dict[str, str]:
        """Return a dictionary of configuration key/values with secrets masked."""
        env_file = _resolve_env_file()
        exists = bool(env_file and os.path.exists(env_file))
        raw_env = os.environ.get("ENV_CONFIG_FILE")
        display_file = env_file if exists else (raw_env or env_file)
        return {
            "ENV_CONFIG_FILE": f"{display_file}" if exists else f"{display_file} (not found)",
            "SINK": self.SINK,
            "DATABASE_URL": _mask_dsn(self.DATABASE_URL),
            "CDC_CONNECTOR_ENABLED": str(self.CDC_CONNECTOR_ENABLED),
            "KAFKA_PRODUCE_ENABLED": str(self.KAFKA_PRODUCE_ENABLED),
            "KAFKA_SEED_ON_START": str(self.KAFKA_SEED_ON_START),
            "KAFKA_BOOTSTRAP_SERVERS": self.KAFKA_BOOTSTRAP_SERVERS or "None",
            "SCHEMA_REGISTRY_URL": self.SCHEMA_REGISTRY_URL or "None",
            "GRAPH_DATA_MODE": self.GRAPH_DATA_MODE,
            "ANALYTICS_DATA_MODE": self.ANALYTICS_DATA_MODE,
            "FLINK_REST_ENDPOINT": self.FLINK_REST_ENDPOINT or "None",
            "RTCE_LIGHTNING": (
                "configured"
                if (self.CLOUD_REGION and self.KAFKA_CLUSTER_ID and self.CONFLUENT_CLOUD_API_KEY)
                else "None"
            ),
        }

    def log_summary(self, target_logger: logging.Logger | None = None) -> None:
        """Log active configuration settings."""
        if target_logger is None:
            log = logging.getLogger("c360.config")
            if not log.handlers and not logging.getLogger().handlers:
                logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        else:
            log = target_logger
        log.info("Active Backend Configuration:")
        for k, v in self.summary().items():
            log.info("  %-25s = %s", k, v)


settings = Settings()
