from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SINK: str = "postgres"

    # When True, a managed CDC connector (Debezium on RDS / remote PostgreSQL)
    # owns publishing to the cdc.public.* topics, so the backend must NOT also
    # dual-write Kafka events — doing so would double-publish. This hard guard
    # overrides the runtime ``kafka_produce_enabled`` toggle. Keep False for
    # local mode (no connector), where the app is the only Kafka producer.
    CDC_CONNECTOR_ENABLED: bool = False

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
    RUNTIME_CONFIG_FILE: str | None = None


settings = Settings()
print(settings.model_dump_json(indent=2))
