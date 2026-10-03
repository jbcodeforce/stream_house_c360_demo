from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SINK: str = "postgres"

    DATABASE_URL: str | None = None
    KAFKA_BOOTSTRAP_SERVERS: str | None = None
    SCHEMA_REGISTRY_URL: str | None = None
    SCHEMA_REGISTRY_API_KEY: str | None = None
    SCHEMA_REGISTRY_API_SECRET: str | None = None
    KAFKA_TOPIC_CUSTOMERS: str = "customers"
    RUNTIME_CONFIG_FILE: str | None = None


settings = Settings()
