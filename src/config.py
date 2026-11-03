"""Configuration module using pydantic-settings to load and validate environment variables."""

from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings and environment variable definitions."""

    # Google Gemini Settings
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.5-flash"
    GOOGLE_API_KEY: Optional[str] = None

    # OpenAI Settings (Optional fallback)
    OPENAI_API_KEY: str = "mock-key"
    OPENAI_MODEL: str = "gpt-4o-mini"

    # Kafka Messaging Settings
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_INVENTORY_EVENTS: str = "inventory-events"
    KAFKA_CONSUMER_GROUP_ID: str = "procurement-agent-group"
    KAFKA_SECURITY_PROTOCOL: Optional[str] = None
    KAFKA_SASL_MECHANISM: Optional[str] = None
    KAFKA_USERNAME: Optional[str] = None
    KAFKA_PASSWORD: Optional[str] = None
    KAFKA_CA_LOCATION: Optional[str] = None
    KAFKA_CA_CERT: Optional[str] = None
    KAFKA_CLIENT_CERT_LOCATION: Optional[str] = None
    KAFKA_CLIENT_KEY_LOCATION: Optional[str] = None

    # Inventory Agent Feature Flags
    INVENTORY_AGENT_ENABLED: bool = True
    INVENTORY_AGENT: Optional[str] = None
    INVENTORY_AGENT_API_KEY: Optional[str] = None

    # PostgreSQL Checkpointing URI
    POSTGRES_URI: str = "postgresql://postgres:postgrespassword@localhost:5432/procurement_db"

    # Mock ERP Service URL
    MOCK_ERP_URL: str = "http://localhost:8002"

    # Slack Socket Mode Settings
    SLACK_BOT_TOKEN: str = ""
    SLACK_APP_TOKEN: str = ""
    SLACK_CHANNEL_ID: str = ""

    # Langfuse Observability Settings
    LANGFUSE_PUBLIC_KEY: Optional[str] = None
    LANGFUSE_SECRET_KEY: Optional[str] = None
    LANGFUSE_HOST: str = "https://cloud.langfuse.com"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )

    @property
    def effective_gemini_api_key(self) -> str:
        """Resolve API key from GEMINI_API_KEY or GOOGLE_API_KEY."""
        return self.GEMINI_API_KEY or self.GOOGLE_API_KEY or ""

    @property
    def is_inventory_agent_enabled(self) -> bool:
        """Check if inventory agent is enabled via boolean flag or 'enabled' string."""
        if self.INVENTORY_AGENT is not None:
            return self.INVENTORY_AGENT.strip().lower() in ("enabled", "true", "1")
        return bool(self.INVENTORY_AGENT_ENABLED)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached instance of application settings."""
    return Settings()
