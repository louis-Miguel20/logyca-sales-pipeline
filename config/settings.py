from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    database_url: str
    azure_storage_connection_string: str
    azure_blob_container_name: str
    azure_queue_name: str
    app_env: str = "development"
    log_level: str = "INFO"
    batch_size: int = 1000
    worker_poll_interval: int = 5

    model_config = SettingsConfigDict(env_file='.env', case_sensitive=False, extra='ignore')

@lru_cache()
def get_settings() -> Settings:
    return Settings()
