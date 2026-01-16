from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_BROKERS: Optional[str] = None
    KAFKA_TOPIC: str = "default-topic"
    KAFKA_GROUP_ID: Optional[str] = None
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"

    @classmethod
    def read_or_update(cls):
        return cls()
