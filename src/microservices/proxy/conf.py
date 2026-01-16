from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONOLITH_URL: str
    MOVIES_SERVICE_URL: str
    EVENTS_SERVICE_URL: str
    GRADUAL_MIGRATION: str
    MOVIES_MIGRATION_PERCENT: int

    @classmethod
    def read_or_update(cls):
        return cls()
