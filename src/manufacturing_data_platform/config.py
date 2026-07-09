from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "manufacturing_data_platform"


def get_settings() -> Settings:
    return Settings(
        mongo_uri=os.getenv("MONGO_URI", Settings.mongo_uri),
        mongo_db=os.getenv("MONGO_DB", Settings.mongo_db),
    )
