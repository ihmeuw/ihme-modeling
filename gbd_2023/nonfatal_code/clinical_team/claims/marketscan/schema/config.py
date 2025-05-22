import functools

import pydantic
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants


class Settings(pydantic.BaseSettings):
    parquet_dir: str = constants.PARQUET_DIR
    write_dir: str = constants.WRITE_DIR
    e2e_test: bool = False

    class Config:
        """Prefix environment variables with "marketscan_"."""

        env_prefix = "ms_schema_"


@functools.lru_cache()
def get_settings() -> Settings:
    """Reads config settings. Caches so that reading only happens once."""
    return Settings()