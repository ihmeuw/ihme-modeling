"""
Assigns defaults for some run-time configurable parameters like input/output directories and
odbc profile.
"""

import functools

import pydantic

from crosscutting_functions.clinical_constants.pipeline import marketscan as constants


class Settings(pydantic.BaseSettings):
    input_dir = "FILENAME"
    denominator_dir = "FILENAME"
    output_dir = "FILENAME"
    metadata_dir = "FILENAME"
    overwrite_final_data = False
    file_format = "parquet"
    odbc_profile = "SERVER"
    threads = 1
    queue = "long.q"
    base_mem = "40G"
    run_time = "40h"
    mem_boost = 6
    jobmon_retries = 3
    release_id_for_us_states = 10
    e2e_test: bool = False

    class Config:
        """Prefix environment variables. Distinction between schema and pipeline"""

        env_prefix = "ms_pipeline_"


@functools.lru_cache()
def get_settings() -> Settings:
    """Reads config settings. Caches so that reading only happens once."""
    return Settings()
