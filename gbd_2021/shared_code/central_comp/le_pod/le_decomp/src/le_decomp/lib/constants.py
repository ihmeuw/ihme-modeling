"""Constants for LE decomp."""
from typing import Dict, List


class AgeGroup:
    """Age group constants."""

    YOUNGEST_AGE_ID: int = 28
    OLDEST_LIFE_TABLE_AGE_ID: int = 33
    OLDEST_OUTPUT_AGE_ID: int = 235


class Database:
    """Database constants."""

    PROD: str = "prod"
    DEV: str = "dev"

    GBD: str = "gbd"
    GBD_TEST: str = "gbd-test"

    conn_def: Dict[str, str] = {
        PROD: GBD,
        DEV: GBD_TEST,
    }


class Demographics:
    """Mortality/demographics constants."""

    ALL_CAUSE_ID: int = 294
    DEATH: int = 1
    LE_DECOMP_ID: int = 37
    METRIC_YEARS: int = 5
    RATE: int = 3

    INDEX_COLUMNS: List[str] = ["location_id", "year_id", "sex_id", "age_group_id"]
    DEMOGRAPHIC_TEMPLATE_COLUMNS: List[str] = INDEX_COLUMNS + ["cause_id"]


class ErrorChecking:
    """Error checking constants."""

    ERROR_TOLERANCE = 1e-5


class Filepaths:
    """Filepath constants."""

    PARENT_DIR: str = "/PARENT_DIR/"

    INPUTS: str = "inputs"
    RESULTS: str = "results"


class LifeTable:
    """Life table constants."""

    LIFE_TABLE_PARAMETERS: List[str] = ["mx", "ax", "qx", "lx", "ex", "pred_ex", "nLx", "Tx"]
    PARAMETER_NAME: str = "parameter_name"
    LIFE_TABLE_PARAMETER_ID: str = "life_table_parameter_id"
    LIFE_EXPECTANCY_ABBR: str = "ex"


class Versioning:
    """GBD outputs versioning constants."""

    DECOMP_PROCESS_ID: int = 27
    CODCORRECT_DECOMP_STEP: str = "step3"
    VERSION_NOTE: str = (
        "Life Expectancy Decomp v{version_id}: Life Table v{life_table_version_id}"
    )
