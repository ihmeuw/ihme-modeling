from typing import Final, List

CACHED_POP_SUBDIR: Final[str] = "FILEPATH"
CLINICAL_RUNS_ROOT: Final[str] = "FILEPATH"
MASTER_DATA_SUBDIR: Final[str] = "FILEPATH"
POP_ID_COLS: Final[List[str]] = [
    "age_group_id",
    "location_id",
    "year_id",
    "sex_id",
    "pop_run_id",
]
SEX_IDS: Final[List[int]] = [1, 2, 3]
