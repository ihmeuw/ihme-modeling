from typing import List, Dict


class AgeGroup:

    AGE_GROUP_IDS: List[int] = [28, 5, 6, 7, 8, 9, 10,
                                11, 12, 13, 14, 15, 16,
                                17, 18, 19, 20, 30,
                                31, 32]
    YOUNGEST_AGE_ID: int = 28
    OLDEST_LIFE_TABLE_AGE_ID: int = 33
    OLDEST_OUTPUT_AGE_ID: int = 235
    OUTPUT_AGE_GROUP_IDS: List[int] = AGE_GROUP_IDS + [OLDEST_OUTPUT_AGE_ID]
    INPUT_AGE_GROUP_IDS: List[int] = AGE_GROUP_IDS + [OLDEST_LIFE_TABLE_AGE_ID]


class LifeTable:

    LIFE_TABLE_PARAMETERS: List[str] = ['mx', 'ax', 'qx', 'lx', 'ex',
                                        'pred_ex', 'nLx', 'Tx']
    PARAMETER_NAME: str = 'parameter_name'
    LIFE_TABLE_PARAMETER_ID: str = 'life_table_parameter_id'
    LIFE_EXPECTANCY_ABBR: str = 'ex'


class Demographics:

    INDEX_COLUMNS: List[str] = ['location_id', 'year_id', 'sex_id',
                                'age_group_id']
    DEMOGRAPHIC_TEMPLATE_COLUMNS: List[str] = INDEX_COLUMNS + ['cause_id']
    DEATH: int = 1
    LE_DECOMP_ID: int = 37
    RATE: int = 3
    METRIC_YEARS: int = 5
    ALL_CAUSE_ID: int = 294


class Versioning:

    DECOMP_PROCESS_ID: int = 27


class Database:

    conn_def: Dict = {
        'prod': 'CONN_DEF',
        'dev': 'CONN_DEF'
    }


class ErrorChecking:

    ERROR_TOLERANCE = 1e-5
