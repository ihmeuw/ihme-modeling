from typing import List

# Demographic columns et al.
AGE_GROUP_ID: str = 'age_group_id'
AGE_GROUP_SORT_ORDER: str = 'age_group_sort_order'
AGE_GROUP_YEARS_START: str = 'age_group_years_start'
CAUSE_ID: str = 'cause_id'
LOCATION_ID: str = 'location_id'
LOCATION_SET_VERSION_ID: str = 'location_set_version_id'
MEASURE_ID: str = 'measure_id'
METRIC_ID: str = 'metric_id'
SEX_ID: str = 'sex_id'
YEAR_ID: str = 'year_id'
YEAR_END_ID: str = 'year_end_id'
YEAR_START_ID: str = 'year_start_id'
DEMOGRAPHICS: List[str] = [LOCATION_ID, AGE_GROUP_ID, SEX_ID, YEAR_ID]
PERCENT_CHANGE_DEMO: List[str] = [
    LOCATION_ID, AGE_GROUP_ID, SEX_ID, YEAR_START_ID, YEAR_END_ID
]

# Life table and mortality columns.
# For descriptions, see mortality.lookup_life_table_parameter
TX: str = 'Tx'
NLX: str = 'nLx'
LX: str = 'lx'
RUN_ID: str = 'run_id'
LIFE_TABLE: List[str] = [TX, NLX, LX]

# YLD columns.
POPULATION: str = 'population'

# HALE calculation columns.
ADJ_LX: str = 'adj_Lx'
ADJ_TX: str = 'adj_Tx'
DRAW: str = 'draw'
HALE: str = 'HALE'

# Summary columns.
LOWER: str = 'lower'
MEAN: str = 'mean'
MEDIAN: str = 'median'
UPPER: str = 'upper'
HALE_LOWER: str = 'HALE_lower'
HALE_MEAN: str = 'HALE_mean'
HALE_UPPER: str = 'HALE_upper'
SUMMARY: List[str] = [HALE_LOWER, HALE_MEAN, HALE_UPPER]

# Upload columns.
VAL: str = 'val'
LOWER: str = 'lower'
UPPER: str = 'upper'
ORDERED_SINGLE: List[str] = [
    MEASURE_ID,
    YEAR_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    METRIC_ID,
    VAL,
    UPPER,
    LOWER
]
ORDERED_MULTI: List[str] = [
    MEASURE_ID,
    YEAR_START_ID,
    YEAR_END_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    METRIC_ID,
    VAL,
    UPPER,
    LOWER
]
