from typing import List

AGE_GROUP_DAYS_START: str = "age_group_days_start"
AGE_GROUP_ID: str = "age_group_id"
AGE_SPECIFIC_DEATH: str = "age_specific_death"
CAUSE_ID: str = "cause_id"
DEATH: str = "death"
LIFE_TABLE_PARAMETER_ID: str = "life_table_parameter_id"
LOCATION_ID: str = "location_id"
LOWER: str = "lower"
LX: str = "lx"
MEAN: str = "mean"
MEASURE_ID: str = "measure_id"
METRIC_ID: str = "metric_id"
QX: str = "qx"
SEX_ID: str = "sex_id"
UPPER: str = "upper"
VAL: str = "val"
YEAR_ID: str = "year_id"

DEMOGRAPHICS: List[str] = [YEAR_ID, LOCATION_ID, SEX_ID, AGE_GROUP_ID]
OUTPUT: List[str] = [
    MEASURE_ID,
    CAUSE_ID,
    YEAR_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    METRIC_ID,
    VAL,
    UPPER,
    LOWER,
]
PRIMARY_KEY: List[str] = [
    MEASURE_ID,
    CAUSE_ID,
    YEAR_ID,
    LOCATION_ID,
    SEX_ID,
    AGE_GROUP_ID,
    METRIC_ID,
]
