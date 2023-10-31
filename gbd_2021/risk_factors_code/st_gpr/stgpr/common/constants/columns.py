from typing import List

AGE_GROUP_ID: str = 'age_group_id'
DATA: str = 'data'
IS_OUTLIER: str = 'is_outlier'
LEVEL: str = 'level'
LOCATION_ID: str = 'location_id'
LOCATION_SET_ID: str = 'location_set_id'
MEASURE: str = 'measure'
ORIGINAL_DATA: str = 'original_data'
ORIGINAL_VARIANCE: str = 'original_variance'
OUTLIER_VALUE: str = 'outlier_value'
SEX: str = 'sex'
SEX_ID: str = 'sex_id'
VAL: str = 'val'
VARIANCE: str = 'variance'
YEAR_ID: str = 'year_id'

# Output columns
STAGE_1: str = 'stage1'
SPACETIME: str = 'st'
GPR_MEAN: str = 'gpr_mean'
GPR_LOWER: str = 'gpr_lower'
GPR_UPPER: str = 'gpr_upper'


DEMOGRAPHICS: List[str] = [LOCATION_ID, YEAR_ID, AGE_GROUP_ID, SEX_ID]
