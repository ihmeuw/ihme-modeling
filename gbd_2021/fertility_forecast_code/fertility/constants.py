"""FHS Pipeline for BMI forecasting Local Constants.

Contains all the global variables used by the fertility modeling steps for
data input and transformation, ccfX, ccf, incremental, and conversion to ASFR.

Model specifications for different modeling steps (note that most are
age-specific) are also defined here.

Global variables for the intercept shift or plotting are NOT included here.
"""
import numpy as np

# scenario information
REFERENCE_SCENARIO_COORD = 0

# past met need start year
# don't change unless GBD estimates are made for prior to 1980
PAST_MET_NEED_YEAR_START = 1970

# define age-related constants for CCF forecast model
# do not recommend changing this as it is the standard definition for CCF50
COHORT_AGE_START = 15  # this is the starting age for "modeled" forecast
COHORT_AGE_END = 49  # CCF50 means [15, 50), so last eligible age year is 49.
FIVE_YEAR_AGE_GROUP_SIZE = 5
MODELED_AGE_STEP_SIZE = 1
MODELED_FERTILE_AGE_IDS = list(range(8, 15))
FERTILE_AGE_IDS = list(range(7, 16))
COVARIATE_START_AGE = 25  # this maps covariate age years to cohorts

# Bounds for forecasting ccf
CCF_LOWER_BOUND = 0.7
CCF_UPPER_BOUND = 10.0
CCF_BOUND_TOLERANCE = 1e-3

# these are single year ages that will be inferred/appended after forecast
YOUNG_TERMINAL_AGES = range(10, 15)
OLD_TERMINAL_AGES = range(50, 55)

PRONATAL_THRESHOLD = 1.75
RAMP_YEARS = 5  # number of years to ramp up pronatal policy


class DimensionConstants:
    """Constants related to data dimensionality."""

    # Common dimensions
    REGION_ID = "region_id"
    SUPER_REGION_ID = "super_region_id"
    SCENARIO = "scenario"
    LOCATION_ID = "location_id"
    AGE_GROUP_ID = "age_group_id"
    YEAR_ID = "year_id"
    COHORT_ID = "cohort_id"
    DRAW = "draw"
    SEX_ID = "sex_id"
    QUANTILE = "quantile"
    WEIGHT = "weight"
    STATISTIC = "statistic"
    AGE = "age"
    CCF_INDEX_DIMS = ["location_id", "year_id"]  # must be list of demog dims
    SINGLE_YEAR_ASFR_INDEX_DIMS = ["location_id", "age", "year_id", "draw"]

    FEMALE_SEX_ID = 2


class StageConstants:
    """Stages in FHS file system."""

    CCF = "ccf"
    ORDERED_COVARIATES = ["education", "met_need", "u5m", "urbanicity"]
    ENSEMBLE_COV_COUNTS = [2, 3, 4]
    STAGE_2_ARIMA_ATTENUATION_END_YEAR = 2050
    STAGE_3_COVARIATES = ["education", "met_need"]


class ModelConstants:
    """Constants for model_strategy.py."""

    log_logit_offset = 1e-8
    knot_placements = np.array([0.0, 0.1, 0.365, 0.75, 1.0])
