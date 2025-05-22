from typing import Dict, List

from gbd.constants import age, measures
from ihme_cc_risk_utils.lib.constants import (  # noqa: F401
    AGGREGATION_REI_SET_ID,
    AIR_PMHAP_REI_ID,
    COMPUTATION_CAUSE_SET_ID,
    COMPUTATION_REI_SET_ID,
    IRON_DEFICIENCY_REI_ID,
    LBWSGA_REI_ID,
    LOW_BIRTH_WEIGHT_REI_ID,
    SEED,
    SHORT_GESTATION_REI_ID,
)

# FILES
ROOT_DIR: str = 
EXISTING_REIS_FILE: str = 
RR_MAX_DRAW_FILE: str = 
CUSTOM_RR_MAX_DRAW_FILE: str = 

RR_MAX_UPLOAD_DIR: str = 
RR_MAX_SUMMARY_FHS_FILE: str = 
RR_MAX_SUMMARY_UPLOAD_FILE: str = 

EXPOSURE_DRAW_FILE: str = 
RISK_PREVALENCE_DRAW_FILE: str = 

PAF_DRAW_DIR: str = 
PAF_DRAW_FILE: str = 

# Duplicated from PAF Calculator; must be kept in sync. Used for pre-conversion PAF draws.
SEV_CALCULATOR_INPUT_DIR: str = 

SEV_DRAW_DIR: str = 
SEV_DRAW_CAUSE_SPECIFIC_DIR: str = 
SEV_DRAW_FILE_PATTERN: str = 
SEV_DRAW_FILE: str = 

# Maps release id -> expected draw dir for temperature SEVs
TEMPERATURE_SEV_DRAW_DIR_MAP: Dict[int, str] = {
    9: ,
    16: ,
}
TEMPERATURE_SEV_FILE_PATTERN: str = 

SEV_UPLOAD_DIR: str = 
SEV_CAUSE_SPECIFIC_UPLOAD_DIR: str = 
SEV_SUMMARY_SINGLE_YEAR_FILE: str = 
SEV_SUMMARY_MULTI_YEAR_FILE: str = 

# MEASURES
RR_MAX: str = "rr_max"
SEV: str = "sev"

MEASURE_MAP: Dict[str, int] = {RR_MAX: measures.RR_MAX, SEV: measures.SEV}

DEMOGRAPHIC_COLS: List[str] = ["location_id", "year_id", "age_group_id", "sex_id"]

# LOCATIONS
DEFAULT_LOCATION_SET_ID: int = 35

# CAUSES
LBWSG_CAUSE_SET_ID: int = 20  # LBW/SG-speciifc cause set, used during SEV calculation
CAUSE_SET_IDS: List[int] = [COMPUTATION_CAUSE_SET_ID, LBWSG_CAUSE_SET_ID]

# Parent causes that have RR (and thus RRmax) estimates but input PAFs are
# at the child cause level. To match, the child cause PAFs are aggregated
# to the parent cause. For LBW/SG, aggregation uses YLLs while for others it
# uses DALYS.
LBWSG_PARENT_CAUSE_ID: int = 1061
OTHER_PARENT_CAUSE_IDS_TO_AGGREGATE_PAFS_FOR: List[int] = [
    417,  # Liver cancer
    521,  # Cirrhosis and other chronic liver diseases
    589,  # Chronic kidney disease
]

# Hypertensive heart disease
HYPERTENSIVE_HD_CAUSE_ID: int = 498

# Hypertension threshold, in units of mmHg
HYPERTENSION_THRESHOLD: int = 140

# Modelable entity ID for cocaine/amphetamine RR model.
COCAINE_AMPHETAMINE_RR_ME_ID: int = 26261

# Modelable entity IDs for anemia prevalence
MODERATE_ANEMIA_ME_ID: int = 1622
SEVERE_ANEMIA_ME_ID: int = 1623

# REIS
REPORTING_REI_SET_ID: int = 1
REI_SET_IDS: List[int] = [
    REPORTING_REI_SET_ID,
    COMPUTATION_REI_SET_ID,
    AGGREGATION_REI_SET_ID,
]

# Misc REI IDs, ordered by REI ID
AIR_PM_REI_ID: int = 86  # Ambient particulate matter pollution
AIR_HAP_REI_ID: int = 87  # Household air pollution from solid fuels
SECONDHAND_SMOKE_REI_ID: int = 100
HIGH_FPG_REI_ID: int = 105  # High fasting plasma glucose
HIGH_SBP_REI_ID: int = 107  # High systolic blood pressure
DRUGS_ILLICIT_SUICIDE_REI_ID: int = 140  # Drug dependence
BMI_ADULT_REI_ID: int = 370

# Modeling team calculates SEV directly for temperature risks
TEMPERATURE_REI_IDS: List[int] = [
    331,  # Non-optimal temperature
    337,  # High temperature
    338,  # Low temperature
]

# Risks we don't have relative risks for due to PAF calc/modeling strategy
NO_RR_REI_IDS: List[int] = [
    201,  # Intimate partner violence (HIV PAF)
    168,  # Intimate partner violence (direct PAF)
    138,  # Injected drug use
    242,  # Lead exposure in blood
    131,  # Occupational injuries
    170,  # Unsafe sex
] + TEMPERATURE_REI_IDS

# Air polution risks with custom RRmax that are mediated through LBW/SG.
# For these risks, we add RRMax for LBW/SG outcomes to the custom RRmax
AIR_PM_MEDIATED_BY_LBWSG_REI_IDS = [AIR_PM_REI_ID, AIR_HAP_REI_ID, AIR_PMHAP_REI_ID]

# RRmax prepped by modeling team, not uploaded centrally (custom RRmax)
CUSTOM_RR_MAX_REI_IDS: List[int] = [
    AIR_PM_REI_ID,  # Ambient particulate matter pollution
    AIR_HAP_REI_ID,  # Household air pollution from solid fuels
    88,  # Ambient ozone pollution
    99,  # Smoking
    102,  # High alcohol use
    363,  # Bullying victimization
    AIR_PMHAP_REI_ID,  # Particulate matter pollution
    404,  # Nitrogen dioxide pollution
]

LBW_PRETERM_REI_IDS: List[int] = [
    SHORT_GESTATION_REI_ID,  # Short gestation
    LOW_BIRTH_WEIGHT_REI_ID,  # Low birth weight
    LBWSGA_REI_ID,  # Low birth weight and short gestation
]

CONVERTED_OUTCOMES_REI_IDS: List[int] = [
    109,  # Low bone mineral density
    130,  # Occupational noise
]

# REIs that we use the R edensity code to simulate the exposure probability density function
EDENSITY_REI_IDS: List[int] = [HIGH_FPG_REI_ID, HIGH_SBP_REI_ID]

# REIs with PAFs of 1 that we create SEVs for
SEVS_FOR_PAFS_OF_ONE_REI_IDS: List[int] = EDENSITY_REI_IDS + [IRON_DEFICIENCY_REI_ID]

CATEGORICAL_RR_ID: int = 1
LOG_LINEAR_RR_ID: int = 2
EXPOSURE_DEPENDENT_RR_ID: int = 3

# EXPOSURE
EXPOSURE_MAX_PERCENTILE: float = 0.05

# AGE GROUPS
# TODO: can this be replaced by a constant from gbd? Should be included in age groups
# reported in process version metadata
SEV_SUMMARY_AGE_GROUP_IDS = [  # decides what aggregate age groups to create for SEV summaries
    1,
    5,
    21,
    23,
    24,
    25,
    26,
    28,
    37,
    39,
    41,
    42,
    157,
    158,
    159,
    160,
    162,
    169,
    228,
    231,
    232,
    234,
    243,
    284,
    420,
    age.ALL_AGES,
    age.AGE_STANDARDIZED,
]

# Which year to pull input machinery estimates (COMO, CodCorrect) for.
# These estimates are used to aggregate CKD and LBW/SG-related child PAFs to the parent cause.
# In theory, the ratio of child cause burden does not vary much by year so any year should do.
# Also, using a single year avoids a dependency on annual COMO/CodCorrect runs.
# Set to 2023 for GBD 2023. Should be in sync with the PAF Calculator.
#: Arbitrary year for YLLs and YLDs
ARBITRARY_MACHINERY_YEAR_ID: int = 2023

# JOBMON
TOOL: str = "sev_calculator"

# Compute resources
MAX_RUNTIME: str = "runtime"
MEMORY: str = "memory"
NUM_CORES: str = "cores"
QUEUE: str = "queue"

# Queue options
ALL_QUEUE: str = "all.q"
LONG_QUEUE: str = "long.q"

# Misc
SLURM: str = "slurm"
PROJECT: str = "proj_centralcomp"
WORKFLOW_ARGS: str = "sev_calculator_{version_id}_{timestamp}"
WORKFLOW_NAME: str = "SEV Calculator v{version_id}"

# Hard-coded years for percent change calculations.
PERCENT_CHANGE_YEARS: List[List[int]] = [
    [1990, 2000],
    [1990, 2023],
    [2000, 2010],
    [2010, 2023],
    [2020, 2023],
]

# Cap for PAF draws that shouldn't be 1.
PAF_DRAW_CAP = 0.9999
