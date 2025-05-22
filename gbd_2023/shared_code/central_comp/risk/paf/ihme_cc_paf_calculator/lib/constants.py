from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from gbd.constants import columns, gbd_process
from ihme_cc_risk_utils.lib.constants import (  # noqa: F401
    AIR_PMHAP_REI_ID,
    COMPUTATION_CAUSE_SET_ID,
    COMPUTATION_REI_SET_ID,
    IRON_DEFICIENCY_REI_ID,
    LBWSGA_REI_ID,
)

# PAF Calculator argument defaults. Need to be available via reticulate in R
# so any change in the defaults automatically carries over to the R wrapper
DEFAULT_N_DRAWS: int = 1000
DEFAULT_REI_SET_ID: int = COMPUTATION_REI_SET_ID
DEFAULT_CAUSE_SET_ID: int = COMPUTATION_CAUSE_SET_ID
DEFAULT_SKIP_SAVE_RESULTS: bool = False
DEFAULT_RESUME: bool = False
DEFAULT_ROOT_RUN_DIR: str = 
DEFAULT_DEV_ROOT_RUN_DIR: str = 
DEFAULT_TEST: bool = False

SEV_CALCULATOR_INPUT_DIR: str = 

MODEL_DESCRIPTION = "PAF{unmediated} for rei_id {rei_id}.{extra_description}"
MAX_DESCRIPTION_LENGTH = 500  # from epi.model_version

RUN_DIR_TEMPLATE: str = 
CACHE_MANIFEST: str = 
DRAW_FILE_PATTERN: str = 
DRAW_UNMEDIATED_FILE_PATTERN: str = 

# LBW/SG child risk subdirectories used for saving their PAF results
LBW_SUBDIR: str = 
SG_SUBDIR: str = 
LBW_DRAW_FILE_PATTERN = 
SG_DRAW_FILE_PATTERN = 

# Unavertable risk subdirectory used for saving PAF results
UNAVERTABLE_SUBDIR: str = 
UNAVERTABLE_FILE_PATTERN = 

# Flat file with shifts by age and hypertension prevalence.
# These are in per 1 unit (both g/day and mmHg) space, no conversion needed
SALT_SBP_DELTA_FILE: str = (
)

JOBMON_URL: str = 
JOBMON_WORKFLOW_NAME: str = (
    "PAF Calculator for rei_id {rei_id} paf modelable_entity_id {me_id} and "
    "model_version_id {mv_id}"
)

JOBMON_WORKFLOW_ARGS_TEMPLATE: str = (
    "paf_calculator_paf_{paf_model_version_id}_"
    "unmediated_{paf_unmediated_model_version_id}_{timestamp}"
)
JOBMON_TOOL: str = "paf_calculator"
PROD_CONDA_INSTALL: str = 
PROD_CONDA_ENV: str = 
SAVE_RESULTS_DB_PROD_ENV = "prod"
SAVE_RESULTS_DB_DEV_ENV = "dev"

# Jobmon resource strings
MAX_RUNTIME: str = "runtime"
MEMORY: str = "memory"
NUM_CORES: str = "cores"
QUEUE: str = "queue"

AGE_GROUP_ID = columns.AGE_GROUP_ID
CAUSE_ID = columns.CAUSE_ID
LOCATION_ID = columns.LOCATION_ID
MEASURE_ID = columns.MEASURE_ID
MORBIDITY = "morbidity"
MORTALITY = "mortality"
REI_ID = columns.REI_ID
SEX_ID = columns.SEX_ID
YEAR_ID = columns.YEAR_ID

DEMOGRAPHIC_COLS = [LOCATION_ID, YEAR_ID, AGE_GROUP_ID, SEX_ID]
PAF_INDEX_COLUMNS = [REI_ID] + DEMOGRAPHIC_COLS + [CAUSE_ID, MEASURE_ID]
MORB_MORT_COLS = [MORBIDITY, MORTALITY]

# Summary files for TMREL, RR, and PAF
TMREL_SUMMARY_FILE: str = 
RR_SUMMARY_FILE: str = 
PAF_SUMMARY_FILE: str = 

# Averted Burden REI set ID
AVERTED_BURDEN_REI_SET_ID: int = 22

# REIs
SALT_REI_ID: int = 124  # Diet high in sodium
OCCUPATIONAL_NOISE_REI_ID: int = 130  # Occupational noise
INJECTED_DRUG_USE_REI_ID: int = 138  # Injected drug use
DRUG_DEPENDENCE_REI_ID: int = 140  # Drug dependence
IPV_REI_ID: int = 168  # intimate partner violence, direct PAF
UNSAFE_SEX_REI_ID: int = 170  # Unsafe sex
BLOOD_LEAD_REI_ID: int = 242  # Lead exposure in blood
FPG_REI_ID: int = 105  # fasting plasma glucose
METAB_SBP_REI_ID: int = 107  # high systolic blood pressure
SHORT_GESTATION_REI_ID: int = 334  # Short gestation
LOW_BIRTH_WEIGHT_REI_ID: int = 335  # Low birth weight

SMOKING_REI_ID: int = 99  # Smoking
BMD_REI_ID: int = 109  # Low bone mineral density
INJURY_REI_IDS: List[int] = [SMOKING_REI_ID, BMD_REI_ID]

# Risks we know where all of their outcomes use CodCorrect or COMO
REI_IDS_THAT_USE_CODCORRECT_FOR_ALL_CAUSES: List[int] = [AIR_PMHAP_REI_ID, LBWSGA_REI_ID]
REI_IDS_THAT_USE_COMO_FOR_ALL_YEARS: List[int] = [
    BLOOD_LEAD_REI_ID,
    OCCUPATIONAL_NOISE_REI_ID,
]

# cause ids with special logic
NEONATAL_PRETERM_CAUSE_ID: int = 381  # neonatal preterm birth
CKD_CAUSE_ID: int = 589  # chronic kidney disease
CKD_HTN_CAUSE_ID: int = 591  # chronic kidney disease due to hypertension
DM_T1_CAUSE_ID: int = 997  # CKD due to diabetes mellitus type 1
DM_T2_CAUSE_ID: int = 998  # CKD due to diabetes mellitus type 2
LBWSG_CAUSE_ID: int = 1061  # low birth weight/short gestation
ALL_CAUSE_ID: int = 294  # all cause
PTB_CAUSE_ID: int = 381  # neonatal preterm birth
HTN_CAUSE_ID: int = 498  # hypertensive heart disease
HEARING_CAUSE_ID: int = 674  # Age-related and other hearing loss

HIP_FRACTURE_CAUSE_ID: int = 878  # Fracture of hip
NON_HIP_FRACTURE_CAUSE_ID: int = 923  # Fractures
FRACTURE_CAUSE_IDS: List[int] = [HIP_FRACTURE_CAUSE_ID, NON_HIP_FRACTURE_CAUSE_ID]

# Parent cause IDs that are split into child causes using CodCorrect or CodCorrect + COMO
CAUSE_IDS_THAT_USE_CODCORRECT: List[int] = [CKD_CAUSE_ID, LBWSG_CAUSE_ID]
CAUSE_IDS_THAT_USE_COMO: List[int] = [CKD_CAUSE_ID]

LBWSG_CAUSE_SET_ID: int = 20  # low birth weight/short gestation has special cause set

# air pmhap and LBW/SG have a custom cause set, other risks default to standard cause set
DEFAULT_CAUSE_SET_PER_REI = defaultdict(
    lambda: DEFAULT_CAUSE_SET_ID,
    {AIR_PMHAP_REI_ID: LBWSG_CAUSE_SET_ID, LBWSGA_REI_ID: LBWSG_CAUSE_SET_ID},
)

# Hardcoded ME IDs
LOW_BIRTH_WEIGHT_ME_ID: int = 10811
SHORT_GESTATION_ME_ID: int = 10812

# Map from machinery process IDs to cause IDs requiring that machinery.
CAUSE_IDS_THAT_USE_MACHINERY: Dict[int, List[int]] = {
    gbd_process.COD: CAUSE_IDS_THAT_USE_CODCORRECT,
    gbd_process.EPI: CAUSE_IDS_THAT_USE_COMO,
}

# Map from machinery process IDs to REI IDs requiring that machinery for all PAF years.
REI_IDS_THAT_USE_MACHINERY_FOR_ALL_YEARS: Dict[int, List[int]] = {
    gbd_process.COD: [],
    gbd_process.EPI: REI_IDS_THAT_USE_COMO_FOR_ALL_YEARS,
}

# Map from machinery process IDs to REI IDs requiring that machinery for all cause outcomes.
REI_IDS_THAT_USE_MACHINERY_FOR_ALL_CAUSES: Dict[int, List[int]] = {
    gbd_process.COD: REI_IDS_THAT_USE_CODCORRECT_FOR_ALL_CAUSES,
    gbd_process.EPI: [],
}

# Map from machinery process IDs to machinery names.
PROCESS_ID_TO_MACHINERY_NAME: Dict[int, str] = {
    gbd_process.COD: "CodCorrect",
    gbd_process.EPI: "COMO",
}


@dataclass
class SubDirs:
    """Subdirs in the model directory that will be created."""

    stderr: str = 
    stdout: str = 


@dataclass
class CacheContents:
    """Variable references for run-specific cache names."""

    SETTINGS: str = "settings"
    MODEL_VERSIONS: str = "models"
    REI_METADATA: str = "rei_metadata"
    MEDIATOR_REI_METADATA: str = "mediator_rei_metadata"
    INTERVENTION_REI_METADATA: str = "intervention_rei_metadata"
    CAUSE_METADATA: str = "cause_metadata"
    AGE_METADATA: str = "age_metadata"
    DEMOGRAPHICS: str = "demographics"
    RR_METADATA: str = "rr_metadata"
    PAF_MVID_STAGED_RESULT: str = "paf_staged_result"
    PAF_UNMED_MVID_STAGED_RESULT: str = "paf_unmediated_staged_result"
    PAF_LBW_MVID_STAGED_RESULT: str = "paf_lbw_staged_result"
    PAF_SG_MVID_STAGED_RESULT: str = "paf_sg_staged_result"
    PAF_UNAVTB_MVID_STAGED_RESULT: str = "paf_unavertable_staged_result"
    EXPOSURE_MIN_MAX: str = "exposure_min_max"
    MEDIATION_FACTORS: str = "mediation_factors"


@dataclass
class LocationCacheContents:
    """Variable references for location-specific cache names."""

    EXPOSURE: str = "exposure"
    EXPOSURE_SD: str = "exposure_sd"
    TMREL: str = "tmrel"
    MEDIATOR_EXPOSURE: str = "mediator_exposure"
    MEDIATOR_EXPOSURE_SD: str = "mediator_exposure_sd"
    MEDIATOR_TMREL: str = "mediator_tmrel"
    INTERVENTION_COVERAGE: str = "intervention_coverage"


# NB: PAF model version IDs created for the given PAF Calculator run must be in 1:1
# correspondence with settings whose names match the pattern paf.*model_version_id,
# ignoring any values of None.
@dataclass
class PafCalculatorSettings:
    """Settings for a PAF Calculator run."""

    rei_id: int
    cluster_proj: str
    year_id: List[int]
    n_draws: int
    release_id: int
    skip_save_results: bool
    rei_set_id: int
    output_dir: str
    resume: bool
    test: bool
    paf_modelable_entity_id: int
    paf_model_version_id: int
    paf_unmediated_modelable_entity_id: Optional[int]
    paf_unmediated_model_version_id: Optional[int]
    paf_lbw_model_version_id: Optional[int] = None  # additional slots for LBW/SG
    paf_sg_model_version_id: Optional[int] = None  # additional slots for LBW/SG
    paf_unavertable_model_version_id: Optional[int] = None  # additional slot for AB
    codcorrect_version_id: Optional[int] = None
    como_version_id: Optional[int] = None
    intervention_rei_id: Optional[int] = None


LAUNCH_SCRIPT_TEMPLATE = """#!
#SBATCH -J paf_calculator_launcher
#SBATCH -A {cluster_proj}
#SBATCH -p long.q
#SBATCH -c 1
#SBATCH --mem=2GB
#SBATCH -t 16-0
#SBATCH -o 
#SBATCH -e 

source 

"""
LAUNCH_SCRIPT_TEMPLATE += (
    "conda run -p {conda_env} python -m ihme_cc_paf_calculator.cli.launch_workflow "
    "--model_root_dir {model_root_dir} --conda_env {conda_env}"
)


@dataclass
class CalculationType:
    """Used to determine categorical vs continuous risk factors. REI metadata type id 8"""

    AGGREGATE = 0
    CATEGORICAL = 1
    CONTINUOUS = 2
    CUSTOM = 3
    DIRECT = 4


@dataclass
class ExposureType:
    """Describes the distribution type of categorical and continuous exposure models.
    REI metadata type id 9.
    """

    DICHOTOMOUS = "dichotomous"
    ORDERED_POLYTOMOUS = "ordered polytomous"
    UNORDERED_POLYTOMOUS = "unordered polytomous"
    LOGNORMAL = "lognormal"
    ENSEMBLE = "ensemble"


@dataclass
class RelativeRiskType:
    """Describes how a relative risk curve is represented. Corresponds to
    epi.relative_risk_type.

    1. Relative risk estimated for discrete categories of exposure
    2. Relative risk estimated as a log-linear per-unit function of exposure
    3. Relative risk estimated at specific exposure levels along the risk curve which can
        be used to approximate a function of exposure
    """

    CATEGORICAL = 1
    LOG_LINEAR = 2
    EXPOSURE_DEPENDENT = 3


@dataclass
class PafModelStatus:
    """Simplified PAF model version statuses for monitoring purposes."""

    SUCCESS = 0
    RUNNING = 1
    FAILED = 2


# For subcause splitting, we have a few hardcoded rules for which subcauses
# are fully attributable
# if parent is CKD: FPG and CKD due to DM T1 and T2, SBP and CKD due to hypertension
# if parent is LBW/SG outcomes: LBW/SG and Neonatal preterm
FULLY_ATTRIBUTABLE_SUBCAUSES = defaultdict(
    list,
    {
        (CKD_CAUSE_ID, FPG_REI_ID): [DM_T1_CAUSE_ID, DM_T2_CAUSE_ID],
        (CKD_CAUSE_ID, METAB_SBP_REI_ID): [CKD_HTN_CAUSE_ID],
        (LBWSG_CAUSE_ID, LBWSGA_REI_ID): [NEONATAL_PRETERM_CAUSE_ID],
    },
)

# if parent cause is CKD, for any risk other than FPG exclude the DM T1 subcause
CAUSES_WITH_SUBCAUSE_EXCLUSIONS = {
    CKD_CAUSE_ID: defaultdict(lambda: [DM_T1_CAUSE_ID], {FPG_REI_ID: []})
}

# causes modeled at the parent level that must be split after PAF calculation
PARENT_LEVEL_CAUSES = [CKD_CAUSE_ID, LBWSG_CAUSE_ID]

# causes that can have multiple mediators through 2-stage mediation, with their mediators
CAUSES_WITH_MULTIPLE_2_STAGE_MEDIATORS = {CKD_CAUSE_ID: [FPG_REI_ID, METAB_SBP_REI_ID]}

# Some risk-cause pairs use an absolute risk curve instead of relative risk.
# We need to know this in order to handle regions of the curve with zeroes.
RISK_CAUSES_WITH_ABS_RISK_CURVES = [(METAB_SBP_REI_ID, HTN_CAUSE_ID)]


@dataclass
class EpiModelStatus:
    """Relevant statuses for epi models."""

    SUBMITTED: int = 0
    DELETED: int = 3
    SOFT_SHELL_DELETED: int = 6
    FAILED: int = 7


# Number of days since last_updated time until a failed PAF model is considered a candidate for
# deletion.
STALE_TIME = 7

# Privileged connection definitions used to manage PAF model statuses.
PRIVILEGED_EPI_CONN_DEF = "privileged-epi-save-results"
PRIVILEGED_TEST_EPI_CONN_DEF = "privileged-epi-save-results-test"

# Query for updating model statuses.
UPDATE_MODEL_STATUSES_QUERY = """
    UPDATE epi.model_version
    SET model_version_status_id = :to_status
    WHERE model_version_id IN :model_version_ids
    AND model_version_status_id = :from_status
    """

# Query for pulling deletion candidates.
GET_DELETION_CANDIDATES_QUERY = """
    SELECT model_version_id
    FROM epi.model_version
    WHERE
    (
        model_version_status_id IN :deleted_model_version_status_ids
        OR
        (
            model_version_status_id = :failed_model_version_status_id
            AND last_updated < DATE_SUB(curdate(), INTERVAL :stale_time DAY)
        )
    )
    """

# At and below this draw cutoff, we cap memory predictions for continuous PAF calculation jobs
# at LOW_MEMORY_CAP. Above it, we cap at HIGH_MEMORY_CAP.
LOW_MEMORY_CAP_DRAW_CUTOFF = 250

# Memory cap (in MB) for low-draw continuous PAF calculation jobs.
LOW_MEMORY_CAP = 5000

# Memory cap (in MB) for high-draw continuous PAF calculation jobs.
HIGH_MEMORY_CAP = 10000
