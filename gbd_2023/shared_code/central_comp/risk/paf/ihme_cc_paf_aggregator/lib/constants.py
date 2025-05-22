from dataclasses import dataclass, fields
from pathlib import Path
from typing import List

from gbd import constants
from ihme_cc_risk_utils.lib.constants import AGGREGATION_REI_SET_ID  # noqa: F401

PAF_ROOT = 
PAF_INPUTS_J_DRIVE = 

PARENT_ID = "parent_id"
MED_ID = "med_id"
MEAN_MEDIATION = "mean_mediation"
DRAW_TYPE = "draw_type"
WORMHOLE_MODEL_VERSION_ID = "wormhole_model_version_id"
REI_ID = constants.columns.REI_ID
CAUSE_ID = constants.columns.CAUSE_ID
LOCATION_ID = constants.columns.LOCATION_ID
SEX_ID = constants.columns.SEX_ID
MEASURE_ID = constants.columns.MEASURE_ID
AGE_GROUP_ID = constants.columns.AGE_GROUP_ID
YEAR_ID = constants.columns.YEAR_ID
MODEL_VERSION_ID = constants.columns.MODEL_VERSION_ID
RELEASE_ID = constants.columns.RELEASE_ID
DECOMP_STEP_ID = "decomp_step_id"
GBD_ROUND_ID = constants.columns.GBD_ROUND_ID
METRIC_ID = constants.columns.METRIC_ID
MODELABLE_ENTITY_ID = constants.columns.MODELABLE_ENTITY_ID
MEASURE_IDS = [constants.measures.YLD, constants.measures.YLL]
COMPUTATION_LOCATION_SET_ID = 35
COMPUTATION_CAUSE_SET_ID = 2
GBD_RISKS_AND_ETIOLOGIES = [2, 12]
GBD_PROCESS_ID = constants.gbd_process.PAF
SPECIAL_STATUS = constants.gbd_process_version_status.SPECIAL
ACTIVE_STATUS = constants.gbd_process_version_status.ACTIVE
PAF_VERSION_METATADA = constants.gbd_metadata_type.RISK
YEAR_IDS_METATADA = constants.gbd_metadata_type.YEAR_IDS
N_DRAWS_METATADA = constants.gbd_metadata_type.N_DRAWS
MEASURE_IDS_METADATA = constants.gbd_metadata_type.MEASURE_IDS
AGE_GROUP_IDS_METADATA = constants.gbd_metadata_type.AGE_GROUP_IDS
THREE_FOUR_FIVE_METADATA = constants.gbd_metadata_type.THREE_FOUR_FIVE_RUN
AGE_MEASURE_METADATA_FILEPATH = 
REI_MEASURE_METADATA_FILEPATH = 
PROD = "prod"
MOST_DETAILED = "most_detailed"
CKD_PARENT_CAUSE_ID = 589
OUTPUT_DRAW_PATTERN = 

AGE_META_COLS = [AGE_GROUP_ID, MEASURE_ID]
REI_META_COLS = [REI_ID, CAUSE_ID, MEASURE_ID, SEX_ID]


@dataclass
class CacheContents:
    """Variable references for cache names."""

    AGE_METADATA: str = "age_metadata"
    INPUT_MODELS: str = "input_models"
    MEDIATION_MATRIX: str = "mediation_matrix"
    RISK_HIERARCHY: str = "risk_hierarchy"
    SETTINGS: str = "settings"

    @staticmethod
    def list() -> List[str]:
        """Returns a list of CacheContent item values."""
        return [i.type(i.default) for i in fields(CacheContents)]


@dataclass
class PostRunCacheContents:
    """Variable references for cache names."""

    AGE_META_FROM_DRAW: str = "age_meta_from_draw"
    REI_META_FROM_DRAW: str = "rei_meta_from_draw"


# Custom PAF-related constants
SMOKING_REI_ID = 99
LOW_EDUCATIONAL_ATTAINMENT_REI_ID: int = 501
FRACTURE_CAUSE_IDS = [878, 923]  # Fracture of hip, Fractures
INJECTED_DRUG_USE_REI_ID: int = 138
ALL_CAUSE_ID = constants.cause.ALL_CAUSE
AGGREGATE_OUTCOMES = [ALL_CAUSE_ID]

# Bugfix PAF-related constants.
ALCOHOL_REI_ID = 102
PAFS_LESS_THAN_ONE_CAP = 0.9999


@dataclass
class PafAggregatorSettings:
    """Settings for a PAF aggregation run."""

    n_draws: int
    year_id: List[int]
    release_id: int
    output_dir: str
    location_id: List[int]
    version_id: int
    resume: bool
    all_paf_causes: List[int]
    skip_aggregation: bool = False
