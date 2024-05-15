"""FHS Pipeline for BMI forecasting Local Constants."""

from fhs_lib_data_transformation.lib import processing
from fhs_lib_database_interface.lib.constants import (
    ScenarioConstants as ImportedScenarioConstants,
)
from frozendict import frozendict


class EntityConstants:
    """Constants related to entities (e.g. acauses)."""

    DEFAULT_ENTITY = "default_entity"
    MALARIA_ENTITIES = ["malaria", "malaria_act", "malaria_itn"]
    NO_SEX_SPLIT_ENTITY = [
        "abuse_csa_male",
        "abuse_csa_female",
        "abuse_ipv",
        "abuse_ipv_exp",
        "met_need",
        "nutrition_iron",
        "inj_homicide_gun_abuse_ipv_paf",
        "inj_homicide_other_abuse_ipv_paf",
        "inj_homicide_knife_abuse_ipv_paf",
    ]
    MALARIA = "malaria"
    ACT_ITN_COVARIATE = "act-itn"


class LocationConstants:
    """Constants used for malaria locations."""

    # locaions with ACT/ITN interventions
    MALARIA_ACT_ITN_LOCS = [
        168,
        175,
        200,
        201,
        169,
        205,
        202,
        171,
        170,
        178,
        179,
        173,
        207,
        208,
        206,
        209,
        172,
        180,
        210,
        181,
        211,
        184,
        212,
        182,
        213,
        214,
        185,
        522,
        216,
        217,
        187,
        435,
        204,
        218,
        189,
        190,
        191,
        198,
        176,
    ]
    # locaions without ACT/ITN interventions
    NON_MALARIA_ACT_ITN_LOCS = [
        128,
        129,
        130,
        131,
        132,
        133,
        7,
        135,
        10,
        11,
        12,
        13,
        139,
        15,
        16,
        142,
        18,
        19,
        20,
        152,
        26,
        28,
        157,
        30,
        160,
        161,
        162,
        163,
        164,
        165,
        68,
        203,
        215,
        108,
        111,
        113,
        114,
        118,
        121,
        122,
        123,
        125,
        127,
        193,
        195,
        196,
        197,
        177,
    ]


class ModelConstants:
    """Constants used in forecasting."""

    FLOOR = 1e-6
    LOGIT_OFFSET = 1e-8
    MIN_RMSE = 1e-8

    DIFF_OVER_MEAN = True  # ARC is computed as the difference over mean values

    MODEL_WEIGHTS_FILE = "all_model_weights.csv"


class ScenarioConstants(ImportedScenarioConstants):
    """Constants related to scenarios."""

    DEFAULT_BETTER_QUANTILE = 0.15
    DEFAULT_WORSE_QUANTILE = 0.85


class FileSystemConstants:
    """Constants for the file system organization."""

    PV_FOLDER = "pv"
    SUBMODEL_FOLDER = "sub_models"


class SEVConstants:
    """Constants used in SEVs forecasting."""

    INTRINSIC_SEV_FILENAME_SUFFIX = "intrinsic"


class TransformConstants:
    """Constants for transformations used during entity forecasting."""

    TRANSFORMS = frozendict(
        {
            "logit": processing.LogitProcessor,
            "log": processing.LogProcessor,
            "no-transform": processing.NoTransformProcessor,
        }
    )


class JobConstants:
    """Constants related to submitting jobs."""

    EXECUTABLE = "fhs_lib_genem_console"
    DEFAULT_RUNTIME = "12:00:00"

    COLLECT_SUBMODELS_RUNTIME = "05:00:00"
    MRBRT_RUNTIME = "16:00:00"



class OrchestrationConstants:
    """Constants used for ensemble model orchestration."""

    OMEGA_MIN = 0.0
    OMEGA_MAX = 3.0
    OMEGA_STEP_SIZE = 0.5

    SUBFOLDER = "risk_acause_specific"

    PV_SUFFIX = "_pv"
    N_HOLDOUT_YEARS = 10  # number of holdout years for predictive validity runs

    ARC_TRANSFORM = "logit"
    ARC_TRUNCATE_QUANTILES = (0.025, 0.975)
    ARC_REFERENCE_SCENARIO = "mean"
