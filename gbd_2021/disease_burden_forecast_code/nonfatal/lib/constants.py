"""FHS Nonfatal Pipeline Local Constants."""
from collections import namedtuple

from fhs_lib_database_interface.lib.constants import StageConstants as ImportedStageConstants
from fhs_lib_database_interface.lib.query.model_strategy import RATIO_INDICATORS


class JobmonConstants:
    """Constants related to Jobmon tasks."""

    TOOL_NAME = "fhs_nonfatal_pipeline_tool"
    PIPELINE_NAME = "fhs_nonfatal_pipeline"
    TIMEOUT = 260000  # giving the entire workflow 3 days to run


class MADTruncateConstants:
    """Constants related to MAD truncation."""

    # Settings for mad_truncate method
    MAX_MULTIPLIER = 6.0
    MEDIAN_DIMS = ("age_group_id",)
    MULTIPLIER_STEP = 0.1
    PCT_COVERAGE = 0.975


class ModelConstants:
    """Constants related to modeling or model specification."""

    DEFAULT_OFFSET = 1e-8
    MIN_VALUE = 1e-10


class StageConstants(ImportedStageConstants):
    """Stages in FHS file system."""

    RATIO_MEASURES = tuple(RATIO_INDICATORS.keys())
    FORECAST_MEASURES = RATIO_MEASURES + ("prevalence", "incidence")
    ALL_MEASURES = FORECAST_MEASURES + ("yld",)

    RatioToIndicatorMap = namedtuple(
        "RatioToIndicatorMap", "target_indicator, available_indicator, ratio"
    )

    PHASE_ONE_RATIO_INDICATOR_MAPS = (
        RatioToIndicatorMap("prevalence", "death", "mp_ratio"),
        RatioToIndicatorMap("incidence", "death", "mi_ratio"),
        RatioToIndicatorMap("yld", "yll", "yld_yll_ratio"),
    )
    PHASE_TWO_RATIO_INDICATOR_MAPS = (
        RatioToIndicatorMap("prevalence", "incidence", "pi_ratio"),
        RatioToIndicatorMap("incidence", "prevalence", "pi_ratio"),
    )

    PREVALENCE_MAX = 1 - 1e-8
