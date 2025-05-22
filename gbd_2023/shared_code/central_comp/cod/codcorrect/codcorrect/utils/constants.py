from os.path import join
from typing import Dict, List


class Draws:
    DOWNSAMPLE: str = 'downsample'
    GBD_ID: str = 'gbd_id'
    GBD_ID_TYPE: str = 'gbd_id_type'
    N_DRAWS_PARAM: str = 'n_draws'
    NUM_WORKERS: str = 'num_workers'
    MAX_DRAWS: int = 1000
    RELEASE_ID: str = 'release_id'
    SOURCE: str = 'codem'
    SOURCE_PARAM: str = 'source'
    VERSION_ID: str = 'version_id'
    YEAR_ID: str = 'year_id'


class Best:
    NOT_BEST: int = 0
    BEST: int = 1
    PREVIOUS_BEST: int = 2


class CauseSetId:
    CODCORRECT: int = 1
    COMPUTATION: int = 2
    REPORTING: int = 3
    REPORTING_AGGREGATES: int = 16
    TEST: int = 18


class Causes:
    ALL_CAUSE: int = 294


class Columns:
    ACAUSE: str = 'acause'
    AGE_GROUP_ID: str = 'age_group_id'
    AGE_GROUP_YEARS_START: str = 'age_group_years_start'
    AGE_END: str = 'age_end'
    AGE_START: str = 'age_start'
    AGE_WEIGHT_VALUE: str = 'age_group_weight_value'
    BACKFILLED_ID: str = 'backfilled_id'
    CAUSE_AGE_END = 'cause_age_end'
    CAUSE_AGE_START = 'cause_age_start'
    CAUSE_AGES = 'cause_ages'
    CAUSE_FRACTION_MEAN = 'mean_cf'
    CAUSE_FRACTION_LOWER = 'lower_cf'
    CAUSE_FRACTION_UPPER = 'upper_cf'
    CAUSE_ID: str = 'cause_id'
    CAUSE_METADATA_TYPE: str = 'cause_metadata_type'
    CAUSE_METADATA_VALUE: str = 'cause_metadata_value'
    CAUSE_METADATA_VERSION_ID: str = 'cause_metadata_version_id'
    CAUSE_SET_ID: str = 'cause_set_id'
    CAUSE_SET_VERSION_ID: str = 'cause_set_version_id'
    COD_MEAN: str = 'mean_death'
    COD_LOWER: str = 'lower_death'
    COD_UPPER: str = 'upper_death'
    CODCORRECT_VERSION_ID: str = 'codcorrect_version_id'
    CODE_VERSION: str = 'code_version'
    DESCRIPTION: str = 'description'
    DIAGNOSTICS_AFTER: str = 'mean_after'
    DIAGNOSTICS_BEFORE: str = 'mean_before'
    DRAW_PREFIX: str = 'draw_'
    ENVELOPE: str = 'envelope'
    ENV_PREFIX: str = 'env_'
    ENV_VERSION: str = 'env_version'
    EXCLUDED_FROM_CORRECTION: str = 'excluded_from_correction'
    IS_BEST: str = 'is_best'
    IS_ESTIMATE: str = 'is_estimate'
    IS_ESTIMATE_COD: str = 'is_estimate_cod'
    IS_SCALED: str = 'is_scaled'
    JOIN_KEY: str = 'join_key'
    LEVEL: str = 'level'
    LOCATION_ID: str = 'location_id'
    LOCATION_NAME: str = 'location_name'
    LOCATION_SET_ID: str = 'location_set_id'
    LOCATION_SET_VERSION_ID: str = 'location_set_version_id'
    LOWER: str = 'lower'
    MAX_RANK: str = 'max_rank'
    MEAN: str = 'mean'
    MEASURE_ID: str = 'measure_id'
    METADATA_TYPE_ID: str = 'metadata_type_id'
    METRIC_ID: str = 'metric_id'
    MODEL_VERSION_ID: str = 'model_version_id'
    MODEL_VERSION_TYPE_ID: str = 'model_version_type_id'
    MOST_DETAILED: str = 'most_detailed'
    OUTPUT_PROCESS_ID: str = 'output_process_id'
    OUTPUT_VERSION_ID: str = 'output_version_id'
    PARENT_ID: str = 'parent_id'
    PCT_CHANGE_MEANS: str = 'pct_change_means'
    PRED_EX: str = 'pred_ex'
    POPULATION: str = 'population'
    RANK: str = 'rank'
    RELEASE_ID: str = 'release_id'
    RESTRICTION_VERSION_ID: str = 'restriction_version_id'
    RUN_ID: str = 'run_id'
    SCALAR: str = 'scalar'
    SCALAR_VERSION_ID: str = 'scalar_version_id'
    SEX_ID: str = 'sex_id'
    SORT_ORDER: str = 'sort_order'
    SHOCK_CAUSE: str = 'shock_cause'
    STATUS: str = 'status'
    TOOL_TYPE_ID: str = 'tool_type_id'
    UPPER: str = 'upper'
    VALUE: str = 'val'
    YEAR_ID: str = 'year_id'
    YEAR_END_ID: str = 'year_end_id'
    YEAR_START_ID: str = 'year_start_id'
    YLD_ONLY: str = 'yld_only'

    CAUSE_FRACTION_SUMMARY = [CAUSE_FRACTION_MEAN, CAUSE_FRACTION_LOWER,
                              CAUSE_FRACTION_UPPER]
    CAUSE_HIERARCHY = [LEVEL, PARENT_ID]
    CAUSE_HIERARCHY_AGGREGATION = [CAUSE_ID, LEVEL, PARENT_ID, MOST_DETAILED]
    COD_SUMMARY = [COD_MEAN, COD_LOWER, COD_UPPER]
    CAUSE_FRACTION_SUMMARY = [CAUSE_FRACTION_MEAN, CAUSE_FRACTION_LOWER,
                              CAUSE_FRACTION_UPPER]
    DEMOGRAPHIC_INDEX: List[str] = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID]
    INDEX: List[str] = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID, CAUSE_ID]
    INDEX_NO_CAUSE = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID]
    KEEP_VALIDATION: List[str] = (
        INDEX + [MODEL_VERSION_TYPE_ID, ENVELOPE, IS_SCALED]
    )
    MODEL_TRACKER_COLS = [CAUSE_ID, SEX_ID, MODEL_VERSION_ID,
                          MODEL_VERSION_TYPE_ID, AGE_START,
                          AGE_END, RELEASE_ID]
    MODEL_TRACKER_SORTBY_COLS = [CAUSE_ID, SEX_ID, MODEL_VERSION_TYPE_ID,
                                 AGE_START]
    MULTI_SUMMARY_COLS = [LOCATION_ID, YEAR_START_ID, YEAR_END_ID, SEX_ID,
                          AGE_GROUP_ID, CAUSE_ID, METRIC_ID, VALUE, UPPER,
                          LOWER]
    PRED_EX_COLS = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID, PRED_EX]
    SPACETIME_RESTRICTIONS = [RESTRICTION_VERSION_ID, LOCATION_ID, YEAR_ID]
    UNIQUE_COLUMNS: List[str] = (
        [CAUSE_ID, SEX_ID, AGE_START, AGE_END, MODEL_VERSION_TYPE_ID]
    )


class Ages:
    END_OF_ROUND_AGE_GROUPS: List[int] = [
        4,
        5,
        37,
        39,
        160,
        197,
        228,
        231,
        232,
        243,
        284,
        420,
    ]
    END_OF_ROUND_AGE_GROUPS_2019: List[int] = [
        5, 
        4, 
        37, 39, 155, 160, 197, 228, 230, 232,
        243, 284, 285, 286, 287, 288, 289, 420, 430]


class COD:
    class DataBase:
        METADATA_TYPE_ID: int = 2  # CodCorrect version
        OUTPUT_PROCESS_ID: int = 1  # CodCorrect process
        SCHEMA: str = 'cod'
        TABLE: str = 'output'
        TOOL_TYPE_ID: int = 22


class Diagnostics:
    class DataBase:
        SCHEMA: str = 'codcorrect'
        TABLE: str = 'diagnostic'
        COLUMNS: List[str] = [
            Columns.CODCORRECT_VERSION_ID, Columns.LOCATION_ID, Columns.YEAR_ID,
            Columns.SEX_ID, Columns.AGE_GROUP_ID, Columns.CAUSE_ID,
            Columns.DIAGNOSTICS_BEFORE
        ]


class Jobmon:
    # COMPUTE RESOURCEES INPUTS
    MAX_RUNTIME: str = "runtime"
    MEMORY: str = "memory"
    NUM_CORES: str = "cores"
    QUEUE: str = "queue"

    # QUEUE OPTIONS
    ALL_QUEUE: str = "all.q"
    LONG_QUEUE: str = "long.q"

    # MISC
    SLURM: str = "slurm"
    PROJECT: str = "proj_codcorrect"
    TOOL: str = "CodCorrect"
    TOOL_VERSION_ID: int = 92
    WORKFLOW_ARGS: str = "codcorrect_v{version_id}_{timestamp}"
    WORKFLOW_NAME: str = "CodCorrect Version {version_id}"


class FilePaths:
    AGE_WEIGHT_FILE: str = 'age_weights.csv'
    AGGREGATED_DIR: str = 'aggregated'
    AGGREGATED_UNSCALED_FILE_PATTERN: str = (
        '{measure_id}/{sex_id}_{location_id}_{year_id}.h5'
    )
    APPEND_SHOCKS_FILE_PATTERN: str = '{sex_id}_{location_id}_{{year_id}}.h5'
    CAUSE_AGGREGATE_FILE_PATTERN: str = (
        '{measure_id}/{sex_id}_{location_id}_{{year_id}}.h5'
    )
    CAUSE_BACKFILL_MAPPING: str = 'cause_map.pkl'
    COD_UPLOAD: str = 'cod'
    CODCORRECT_ROOT_DIR: str = 'FILEPATH'
    CORRECTION_HIERARCHY: str = 'correction_hierarchy.csv'
    DEATHS_DIR: str = '1'
    DIAGNOSTICS_DIR: str = 'diagnostics'
    DIAGNOSTICS_UPLOAD_FILE: str = 'upload_diagnostics.csv'
    DIAGNOSTICS_DETAILED_FILE_PATTERN: str = (
        'diagnostics_{sex_id}_{location_id}.csv'
    )
    DRAWS_DIR: str = 'draws'
    DRAWS_FILE_PATTERN: str = (
        '{measure_id}/{sex_id}_{location_id}_{year_id}.h5'
    )
    DRAWS_SCALED_DIR: str = 'draws/scaled'
    DRAWS_UNSCALED_DIR: str = 'draws/unscaled'
    ENVELOPE_DRAWS_FILE: str = 'envelope_draws.h5'
    ENVELOPE_SUMMARY_FILE: str = 'envelope_summary.h5'
    GBD_UPLOAD: str = 'gbd'
    INPUT_FILES_DIR: str = 'input_files'
    INPUT_MODELS_FILE: str = '{process}_v{version_id}_models.csv'
    LOCATION_AGGREGATE_FILE_PATTERN: str = (
        '{{sex_id}}_{{location_id}}_{year_id}.h5'
    )
    LOCATION_AGGREGATES: str = 'location_aggregates'
    LOCATION_BACKFILL_MAPPING: str = 'location_map.pkl'
    LOG_DIR: str = 'logs'
    MORT_ENVELOPE_DRAW_DIR: str = (
        'FILEPATH'
    )
    MORT_ENVELOPE_FILE: str = 'combined_env_aggregated_{year_id}.h5'
    MULTI_DIR: str = 'multi'
    PARAM_DIR: str = 'parameters'
    PARAM_FILE: str = 'parameters.pkl'
    POPULATION_FILE: str = 'pop.h5'
    PRED_EX: str = 'pred_ex.h5'
    RESCALED_DIR: str = 'rescaled'
    RESCALED_DRAWS_FILE_PATTERN: str = 'rescaled_{location_id}_{sex_id}.h5'
    REGIONAL_SCALARS: str = 'regional_scalars.h5'
    ROOT_DIR: str = 'FILEPATH'
    SCALAR_SOURCE_DIR: str = 'summaries/scalars'
    SCALARS: str = 'scalars.h5'
    SCALE_DRAWS_FILE_PATTERN: str = '{sex_id}_{location_id}_{{year_id}}.h5'
    SHOCKS_DIR: str = 'shocks'
    SINGLE_DIR: str = 'single'
    SPACETIME_RESTRICTIONS: str = 'spacetime_restrictions.h5'
    STDERR: str = 'stderr'
    STDOUT: str = 'stdout'
    SUMMARY_DIR: str = 'summaries'
    SUMMARY_AGGREGATE_READ_PATTERN: str = (
        '{measure_id}/{{sex_id}}_{location_id}_{year_id}.h5')
    SUMMARY_INPUT_FILE_PATTERN: str = '{{sex_id}}_{location_id}_{year_id}.h5'
    SUMMARY_OUTPUT_FILE_PATTERN: str = '{year_id}/{location_id}.csv'
    TEMP_DIR: str = 'FILEPATH'
    UNAGGREGATED_DIR: str = 'unaggregated'
    UNAGGREGATED_SHOCKS_FILE_PATTERN: str = '{model_version_id}.h5'
    UNAGGREGATED_UNSCALED_FILE_PATTERN: str = (
        'unscaled_{location_id}_{sex_id}.h5'
    )
    UNSCALED_DRAWS_FILE_PATTERN: str = '{model_version_id}.h5'
    UNSCALED_DIR: str = 'unscaled'
    YLL_DRAWS_FILE_PATTERN: str = '{sex_id}_{location_id}_{{year_id}}.h5'
    YLLS_DIR: str = '4'

    MEASURE_DIRS: List[str] = [DEATHS_DIR, YLLS_DIR]
    STD_DIRS: List[str] = [STDOUT, STDERR]


class LocationAggregation:
    class Type:
        RESCALED: str = 'rescaled'
        SHOCKS: str = 'shocks'
        UNSCALED: str = 'unscaled'

        CODCORRECT: List[str] = [RESCALED, SHOCKS, UNSCALED]

    class Ids:
        WHO: int = 3
        WORLD_BANK: int = 5
        EU: int = 11
        COMMONWEALTH: int = 20
        FOUR_WORLD: int = 24
        WORLD_BANK_INCOME_LEVELS: int = 26
        OECD: int = 28
        G20: int = 31
        AU: int = 32
        NORDIC_REGION: int = 46

        SPECIAL_LOCATIONS: List[int] = [
            WHO, WORLD_BANK, EU, COMMONWEALTH, FOUR_WORLD,
            WORLD_BANK_INCOME_LEVELS, OECD, G20, AU, NORDIC_REGION
        ]


class LocationSetId:
    OUTPUTS: int = 89
    STANDARD: int = 35
    SDI: int = 40
    TEST: int = 110


class ModelVersionTypeId:
    HYBRID: int = 3
    CUSTOM: int = 4
    SHOCKS: int = 5
    IMPORTED_CASES: int = 7

    ALL_TYPE_IDS: List[int] = [HYBRID, CUSTOM, SHOCKS, IMPORTED_CASES]
    EXEMPT_TYPE_IDS: List[int] = [SHOCKS, IMPORTED_CASES]
    NON_SHOCK_TYPE_IDS: List[int] = [HYBRID, CUSTOM, IMPORTED_CASES]


class MortalityInputs:
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'

    ALL_INPUTS: List[str] = [ENVELOPE_DRAWS, ENVELOPE_SUMMARY, POPULATION]


class MortalityEnvelope:
    WITH_HIV: int = 1


class MortalityProcessId:
    LIFE_TABLE_NO_SHOCK: int = 27
    LIFE_TABLE_WITH_SHOCK: int = 29
    TMRLT: int = 30


class Status:
    RUNNING: int = 0
    COMPLETE: int = 1
    SUBMITTED: int = 2
    DELETED: int = 3


class Slack:
    CODCORRECT_DIAGNOSTIC: str = "TOKEN"

    SHADES_DMS: str = "TOKEN"

class Scatters:
    TITLE: str = "v{version_id}"
    PLOT_DIR: str = "FILEPATH"
    SCRIPT_PATH: str = "FILEPATH"


class SummaryType:
    SINGLE: str = "single"
    MULTI: str = "multi" 
