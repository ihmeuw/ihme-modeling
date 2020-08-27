from os.path import join
from typing import Dict, List

from db_queries import get_demographics
import gbd.constants as gbd


class Draws:
    DECOMP_STEP: str = 'decomp_step'
    DOWNSAMPLE: str = 'downsample'
    GBD_ID: str = 'gbd_id'
    GBD_ID_TYPE: str = 'gbd_id_type'
    GBD_ROUND_ID: str = 'gbd_round_id'
    N_DRAWS: int = 1000
    N_DRAWS_PARAM: str = 'n_draws'
    NUM_WORKERS: str = 'num_workers'
    MAX_DRAWS: int = 1000
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
    FAUXCORRECT: int = 14
    REPORTING: int = 3
    REPORTING_AGGREGATES: int = 16


class Decomp:
    class Causes:
        # Represents ntd causes for which we expect decomp models
        # Not used anywhere, just for reference.
        DECOMP_NTS: List[int] = [
            344,
            351,
            # 360,
            361,
            362,
            363
        ]
        # Represents ntd causes and new causes for the current GBD round
        EXEMPT_NTDS: List[int] = [
            # 344
            346,
            347,
            348,
            349,
            350,
            # 351,
            352,
            353,
            354,
            355,
            356,
            357,
            358,
            359,
            # 360,
            # 361,
            # 362,
            # 363,
            364,
            843,
            935,
            936,
            365
        ]

        # 1014, 1015, 1016, 1017 are new GBD 2019 causes but they are yld only
        NEW_ROUND_CAUSES: List[int] = [
            1004,
            1005,
            1006,
            1007,
            1008,
            1009,
            1010,
            1011,
            1012,
            1013
        ]
        EXEMPT_CAUSE_IDS: List[int] = EXEMPT_NTDS + NEW_ROUND_CAUSES
        ALL_CAUSE: int = 294

    class Step:
        ONE: int = 1
        TWO: int = 2
        THREE: int = 3
        FOUR: int = 4
        FIVE: int = 5
        ITERATIVE: int = 7


class Columns:
    ACAUSE: str = 'acause'
    AGE_GROUP_ID: str = 'age_group_id'
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
    CODE_VERSION: str = 'code_version'
    DECOMP_STEP_ID: str = 'decomp_step_id'
    DESCRIPTION: str = 'description'
    DIAGNOSTICS_AFTER: str = 'mean_after'
    DIAGNOSTICS_BEFORE: str = 'mean_before'
    DRAW_PREFIX: str = 'draw_'
    ENVELOPE: str = 'envelope'
    ENV_PREFIX: str = 'env_'
    ENV_VERSION: str = 'env_version'
    FAUXCORRECT_VERSION_ID: str = 'fauxcorrect_version_id'
    GBD_ROUND: str = 'gbd_round'
    GBD_ROUND_ID: str = 'gbd_round_id'
    IS_BEST: str = 'is_best'
    IS_ESTIMATE: str = 'is_estimate'
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
    METRIC_ID: str = 'metric_id'
    MODEL_VERSION_ID: str = 'model_version_id'
    MODEL_VERSION_TYPE_ID: str = 'model_version_type_id'
    MOST_DETAILED: str = 'most_detailed'
    OUTPUT_VERSION_ID: str = 'output_version_id'
    PARENT_ID: str = 'parent_id'
    PCT_CHANGE_MEANS: str = 'pct_change_means'
    PRED_EX: str = 'pred_ex'
    POPULATION: str = 'population'
    RANK: str = 'rank'
    RUN_ID: str = 'run_id'
    SCALAR: str = 'scalar'
    SCALAR_VERSION_ID: str = 'scalar_version_id'
    SEX_ID: str = 'sex_id'
    SORT_ORDER: str = 'sort_order'
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
    DRAWS: List[str] = [f'draw_{draw_num}' for draw_num in range(Draws.N_DRAWS)]
    ENVELOPE_DRAWS: List[str] = (
        [f'env_{draw_num}' for draw_num in range(Draws.N_DRAWS)]
    )
    INDEX: List[str] = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID, CAUSE_ID]
    INDEX_NO_CAUSE = [LOCATION_ID, YEAR_ID, SEX_ID, AGE_GROUP_ID]
    KEEP_VALIDATION: List[str] = (
            INDEX + [MODEL_VERSION_TYPE_ID, ENVELOPE, IS_SCALED] + DRAWS
    )
    MODEL_TRACKER_COLS = [CAUSE_ID, SEX_ID, MODEL_VERSION_ID,
                          MODEL_VERSION_TYPE_ID, IS_BEST, AGE_START,
                          AGE_END, DECOMP_STEP_ID]
    MODEL_TRACKER_SORTBY_COLS = [CAUSE_ID, SEX_ID, MODEL_VERSION_TYPE_ID,
                                 AGE_START]
    MULTI_SUMMARY_COLS = [LOCATION_ID, YEAR_START_ID, YEAR_END_ID, SEX_ID,
                          AGE_GROUP_ID, CAUSE_ID, METRIC_ID, VALUE, UPPER,
                          LOWER]
    UNIQUE_COLUMNS: List[str] = (
        [CAUSE_ID, SEX_ID, AGE_START, AGE_END, MODEL_VERSION_TYPE_ID]
    )


class Ages:
    MOST_DETAILED_GROUP_IDS: List[int] = get_demographics(
        gbd_round_id=gbd.GBD_ROUND_ID, gbd_team='cod'
    )[Columns.AGE_GROUP_ID]
    ALL_AGE_GROUPS: List[int] = MOST_DETAILED_GROUP_IDS + gbd.GBD_COMPARE_AGES
    END_OF_ROUND_AGE_GROUPS: List[int] = [37, 39, 155, 160, 197, 228, 230, 232,
        243, 284, 285, 286, 287, 288, 289, 420, 430]


class COD:
    class DataBase:
        SCHEMA: str = 'SCHEMA'
        TABLE: str = 'TABLE'
        TOOL_TYPE_ID: int = 22


class Diagnostics:
    class DataBase:
        SCHEMA: str = 'SCHEMA'
        TABLE: str = 'TABLE'
        COLUMNS: List[str] = [
            Columns.OUTPUT_VERSION_ID, Columns.LOCATION_ID, Columns.YEAR_ID,
            Columns.SEX_ID, Columns.AGE_GROUP_ID, Columns.CAUSE_ID,
            Columns.DIAGNOSTICS_BEFORE, Columns.DIAGNOSTICS_AFTER
        ]


class ConnectionDefinitions:
    COD: str = 'SCHEMA'
    CODCORRECT: str = 'SCHEMA'
    GBD: str = 'SCHEMA'
    MORTALITY: str = 'SCHEMA'
    TEST: str = 'SCHEMA'


class DAG:
    class Executables:
        APPEND_SHOCKS: str = 'append_shocks.py'
        BACKFILL_MAPPINGS: str = 'run_backfill.py'
        CACHE_MORT_INPUT: str = 'cache_mortality_inputs.py'
        CAUSE_AGG: str = 'apply_cause_aggregation.py'
        CORRECT: str = 'apply_correction.py'
        LOC_AGG: str = 'aggregate_locations.py'
        SCALARS: str = 'apply_scalars.py'
        SUMMARIZE_COD: str = 'summarize_cod.py'
        SUMMARIZE_GBD: str = 'summarize_gbd.py'
        SUMMARIZE_PCT_CHANGE: str = 'summarize_pct_change.py'
        UPLOAD: str = 'upload.py'
        VALIDATE_DRAWS: str = 'validate_draws.py'
        YLLS: str = 'calculate_ylls.py'

    class Tasks:
        class Cores:
            APPEND_SHOCKS: int = 20
            APPLY_CORRECTION: int = 20
            APPLY_SCALARS: int = 20
            BACKFILL: int = 1
            CACHE_MORTALITY: int = 1
            CACHE_PRED_EX: int = 1
            CACHE_REGIONAL_SCALARS: int = 1
            CACHE_SPACETIME_RESTRICTIONS: int = 1
            CAUSE_AGGREGATION: int = 20
            LOCATION_AGGREGATION: int = 12
            PCT_CHANGE: int = 2
            SUMMARIZE: int = 2
            UPLOAD: int = 20
            VALIDATE_DRAWS: int = 10
            YLLS: int = 20

        class Memory:
            APPEND_SHOCKS: str = '20G'
            APPLY_CORRECTION: str = '20G'
            APPLY_SCALARS: str = '15G'
            BACKFILL: str = '128M'
            CACHE_ENVELOPE_DRAWS: str = '150G'
            CACHE_ENVELOPE_SUMMARY: str = '5G'
            CACHE_POPULATION: str = '5G'
            CACHE_PRED_EX: str = '5G'
            CACHE_REGIONAL_SCALARS: str = '128M'
            CACHE_SPACETIME_RESTRICTIONS: str = '256M'
            CAUSE_AGGREGATION: str = '45G'
            LOCATION_AGGREGATION: str = '45G'
            PCT_CHANGE: str = '10G'
            SUMMARIZE: str = '5G'
            UPLOAD: str = '100G'
            VALIDATE_DRAWS: str = '100G'
            YLLS: str = '15G'

            CACHE_ALL_MORTALITY_INPUTS: List[str] = (
                [CACHE_ENVELOPE_DRAWS, CACHE_ENVELOPE_SUMMARY, CACHE_POPULATION]
            )

        class Name:
            APPEND_SHOCKS: str = 'append_shocks_{location}_{sex}'
            APPLY_CORRECTION: str = 'apply_correction_{location}_{sex}'
            APPLY_SCALARS: str = 'apply_scalars_{location}_{sex}'
            BACKFILL_MAPPINGS: str = 'backfill_{mapping_type}'
            CACHE_MORTALITY: str = 'cache_{mort_process}'
            CACHE_PRED_EX: str = 'cache_pred_ex'
            CACHE_REGIONAL_SCALARS: str = 'cache_regional_scalars'
            CACHE_SPACETIME: str = 'cache_spacetime_restrictions'
            CALC_YLLS: str = 'calc_ylls_{location}_{sex}'
            CAUSE_AGGREGATION: str = 'agg_cause_{location}_{sex}'
            LOCATION_AGGREGATION: str = (
                'agg_location_{aggregation_type}_{location_set}_{measure}_'
                '{year}'
            )
            SUMMARIZE_COD: str = 'summarize_cod_{measure}_{location}_{year}'
            SUMMARIZE_GBD: str = 'summarize_gbd_{measure}_{location}_{year}'
            SUMMARIZE_PCT_CHANGE: str = (
                'summarize_pct_change_{measure}_{location}_{year_start}'
                '_{year_end}'
            )
            UPLOAD: str = 'upload_{database}_{uploadtype}_{measure}'
            VALIDATE_DRAWS: str = 'validate_draws_{model_version_id}'

        class Runtime:
            APPEND: int = 7200
            APPLY_CORRECTION: int = 7200
            APPLY_SCALARS: int = 7200
            BACKFILL: int = 600
            CACHE_MORTALITY: int = 7200
            CACHE_PRED_EX: int = 10800
            CACHE_REGIONAL_SCALARS: int = 600
            CACHE_SPACETIME_RESTRICTIONS: int = 600
            CALCULATE: int = 7200
            CAUSE_AGGREGATION: int = 10800
            LOCATION_AGGREGATION: int = 21600
            SUMMARIZE: int = 10800
            # setting upload to 8 days but we should never need this long
            UPLOAD: int = 691200
            VALIDATE_DRAWS: int = 7200

        class Type:
            APPEND: str = 'append'
            CACHE: str = 'cache'
            CAUSE_AGG: str = 'cause_aggregation'
            CORRECT: str = 'correct'
            CALCULATE: str = 'calc'
            DIAGNOSTIC: str = 'diagnostic'
            LAUNCH: str = 'launch'
            LOC_AGG: str = 'location_aggregation'
            MAP: str = 'create_mapping'
            SCALE: str = 'scale'
            SUMMARIZE: str = 'summarize'
            UPLOAD: str = 'upload'
            VALIDATE: str = 'validate'
            YLLS: str = 'ylls'

    class Workflow:
        PROJECT: str = 'proj_codcorrect'
        CODCORRECT_NAME: str = 'CoDCorrect Version {version_id}'
        CODCORRECT_WORKFLOW_ARGS: str = 'codcorrect_v{version_id}_{timestamp}'
        FAUXCORRECT_NAME: str = 'FauxCorrect Version {version_id}'
        FAUXCORRECT_WORKFLOW_ARGS: str = 'fauxcorrect_v{version_id}'

    QUEUE = 'all.q'
    UPLOAD_QUEUE = 'long.q'


class DataBases:
    COD: str = 'SCHEMA'
    GBD: str = 'SCHEMA'
    CODCORRECT: str = 'SCHEMA'
    DATABASES: List[str] = [COD, GBD, CODCORRECT]


class GBD:
    class DataBase:
        SCHEMA: str = 'SCHEMA'
        FAUXCORRECT_TABLE: str = (
            'TABLE'
        )
        CODCORRECT_TABLE: str = (
            'TABLE'
        )

    class MetadataTypeIds:
        CODCORRECT: int = 1
        ENVELOPE: int = 6
        FAUXCORRECT: int = 12
        LIFE_TABLE: int = 3
        POPULATION: int = 7

    class Process:
        class Id:
            CODCORRECT: int = 3
            FAUXCORRECT: int = 35

        class Name:
            CODCORRECT: str = 'codcorrect'
            FAUXCORRECT: str = 'fauxcorrect'
            OPTIONS: List[str] = [CODCORRECT, FAUXCORRECT]
            SHOCKS: str = 'shocks'

        class VersionNote:
            CODCORRECT: str = (
                'CoDCorrect v{version_id}, Population v{population_version}, '
                'Envelope v{envelope_version}, Life Table v{life_table_version}'
            )
            FAUXCORRECT: str = (
                'FauxCorrect v{version_id}, Scalar v{scalar_version}, '
                'Population v{population_version}, Envelope v{envelope_version}'
            )


class FilePaths:
    AGE_WEIGHT_FILE: str = 'FILEPATH'
    AGGREGATED_DIR: str = 'FILEPATH'
    AGGREGATED_UNSCALED_FILE_PATTERN: str = 'FILEPATH'
    APPEND_SHOCKS_FILE_PATTERN: str = 'FILEPATH'
    CAUSE_AGGREGATE_FILE_PATTERN: str = 'FILEPATH'
    CAUSE_BACKFILL_MAPPING: str = 'FILEPATH'
    COD_UPLOAD: str = 'FILEPATH'
    CODCORRECT_ROOT_DIR: str = 'FILEPATH'
    DEATHS_DIR: str = 'FILEPATH'
    DIAGNOSTICS_DIR: str = 'FILEPATH'
    DIAGNOSTICS_UPLOAD_FILE: str = 'FILEPATH'
    DIAGNOSTICS_DETAILED_FILE_PATTERN: str = 'FILEPATH'
    DRAWS_DIR: str = 'FILEPATH'
    DRAWS_FILE_PATTERN: str = 'FILEPATH'
    DRAWS_SCALED_DIR: str = 'FILEPATH'
    DRAWS_UNSCALED_DIR: str = 'FILEPATH'
    ENVELOPE_DRAWS_FILE: str = 'FILEPATH'
    ENVELOPE_SUMMARY_FILE: str = 'FILEPATH'
    GBD_UPLOAD: str = 'FILEPATH'
    INPUT_FILES_DIR: str = 'FILEPATH'
    INPUT_MODELS_FILE: str = 'FILEPATH'
    LOCATION_AGGREGATE_FILE_PATTERN: str = 'FILEPATH'
    LOCATION_AGGREGATES: str = 'FILEPATH'
    LOCATION_BACKFILL_MAPPING: str = 'FILEPATH'
    LOG_DIR: str = 'FILEPATH'
    MORT_ENVELOPE_DRAW_DIR: str = 'FILEPATH'
    MORT_ENVELOPE_FILE: str = 'FILEPATH'
    MULTI_DIR: str = 'FILEPATH'
    PARAM_DIR: str = 'FILEPATH'
    PARAM_FILE: str = 'FILEPATH'
    POPULATION_FILE: str = 'FILEPATH'
    PRED_EX: str = 'FILEPATH'
    RESCALED_DIR: str = 'FILEPATH'
    RESCALED_DRAWS_FILE_PATTERN: str = 'FILEPATH'
    REGIONAL_SCALARS: str = 'FILEPATH'
    ROOT_DIR: str = 'FILEPATH'
    SCALAR_SOURCE_DIR: str = 'FILEPATH'
    SCALARS: str = 'FILEPATH'
    SCALE_DRAWS_FILE_PATTERN: str = 'FILEPATH'
    SHOCKS_DIR: str = 'FILEPATH'
    SINGLE_DIR: str = 'FILEPATH'
    SPACETIME_RESTRICTIONS: str = 'FILEPATH'
    STDERR: str = 'FILEPATH'
    STDOUT: str = 'FILEPATH'
    SUMMARY_DIR: str = 'FILEPATH'
    SUMMARY_AGGREGATE_READ_PATTERN: str = 'FILEPATH'
    SUMMARY_INPUT_FILE_PATTERN: str = 'FILEPATH'
    SUMMARY_OUTPUT_FILE_PATTERN: str = 'FILEPATH'
    UNAGGREGATED_DIR: str = 'FILEPATH'
    UNAGGREGATED_SHOCKS_FILE_PATTERN: str = 'FILEPATH'
    UNAGGREGATED_UNSCALED_FILE_PATTERN: str = 'FILEPATH'
    UNSCALED_DRAWS_FILE_PATTERN: str = 'FILEPATH'
    UNSCALED_DIR: str = 'FILEPATH'
    YLL_DRAWS_FILE_PATTERN: str = 'FILEPATH'
    YLLS_DIR: str = 'FILEPATH'

    FAUXCORRECT_DRAW_DIRS: List[str] = [DRAWS_SCALED_DIR, DRAWS_UNSCALED_DIR]
    MEASURE_DIRS: List[str] = [DEATHS_DIR, YLLS_DIR]
    STD_DIRS: List[str] = [STDOUT, STDERR]


class Keys:
    DRAWS: str = 'draws'
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'
    PRED_EX: str = 'pred_ex'
    REGIONAL_SCALARS: str = 'regional_scalars'
    SCALARS: str = 'scalars'
    SPACETIME_RESTRICTIONS: str = 'spacetime_restrictions'


class LocationAggregation:
    class Type:
        CAUSE_AGGREGATED_RESCALED: str = join(
            FilePaths.AGGREGATED_DIR,
            FilePaths.RESCALED_DIR
        )
        CAUSE_AGGREGATED_SHOCKS: str = join(
            FilePaths.AGGREGATED_DIR,
            FilePaths.SHOCKS_DIR
        )
        CAUSE_AGGREGATED_UNSCALED: str = join(
            FilePaths.AGGREGATED_DIR,
            FilePaths.UNSCALED_DIR
        )
        SCALED: str = FilePaths.DRAWS_SCALED_DIR
        UNAGGREGATED_SHOCKS: str = join(
            FilePaths.UNAGGREGATED_DIR,
            FilePaths.SHOCKS_DIR
        )

        CODCORRECT: List[str] = (
            [
                CAUSE_AGGREGATED_RESCALED, CAUSE_AGGREGATED_SHOCKS,
                CAUSE_AGGREGATED_UNSCALED
            ]
        )
        FAUXCORRECT: List[str] = [SCALED, UNAGGREGATED_SHOCKS]

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


class Measures:
    class Ids:
        DEATHS: int = 1
        YLLS: int = 4

    class Names:
        DEATHS: str = 'deaths'
        YLLS: str = 'ylls'


class ModelVersionTypeId:
    HYBRID: int = 3
    CUSTOM: int = 4
    SHOCKS: int = 5
    HIV: int = 6
    IMPORTED_CASES: int = 7

    ALL_TYPE_IDS: List[int] = [HYBRID, CUSTOM, SHOCKS, HIV, IMPORTED_CASES]
    EXEMPT_TYPE_IDS: List[int] = [SHOCKS, HIV, IMPORTED_CASES]


class MortalityInputs:
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'

    ALL_INPUTS: List[str] = [ENVELOPE_DRAWS, ENVELOPE_SUMMARY, POPULATION]
    FAUXCORRECT_INPUTS: List[str] = [ENVELOPE_SUMMARY, POPULATION]


class Status:
    RUNNING: int = 0
    COMPLETE: int = 1
    SUBMITTED: int = 2
    DELETED: int = 3


class Years:
    ALL: List[int] = list(range(1980, gbd.GBD_ROUND + 1))
    ESTIMATION: List[int] = [1990, 2000, 2017]


class SpecialMappings:
    NO_SCALE_VALUE: int = -1
    LOCATIONS: Dict[int, int] = {
        367: -1, 396: -1, 393: -1, 320: -1, 369: -1, 374: -1, 380: -1, 413: -1,
        416: -1
    }
    CAUSES: Dict[int, int] = {}
    NON_MELANOMA: int = 462
    EYE_CANCER: int = 1008