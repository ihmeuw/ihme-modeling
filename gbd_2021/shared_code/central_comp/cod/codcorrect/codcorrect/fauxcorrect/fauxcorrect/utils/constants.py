from os.path import join
from typing import Dict, List


class Draws:
    DECOMP_STEP: str = 'decomp_step'
    DOWNSAMPLE: str = 'downsample'
    GBD_ID: str = 'gbd_id'
    GBD_ID_TYPE: str = 'gbd_id_type'
    GBD_ROUND_ID: str = 'gbd_round_id'
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
                          MODEL_VERSION_TYPE_ID, IS_BEST, AGE_START,
                          AGE_END, DECOMP_STEP_ID]
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
        37, 39, 155, 160, 197, 228, 230, 232,
        243, 284, 285, 286, 287, 288, 289, 420, 430]


class COD:
    class DataBase:
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


class ConnectionDefinitions:
    COD: str = 'cod'
    CODCORRECT: str = 'codcorrect'
    GBD: str = 'gbd-codcorrect'
    JOBMON: str = 'jobmon-2'
    MORTALITY: str = 'mortality'
    TEST: str = 'codcorrect-test'
    USER: str = 'codcorrect_user'

    # Expected conn defs in the runners PERSONAL odbc
    EXPECTED: List[str] = [COD, CODCORRECT, GBD, MORTALITY]


class Jobmon:
    # EXEC PARAMS INPUTS
    MAX_RUNTIME: str = "max_runtime_seconds"
    MEM_FREE: str = "m_mem_free"
    NUM_CORES: str = "num_cores"
    QUEUE: str = "queue"

    # QUEUE OPTIONS
    ALL_QUEUE: str = "all.q"
    LONG_QUEUE: str = "long.q"

    # MISC
    PROJECT: str = "proj_codcorrect"
    TOOL: str = "CodCorrect"
    TOOL_VERSION_ID: int = 17
    WORKFLOW_ARGS: str = "codcorrect_v{version_id}_{timestamp}"
    WORKFLOW_NAME: str = "CodCorrect Version {version_id}"


class DataBases:
    COD: str = 'cod'
    GBD: str = 'gbd'
    CODCORRECT: str = 'codcorrect'
    DATABASES: List[str] = [COD, GBD, CODCORRECT]


class GBD:
    class DataBase:
        SCHEMA: str = 'gbd'
        FAUXCORRECT_TABLE: str = (
            'output_fauxcod_{upload_type}_year_v{process_version_id}'
        )
        CODCORRECT_TABLE: str = (
            'output_cod_{upload_type}_year_v{process_version_id}'
        )

    class MetadataTypeIds:
        CODCORRECT: int = 1
        ENVELOPE: int = 6
        FAUXCORRECT: int = 12
        LIFE_TABLE: int = 3
        POPULATION: int = 7
        LIFE_TABLE_WITH_SHOCK: int = 13
        TMRLT: int = 15

    class Process:
        class Id:
            COMO: int = 1
            CODCORRECT: int = 3
            BURDENATOR: int = 4
            PAFS: int = 5
            DALYNATOR: int = 6
            MMR: int = 12
            SEV: int = 14
            HALE: int = 15
            FAUXCORRECT: int = 35

            # these machinery have no versioning of their own,
            # like MMR v102, HALE v43
            PROCESS_HAS_NO_INTERNAL_VERSIONING = [MMR, HALE]

        class Name:
            CODCORRECT: str = 'codcorrect'
            CODCORRECT_PROPER: str = 'CoDCorrect'
            FAUXCORRECT: str = 'fauxcorrect'
            FAUXCORRECT_PROPER: str = 'FauxCorrect'
            OPTIONS: List[str] = [CODCORRECT, FAUXCORRECT]
            SHOCKS: str = 'shocks'

        class VersionNote:
            CODCORRECT: str = (
                'CoDCorrect v{version_id}{test_str}, Population v{population_version}, '
                'Envelope v{envelope_version}, Life Table v{life_table_version}, '
                'TMRLT v{tmrlt_version}'
            )
            FAUXCORRECT: str = (
                'FauxCorrect v{version_id}{test_str}, Scalar v{scalar_version}, '
                'Population v{population_version}, Envelope v{envelope_version}'
                'Life Table v{life_table_version}, TMRLT v{tmrlt_version}'
            )


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
    CODCORRECT_ROOT_DIR: str = '/ROOT/{version_id}'
    CORRECTION_HIERARCHY: str = 'correction_hierarchy.csv'
    COVID_SCALARS: str = 'covid_scalars'
    COVID_SCALARS_FILE_PATTERN: str = '{cause_id}_{sex_id}_{location_id}.h5'
    COVID_SCALARS_SUMMARIES_FILE_PATTERN: str = '{sex_id}_{location_id}.h5'
    COVID_SCALARS_SUMMARY_FILE: str = 'covid_scalars_summaries_{version_id}.csv'
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
        '/MOR_ENV_DIR'
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
    ROOT_DIR: str = '/ROOT'
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
    TEMP_DIR: str = '/TEMP_DIR'
    UNAGGREGATED_DIR: str = 'unaggregated'
    UNAGGREGATED_SHOCKS_FILE_PATTERN: str = '{model_version_id}.h5'
    UNAGGREGATED_UNSCALED_FILE_PATTERN: str = (
        'unscaled_{location_id}_{sex_id}.h5'
    )
    UNSCALED_DRAWS_FILE_PATTERN: str = '{model_version_id}.h5'
    UNSCALED_DIR: str = 'unscaled'
    YLL_DRAWS_FILE_PATTERN: str = '{sex_id}_{location_id}_{{year_id}}.h5'
    YLLS_DIR: str = '4'

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
    TEST: int = 110


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
    INDIRECT_COVID: int = 12

    ALL_TYPE_IDS: List[int] = [HYBRID, CUSTOM, SHOCKS, HIV, IMPORTED_CASES, INDIRECT_COVID]
    EXEMPT_TYPE_IDS: List[int] = [SHOCKS, HIV, IMPORTED_CASES, INDIRECT_COVID]
    NON_SHOCK_TYPE_IDS: List[int] = [HYBRID, CUSTOM, HIV, IMPORTED_CASES]


class MortalityInputs:
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'

    ALL_INPUTS: List[str] = [ENVELOPE_DRAWS, ENVELOPE_SUMMARY, POPULATION]
    FAUXCORRECT_INPUTS: List[str] = [ENVELOPE_SUMMARY, POPULATION]


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
    CODCORRECT_DIAGNOSTIC: str = "HTTP"

    CHANNEL: str = "TOKEN"
    MACHINERY_RUNS: str = "TOKEN"


class Scatters:
    TITLE: str = "v{version_id}"
    PLOT_DIR: str = "/SCATTERS_DIR/v{version_id}"
    SCRIPT_PATH: str = "/SCRIPT" 
