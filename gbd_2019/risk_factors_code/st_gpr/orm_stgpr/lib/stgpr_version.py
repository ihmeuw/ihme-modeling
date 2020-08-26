import getpass
import logging
from typing import Any, Dict, List, Optional
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import orm

import elmo
import gbd
from rules import RulesManager, enums as rules_enums

from orm_stgpr.lib import model_version
from orm_stgpr.lib.constants import columns, demographics, parameters
from orm_stgpr.lib.util import (
    config_utils,
    custom_input_utils,
    files,
    helpers,
    knockouts,
    offset,
    query,
    square,
    step_control
)
from orm_stgpr.lib.validation import data_validation, parameter_validation
from orm_stgpr.db import models, lookup_tables

warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
warnings.filterwarnings('ignore', category=pd.errors.ParserWarning)


def create_stgpr_version(
        session: orm.Session,
        path_to_config: str,
        model_index_id: Optional[int] = None,
        output_path: Optional[str] = None
) -> int:
    """
    Creates a new ST-GPR version (and model version) and adds it to
    the database.

    Args:
        path_to_config: path to config CSV containing model parameters
        model_index_id: index of config parameters to use, if config contains
            multiple sets of model parameters
        output_path: where to save files

    Returns:
        Created ST-GPR version ID.

    Raises:
        ValueError: for config validation errors
    """
    params = config_utils.get_parameters_from_config(
        path_to_config, model_index_id
    )

    # Before pulling anything, make sure the parameters are valid.
    rules_manager = _get_rules_manager(params)
    parameter_validation.validate_rules(rules_manager, params, session)
    parameter_validation.validate_parameters(params, session)
    best_model_id = _get_best_model_id(params, session)
    parameter_validation.validate_parameter_changes(
        rules_manager, best_model_id, params, session
    )

    # Density cutoffs can be input as e.g. both [5, 10] and [0, 5, 10],
    # and orm_stgpr.get_parameters returns density cutoffs in the [5, 10]
    # format. So when reading in configs, we remove the leading zero if present
    # before validating parameters, then we explicitly add the leading 0 here
    # before continuing with registration.
    params[parameters.DENSITY_CUTOFFS].insert(0, 0)

    # Read custom inputs (either custom covariates or custom stage 1
    # estimates) and validate parameter changes between decomp steps.
    custom_stage_1_df = custom_input_utils.read_custom_stage_1(
        params[parameters.PATH_TO_CUSTOM_STAGE_1],
        params[parameters.PATH_TO_CUSTOM_COVARIATES],
        params[parameters.STAGE_1_MODEL_FORMULA]
    )
    custom_covariates_df = custom_input_utils.read_custom_covariates(
        params[parameters.PATH_TO_CUSTOM_COVARIATES],
        params[parameters.PATH_TO_CUSTOM_STAGE_1],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        best_model_id,
        session
    )

    # Prepare basic demographics.
    (location_hierarchy_df,
     location_set_version_id,
     standard_location_set_version_id,
     location_ids) = query.get_locations(
         params[parameters.LOCATION_SET_ID],
         params[parameters.GBD_ROUND_ID],
         params[parameters.DECOMP_STEP],
         session
    )
    year_ids = list(range(
        params[parameters.YEAR_START], params[parameters.YEAR_END] + 1
    ))
    age_group_ids = params[parameters.PREDICTION_AGE_GROUP_IDS]
    sex_ids = params[parameters.PREDICTION_SEX_IDS]

    # Pull data from crosswalk version or path_to_data.
    data_df = _get_data(
        params,
        location_ids,
        year_ids,
        age_group_ids,
        sex_ids
    )

    # Calculate offset.
    params[parameters.IS_CUSTOM_OFFSET] = \
        params[parameters.TRANSFORM_OFFSET] is not None
    params[parameters.TRANSFORM_OFFSET] = offset.calculate_offset(
        data_df[columns.DATA],
        params[parameters.DATA_TRANSFORM],
        params[parameters.TRANSFORM_OFFSET],
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP]
    )
    logging.info(f'Using offset {params[parameters.TRANSFORM_OFFSET]}')

    # Make square and merge on location columns, custom inputs, GBD
    # covariates, and data. Subset to requested demographics, then add
    # knockouts.
    random_seed = np.random.randint(0, np.iinfo(np.int32).max - 1)
    square_df = square.get_square(
        location_ids, year_ids, age_group_ids, sex_ids
    )
    stgpr_df = square_df\
        .pipe(lambda df: square.merge_location_columns_onto_square(
            df, location_hierarchy_df))\
        .pipe(lambda df: square.merge_custom_inputs_onto_square(
            df, custom_covariates_df, custom_stage_1_df))\
        .pipe(lambda df: square.add_gbd_covariates_to_square(
            params, df, year_ids, session))\
        .pipe(lambda df: square.merge_data_onto_square(
            df, data_df, location_ids, year_ids, age_group_ids, sex_ids))\
        .pipe(lambda df: knockouts.add_knockouts_to_dataframe(
            df, params[parameters.HOLDOUTS], random_seed))

    # Create ST-GPR version.
    stgpr_version_id = _create_stgpr_version(
        params,
        custom_covariates_df,
        custom_stage_1_df,
        location_set_version_id,
        standard_location_set_version_id,
        year_ids,
        random_seed,
        session
    )

    # Create model version.
    if not params[parameters.PATH_TO_DATA]:
        model_version.create_model_version(
            session,
            stgpr_version_id=stgpr_version_id,
            decomp_step=params[parameters.DECOMP_STEP],
            gbd_round_id=params[parameters.GBD_ROUND_ID],
            modelable_entity_id=params[parameters.MODELABLE_ENTITY_ID],
            covariate_id=params[parameters.COVARIATE_ID]
        )

    # Save things to files.
    files.save_to_files(
        stgpr_version_id,
        stgpr_df,
        custom_stage_1_df,
        square_df,
        data_df,
        location_hierarchy_df,
        params,
        year_ids,
        age_group_ids,
        sex_ids,
        path_to_config,
        output_path
    )

    logging.info(
        'Created ST-GPR version with ST-GPR version ID (run ID) '
        f'{stgpr_version_id}'
    )
    return stgpr_version_id


def _get_rules_manager(
        params: Dict[str, Any]
) -> Optional[RulesManager.RulesManager]:
    """
    Returns an instantiated RulesManager iff there is no path_to_data
    in params. Else, returns None
    """
    path_to_data: Optional[str] = params[parameters.PATH_TO_DATA]
    gbd_round_id: Optional[int] = params[parameters.GBD_ROUND_ID]
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    if path_to_data:
        return None

    return RulesManager.RulesManager(
        rules_enums.ResearchAreas.EPI,
        rules_enums.Tools.STGPR,
        decomp_step,
        gbd_round_id=gbd_round_id
    )


def _get_best_model_id(
        params: Dict[str, Any],
        session: orm.Session
) -> Optional[int]:
    path_to_data: Optional[str] = params[parameters.PATH_TO_DATA]
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    gbd_round_id: int = params[parameters.GBD_ROUND_ID]
    me_id: Optional[int] = params[parameters.MODELABLE_ENTITY_ID]

    # Do not look for best model IDs for these decomp steps.
    if path_to_data or \
        step_control.exclude_from_best_model_requirement(
            gbd_round_id, decomp_step):
        return None

    return query.get_best_model_id(gbd_round_id, decomp_step, me_id, session)


def _get_data(
        params: Dict[str, Any],
        location_ids: List[int],
        year_ids: List[int],
        age_group_ids: List[int],
        sex_ids: List[int]
) -> pd.DataFrame:
    logging.info('Pulling data')
    crosswalk_version_id: Optional[int] = \
        params[parameters.CROSSWALK_VERSION_ID]
    path_to_data: Optional[str] = params[parameters.PATH_TO_DATA]

    # Get crosswalk version or read from path_to_data.
    # For now, crosswalk versions need to have sex_id assigned.
    data_df = elmo.run\
        .get_crosswalk_version(crosswalk_version_id.item())\
        .assign(sex_id=lambda df:
                df[columns.SEX].str.lower().map(demographics.SEX_MAP))\
        .drop(columns=columns.SEX)\
        if crosswalk_version_id \
        else pd.read_csv(path_to_data)

    # Drop unneeded columns and rename val to data.
    data_df = data_df\
        .pipe(lambda df:
              df[[c for c in df.columns if c in columns.CROSSWALK_DATA]])\
        .pipe(data_validation.validate_data_columns)\
        .pipe(lambda df: data_validation.validate_no_nan_infinity(
            df, [columns.VAL, columns.VARIANCE], data_type='input data'))\
        .rename(columns={columns.VAL: columns.DATA})

    data_validation.validate_data_demographics(
        data_df, location_ids, year_ids, age_group_ids, sex_ids
    )
    return data_df


def _create_stgpr_version(
        params: Dict[str, Any],
        custom_covariates_df: pd.DataFrame,
        custom_stage_1_df: pd.DataFrame,
        location_set_version_id: int,
        standard_location_set_version_id: int,
        year_ids: List[int],
        random_seed: int,
        session: orm.Session
) -> int:
    """Creates a new ST-GPR version in the database."""
    logging.info('Adding ST-GPR version to database')
    stgpr_version = models.StgprVersion(
        bundle_id=(
            int(params[parameters.BUNDLE_ID])
            if params[parameters.BUNDLE_ID]
            else None
        ),
        crosswalk_version_id=(
            int(params[parameters.CROSSWALK_VERSION_ID])
            if params[parameters.CROSSWALK_VERSION_ID]
            else None
        ),
        modelable_entity_id=(
            int(params[parameters.MODELABLE_ENTITY_ID])
            if params[parameters.MODELABLE_ENTITY_ID]
            else None
        ),
        covariate_id=(
            int(params[parameters.COVARIATE_ID])
            if params[parameters.COVARIATE_ID]
            else None
        ),
        gbd_round_id=params[parameters.GBD_ROUND_ID],
        decomp_step_id=gbd.decomp_step.decomp_step_id_from_decomp_step(
            params[parameters.DECOMP_STEP], params[parameters.GBD_ROUND_ID]
        ),
        model_type_id=helpers.determine_run_type(params).value,
        prediction_units=params[parameters.PREDICTION_UNITS],
        prediction_location_set_version_id=location_set_version_id,
        standard_location_set_version_id=standard_location_set_version_id,
        prediction_year_ids=helpers.join_or_none(year_ids),
        prediction_sex_ids=helpers.join_or_none(
            params[parameters.PREDICTION_SEX_IDS]),
        prediction_age_group_ids=helpers.join_or_none(
            params[parameters.PREDICTION_AGE_GROUP_IDS]),
        transform_type_id=lookup_tables.TransformType[
            params[parameters.DATA_TRANSFORM]
        ].value,
        transform_offset=params[parameters.TRANSFORM_OFFSET],
        is_custom_offset=params[parameters.IS_CUSTOM_OFFSET],
        st_version=params[parameters.ST_VERSION],
        add_nsv=int(params[parameters.ADD_NSV]),
        custom_stage_1=int(custom_stage_1_df is not None),
        rake_logit=int(params[parameters.RAKE_LOGIT]),
        agg_level_4_to_3=helpers.join_or_none(
            params[parameters.LEVEL_4_TO_3_AGGREGATE]),
        agg_level_5_to_4=helpers.join_or_none(
            params[parameters.LEVEL_5_TO_4_AGGREGATE]),
        agg_level_6_to_5=helpers.join_or_none(
            params[parameters.LEVEL_6_TO_5_AGGREGATE]),
        gpr_draws=params[parameters.GPR_DRAWS],
        holdouts=int(params[parameters.HOLDOUTS]),
        random_seed=random_seed,
        me_name=params[parameters.ME_NAME],
        notes=params[parameters.NOTES],
        user=getpass.getuser()
    )
    session.add(stgpr_version)
    session.flush()

    stgpr_version.create_model_iterations(params)
    stgpr_version.create_stage_1_param_set(params)
    session.flush()

    if custom_covariates_df is not None:
        stgpr_version.load_custom_covariate(session, custom_covariates_df)
        session.flush()

    if custom_stage_1_df is not None:
        stgpr_version.load_custom_stage_1_estimates(session, custom_stage_1_df)
        session.flush()

    return stgpr_version.stgpr_version_id
