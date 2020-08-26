import logging
from typing import List, Optional, Tuple
import warnings

import pandas as pd
from sqlalchemy import orm

import db_queries
from db_queries.core import location as loc_queries
from db_tools import query_tools
import gbd
from gbd.constants import decomp_step as ds

from orm_stgpr.lib.constants import (
    columns, demographics, modelable_entities, location, parameters, queries
)
from orm_stgpr.lib.util import helpers, step_control
from orm_stgpr.lib.validation import data_validation


def get_linked_cause_or_rei(
        modelable_entity_id: int,
        session: orm.Session
) -> Tuple[Optional[int], str]:
    """
    Returns a tuple of the id of interest (cause_id or rei_id) and
    a description string ('cause', 'rei') saying what kind of id it is.
    Id of interest is None is ME is a covariate.

    Note:
        MEs can only be linked to one of a rei, cause, or covariate.

    Returns:
        tuple, linked id (cause, rei or None if covariate) and a
        description string
    """
    linked_cause_id = query_tools.exec_query(
        queries.GET_LINKED_CAUSE,
        session,
        parameters={'modelable_entity_id': modelable_entity_id}
    ).scalar()
    if linked_cause_id:
        return linked_cause_id, modelable_entities.CAUSE

    linked_rei_id = query_tools.exec_query(
        queries.GET_LINKED_REI,
        session,
        parameters={'modelable_entity_id': modelable_entity_id}
    ).scalar()
    if linked_rei_id:
        return linked_rei_id, modelable_entities.REI

    # If neither a cause nor a REI is linked, then this is a covariate.
    return None, modelable_entities.COVARIATE


def modelable_entity_id_exists(me_id: int, session: orm.Session) -> bool:
    """
    Checks whether a modelable entity ID is present in the database.

    Args:
        modelable_entity_id: the modelable entity ID to check
        session: an epi database session

    Returns:
        Boolean indicating whether a modelable entity ID exists
    """
    modelable_entity_id_count = query_tools.exec_query(
        queries.GET_NUM_MODELABLE_ENTITY_ID,
        session,
        parameters={'modelable_entity_id': me_id}
    ).scalar()
    return modelable_entity_id_count == 1


def covariate_id_exists(covariate_id: int, session: orm.Session) -> bool:
    """
    Checks whether a covariate ID is present in the database.

    Args:
        covariate_id: the covariate ID to check
        session: an epi database session

    Returns:
        Boolean indicating whether a covariate ID exists
    """
    covariate_id_count = query_tools.exec_query(
        queries.GET_NUM_COVARIATE_ID,
        session,
        parameters={'covariate_id': covariate_id}
    ).scalar()
    return covariate_id_count == 1


def get_bundle_id_from_crosswalk(
        crosswalk_version_id: Optional[int],
        session: orm.Session
) -> Optional[int]:
    """
    Gets a bundle version ID from a crosswalk version ID.

    Args:
        crosswalk_version_id: the crosswalk version ID linked to a bundle ID
        session: an epi database session

    Returns:
        Bundle ID linked to the passed crosswalk version ID.

    Raises:
        ValueError: if crosswalk version ID does not exist or is linked to
            a bundle other than the bundle in the config
    """
    if not crosswalk_version_id:
        return None

    bundle_id_df = query_tools.query_2_df(
        queries.GET_BUNDLE_FROM_CROSSWALK,
        session=session,
        parameters={'crosswalk_version_id': crosswalk_version_id}
    )
    if bundle_id_df.empty:
        raise ValueError(
            'Could not retrieve a bundle associated with crosswalk version '
            f'{crosswalk_version_id}'
        )

    return bundle_id_df.at[0, columns.BUNDLE_ID]


def get_locations(
        location_set_id: int,
        gbd_round_id: int,
        decomp_step: str,
        session: orm.Session
) -> Tuple[pd.DataFrame, int, int, List[int]]:
    """
    Pulls location hierarchy, active location set version ID, and location IDs.

    Args:
        location_set_id: the location set ID with which to pull location data
        gbd_round_id: the GBD round ID for which to pull location data

    Returns:
        tuple of location hierarchy, active location set version ID, standard
        location set version ID, location IDs
    """
    logging.info('Pulling location hierarchy')

    # Get location set version IDs.
    prediction_location_set_version_id = _get_location_set_version_id(
        location_set_id, gbd_round_id, decomp_step,
        standard_locations=False, session=session
    )
    standard_location_set_version_id = _get_location_set_version_id(
        location.STANDARD_LOCATION_SET_ID, gbd_round_id, decomp_step,
        standard_locations=True, session=session
    )

    # Get location hierarchy.
    standard_locations = set()
    if standard_location_set_version_id:
        standard_locations = set(
            _get_location_hierarchy(
                standard_location_set_version_id,
                standard_locations=True
            )[columns.LOCATION_ID].unique()
        )
    location_hierarchy_df = _get_location_hierarchy(
        prediction_location_set_version_id,
        standard_locations=False)\
        .pipe(_add_levels_to_location_hierarchy)\
        .assign(standard_location=lambda df:
                df[columns.LOCATION_ID].isin(standard_locations))

    # Get list of locations at or above national level.
    location_ids = location_hierarchy_df[
        location_hierarchy_df.level >= location.NATIONAL_LEVEL
    ][columns.LOCATION_ID].tolist()
    if not location_ids:
        raise ValueError(
            f'Location set {location_set_id} version '
            f'{prediction_location_set_version_id} does not contain any '
            'locations at or above (more specific than) national level'
        )

    keep_cols = columns.LOCATION + \
        [col for col in location_hierarchy_df.columns if 'level' in col]

    return (
        location_hierarchy_df[keep_cols],
        prediction_location_set_version_id,
        standard_location_set_version_id,
        location_ids
    )


def get_gbd_covariate_ids(
        gbd_covariates: List[str],
        session: orm.Session
) -> pd.DataFrame:
    """
    Gets all GBD covariate IDs and covariate short names.

    Args:
        gbd_covariates: List of GBD covariate short names for which to
            pull covariate IDs
        session: session with the epi database server

    Returns:
        Dataframe containing GBD covariate IDs and covariate short names.

    Raises:
        ValueError: if a covariate short name was specified that does not
            exist in the database
    """
    covariate_df = query_tools.query_2_df(queries.GET_COVARIATE_IDS, session)
    covariates = set(covariate_df[columns.COVARIATE_NAME_SHORT].unique())
    missing_covariates = set(gbd_covariates) - set(covariates)
    if missing_covariates:
        raise ValueError(
            f'Invalid {parameters.GBD_COVARIATES}: '
            f'{", ".join(sorted(missing_covariates))}'
        )
    return covariate_df


def get_best_model_id(
        gbd_round_id: int,
        decomp_step: str,
        modelable_entity_id: Optional[int],
        session: orm.Session
) -> Optional[int]:
    """
    Gets the ID of the best ST-GPR model from the previous decomp step, if
    there were any ST-GPR models for the previous decomp step.

    Args:
        gbd_round_id: the GBD round for which this model is running
        decomp_step: the decomp step for which this model is running
        modelable_entity_id: the ME ID associated with this model
        session: a session with the epi database

    Returns:
        The ID of the best ST-GPR model from the previous decomp step

    Raises:
        ValueError: if no best model can be found
    """
    decomp_step_id = gbd.decomp_step.decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id
    )

    previous_decomp_step_id = get_previous_modeling_decomp_step_id(
        gbd_round_id, decomp_step_id, modelable_entity_id, session
    )

    num_previous_step_models = query_tools.exec_query(
        queries.GET_NUM_PREVIOUS_STEP_MODELS,
        session,
        parameters={
            'modelable_entity_id': modelable_entity_id,
            'decomp_step_id': previous_decomp_step_id
        }
    ).scalar()
    if gbd_round_id == 6 and not num_previous_step_models and \
            decomp_step == gbd.constants.decomp_step.FOUR:
        # If GBD 2019, step 4: if there are no models from step 3, check step 2.
        num_previous_step_models = query_tools.exec_query(
            queries.GET_NUM_PREVIOUS_STEP_MODELS,
            session,
            parameters={
                'modelable_entity_id': modelable_entity_id,
                'decomp_step_id':
                gbd.decomp_step.decomp_step_id_from_decomp_step(
                    gbd.constants.decomp_step.TWO, gbd_round_id
                )
            }
        ).scalar()
    if not num_previous_step_models:
        action = step_control.get_no_previous_model_action(
            gbd_round_id, decomp_step)
        previous_decomp_step = gbd.decomp_step.decomp_step_from_decomp_step_id(
            previous_decomp_step_id
        )
        if action == step_control.WARN:
            # A model doesn't need to be marked best for the previous step if
            # it uses new ME in the current step. These new MEs are considered
            # methods changes.
            # Ex: A new ME in GBD 2019 that started modeling in step 3 doesn't
            # need a best model from step 2
            warnings.warn(
                f'No ST-GPR models found for decomp step '
                f'\'{previous_decomp_step}\'. If you are using new MEs '
                f'(including new causes) in GBD round {gbd_round_id}, '
                f'decomp step \'{decomp_step}\', then this is okay. If not, '
                f'your ST-GPR model is invalid and will break down the line. '
                f'Please run \'{previous_decomp_step}\' modeling first and '
                f'try again.'
            )
            return None
        elif action == step_control.ERROR:
            raise ValueError(
                f'Cannot run ST-GPR for GBD round {gbd_round_id}, decomp step '
                f'\'{decomp_step}\' because there are no ST-GPR models for '
                f'the previous decomp step ({previous_decomp_step}, id '
                f'{previous_decomp_step_id}.'
            )

    logging.info(
        f'Found {num_previous_step_models} ST-GPR models for the previous '
        'decomp step'
    )
    best_model_id = query_tools.exec_query(
        queries.GET_BEST_MODEL,
        session,
        parameters={
            'modelable_entity_id': modelable_entity_id,
            'decomp_step_id': previous_decomp_step_id
        }
    ).scalar()
    if gbd_round_id == 6 and not best_model_id and \
            decomp_step == gbd.constants.decomp_step.FOUR:
        # If GBD 2019, step 4: check for a step 2 model marked best
        # if none is found for step 3.
        previous_decomp_step_id = \
            gbd.decomp_step.decomp_step_id_from_decomp_step(
                gbd.constants.decomp_step.TWO, gbd_round_id
            )
        best_model_id = query_tools.exec_query(
            queries.GET_BEST_MODEL,
            session,
            parameters={
                'modelable_entity_id': modelable_entity_id,
                'decomp_step_id': previous_decomp_step_id
            }
        ).scalar()

    previous_decomp_step = gbd.decomp_step.decomp_step_from_decomp_step_id(
        previous_decomp_step_id
    )
    if not best_model_id:
        raise ValueError(
            f'Cannot run ST-GPR for GBD round {gbd_round_id}, decomp step '
            f'\'{decomp_step}\': no best model found for the previous decomp '
            f'step ({previous_decomp_step}, step id {previous_decomp_step_id})'
        )

    logging.info(
        f'Found best model from GBD round {gbd_round_id}, decomp step '
        f'{previous_decomp_step}: {best_model_id}'
    )
    return best_model_id


def get_gbd_covariate_estimates(
        covariate_id: int,
        covariate_name_short: str,
        location_set_id: int,
        year_ids: List[int],
        gbd_round_id: int,
        decomp_step: str
) -> pd.DataFrame:
    """
    Pulls estimates for a single GBD covariate.

    Args:
        covariate_id: the covariate ID for which to pull estimates
        covariate_name_short: the short name of the covariate
        location_set_id: the location set ID for which to pull estimates
        year_ids: the year IDs for which to pull estimates
        gbd_round_id: the GBD round ID for which to pull estimates
        decomp_step: the decomp step for which to pull estimates

    Returns:
        Dataframe of demographic information and covariate estimates

    Raises:
        ValueError: if covariate does not have best values for a given
            GBD round and decomp step
    """
    logging.info(f'Pulling covariate {covariate_name_short}')
    covariate_df = (
        db_queries.get_covariate_estimates(
            covariate_id,
            location_set_id=location_set_id,
            year_id=year_ids,
            gbd_round_id=gbd_round_id,
            decomp_step=(
                None if gbd_round_id < 6
                else decomp_step
            )
        )
        .rename(columns={columns.MEAN_VALUE: covariate_name_short})
        [columns.DEMOGRAPHICS + [covariate_name_short]])

    if covariate_df.empty:
        raise ValueError(
            f'No best values for covariate {covariate_name_short} for '
            f'gbd round ID {gbd_round_id}, decomp step {decomp_step}'
        )

    return covariate_df


def get_population(
        gbd_round_id: int,
        decomp_step: str,
        location_set_id: int,
        year_ids: List[int],
        year_end: int,
        age_group_ids: List[int],
        sex_ids: List[int],
        square_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Pulls population estimates for given demographics.
    Requests forecasted population if modeler's prediction years go beyond the
    current cycle, and passes null decomp step for rounds prior to 2019.

    Args:
        gbd_round_id: the GBD round for which to pull population
        decomp_step: the decomp step for which to pull population
        location_set_id: the location set for which to pull population
        year_ids: year IDs for which to pull population
        year_end: the last year for which to pull population
        age_group_ids: age group IDs for which to pull population
        sex_ids: sex IDs for which to pull population
        square_df: square dataframe to use to validate that population
            contains required demographics

    Returns:
        DataFrame of population estimates. Has demographics columns and
        population column
    """
    logging.info('Pulling population')
    is_forecasting_model = (
        decomp_step == gbd.constants.decomp_step.ITERATIVE and
        year_end > demographics.FORECASTING_YEAR
    )
    population_df = db_queries.get_population(
        location_set_id=location_set_id,
        location_id='all',
        year_id=year_ids,
        age_group_id=age_group_ids,
        sex_id=sex_ids,
        gbd_round_id=gbd_round_id,
        decomp_step=(
            None if gbd_round_id < 6
            else decomp_step
        ),
        forecasted_pop=is_forecasting_model
    )[columns.DEMOGRAPHICS + [columns.POPULATION]]
    data_validation.validate_population_matches_data(
        population_df, square_df
    )
    return population_df


def get_previous_modeling_decomp_step_id(
        gbd_round_id: int,
        decomp_step_id,
        modelable_entity_id: Optional[int],
        session: orm.Session
) -> int:
    """
    Returns the previous decomp step id relevant for modeling purposes,
    handling both in-round and cross-round previous decomp step requests.

    If the previous step was in the previous round, the decomp step id
    returned depends on whether or not the ME's best model is expected
    to be in the final *modeling* step or in iterative.

    Args:
        gbd_round_id: the current GBD round
        decomp_step_id: the step id of the current decomp step
        modelable_entity_id: the ME ID of the ME
        session: an epi database session

    Returns:
        int, the decomp step id of the previous decomp step that
        is relevant to modeling.
    """
    previous_step_id = helpers.get_previous_decomp_step_id(
        decomp_step_id
    )

    if not previous_step_id in \
            step_control.FINAL_STEP_ID_TO_FINAL_MODELING_STEP:
        return previous_step_id

    # otherwise, we have to link back to the previous round's best,
    # checking with rules if the cause/rei was part of decomp in
    # the final modeling step. If not, check in iterative
    gbd_round_id -= 1
    step_to_check = step_control.FINAL_STEP_ID_TO_FINAL_MODELING_STEP[
        previous_step_id]
    id_of_interest, id_type = get_linked_cause_or_rei(
        modelable_entity_id, session)

    had_to_run_in_last_step = helpers.cause_or_rei_can_run_models(
        id_of_interest, id_type, gbd_round_id, step_to_check)

    # If the cause ran in the last modeling step, check there
    # Otherwise, she's in iterative
    if had_to_run_in_last_step:
        return gbd.decomp_step.decomp_step_id_from_decomp_step(
            step_to_check, gbd_round_id)
    else:
        return gbd.decomp_step.decomp_step_id_from_decomp_step(
            ds.ITERATIVE, gbd_round_id)


def _get_location_set_version_id(
        location_set_id: int,
        gbd_round_id: int,
        decomp_step: str,
        standard_locations: bool,
        session: orm.Session
):
    """
    Returns the active location set version id associated with a given
    gbd_round_id and decomp step.

    Returns:
        A location version set ID guaranteed to be active

    Raises:
        ValueError if no active location version set is found
    """
    decomp_step_id = gbd.decomp_step.decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id
    )

    location_set_version_id = query_tools.exec_query(
        queries.GET_ACTIVE_LOCATION_SET_VERSION,
        session,
        parameters={
            'location_set_id': location_set_id,
            'gbd_round_id': gbd_round_id,
            'decomp_step_id': (
                None if gbd_round_id < 6
                else decomp_step_id
            )
        }
    ).scalar()
    # Standard locations didn't exist until GBD round 6, and there must always
    # be an active location set version for the prediction location set.
    loc_set_must_be_active = gbd_round_id > 5 or not standard_locations
    if not location_set_version_id and loc_set_must_be_active:
        standard = 'standard ' if standard_locations else ''
        raise ValueError(
            f'There is no active location set version for {standard}location '
            f'set {location_set_id}, GBD round ID {gbd_round_id}, decomp step '
            f'\'{decomp_step}\''
        )

    return location_set_version_id


def _get_location_hierarchy(
        location_set_version_id: Optional[int],
        standard_locations: bool
) -> pd.DataFrame:
    """
    Pulls the location hierarchy (or just the location IDs if standard_locations
    is true) for given version of a location set ID.
    """
    location_hierarchy = loc_queries.view_location_hierarchy_history(
        location_set_version_id
    )
    if standard_locations:
        location_hierarchy = location_hierarchy[[columns.LOCATION_ID]]
    return location_hierarchy


def _add_levels_to_location_hierarchy(
        hierarchy_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds level_{i} columns to location hierarchy dataframe.
    """
    max_level = hierarchy_df.level.max()
    paths_to_parent = hierarchy_df.path_to_top_parent.apply(
        lambda col: pd.Series(col.split(','), dtype=float)
    ).astype(pd.Int32Dtype())
    levels = paths_to_parent.rename(
        columns=dict(zip(
            paths_to_parent.columns,
            [f'level_{level}' for level in range(max_level + 1)]
        ))
    )
    return pd.concat([hierarchy_df, levels], axis=1, sort=False)
