"""Helpers for fetching data, results, and statistics from the database.

Most selects follow the same pattern, so we define `get_results` as a generic function for
pulling estimates/statistics from the database.

Some selects have special logic and require their own functions:
    - Input data.
    - Custom covariates.
    - Stage 1 estimates.
    - Stage 1 statistics.
"""

from typing import List, Optional, Union

import pandas as pd
from sqlalchemy import orm

import db_stgpr
import stgpr_schema
from db_stgpr import tables
from db_stgpr.api.types import Filters

from stgpr_helpers.lib import general_utils, model_type_utils, transform_utils
from stgpr_helpers.lib.constants import columns
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.validation import data as data_validation
from stgpr_helpers.lib.validation import stgpr_version as stgpr_version_validation


def get_results(
    stgpr_version_id: int,
    model_iteration_id: Optional[Union[int, List[int]]],
    entity: str,
    filters: Filters,
    session: orm.Session,
    level_space: bool = True,
) -> pd.DataFrame:
    """Pulls estimates or statistics associated with an ST-GPR version."""
    table = {
        "Spacetime estimates": tables.ST_ESTIMATE,
        "GPR estimates": tables.GPR_ESTIMATE,
        "Final estimates": tables.FINAL_ESTIMATE,
        "Fit statistics": tables.FIT_STAT,
        "Amplitude/NSV": tables.AMP_NSV,
    }[entity]
    # Validate inputs.
    stgpr_version_validation.validate_stgpr_version_exists(stgpr_version_id, session)
    stgpr_version_validation.validate_stgpr_version_not_deleted(stgpr_version_id, session)
    data_validation.validate_demographic_filter_types(filters)

    # For selection models, if model_iteration_id is not given, attempt to pull best
    if not model_iteration_id and model_type_utils.is_selection_model(
        stgpr_version_id, session=session
    ):
        model_iteration_id = db_stgpr.get_best_model_iteration(stgpr_version_id, session)
        if not model_iteration_id:
            raise exc.NoModelIterationProvided(
                "Selection model requires either model iteration ID or must have a best "
                f"model iteration to pull {entity.lower()}"
            )

    # Get the results, leave model_iteration_id column if pulling for multiple iterations
    model_iteration_id = (
        model_iteration_id or db_stgpr.get_model_iterations(stgpr_version_id, session)[0]
    )
    results_df = db_stgpr.get_results(model_iteration_id, table, session, filters)
    if isinstance(model_iteration_id, int) or len(model_iteration_id) == 1:
        results_df = results_df.drop(columns=columns.MODEL_ITERATION_ID)

    # Transform as needed. Spacetime results are in modeling space, GP and raked results are
    # in level space.
    if level_space and entity == "Spacetime estimates":
        transform = db_stgpr.get_stgpr_version(
            stgpr_version_id, session, fields=[columns.TRANSFORM_TYPE], epi_best=False
        )[columns.TRANSFORM_TYPE]
        results_df[columns.VAL] = transform_utils.transform_data(
            results_df[columns.VAL], transform, reverse=True
        )
    elif not level_space and entity in ["GPR estimates", "Final estimates"]:
        transform = db_stgpr.get_stgpr_version(
            stgpr_version_id, session, fields=[columns.TRANSFORM_TYPE], epi_best=False
        )[columns.TRANSFORM_TYPE]
        results_df[[columns.VAL, columns.UPPER, columns.LOWER]] = (
            transform_utils.transform_data(
                results_df[[columns.VAL, columns.UPPER, columns.LOWER]], transform
            )
        )

    # Sort as needed.
    if entity not in ["Fit statistics", "Amplitude/NSV"]:
        return results_df.pipe(general_utils.sort_columns)
    return results_df


def get_data(
    stgpr_version_id: int,
    model_iteration_id: Optional[int],
    data_stage: str,
    filters: Filters,
    session: orm.Session,
) -> pd.DataFrame:
    """Gets data associated with an ST-GPR version."""
    stgpr_version_validation.validate_stgpr_version_exists(stgpr_version_id, session)
    stgpr_version_validation.validate_stgpr_version_not_deleted(stgpr_version_id, session)
    if model_iteration_id:
        stgpr_version_validation.validate_model_iteration_matches_stgpr_version(
            stgpr_version_id, model_iteration_id, session
        )
    else:
        model_iteration_id = db_stgpr.get_best_model_iteration(stgpr_version_id, session)
        if not model_iteration_id and data_stage == stgpr_schema.DataStage.with_nsv.name:
            raise exc.NoModelIterationProvided(
                "Either model iteration ID or a best model iteration is required to pull "
                "data with NSV."
            )

    data_validation.validate_demographic_filter_types(filters)

    data_stage_enum = (
        stgpr_schema.DataStage.prepped
        if data_stage == stgpr_schema.DataStage.with_nsv.name
        else stgpr_schema.DataStage[data_stage]
    )
    data_df = db_stgpr.get_data(
        stgpr_version_id, data_stage_enum, session, model_iteration_id, filters
    )
    if data_stage == stgpr_schema.DataStage.with_nsv.name:
        # Add NSV to variance before returning.
        # NSV is stored in level space, and NSV addition happens in level space. However,
        # variance is stored in modeling space, so first back-transform before adding NSV
        # to variance, then transform again before returning.
        transform = db_stgpr.get_stgpr_version(
            stgpr_version_id, session, fields=[columns.TRANSFORM_TYPE], epi_best=False
        )[columns.TRANSFORM_TYPE]
        data_df = _add_nsv_to_variance(data_df, transform)

    return data_df.drop(columns=[columns.NON_SAMPLING_VARIANCE])


def get_custom_covariates(
    stgpr_version_id: int, filters: Filters, session: orm.Session
) -> Optional[pd.DataFrame]:
    """Pulls custom covariate data associated with an ST-GPR version ID."""
    stgpr_version_validation.validate_stgpr_version_exists(stgpr_version_id, session)
    stgpr_version_validation.validate_stgpr_version_not_deleted(stgpr_version_id, session)
    data_validation.validate_demographic_filter_types(filters)

    covariate_df = db_stgpr.get_custom_covariate_data(stgpr_version_id, session, filters)
    if covariate_df.empty:
        return None

    covariate_df = covariate_df.drop(columns=columns.STGPR_VERSION_ID).pipe(
        lambda df: _look_up_covariate_columns(df, session)
    )
    return (
        pd.pivot_table(
            covariate_df,
            values=columns.VAL,
            index=columns.DEMOGRAPHICS,
            columns=columns.COVARIATE_LOOKUP_ID,
        )
        .reset_index()
        .pipe(general_utils.reset_column_names)
        .pipe(general_utils.sort_columns)
    )


def get_stage_1_estimates(
    stgpr_version_id: int, filters: Filters, session: orm.Session, level_space: bool
) -> pd.DataFrame:
    """Pulls stage 1 estimates associated with an ST-GPR version."""
    stgpr_version_validation.validate_stgpr_version_exists(stgpr_version_id, session)
    stgpr_version_validation.validate_stgpr_version_not_deleted(stgpr_version_id, session)
    data_validation.validate_demographic_filter_types(filters)
    stage_1_estimates = (
        db_stgpr.get_stage_1_estimates(stgpr_version_id, session, filters)
        .drop(columns=columns.MODEL_ITERATION_ID)
        .pipe(general_utils.sort_columns)
    )
    if level_space:
        transform = db_stgpr.get_stgpr_version(
            stgpr_version_id, session, fields=[columns.TRANSFORM_TYPE], epi_best=False
        )[columns.TRANSFORM_TYPE]
        stage_1_estimates[columns.VAL] = transform_utils.transform_data(
            stage_1_estimates[columns.VAL], transform, reverse=True
        )
    return stage_1_estimates


def _look_up_covariate_columns(df: pd.DataFrame, session: orm.Session) -> pd.DataFrame:
    """Looks up covariate column names from IDs."""
    lookup_ids = df[columns.COVARIATE_LOOKUP_ID].unique().tolist()
    lookup_mapper = db_stgpr.get_custom_covariate_columns(lookup_ids, session)
    prefixed_lookup_mapper = {
        lookup_id: f"{columns.CV_PREFIX}{lookup}"
        for lookup_id, lookup in lookup_mapper.items()
    }
    return df.assign(
        **{
            columns.COVARIATE_LOOKUP_ID: df[columns.COVARIATE_LOOKUP_ID].map(
                prefixed_lookup_mapper
            )
        }
    )


def _add_nsv_to_variance(df: pd.DataFrame, transform: str) -> pd.DataFrame:
    mean_level_space = transform_utils.transform_data(
        df[columns.VAL], transform, reverse=True
    )
    variance_level_space = transform_utils.transform_variance(
        df[columns.VAL], df[columns.VARIANCE], transform, reverse=True
    )
    variance_plus_nsv = variance_level_space + df[columns.NON_SAMPLING_VARIANCE]
    return df.assign(
        **{
            columns.VARIANCE: transform_utils.transform_variance(
                mean_level_space, variance_plus_nsv, transform
            )
        }
    )
