"""Helpers for GBD covariates."""

import logging
import warnings
from typing import List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import orm

import db_queries
import db_stgpr

from stgpr_helpers.lib.constants import columns, demographics
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.constants import parameters
from stgpr_helpers.lib.validation import data as data_validation


def get_gbd_covariate_info(
    gbd_covariates: List[str],
    gbd_covariate_model_version_ids: List[Union[str, int]],
    release_id: Optional[int],
    session: orm.Session,
) -> pd.DataFrame:
    """Gets all GBD covariate IDs, covariate short names, and best model versions.

    GBD covariate short names have already been checked for existence.

    Args:
        gbd_covariates: List of GBD covariate short names for which to pull covariate IDs.
            Already validated to not be empty.
        gbd_covariate_model_version_ids: List of covariate model version ids aligned with
            gbd_covariates and specified by the user or "best", indicating that the best
            version should be used. MVIDs are already validated to the exist for the GBD
            covariate in question. May be empty, in which case all GBD covariates should
            use best version.
        release_id: release id for the model
        session: active session with the epi database.

    Returns:
        Dataframe containing GBD covariate IDs, covariate short names, model version IDs,
        and bool for if the model version id was custom specified (1) or not (0)

    Raises:
        ValueError: If a covariate does not have a best model.
    """
    # Fill in 'best' for all covariates if not MVIDs given
    if not gbd_covariate_model_version_ids:
        gbd_covariate_model_version_ids = [parameters.BEST_MVID] * len(gbd_covariates)

    # Align covariate name and MVID and use mapping of name -> id to add covariate_id column
    covariate_df = pd.DataFrame(
        {
            columns.COVARIATE_NAME_SHORT: gbd_covariates,
            columns.COVARIATE_MODEL_VERSION_ID: gbd_covariate_model_version_ids,
            columns.CUSTOM_MVID_SPECIFIED: [
                int(mvid != parameters.BEST_MVID) for mvid in gbd_covariate_model_version_ids
            ],
        }
    )
    covariate_ids_by_name = db_stgpr.get_covariate_ids_by_name(gbd_covariates, session)
    covariate_df[columns.COVARIATE_ID] = covariate_df[columns.COVARIATE_NAME_SHORT].map(
        covariate_ids_by_name
    )

    # Get best model versions for covariates that do not have versions specified by user
    covariate_ids_of_defaults = covariate_df.loc[
        covariate_df[columns.COVARIATE_MODEL_VERSION_ID] == parameters.BEST_MVID,
        columns.COVARIATE_ID,
    ].tolist()

    if covariate_ids_of_defaults:
        best_model_versions = db_queries.get_best_model_versions(
            "covariate", covariate_ids_of_defaults, release_id=release_id
        ).rename(columns={"model_version_id": columns.COVARIATE_MODEL_VERSION_ID})

        # Update original df with MVIDs we just pulled
        covariate_df.update(best_model_versions.reset_index(drop=True))

    # Check if any default covariate did not have best MVIDs
    missing_covariates = covariate_df.loc[
        covariate_df[columns.COVARIATE_MODEL_VERSION_ID] == parameters.BEST_MVID,
        columns.COVARIATE_NAME_SHORT,
    ].tolist()
    if missing_covariates:
        raise exc.NoBestCovariateModelFound(
            f"Covariate(s) {missing_covariates} do not have best model versions for release "
            f"{release_id}"
        )
    return covariate_df[
        [
            columns.COVARIATE_ID,
            columns.COVARIATE_NAME_SHORT,
            columns.COVARIATE_MODEL_VERSION_ID,
            columns.CUSTOM_MVID_SPECIFIED,
        ]
    ]


def get_gbd_covariate_estimates(
    stgpr_version_id: int,
    square_df: pd.DataFrame,
    gbd_covariates: List[str],
    release_id: Optional[int],
    year_ids: List[int],
    location_set_id: int,
    session: orm.Session,
) -> Optional[pd.DataFrame]:
    """Pulls GBD covariates one at a time and merges them onto the square.

    Args:
        stgpr_version_id: the ST-GPR version id
        square_df: the square DataFrame.
        gbd_covariates: list of covariate short names.
        release_id: release id to pull covariates
        year_ids: IDs of the years for which to pull covariates.
        location_set_id: location set for which to pull covariates.
        session: active session with the ST-GPR database.

    Returns:
        Square dataframe with GBD covariates merged on.
    """
    if not gbd_covariates:
        return None

    square_with_covariates_df = square_df.copy()
    covariate_info = db_stgpr.get_gbd_covariates(stgpr_version_id, session)
    for row in covariate_info:
        logging.info(f"Pulling covariate id {row[columns.COVARIATE_ID]}")
        covariate_df: pd.DataFrame
        with warnings.catch_warnings():
            # get_covariate_estimates throws a warning when model_version_id is specified, so
            # filter out that warning.
            warnings.simplefilter("ignore", UserWarning)
            covariate_df = db_queries.get_covariate_estimates(
                covariate_id=row[columns.COVARIATE_ID],
                location_set_id=location_set_id,
                year_id=year_ids,
                release_id=release_id,
                model_version_id=row[columns.COVARIATE_MODEL_VERSION_ID],
            )
            covariate_name_short = covariate_df[columns.COVARIATE_NAME_SHORT].iat[0]
            covariate_df = covariate_df.rename(
                columns={columns.MEAN_VALUE: covariate_name_short}
            )[columns.DEMOGRAPHICS + [covariate_name_short]]

        square_with_covariates_df = _merge_gbd_covariates_onto_square(
            square_with_covariates_df, covariate_df, covariate_name_short
        )
    return square_with_covariates_df


def _merge_gbd_covariates_onto_square(
    square_df: pd.DataFrame, covariate_df: pd.DataFrame, covariate_name_short: str
) -> pd.DataFrame:
    """Merges data for a single GBD covariate onto the square."""
    merge_columns, drop_columns = _get_covariate_columns(
        square_df, covariate_df, covariate_name_short
    )
    data_validation.validate_squareness(
        square_df, covariate_df, entity="GBD covariates", cols=merge_columns
    )
    return square_df.merge(
        covariate_df.drop(columns=drop_columns), on=merge_columns, how="left"
    )


def _get_covariate_columns(
    square_df: pd.DataFrame, covariate_df: pd.DataFrame, covariate_name_short: str
) -> Tuple[List[str], List[str]]:
    """Gets the column names that should be used to merge GBD covariates onto the square.

    There are three cases (age is used as an example):
    1. The ages in the square and the ages in the covariate estimates are both age specific.
        Merging on age is fine because the age group IDs are present in the square and in the
        covariate estimates.
    2. The ages in the covariate estimates are all age. This is valid both when ages in the
        square are and are not age specific. Merging on age would break since there are
        different age group IDs in the square and in the covariate. Merging without age will
        apply the all-age estimates to each unique location-year-sex pair,
        which is what we want.
    3. The ages in the square are all age and the ages in the covariate estimates are age
        specific. This is not valid.

    Args:
        square_df: the square DataFrame.
        covariate_df: the DataFrame of GBD covariates.
        covariate_name_short: the name of the covariate being merged onto the square.

    Returns:
        Tuple of columns that should be used to merge GBD covariates onto the square and
        columns that should be dropped if the prediction demographics are more specific than
        the covariate demographics.

    Raises:
        ValueError: if a covariate is age- or sex-specific but the prediction ages/sexes are
            all-age/all-sex.
    """
    square_ages = set(square_df[columns.AGE_GROUP_ID].unique())
    square_sexes = set(square_df[columns.SEX_ID].unique())
    covariate_ages = set(covariate_df[columns.AGE_GROUP_ID].unique())
    covariate_sexes = set(covariate_df[columns.SEX_ID].unique())

    square_is_age_specific = not square_ages.isdisjoint(demographics.AGE_SPECIFIC)
    square_is_sex_specific = not square_sexes.isdisjoint(demographics.SEX_SPECIFIC)
    covariate_is_age_specific = not covariate_ages.isdisjoint(demographics.AGE_SPECIFIC)
    covariate_is_sex_specific = not covariate_sexes.isdisjoint(demographics.SEX_SPECIFIC)

    merge_columns = [columns.LOCATION_ID, columns.YEAR_ID]
    drop_columns = []
    if square_is_age_specific and covariate_is_age_specific:
        merge_columns.append(columns.AGE_GROUP_ID)
    elif not covariate_is_age_specific:
        drop_columns.append(columns.AGE_GROUP_ID)
    elif not square_is_age_specific and covariate_is_age_specific:
        raise ValueError(
            f"Covariate {covariate_name_short} is age specific, but the prediction age group "
            "IDs in the config include all-age age group IDs"
        )

    if square_is_sex_specific == covariate_is_sex_specific:
        merge_columns.append(columns.SEX_ID)
    elif square_is_sex_specific and not covariate_is_sex_specific:
        drop_columns.append(columns.SEX_ID)
    elif not square_is_sex_specific and covariate_is_sex_specific:
        raise ValueError(
            f"Covariate {covariate_name_short} is sex specific, but the prediction sex IDs "
            "in the config include both-sex sex IDs"
        )

    return merge_columns, drop_columns
