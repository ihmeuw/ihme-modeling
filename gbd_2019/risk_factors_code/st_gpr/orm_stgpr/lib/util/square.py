import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import orm

from orm_stgpr.lib.constants import columns, demographics, parameters
from orm_stgpr.lib.util import helpers, old, query


def get_square(
        locations: List[int],
        years: List[int],
        ages: List[int],
        sexes: List[int]
) -> pd.DataFrame:
    """
    Makes the outline of the square using a cartesian product of demographics.
    Data is square iff it has data for every combination of location, year,
    age, and sex.

    Args:
        locations: list of prediction location IDs
        years: list of prediction year IDs
        ages: list of prediction age group IDs
        sexes: list of prediction sex IDs

    Returns:
        Square dataframe of location_id, year_id, age_group_id, and sex_id

    """
    logging.info(
        f'Making square from {len(locations)} locations, {len(years)} year(s),'
        f' {len(ages)} age group(s), and {len(sexes)} sex(es)'
    )
    square = pd.DataFrame(
        itertools.product(locations, years, ages, sexes),
        columns=columns.DEMOGRAPHICS,
        dtype=np.uint32
    )
    logging.info(f'Square has {len(square)} rows')
    return square


def merge_location_columns_onto_square(
        square_df: pd.DataFrame,
        location_hierarchy_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds level columns and standard location column onto square outline.

    Args:
        square_df: the square outline dataframe
        location_hierarchy_df: the location hierarchy dataframe

    Returns:
        Square dataframe with demographic columns plus level and standard
        location
    """
    level_columns = [c for c in location_hierarchy_df.columns if 'level' in c]
    location_df = location_hierarchy_df[
        [columns.LOCATION_ID] + level_columns + [columns.STANDARD_LOCATION]
    ]
    return square_df.join(
        location_df.set_index(columns.LOCATION_ID), on=columns.LOCATION_ID
    )


def merge_custom_inputs_onto_square(
        square_df: pd.DataFrame,
        custom_covariates_df: pd.DataFrame,
        custom_stage_1_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds custom covariates or custom stage 1 onto square.

    Args:
        square_df: the square dataframe
        custom_covariates_df: a possibly empty custom covariates dataframe
        custom_stage_1_df: a possibly empty custom stage 1 dataframe

    Returns:
        Square dataframe with custom covariates or custom stage 1 merged on

    Raises:
        ValueError: if custom covariates or custom stage 1 is not square
    """
    if custom_covariates_df is None and custom_stage_1_df is None:
        return square_df

    to_join = (
        custom_covariates_df
        if custom_covariates_df is not None
        else custom_stage_1_df
    )
    square_with_custom_df = square_df.merge(to_join, on=columns.DEMOGRAPHICS)
    if len(square_with_custom_df) != len(square_df):
        square_indices = square_df.set_index(columns.DEMOGRAPHICS).index
        merged_indices = square_with_custom_df.set_index(
            columns.DEMOGRAPHICS
        ).index
        missing_rows = square_df[~square_indices.isin(merged_indices)]
        sample_missing_row = missing_rows[columns.DEMOGRAPHICS].iloc[0]
        raise ValueError(
            'Custom inputs are not square: your custom inputs have '
            f'{len(to_join)} rows, and the square has {len(square_df)} rows. '
            'After merging custom inputs with the square, there are '
            f'{len(square_with_custom_df)} rows. An example of a row that is '
            'present in the square but is missing from your custom inputs is '
            f'{sample_missing_row.to_dict()}'
        )

    return square_with_custom_df


def add_gbd_covariates_to_square(
        params: Dict[str, Any],
        square_df: pd.DataFrame,
        year_ids: List[int],
        session: orm.Session
) -> pd.DataFrame:
    """
    Adds GBD covariates onto square.

    Args:
        square_df: the square dataframe
        gbd_covariates: list of covariate short names
        gbd_round_id: the GBD round ID for which to pull covariates
        decomp_step: the decomp step for which to pull covariates
        location_set_id: the location set for which to pull covariates
        year_ids: the years for which to pull covariates
        session: session on the epi server

    Returns:
        Square dataframe with GBD covariates merged on
    """
    gbd_covariates: List[str] = params[parameters.GBD_COVARIATES]
    gbd_round_id: int = params[parameters.GBD_ROUND_ID]
    decomp_step: Optional[str] = params[parameters.DECOMP_STEP]
    location_set_id: int = params[parameters.LOCATION_SET_ID]

    gbd_covariates_df = (
        query.get_gbd_covariate_ids(gbd_covariates, session)
        .pipe(lambda df:
              df[df[columns.COVARIATE_NAME_SHORT].isin(gbd_covariates)]))
    square_with_covariates_df = square_df.copy()
    for _, row in gbd_covariates_df.iterrows():
        # Unfortunately have to use iterrows here instead of joining
        # all of the covariates at once because some of the covariates
        # join on different keys.
        covariate_id = row[columns.COVARIATE_ID]
        covariate_name_short = row[columns.COVARIATE_NAME_SHORT]
        covariate_df = query.get_gbd_covariate_estimates(
            covariate_id,
            covariate_name_short,
            location_set_id,
            year_ids,
            gbd_round_id,
            decomp_step
        )
        square_with_covariates_df = _merge_gbd_covariates_onto_square(
            covariate_df, square_with_covariates_df, covariate_name_short
        )

    return square_with_covariates_df


def merge_data_onto_square(
        square_df: pd.DataFrame,
        data_df: pd.DataFrame,
        location_ids: List[int],
        year_ids: List[int],
        age_group_ids: List[int],
        sex_ids: List[int]
) -> pd.DataFrame:
    """
    Adds data onto square.
    First subsets data and marks outliers.

    Args:
        square_df: the square dataframe
        data_df: the data dataframe

    Returns:
        Square dataframe with data merged on
    """
    prepped_df = data_df\
        .pipe(lambda df: helpers.subset_data_by_demographics(
            df, location_ids, year_ids, age_group_ids, sex_ids))\
        .pipe(old.mark_outliers_for_viz)
    return square_df.merge(prepped_df, on=columns.DEMOGRAPHICS, how='left')


def _merge_gbd_covariates_onto_square(
        covariate_df: pd.DataFrame,
        square_df: pd.DataFrame,
        covariate_name_short: str
) -> pd.DataFrame:
    """
    Merges data for a single GBD covariate onto square.
    Raises ValueError if GBD covariate is not square.
    """
    merge_columns, drop_columns = _get_covariate_columns(
        covariate_df, square_df, covariate_name_short
    )
    combined_df = square_df.merge(
        covariate_df.drop(columns=drop_columns),
        on=merge_columns,
        how='left'
    )
    if combined_df[covariate_name_short].isnull().any():
        raise ValueError(
            f'Covariate estimates for {covariate_name_short} are not square, '
            'i.e. they are missing estimates for some combination of the '
            'location, year, age, sex specified in the config'
        )
    return combined_df


def _get_covariate_columns(
        covariate_df: pd.DataFrame,
        square_df: pd.DataFrame,
        covariate_name_short: str
) -> Tuple[List[str], List[str]]:
    """
    Gets the column names that should be used to merge GBD covariates onto
    square. There are four cases (for year as well, though age is shown here):
        1. The ages in the square and the ages in the covariate estimates
        are both age specific. Merging on age is fine because the age group
        IDs are present in the square and in the covariate estimates.
        2. The ages in the square and the ages in the covariate estimates
        are both all age. Merging on age is fine because the age group IDs
        are present in the square and in the covariate estimates.
        3. The ages in the square are age specific and the ages in the
        covariate estimates are all age. This is valid, but merging on age
        would break since there are different age group IDs in the square and
        in the covariate. Merging without age will apply the all-age estimates
        to each unique location-year-sex pair, which is what we want.
        4. The ages in the square are all age and the ages in the
        covariate estimates are age specific. This is not valid.

    Also returns the columns that should be dropped if the prediction
    demographics are more specific than the covariate demographics.
    """
    square_ages = set(square_df[columns.AGE_GROUP_ID].unique())
    square_sexes = set(square_df[columns.SEX_ID].unique())
    covariate_ages = set(covariate_df[columns.AGE_GROUP_ID].unique())
    covariate_sexes = set(covariate_df[columns.SEX_ID].unique())

    square_is_age_specific = not square_ages.isdisjoint(
        demographics.AGE_SPECIFIC
    )
    square_is_sex_specific = not square_sexes.isdisjoint(
        demographics.SEX_SPECIFIC
    )
    covariate_is_age_specific = not covariate_ages.isdisjoint(
        demographics.AGE_SPECIFIC
    )
    covariate_is_sex_specific = not covariate_sexes.isdisjoint(
        demographics.SEX_SPECIFIC
    )

    merge_columns = [columns.LOCATION_ID, columns.YEAR_ID]
    drop_columns = []
    if square_is_age_specific == covariate_is_age_specific:
        merge_columns.append(columns.AGE_GROUP_ID)
    elif square_is_age_specific and not covariate_is_age_specific:
        drop_columns.append(columns.AGE_GROUP_ID)
    elif not square_is_age_specific and covariate_is_age_specific:
        raise ValueError(
            f'Covariate {covariate_name_short} is age specific, but the '
            'prediction age group IDs in the config include all-age age '
            'group IDs'
        )

    if square_is_sex_specific == covariate_is_sex_specific:
        merge_columns.append(columns.SEX_ID)
    elif square_is_sex_specific and not covariate_is_sex_specific:
        drop_columns.append(columns.SEX_ID)
    elif not square_is_sex_specific and covariate_is_sex_specific:
        raise ValueError(
            f'Covariate {covariate_name_short} is sex specific, but the '
            'prediction sex IDs in the config include both-sex sex IDs'
        )

    return merge_columns, drop_columns
