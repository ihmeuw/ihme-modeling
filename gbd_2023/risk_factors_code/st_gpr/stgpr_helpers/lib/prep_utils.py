"""Helpers for prepping data."""

import logging
from typing import List, Optional

import pandas as pd
from sqlalchemy import orm

import db_queries
import db_stgpr
import elmo
import gbd.estimation_years
import stgpr_schema
from gbd import constants as gbd_constants

from stgpr_helpers.legacy import files
from stgpr_helpers.lib import (
    covariate_utils,
    custom_input_utils,
    file_utils,
    holdout_utils,
    load_utils,
    location_utils,
    offset_utils,
    square_utils,
)
from stgpr_helpers.lib.constants import columns, demographics, parameters
from stgpr_helpers.lib.validation import data as data_validation


def prep(stgpr_version_id: int, session: orm.Session) -> None:  # noqa: C901
    """Preps data, covariates, and parameters for model run."""
    session.execute("SET SESSION wait_timeout = 600")
    file_utility = file_utils.StgprFileUtility(stgpr_version_id)
    file_utility.make_root_directory()

    params = file_utility.read_parameters()

    logging.info("Pulling prediction location hierarchy")
    location_hierarchy_df, location_ids = location_utils.get_locations(
        params[parameters.PREDICTION_LOCATION_SET_VERSION_ID],
        params[parameters.STANDARD_LOCATION_SET_VERSION_ID],
        session,
    )
    logging.info("Pulling aggregation location hierarchy")
    aggregation_location_hierarchy_df, aggregation_location_ids = (
        location_utils.get_locations(
            params[parameters.AGGREGATION_LOCATION_SET_VERSION_ID],
            params[parameters.STANDARD_LOCATION_SET_VERSION_ID],
            session,
        )
    )
    data_validation.validate_prediction_and_aggregation_locations(
        prediction_location_ids=location_ids,
        aggregation_location_ids=aggregation_location_ids,
    )

    params[parameters.LOCATION_IDS] = location_ids
    params[parameters.AGE_GROUP_IDS] = params[parameters.PREDICTION_AGE_GROUP_IDS]
    params[parameters.SEX_IDS] = params[parameters.PREDICTION_SEX_IDS]
    params[parameters.YEAR_IDS] = params[parameters.PREDICTION_YEAR_IDS]
    logging.info(
        f"Making square from {len(location_ids)} locations, "
        f"{len(params[parameters.YEAR_IDS])} year(s), "
        f"{len(params[parameters.AGE_GROUP_IDS])} age group(s), and "
        f"{len(params[parameters.SEX_IDS])} sex(es)"
    )
    square_df = square_utils.get_square(
        params[parameters.YEAR_IDS],
        params[parameters.LOCATION_IDS],
        params[parameters.SEX_IDS],
        params[parameters.AGE_GROUP_IDS],
    )
    logging.info(f"Square has {len(square_df)} rows")

    logging.info("Pulling data")
    data_df = _get_data(
        params[parameters.CROSSWALK_VERSION_ID],
        params[parameters.PATH_TO_DATA],
        location_hierarchy_df,
        location_ids,
        params[parameters.YEAR_IDS],
        params[parameters.AGE_GROUP_IDS],
        params[parameters.SEX_IDS],
    )

    logging.info("Calculating offset")
    params[parameters.TRANSFORM_OFFSET] = offset_utils.calculate_offset(
        data_df.loc[
            data_df[columns.IS_OUTLIER] == stgpr_schema.Outlier.IS_NOT_OUTLIER.value,
            columns.VAL,
        ],
        params[parameters.DATA_TRANSFORM],
        params[parameters.TRANSFORM_OFFSET],
    )

    logging.info("Offsetting and transforming")
    prepped_data_df = offset_utils.offset_and_transform_data(
        data_df, params[parameters.TRANSFORM_OFFSET], params[parameters.DATA_TRANSFORM]
    )

    logging.info("Pulling custom inputs")
    custom_stage_1_df = custom_input_utils.read_custom_stage_1(
        params[parameters.PATH_TO_CUSTOM_STAGE_1]
    )
    if custom_stage_1_df is not None:
        data_validation.validate_squareness(
            square_df, custom_stage_1_df, entity="custom stage 1 estimates"
        )
    custom_covariates_df = custom_input_utils.read_custom_covariates(
        params[parameters.PATH_TO_CUSTOM_COVARIATES]
    )
    if custom_covariates_df is not None:
        data_validation.validate_squareness(
            square_df, custom_covariates_df, entity="custom covariate estimates"
        )
        # drop any rows for extra demographics that were included, already validated for
        # squareness
        custom_covariates_df = custom_covariates_df.merge(
            square_df, on=columns.DEMOGRAPHICS, how="inner"
        )

    logging.info("Pulling GBD covariates")
    gbd_covariates_df = covariate_utils.get_gbd_covariate_estimates(
        stgpr_version_id,
        square_df,
        params[parameters.GBD_COVARIATES],
        params[parameters.RELEASE_ID],
        params[parameters.YEAR_IDS],
        params[parameters.LOCATION_SET_ID],
        session,
    )

    logging.info("Making holdouts")
    holdout_df = holdout_utils.make_holdouts(
        square_df,
        data_df[data_df[columns.IS_OUTLIER] == stgpr_schema.Outlier.IS_NOT_OUTLIER.value],
        location_hierarchy_df,
        params[parameters.HOLDOUTS],
        params[parameters.RANDOM_SEED],
        params[parameters.RELEASE_ID] == gbd_constants.release.USRE,
    )

    logging.info("Saving measure and offset to database")
    db_stgpr.update_stgpr_version(
        stgpr_version_id,
        {
            columns.MEASURE_ID: int(data_df[columns.MEASURE_ID].iat[0]),  # type: ignore
            parameters.TRANSFORM_OFFSET: params[parameters.TRANSFORM_OFFSET],
        },
        session,
    )

    if custom_covariates_df is not None:
        logging.info("Saving custom covariates to database")
        load_utils.load_custom_covariates(stgpr_version_id, custom_covariates_df, session)

    logging.info("Saving original data to database")
    load_utils.load_data(stgpr_version_id, stgpr_schema.DataStage.original, data_df, session)

    logging.info("Saving prepped data to database")
    load_utils.load_data(
        stgpr_version_id, stgpr_schema.DataStage.prepped, prepped_data_df, session
    )

    logging.info("Pulling population")
    last_estimation_year = gbd.estimation_years.estimation_years_from_release_id(
        params[parameters.RELEASE_ID]
    )[-1]
    is_forecasting_model = last_estimation_year < params[parameters.YEAR_IDS][-1] and params[
        parameters.RELEASE_ID
    ] not in [
        gbd_constants.release.VACCINE_COVERAGE_2021,
        gbd_constants.release.VACCINE_COVERAGE_2023,
    ]
    population_group_id = (
        gbd_constants.population_group.PREGNANT
        if params[parameters.IS_PREGNANCY_ME]
        else None
    )
    population_df = db_queries.get_population(
        location_set_id=params[parameters.LOCATION_SET_ID],
        location_id="all",
        year_id=params[parameters.YEAR_IDS],
        age_group_id=params[parameters.AGE_GROUP_IDS],
        sex_id=params[parameters.SEX_IDS],
        release_id=params[parameters.RELEASE_ID],
        forecasted_pop=is_forecasting_model,
        population_group_id=population_group_id,
    )[columns.DEMOGRAPHICS + [columns.POPULATION, columns.RUN_ID]]
    data_validation.validate_population_matches_data(population_df, square_df)
    logging.info("Uploading population run ID")
    db_stgpr.update_stgpr_version(
        stgpr_version_id,
        {
            columns.POPULATION_RUN_ID: int(
                population_df[columns.RUN_ID].iat[0]
            )  # type: ignore
        },
        session,
    )
    population_df.drop(columns=[columns.RUN_ID], inplace=True)

    logging.info("Caching model inputs")
    covariates_df: Optional[pd.DataFrame] = None
    if gbd_covariates_df is not None and custom_covariates_df is not None:
        covariates_df = gbd_covariates_df.merge(custom_covariates_df, on=columns.DEMOGRAPHICS)
    elif gbd_covariates_df is not None and custom_covariates_df is None:
        covariates_df = gbd_covariates_df
    elif custom_covariates_df is not None and gbd_covariates_df is None:
        covariates_df = custom_covariates_df
    files.save_to_files(
        stgpr_version_id,
        data_df,
        prepped_data_df,
        covariates_df,
        custom_stage_1_df,
        square_df,
        location_hierarchy_df,
        aggregation_location_hierarchy_df,
        holdout_df,
        params,
        population_df,
    )
    file_utility.cache_metadata(params)
    file_utility.cache_prepped_data(prepped_data_df.reset_index(drop=True))
    file_utility.cache_location_hierarchy(location_hierarchy_df)
    file_utility.cache_aggregation_location_hierarchy(aggregation_location_hierarchy_df)
    file_utility.cache_population(population_df)
    file_utility.cache_model_storage_metadata(params[parameters.GPR_DRAWS])
    if holdout_df is not None:
        file_utility.cache_holdouts(holdout_df)
    if custom_stage_1_df is not None:
        file_utility.cache_custom_stage_1(custom_stage_1_df)
    if covariates_df is not None:
        file_utility.cache_covariates(covariates_df)


def _get_data(
    crosswalk_version_id: Optional[int],
    path_to_data: Optional[str],
    location_hierarchy_df: pd.DataFrame,
    location_ids: List[int],
    year_ids: List[int],
    age_group_ids: List[int],
    sex_ids: List[int],
) -> pd.DataFrame:
    """Pulls data from crosswalk version or CSV and subsets to model demographics."""
    data_validation.validate_path_to_data_location(path_to_data)
    data_df = (
        elmo.get_crosswalk_version(crosswalk_version_id)
        .assign(
            **{
                columns.SEX_ID: lambda df: df[columns.SEX]
                .str.lower()
                .map(demographics.SEX_MAP),
                columns.MEASURE_ID: lambda df: df[columns.MEASURE]
                .str.lower()
                .map(demographics.MEASURE_MAP),
            }
        )
        .drop(columns=[columns.SEX, columns.MEASURE])
        if crosswalk_version_id
        else pd.read_csv(path_to_data)
    )

    # Validate that required columns are present and don't have NA/infinity.
    data_cols = [col for col in columns.CROSSWALK_DATA if col != columns.SEQ]
    data_df = data_df.pipe(
        lambda df: data_validation.validate_columns_exist(df, "Data", set(data_cols))
    ).pipe(
        lambda df: data_validation.validate_no_nan_infinity(
            df, "input data", [col for col in data_cols if col != columns.SAMPLE_SIZE]
        )
    )

    # Validate seq if present. Assign seq if missing.
    if columns.SEQ in data_df:
        data_validation.validate_no_duplicates(data_df, "input data", [columns.SEQ])
        data_validation.validate_no_nan_infinity(data_df, "input_data", [columns.SEQ])
    else:
        data_df[columns.SEQ] = range(len(data_df))

    # Validate demographics and subset to model demographics.
    data_validation.validate_data_demographics(
        data_df, location_ids, year_ids, age_group_ids, sex_ids
    )
    subset_df = data_df[columns.CROSSWALK_DATA][
        (data_df[columns.LOCATION_ID].isin(location_ids))
        & (data_df[columns.YEAR_ID].isin(year_ids))
        & (data_df[columns.AGE_GROUP_ID].isin(age_group_ids))
        & (data_df[columns.SEX_ID].isin(sex_ids))
    ]
    if subset_df.empty:
        raise ValueError(
            "Dataset is empty after subsetting to modeling demographics (as specified by the "
            f"parameters {parameters.LOCATION_SET_ID}, "
            f"{parameters.PREDICTION_AGE_GROUP_IDS}, {parameters.PREDICTION_YEAR_IDS}, and "
            f"{parameters.PREDICTION_SEX_IDS})"
        )
    logging.info(
        f"Dropped {len(data_df) - len(subset_df)} rows subsetting to modeling demographics. "
        f"There are {len(subset_df)} remaining rows of data"
    )

    # After subsetting, confirm we have at least the minimum number of data points
    data_validation.validate_minimum_required_data_points(subset_df, location_hierarchy_df)

    return subset_df
