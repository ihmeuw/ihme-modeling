"""Helpers for loading data into a table."""
import os
from typing import List, Optional

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import orm

import db_stgpr
from db_stgpr import tables
from db_stgpr.api.enums import DataStage

from stgpr_helpers.lib.constants import columns, paths
from stgpr_helpers.lib.validation import data as data_validation
from stgpr_helpers.lib.validation import stgpr_version as stgpr_version_validation

# Infile in batches of 500,000 rows.
_CHUNK_SIZE = 500000


def load_data(
    stgpr_version_id: int,
    data_stage: DataStage,
    data_df: pd.DataFrame,
    session: orm.Session,
    output_root: Optional[str] = None,
) -> None:
    """Loads input data into the database.

    Also assigns lower and upper if not present. Lower and upper cannot be calculated for
    prepped data.

    Args:
        stgpr_version_id: ID of the ST-GPR version.
        data_stage: Stage of data to upload.
        data_df: DataFrame of data to upload.
        session: Active session with the epi database.
        output_root: Path to this model's output directory.
    """
    output_root = str(output_root or db_stgpr.get_output_path(stgpr_version_id, session))
    to_upload_df = data_df.assign(
        **{
            columns.STGPR_VERSION_ID: stgpr_version_id,
            columns.DATA_STAGE_ID: data_stage.value,
        }
    )
    if data_stage == DataStage.prepped:
        to_upload_df[columns.LOWER] = None
        to_upload_df[columns.UPPER] = None
    else:
        if columns.LOWER not in to_upload_df:
            to_upload_df[columns.LOWER] = to_upload_df[columns.VAL] - 1.96 * np.sqrt(
                to_upload_df[columns.VARIANCE]
            )
        if columns.UPPER not in to_upload_df:
            to_upload_df[columns.UPPER] = to_upload_df[columns.VAL] + 1.96 * np.sqrt(
                to_upload_df[columns.VARIANCE]
            )
    _infile(to_upload_df[columns.UPLOAD_DATA], tables.INPUT_DATA, output_root, session)


def load_custom_covariates(
    stgpr_version_id: int,
    custom_covariates_df: pd.DataFrame,
    output_root: str,
    session: orm.Session,
) -> None:
    """Loads custom covariates into the database.

    First adds covariates to covariate lookup.

    Args:
        stgpr_version_id: ID of the ST-GPR version with custom covariates.
        custom_covariates_df: DataFrame with demographic columns and `val`.
        output_root: Path to this model's output directory.
        session: Active session with the epi database.
    """
    covariate_names = [
        col for col in custom_covariates_df.columns if col.startswith(columns.CV_PREFIX)
    ]
    without_prefixes = [name[len(columns.CV_PREFIX) :] for name in covariate_names]
    covariate_lookup_ids = db_stgpr.insert_covariate_lookups(without_prefixes, session)
    rename_mapper = {
        name: lookup_id for name, lookup_id in zip(covariate_names, covariate_lookup_ids)
    }
    with_lookups_df = custom_covariates_df.rename(rename_mapper, axis=1)

    to_upload_df = pd.melt(
        with_lookups_df,
        id_vars=columns.DEMOGRAPHICS,
        var_name=columns.COVARIATE_LOOKUP_ID,
        value_name=columns.COVARIATE_VALUE,
    ).assign(**{columns.STGPR_VERSION_ID: stgpr_version_id})[
        [
            columns.STGPR_VERSION_ID,
            columns.COVARIATE_LOOKUP_ID,
            *columns.DEMOGRAPHICS,
            columns.COVARIATE_VALUE,
        ]
    ]
    _infile(to_upload_df, tables.CUSTOM_COVARIATE, output_root, session)


def load_stage_1_estimates(
    stgpr_version_id: int,
    stage_1_df: pd.DataFrame,
    session: orm.Session,
    output_root: Optional[str] = None,
) -> None:
    """Loads stage 1 estimates into the database."""
    entity = "Stage 1 estimates"
    data_validation.validate_columns_exist(stage_1_df, entity, set(columns.STAGE_1_ESTIMATE))
    data_validation.validate_no_nan_infinity(stage_1_df, entity, columns.STAGE_1_ESTIMATE)
    output_root = str(output_root or db_stgpr.get_output_path(stgpr_version_id, session))
    model_iteration_ids = db_stgpr.get_model_iterations(stgpr_version_id, session)
    for model_iteration_id in model_iteration_ids:
        to_upload_df = stage_1_df.assign(**{columns.MODEL_ITERATION_ID: model_iteration_id})[
            [columns.MODEL_ITERATION_ID, *columns.DEMOGRAPHICS, columns.VAL]
        ]
        _infile(to_upload_df, tables.STAGE_1_ESTIMATE, output_root, session)


def load_stage_1_statistics(
    stgpr_version_id: int, stage_1_stats_df: pd.DataFrame, session: orm.Session
) -> None:
    """Loads stage 1 statistics into the database."""
    entity = "Stage 1 statistics"
    data_validation.validate_columns_exist(
        stage_1_stats_df, entity, set(columns.STAGE_1_STATISTICS)
    )
    data_validation.validate_no_nan_infinity(
        stage_1_stats_df,
        entity,
        [col for col in columns.STAGE_1_STATISTICS if col != columns.FACTOR],
    )
    to_upload_df = stage_1_stats_df.assign(**{columns.STGPR_VERSION_ID: stgpr_version_id})[
        [columns.STGPR_VERSION_ID, *columns.STAGE_1_STATISTICS]
    ]
    db_stgpr.insert_stage_1_statistics(to_upload_df, session)


def load_estimates_or_statistics(
    stgpr_version_id: int,
    estimates_df: pd.DataFrame,
    model_iteration_id: Optional[int],
    entity: str,
    session: orm.Session,
    output_root: Optional[str] = None,
) -> None:
    """Loads spacetime, GPR, or final estimates or fit statistics."""
    table, cols = {
        "Spacetime estimates": (tables.ST_ESTIMATE, columns.SPACETIME_ESTIMATE),
        "GPR estimates": (tables.GPR_ESTIMATE, columns.GPR_ESTIMATE),
        "Final estimates": (tables.FINAL_ESTIMATE, columns.FINAL_ESTIMATE),
        "Fit statistics": (tables.FIT_STAT, columns.FIT_STATISTICS),
        "Amplitude/NSV": (tables.AMP_NSV, columns.AMP_NSV_UPLOAD),
    }[entity]
    if model_iteration_id:
        stgpr_version_validation.validate_model_iteration_matches_stgpr_version(
            stgpr_version_id, model_iteration_id, session
        )
    else:
        model_iteration_id = stgpr_version_validation.validate_only_one_model_iteration(
            stgpr_version_id, session
        )
    data_validation.validate_columns_exist(estimates_df, entity, set(cols))
    data_validation.validate_no_nan_infinity(
        estimates_df,
        entity,
        [
            col
            for col in cols
            if col not in [columns.OUT_OF_SAMPLE_RMSE, columns.NON_SAMPLING_VARIANCE]
        ],
    )
    output_root = str(output_root or db_stgpr.get_output_path(stgpr_version_id, session))

    to_upload_df = estimates_df.assign(**{columns.MODEL_ITERATION_ID: model_iteration_id})[
        [columns.MODEL_ITERATION_ID, *cols]
    ]
    _infile(to_upload_df, table, output_root, session)


def load_amplitude_nsv(
    stgpr_version_id: int,
    amplitude_df: pd.DataFrame,
    nsv_df: Optional[pd.DataFrame],
    model_iteration_id: Optional[int],
    session: orm.Session,
    output_root: Optional[str] = None,
) -> None:
    """Loads amplitude and NSV."""
    amp_cols = [col for col in columns.AMP_NSV_UPLOAD if col != columns.NON_SAMPLING_VARIANCE]
    data_validation.validate_columns_exist(amplitude_df, "Amplitude", set(amp_cols))
    data_validation.validate_no_nan_infinity(amplitude_df, "Amplitude", amp_cols)
    if nsv_df is None:
        load_estimates_or_statistics(
            stgpr_version_id,
            amplitude_df.assign(**{columns.NON_SAMPLING_VARIANCE: None}),
            model_iteration_id,
            "Amplitude/NSV",
            session,
            output_root,
        )
        return

    nsv_cols = [col for col in columns.AMP_NSV_UPLOAD if col != columns.AMPLITUDE]
    data_validation.validate_columns_exist(nsv_df, "NSV", set(nsv_cols))
    data_validation.validate_no_nan_infinity(
        nsv_df, "NSV", [columns.LOCATION_ID, columns.SEX_ID]
    )
    combined_df = amplitude_df.merge(
        nsv_df, how="inner", on=[columns.LOCATION_ID, columns.SEX_ID]
    )
    if len(combined_df) != len(amplitude_df) or len(combined_df) != len(nsv_df):
        raise RuntimeError(
            f"After merging with NSV, DataFrame has {len(combined_df)} rows. However, NSV "
            f"DataFrame has {len(nsv_df)} rows and amplitude DataFrame has "
            f"{len(amplitude_df)} rows"
        )
    load_estimates_or_statistics(
        stgpr_version_id,
        combined_df,
        model_iteration_id,
        "Amplitude/NSV",
        session,
        output_root,
    )


def _infile(
    df: pd.DataFrame, table: sqlalchemy.Table, output_root: str, session: orm.Session
) -> None:
    """Checks primary key columnns for uniqueness then loads data into a table.

    Loads data in batches of 500,000.

    NaNs/Nones are replaced with null character so that infiling handles nulls correctly.
    """
    num_chunks = (len(df) // _CHUNK_SIZE) + 1
    chunks: List[pd.DataFrame] = np.array_split(df, num_chunks)
    for chunk in chunks:
        primary_key_cols = [col.name for col in table.primary_key.columns]
        to_upload_df = chunk.where(pd.notnull(chunk), r"\N")
        if table.name != tables.INPUT_DATA:
            # Data can have duplicates.
            data_validation.validate_no_duplicates(
                chunk,
                data_type=table.name,
                columns_to_check=primary_key_cols,
            )
            # Data can't be sorted by primary key columns.
            to_upload_df.sort_values(primary_key_cols, inplace=True)

        stgpr_paths = paths.StgprPaths(output_root)
        infile_path = stgpr_paths.INFILE_TMP
        to_upload_df.to_csv(infile_path, index=False)
        db_stgpr.infile(table, infile_path, [col for col in chunk], session)
        os.remove(infile_path)
