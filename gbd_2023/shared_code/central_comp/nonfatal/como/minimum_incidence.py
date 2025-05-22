import pathlib
from typing import List, Union

import numpy as np
import pandas as pd

from db_queries import get_outputs
from gbd import constants as gbd_constants

from como.lib import constants as como_constants

_DEATHS_VALUE_COL = "val"
_UNSCALABLE_MEAN = "unscalable_mean"


def minimum_incidence_from_codcorrect(
    cause_id: int,
    codcorrect_process_version_id: int,
    release_id: int,
    case_fatality_df: pd.DataFrame,
    year_id: Union[str, int, List[int]] = "all",
    location_id: Union[str, int, List[int]] = "most_detailed",
) -> pd.DataFrame:
    """Minimum incidence based on codcorrect deaths."""
    deaths_df = get_outputs(
        topic="cause",
        cause_id=cause_id,
        metric_id=gbd_constants.metrics.RATE,
        measure_id=gbd_constants.measures.DEATH,
        process_version_id=codcorrect_process_version_id,
        sex_id=[gbd_constants.sex.MALE, gbd_constants.sex.FEMALE],
        age_group_id="most_detailed",
        year_id=year_id,
        location_id=location_id,
        drop_restrictions=True,
        release_id=release_id,
    )

    # add a potentially age-specific case fatality rate column
    cfr_per_cause = deaths_df.copy().drop(columns=_DEATHS_VALUE_COL)
    case_fatality_age_specific = case_fatality_df.query(
        f"cause_id == {cause_id} & age_group_id != -1"
    )
    cfr_columns = ["cause_id", "age_group_id", como_constants.CASE_FATALITY_COL]
    cfr_per_cause = cfr_per_cause.merge(
        case_fatality_age_specific[cfr_columns], how="left", on=["cause_id", "age_group_id"]
    )
    if cfr_per_cause[como_constants.CASE_FATALITY_COL].isna().any():
        default_cfr = case_fatality_df.query(f"cause_id == {cause_id} & age_group_id == -1")[
            como_constants.CASE_FATALITY_COL
        ].tolist()[0]
        cfr_per_cause[como_constants.CASE_FATALITY_COL] = cfr_per_cause[
            como_constants.CASE_FATALITY_COL
        ].fillna(value=default_cfr)

    # perform calculation of TMI
    death_index_cols = [i for i in deaths_df.columns if i != _DEATHS_VALUE_COL]
    incidence = deaths_df.merge(cfr_per_cause, on=death_index_cols)
    incidence[gbd_constants.columns.MEASURE_ID] = gbd_constants.measures.INCIDENCE
    incidence[como_constants.TMI_COL] = (
        incidence[_DEATHS_VALUE_COL] / incidence[como_constants.CASE_FATALITY_COL]
    )
    save_cols = [
        gbd_constants.columns.AGE_GROUP_ID,
        gbd_constants.columns.CAUSE_ID,
        gbd_constants.columns.LOCATION_ID,
        gbd_constants.columns.MEASURE_ID,
        gbd_constants.columns.METRIC_ID,
        gbd_constants.columns.SEX_ID,
        gbd_constants.columns.YEAR_ID,
        como_constants.TMI_COL,
    ]
    incidence = incidence[save_cols]

    return incidence


def _compute_tmi_scalar(
    tmi_df: pd.DataFrame, incidence_df: pd.DataFrame, index_cols: List[str]
) -> pd.DataFrame:
    """Generates TMI scalars by dividing the TMI value by incidence mean.

    Also sets a floor of 1 on the scalar values, since we only want to scale up.
    """
    incidence_df = incidence_df.merge(tmi_df, how="left", on=index_cols)
    # If TMI has missing values, set missing TMI equal to incidence mean
    incidence_df[como_constants.TMI_COL] = incidence_df[como_constants.TMI_COL].fillna(
        incidence_df[gbd_constants.columns.MEAN]
    )
    # Generate TMI scalar by dividing TMI by COMO incidence
    incidence_df[como_constants.TMI_SCALAR_COL] = (
        incidence_df[como_constants.TMI_COL] / incidence_df[gbd_constants.columns.MEAN]
    )
    # Where TMI is equal to 0, set the TMI scalar equal to 1
    incidence_df.loc[
        incidence_df[como_constants.TMI_COL] == 0, como_constants.TMI_SCALAR_COL
    ] = 1
    # Scalars have a floor of 1 since we don't want to reduce any values
    incidence_df.loc[
        incidence_df[como_constants.TMI_SCALAR_COL] < 1, como_constants.TMI_SCALAR_COL
    ] = 1
    # Infinite TMI scalars should be replaced with 0s (for now)
    # NOTE: This only impacts causes 387 (Protein energy malnutrition) and 401 (Hep A)
    incidence_df.loc[
        incidence_df[como_constants.TMI_SCALAR_COL] == np.inf, como_constants.TMI_SCALAR_COL
    ] = 0
    if incidence_df[como_constants.TMI_SCALAR_COL].isna().any():
        raise RuntimeError("Null TMI scalars, something is wrong.")
    return incidence_df[index_cols + [como_constants.TMI_SCALAR_COL]]


def get_scalars(
    tmi_df: pd.DataFrame,
    scalable_seq_df: pd.DataFrame,
    static_seq_df: pd.DataFrame,
    index_cols: List[str],
    draw_cols: List[str],
) -> pd.DataFrame:
    """Generates TMI scalars.

    Given a dataframe with Theoretical Minimum Incidence (TMI) values, a dataframe with
    scalable sequela values, and a dataframe with sequela values that should be held static,
    generate scalars for scaling incidence at the sequela level so that incidence at the
    cause level is at least TMI.

    The static sequela values must be aggregated up to the cause level and subtracted from
    the TMI values prior to scalar generation, so that we don't take into account unscalable
    sequela results when determining how much to scale by. After removing static sequela
    results from TMI, the TMI scalar is calculated as cause-level TMI divided by the mean
    scalable sequela results aggregated to the cause level. The TMI scalar is floored at 1,
    since we don't want to shrink any results.

    Arguments:
        tmi_df (pd.DataFrame): Dataframe containing TMI values by cause and standard GBD
            demographics.
        scalable_seq_df (pd.DataFrame): Dataframe containing sequela-level results that can
            be scaled (aren't static) to bring cause-level incidence results up to TMI.
        static_seq_df (pd.DataFrame): Dataframe containing sequela-level results that cannot
            be scaled (are static).
        index_cols (strlist): List of index columns used for merge operations and TMI scalar
            calculation.
        draw_cols (strlist): List of draw columns used to calculate means.

    Raises:
        RuntimeError: If any mean level 0s exist in the scalable cause values.

    Returns:
        A dataframe containing scalars, unique on the passed index columns, for scaling
            incidence up to TMI where necessary.
    """

    def _aggregate_to_cause_mean(df: pd.DataFrame) -> pd.DataFrame:
        agg_df = df.groupby(index_cols).sum().reset_index()
        agg_df[gbd_constants.columns.MEAN] = agg_df[draw_cols].mean(axis=1)
        agg_df = agg_df[index_cols + [gbd_constants.columns.MEAN]]
        return agg_df

    # Get the mean of static cause-level incidence
    static_cause_df = _aggregate_to_cause_mean(df=static_seq_df)
    static_cause_df = static_cause_df.rename(
        columns={gbd_constants.columns.MEAN: _UNSCALABLE_MEAN}
    )
    # Subtract static cause-level incidence from TMI
    tmi_df = tmi_df.merge(static_cause_df, how="left", on=index_cols)
    tmi_df[_UNSCALABLE_MEAN] = tmi_df[_UNSCALABLE_MEAN].fillna(0)
    tmi_df[como_constants.TMI_COL] = tmi_df[como_constants.TMI_COL] - tmi_df[_UNSCALABLE_MEAN]
    tmi_df = tmi_df.drop(_UNSCALABLE_MEAN, axis=1)
    # Set a floor of 0 on the static-deleted TMI
    tmi_df.loc[tmi_df[como_constants.TMI_COL] < 0, como_constants.TMI_COL] = 0
    # Aggregate scalable seqs up to cause- and mean-level incidence
    scalable_cause_df = _aggregate_to_cause_mean(df=scalable_seq_df)
    # Calculate incidence scalars using the custom aggregates and static-deleted TMI
    scalar_df = _compute_tmi_scalar(
        tmi_df=tmi_df, incidence_df=scalable_cause_df, index_cols=index_cols
    )
    return scalar_df


def save_scalars(
    scalar_df: pd.DataFrame, tmi_cache_filepath: pathlib.Path, location_id: int, sex_id: int
) -> None:
    """Saves scalar CSVs to disk."""
    filepath = como_constants.TMI_SCALAR_FILEPATH.format(
        location_id=location_id, sex_id=sex_id
    )
    scalar_df.to_csv(tmi_cache_filepath / filepath, index=False)


def scale_seqs(
    scalable_seq_df: pd.DataFrame,
    scalar_df: pd.DataFrame,
    index_cols: List[str],
    draw_cols: List[str],
) -> pd.DataFrame:
    """Applies TMI scalars to scalable seqs."""
    # Merge scalars onto scalable seqs and multiply
    scalable_seq_df = scalable_seq_df.merge(scalar_df, on=index_cols)
    scalable_seq_df[draw_cols] = scalable_seq_df[draw_cols].multiply(
        scalable_seq_df[como_constants.TMI_SCALAR_COL], axis=0
    )
    scalable_seq_df = scalable_seq_df.drop(como_constants.TMI_SCALAR_COL, axis=1)
    return scalable_seq_df
