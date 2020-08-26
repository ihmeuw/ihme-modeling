import os
from typing import Dict, List

import pandas as pd

import db_queries
import gbd

from probability_of_death.lib import age_helpers
from probability_of_death.lib.constants import columns, parameters, paths


def generate_probability_of_death(
    location_id: int,
    year_ids: List[int],
    sex_ids: List[int],
    aggregate_age_group_ids: List[int],
    gbd_round_id: int,
    decomp_step: str,
    deaths_version: str,
) -> None:
    """Makes probability of death estimates for one location."""
    age_group_map = age_helpers.get_age_group_map(gbd_round_id, aggregate_age_group_ids)
    all_detailed_age_group_ids = list(
        {
            age_group_id
            for age_group_ids in age_group_map.values()
            for age_group_id in age_group_ids
        }
    )
    inputs_df = _read_inputs(
        location_id,
        year_ids,
        sex_ids,
        all_detailed_age_group_ids,
        gbd_round_id,
        decomp_step,
        deaths_version,
    )

    prob_of_death_df = _calculate(inputs_df, age_group_map)
    _save_output(prob_of_death_df, location_id)


def _read_inputs(
    location_id: int,
    year_ids: List[int],
    sex_ids: List[int],
    detailed_age_group_ids: List[int],
    gbd_round_id: int,
    decomp_step: str,
    deaths_version: str,
) -> pd.DataFrame:
    """Pulls inputs to probability of death calculation.

    Reads in qx and lx from life tables, which need to be reformatted from long to wide.
    LX is 100,000 at birth, so we divide by 100,000 to adjust the scale.

    Deaths are pulled as cause fractions from get_outputs.
    Nonfatal death estimates are NA, so we fill those with 0.
    """
    life_table_df = (
        db_queries.get_life_table_with_shock(
            location_id=location_id,
            year_id=year_ids,
            sex_id=sex_ids,
            age_group_id=detailed_age_group_ids,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            life_table_parameter_id=[parameters.QX, parameters.LX],
        )
        .pivot_table(
            index=columns.DEMOGRAPHICS,
            columns=[columns.LIFE_TABLE_PARAMETER_ID],
            values=[columns.MEAN],
        )
        .set_axis([columns.QX, columns.LX], axis=1, inplace=False)
        .reset_index()
        .assign(**{columns.LX: lambda df: df[columns.LX] / 100000})
    )
    death_df = (
        db_queries.get_outputs(
            topic="cause",
            cause_id="all",
            measure_id=gbd.constants.measures.DEATH,
            metric_id=gbd.constants.metrics.PERCENT,
            location_id=location_id,
            year_id=year_ids,
            sex_id=sex_ids,
            age_group_id=detailed_age_group_ids,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            version=deaths_version,
        )
        .fillna(0)
        .rename(columns={columns.VAL: columns.DEATH})
        .loc[:, columns.DEMOGRAPHICS + [columns.CAUSE_ID, columns.DEATH]]
    )
    return death_df.merge(life_table_df, on=columns.DEMOGRAPHICS)


def _calculate(inputs_df: pd.DataFrame, age_group_map: Dict[int, List[int]]) -> pd.DataFrame:
    """Calculates probability of death.

    Probability of death is calculated for certain aggregate age groups. For each one of
    these aggregate age groups, take the following steps:
        1. Subset to age groups that comprise the aggregate
        2. Save the survivorship for the youngest age group
        3. Compute age-specific deaths as survivorship * probability of death * cause fraction
        4. Aggregate (3) by age then divide by (2)

    Args:
        inputs_df: life table inputs merged with deaths inputs.
        age_group_map: dictionary of aggregate age group ID to age group IDs in the aggregate.

    Returns:
        DataFrame of probability of death output.
    """
    prob_of_death_dfs: List[pd.DataFrame] = []
    for agg_age_group_id, age_group_ids in age_group_map.items():
        subset_df = inputs_df[inputs_df[columns.AGE_GROUP_ID].isin(age_group_ids)].copy()
        lx_initial_df = subset_df.loc[
            subset_df[columns.AGE_GROUP_ID] == age_group_ids[0], columns.LX
        ]
        subset_df[columns.AGE_SPECIFIC_DEATH] = (
            subset_df[columns.LX]
            .multiply(subset_df[columns.QX])
            .multiply(subset_df[columns.DEATH])
        )
        final_df = (
            subset_df.groupby(
                [columns.LOCATION_ID, columns.YEAR_ID, columns.SEX_ID, columns.CAUSE_ID],
                as_index=False,
            )[columns.AGE_SPECIFIC_DEATH]
            .sum()
            .assign(
                **{
                    columns.AGE_GROUP_ID: agg_age_group_id,
                    columns.VAL: lambda df: df[columns.AGE_SPECIFIC_DEATH].divide(
                        lx_initial_df.values
                    ),
                }
            )
            .drop(columns=columns.AGE_SPECIFIC_DEATH)
        )
        prob_of_death_dfs.append(final_df)
    return pd.concat(prob_of_death_dfs)


def _save_output(df: pd.DataFrame, location_id: int) -> None:
    """Saves calculated probability of death to a CSV."""
    os.umask(0o0002)
    output_file = paths.OUTPUT_FORMAT.format(location_id=location_id)
    df.to_csv(output_file, index=False)
