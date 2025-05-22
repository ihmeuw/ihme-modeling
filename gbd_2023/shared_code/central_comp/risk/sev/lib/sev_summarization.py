"""Summarize SEVs by location.

NOTE: this is formatted legacy code. It does not follow all the patterns of fully translated
code in this repo.
"""

import itertools
import os
from concurrent.futures import ProcessPoolExecutor as Pool
from typing import List, Tuple

import numpy as np
import pandas as pd

from core_maths.aggregate import aggregate
from core_maths.summarize import get_summary, pct_change
from draw_sources.draw_sources import DrawSource
from gbd.constants import age, sex

from ihme_cc_sev_calculator.lib import constants, parameters

SINGLE_COLS = [
    "measure_id",
    "year_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "rei_id",
    "cause_id",
    "metric_id",
    "val",
    "lower",
    "upper",
]
MULTI_COLS = [
    "measure_id",
    "year_start_id",
    "year_end_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "rei_id",
    "cause_id",
    "metric_id",
    "val",
    "lower",
    "upper",
]


def summarize_sevs_for_location(
    location_id: int, by_cause: bool, params: parameters.Parameters
) -> None:
    """Summarize every REI ID for a single location."""
    change_years = params.percent_change_years

    # Read age weights, age spans and population
    age_weights = params.read_file("age_weights")
    age_spans = params.read_file("age_spans")
    population = params.read_population().query(
        f"location_id == {location_id} & year_id.isin({params.year_ids})"
    )

    # Instantiate draw source
    draw_dir = (
        constants.SEV_DRAW_DIR.format(root_dir=params.output_dir)
        if not by_cause
        else constants.SEV_DRAW_CAUSE_SPECIFIC_DIR.format(root_dir=params.output_dir)
    )
    summary_dir = (
        constants.SEV_UPLOAD_DIR.format(root_dir=params.output_dir)
        if not by_cause
        else constants.SEV_CAUSE_SPECIFIC_UPLOAD_DIR.format(root_dir=params.output_dir)
    )
    draw_pattern = constants.SEV_DRAW_FILE_PATTERN.replace("{location_id}", str(location_id))
    source = DrawSource(params={"draw_dir": draw_dir, "file_pattern": draw_pattern})

    pool = Pool(10)
    results = pool.map(
        summ_loc,
        [
            (
                (
                    source,
                    location_id,
                    rei_id,
                    params.year_ids,
                    change_years,
                    params.release_id,
                    population,
                    age_weights,
                    age_spans,
                ),
                {},
            )
            for rei_id in params.all_rei_ids
        ],
    )
    pool.shutdown()
    results = [res for res in results if isinstance(res, tuple)]
    results = list(zip(*results))

    single_year = pd.concat([res for res in results[0] if res is not None], sort=False)
    single_year = single_year[[col for col in SINGLE_COLS if col in single_year.columns]]
    id_cols = [c for c in single_year if c.endswith("_id")]
    single_year[id_cols] = single_year[id_cols].astype(int)
    single_file = constants.SEV_SUMMARY_SINGLE_YEAR_FILE.format(
        summary_dir=summary_dir, location_id=location_id
    )
    single_year.to_csv(single_file, index=False)
    os.chmod(single_file, 0o775)

    multi_year = pd.concat(results[1], sort=False)
    if len(multi_year) > 0:
        multi_year = multi_year[[col for col in MULTI_COLS if col in multi_year.columns]]
        multi_year.replace([np.inf, -np.inf], np.nan)
        multi_year.dropna(inplace=True)
        id_cols = [c for c in multi_year if c.endswith("_id")]
        multi_year[id_cols] = multi_year[id_cols].astype(int)
        multi_file = constants.SEV_SUMMARY_MULTI_YEAR_FILE.format(
            summary_dir=summary_dir, location_id=location_id
        )
        multi_year.to_csv(multi_file, index=False)
        os.chmod(multi_file, 0o775)


def summ_loc(args: Tuple[pd.DataFrame, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Wrapper function for summarize_loc_rei."""
    summ, change_summ = summarize_loc_rei(*args[0])
    return summ, change_summ


def summarize_loc_rei(
    source: DrawSource,
    location_id: int,
    rei_id: int,
    year_id: List[int],
    change_years: List[List[int]],
    release_id: int,
    population: pd.DataFrame,
    age_weights: pd.DataFrame,
    age_spans: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run SEV aggregation for a location ID.

    Aggregate age and sex then calculate mean/upper/lower for single and multi year
    for one location and REI ID.
    """
    if change_years:
        flat_change_years = [i for i in itertools.chain(*change_years)]
    else:
        flat_change_years = []

    multi_yrs = []
    single = []
    for year in year_id:
        df = source.content(
            filters={"location_id": location_id, "year_id": year, "rei_id": rei_id}
        )
        both_sex = combine_sexes(df, population)
        df = df.append(both_sex)
        age_agg = combine_ages(df, population, age_weights, age_spans)
        df = df.append(age_agg)
        draw_cols = [c for c in df if c.startswith("draw_")]
        single.append(get_summary(df, draw_cols))
        if year in flat_change_years:
            multi_yrs.append(df)

    single = pd.concat(single, sort=False)
    single.rename(columns={"mean": "val"}, inplace=True)
    single = single[[col for col in SINGLE_COLS if col in single.columns]]

    multi = []
    if len(year_id) > 1 and flat_change_years:
        multi_yrs = pd.concat(multi_yrs, sort=False)
        for ci in change_years:
            draw_cols = [c for c in multi_yrs if c.startswith("draw_")]
            chg_df = pct_change(multi_yrs, ci[0], ci[1], "year_id", draw_cols)
            draw_cols = [c for c in chg_df if c.startswith("draw_")]
            multi.append(get_summary(chg_df, draw_cols))
        multi = pd.concat(multi, sort=False)
        multi.rename(columns={"pct_change_means": "val"}, inplace=True)
        multi = multi[[col for col in MULTI_COLS if col in multi.columns]]
    else:
        multi = pd.DataFrame(multi)

    return single, multi


def combine_sexes(df: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sexes."""
    draw_cols = list(df.filter(like="draw_").columns)
    index_cols = list(set(df.columns) - set(draw_cols) - {"sex_id"})
    both_sex = df.merge(population, on=["location_id", "year_id", "age_group_id", "sex_id"])
    both_sex = aggregate(
        both_sex[index_cols + draw_cols + ["population"]],
        draw_cols,
        index_cols,
        "wtd_sum",
        weight_col="population",
    )
    both_sex["sex_id"] = sex.BOTH
    return both_sex


def combine_ages(
    df: pd.DataFrame,
    population: pd.DataFrame,
    age_weights: pd.DataFrame,
    age_spans: pd.DataFrame,
) -> pd.DataFrame:
    """Returns all ages, age standardized, and gbd compare age groups."""
    # TODO: would be better to pass aggregate age groups in from params directly
    extra_ages = constants.SEV_SUMMARY_AGE_GROUP_IDS
    age_aggregates = age_spans.loc[
        age_spans.age_group_id.isin(extra_ages),
        ["age_group_id", "age_group_years_start", "age_group_years_end"],
    ]
    age_aggregates = age_aggregates.to_dict("split")
    age_aggregates = {int(x): (float(y), float(z)) for (x, y, z) in age_aggregates["data"]}

    draw_cols = list(df.filter(like="draw_").columns)
    index_cols = list(set(df.columns) - set(draw_cols) - {"age_group_id"})

    results = []
    for age_group_id, span in age_aggregates.items():
        # skip if age group id exists in data
        if age_group_id in df.age_group_id.unique():
            continue

        # get aggregate age cases
        if age_group_id != age.AGE_STANDARDIZED:
            # filter to the age groups included in the span
            aadf = df.merge(age_spans, on="age_group_id")
            aadf = aadf[
                (span[0] <= aadf.age_group_years_start)
                & (span[1] >= aadf.age_group_years_end)
            ]
            aadf.drop(["age_group_years_start", "age_group_years_end"], axis=1, inplace=True)
            # merge on population
            len_in = len(aadf)
            aadf = aadf.merge(
                population, on=["location_id", "year_id", "age_group_id", "sex_id"]
            )
            if len(aadf) != len_in:
                raise RuntimeError("Uh oh, some population rows are missing...")

            # aggregate
            aadf = aggregate(
                aadf[index_cols + draw_cols + ["population"]],
                draw_cols,
                index_cols,
                "wtd_sum",
                weight_col="population",
            )
            aadf["age_group_id"] = age_group_id

        else:
            aadf = df.merge(age_weights, on="age_group_id")
            aadf["age_group_id"] = age.AGE_STANDARDIZED
            # rescale age weights
            aadf.loc[:, "weight_total"] = aadf.groupby(index_cols + ["age_group_id"])[
                "age_group_weight_value"
            ].transform(sum)
            aadf["age_group_weight_value"] = (
                aadf["age_group_weight_value"] / aadf["weight_total"]
            )
            aadf = aadf.drop(["weight_total"], axis=1)
            # aggregate with weighted sum of age weights
            aadf = aggregate(
                aadf[index_cols + ["age_group_id"] + draw_cols + ["age_group_weight_value"]],
                draw_cols,
                index_cols + ["age_group_id"],
                "wtd_sum",
                weight_col="age_group_weight_value",
                normalize=None,
            )

        results.append(aadf)

    return pd.concat(results, sort=False)
