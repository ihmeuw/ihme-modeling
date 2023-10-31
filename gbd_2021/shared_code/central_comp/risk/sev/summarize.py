import argparse
import itertools
import os
from concurrent.futures import ProcessPoolExecutor as Pool
from glob import glob
from typing import List, Tuple

import numpy as np
import pandas as pd

from core_maths.aggregate import aggregate
from core_maths.summarize import get_summary, pct_change
from db_queries import get_age_spans, get_age_weights
from draw_sources.draw_sources import DrawSource
from gbd.constants import GBD_COMPARE_AGES

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


def combine_sexes(df: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
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
    both_sex["sex_id"] = 3
    return both_sex


def combine_ages(
    df: pd.DataFrame, population: pd.DataFrame, age_weights: pd.DataFrame
) -> pd.DataFrame:
    """Returns all ages, age standardized, and gbd compare age groups."""
    age_spans = get_age_spans()
    extra_ages = [22, 27] + GBD_COMPARE_AGES
    age_aggregates = age_spans.loc[age_spans.age_group_id.isin(extra_ages)]
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
        if age_group_id != 27:
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
            assert len(aadf) == len_in, "pops are missing"
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
            aadf["age_group_id"] = 27
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


def parse_arguments() -> Tuple:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sev_version_id", type=int)
    parser.add_argument("--location_id", type=int)
    parser.add_argument("--year_id", type=int, nargs="+")
    parser.add_argument("--change", action="store_true", default=False)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--by_cause", type=int)

    args = parser.parse_args()
    sev_version_id = args.sev_version_id
    location_id = args.location_id
    year_id = args.year_id
    change = args.change
    gbd_round_id = args.gbd_round_id
    by_cause = bool(args.by_cause)

    if change:
        change_intervals = [(1990, 2000), (1990, 2020), (2000, 2010), (2010, 2020), (2019, 2020)]
    else:
        change_intervals = None

    return (sev_version_id, location_id, gbd_round_id, year_id, change_intervals, by_cause)


def summarize_loc_rei(
    source: DrawSource,
    location_id: int,
    rei_id: int,
    year_id: List[int],
    change_intervals: Tuple,
    gbd_round_id: int,
    population: pd.DataFrame,
    age_weights: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """aggregate age and sex then calc mean ui for single and multi year
    for one location risk pair"""
    if change_intervals:
        change_years = [i for i in itertools.chain(*change_intervals)]
    else:
        change_years = []

    multi_yrs = []
    single = []
    for year in year_id:
        df = source.content(
            filters={"location_id": location_id, "year_id": year, "rei_id": rei_id}
        )
        both_sex = combine_sexes(df, population)
        df = df.append(both_sex)
        age_agg = combine_ages(df, population, age_weights)
        df = df.append(age_agg)
        draw_cols = [c for c in df if c.startswith("draw_")]
        single.append(get_summary(df, draw_cols))
        if year in change_years:
            multi_yrs.append(df)

    single = pd.concat(single, sort=False)
    single.rename(columns={"mean": "val"}, inplace=True)
    single = single[[col for col in SINGLE_COLS if col in single.columns]]

    multi = []
    if len(year_id) > 1 and change_years:
        multi_yrs = pd.concat(multi_yrs, sort=False)
        for ci in change_intervals:
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


def summ_loc(args) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summ, change_summ = summarize_loc_rei(*args[0])
    return summ, change_summ


def summarize_loc(
    base_dir: str,
    location_id: int,
    year_id: List[int],
    gbd_round_id: int,
    change_intervals: Tuple = None,
    by_cause: bool = False,
) -> None:
    """summarize every rei for a single location"""
    # age weights and population
    age_weights = get_age_weights(gbd_round_id=int(gbd_round_id))
    population = []
    popfiles = glob(os.path.join(base_dir, "population_*.csv"))
    for popfile in popfiles:
        population.append(pd.read_csv(popfile))
    population = pd.concat(population, sort=False).drop_duplicates(
        subset=["location_id", "age_group_id", "year_id", "sex_id"]
    )
    population = population.loc[
        (population.location_id == location_id) & (population.year_id.isin(year_id))
    ]

    # Instantiate draw source
    draw_dir = f"{base_dir}/risk_cause/draws" if by_cause else f"{base_dir}/draws"
    out_dir = f"{base_dir}/risk_cause/summaries" if by_cause else f"{base_dir}/summaries"
    source = DrawSource(
        params={"draw_dir": draw_dir, "file_pattern": "{rei_id}/{location_id}.csv"}
    )

    rei_ids = glob_rei_ids_from_draw_dir(draw_dir)
    pool = Pool(10)
    results = pool.map(
        summ_loc,
        [
            (
                (
                    source,
                    location_id,
                    rei,
                    year_id,
                    change_intervals,
                    gbd_round_id,
                    population,
                    age_weights,
                ),
                {},
            )
            for rei in rei_ids
        ],
    )
    pool.shutdown()
    results = [res for res in results if isinstance(res, tuple)]
    results = list(zip(*results))

    single_year = pd.concat([res for res in results[0] if res is not None], sort=False)
    single_year = single_year[[col for col in SINGLE_COLS if col in single_year.columns]]
    id_cols = [c for c in single_year if c.endswith("_id")]
    single_year[id_cols] = single_year[id_cols].astype(int)
    single_file = os.path.join(out_dir, f"single_year_{location_id}.csv")
    single_year.to_csv(single_file, index=False)
    os.chmod(single_file, 0o775)

    multi_year = pd.concat(results[1], sort=False)
    if len(multi_year) > 0:
        multi_year = multi_year[[col for col in MULTI_COLS if col in multi_year.columns]]
        multi_year.replace([np.inf, -np.inf], np.nan)
        multi_year.dropna(inplace=True)
        id_cols = [c for c in multi_year if c.endswith("_id")]
        multi_year[id_cols] = multi_year[id_cols].astype(int)
        multi_file = os.path.join(out_dir, f"multi_year_{location_id}.csv")
        multi_year.to_csv(multi_file, index=False)
        os.chmod(multi_file, 0o775)


def glob_rei_ids_from_draw_dir(draw_dir: str) -> List[int]:
    """Identify rei_ids from the csvs in the draw_dir."""
    return [int(os.path.basename(file)) for file in glob(os.path.join(draw_dir, "*"))]


if __name__ == "__main__":
    (
        sev_version_id,
        location_id,
        gbd_round_id,
        year_id,
        change_intervals,
        by_cause,
    ) = parse_arguments()

    base_dir = f"FILEPATH/{sev_version_id}"
    summarize_loc(
        base_dir=base_dir,
        location_id=location_id,
        year_id=year_id,
        gbd_round_id=gbd_round_id,
        change_intervals=change_intervals,
        by_cause=False,
    )
    if by_cause:
        summarize_loc(
            base_dir=base_dir,
            location_id=location_id,
            year_id=year_id,
            gbd_round_id=gbd_round_id,
            change_intervals=change_intervals,
            by_cause=True,
        )
