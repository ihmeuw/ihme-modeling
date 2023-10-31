import argparse
import pandas as pd
import os
from functools import partial
from pathlib import Path
from typing import List, Tuple

from db_queries import get_cause_metadata
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import standard_write_func
from gbd.constants import cause, columns, measures, metrics
from ihme_dimensions import dfutils


TEMPERATURE_REI_IDS = [331, 337, 338]
TEMPERATURE_DIR = "FILEPATH"


def parse_arguments() -> Tuple:
    parser = argparse.ArgumentParser()
    parser.add_argument("--location_id", type=int)
    parser.add_argument("--year_id", type=int, nargs="+")
    parser.add_argument("--n_draws", type=int)
    parser.add_argument("--sev_version_id", type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    parser.add_argument("--by_cause", type=int)

    args = parser.parse_args()
    location_id = args.location_id
    year_id = args.year_id
    n_draws = args.n_draws
    sev_version_id = args.sev_version_id
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    by_cause = bool(args.by_cause)

    return (location_id, year_id, n_draws, sev_version_id, gbd_round_id, decomp_step, by_cause)


def validate_sevs(df: pd.DataFrame) -> None:
    # should only include temperature risks
    if not df[columns.REI_ID].isin(TEMPERATURE_REI_IDS).all():
        raise ValueError("Found draws for non-temperature risks")

    # should include all temperature risks
    missing_risks = set(TEMPERATURE_REI_IDS).difference(set(df.rei_id))
    if len(missing_risks) > 0:
        raise ValueError(f"Input draws are missing temperature risks {missing_risks}")

    # should only include most-detailed causes and all-cause
    most_detailed_causes = get_cause_metadata(
        cause_set_id=2, gbd_round_id=gbd_round_id, decomp_step=decomp_step
    ).query("most_detailed == 1")[columns.CAUSE_ID].tolist()
    if not df[columns.CAUSE_ID].isin(most_detailed_causes + [cause.ALL_CAUSE]).all():
        raise ValueError("Found draws for aggregate causes")

    # must include both all-cause SEVs and most-detailed-cause SEVs
    if len(df.loc[df[columns.CAUSE_ID] == cause.ALL_CAUSE]) == 0:
        raise ValueError("Input draws are missing all-cause SEVs")
    if len(df.loc[df[columns.CAUSE_ID] != cause.ALL_CAUSE]) == 0:
        raise ValueError("Input draws are missing most-detailed causes")


def save_draws(df: pd.DataFrame, index_cols: List[str], draw_cols: List[str], draw_dir: str) -> None:
    # keep desired columns
    df[columns.MEASURE_ID] = measures.SEV
    df[columns.METRIC_ID] = metrics.RATE
    df = df[index_cols + draw_cols]

    # create directories if they don't exist
    for rei_id in TEMPERATURE_REI_IDS:
        dir = os.path.join(draw_dir, str(rei_id))
        Path(dir).mkdir(parents=True, exist_ok=True)

    # write the draws
    sink_params = {"draw_dir": draw_dir, "file_pattern": "{rei_id}/{location_id}.csv"}
    sink = DrawSink(sink_params, write_func=partial(standard_write_func, index=False))
    sink.push(df)


if __name__ == "__main__":
    (location_id, year_id, n_draws, sev_version_id, gbd_round_id, decomp_step, by_cause) = parse_arguments()

    base_dir = f"FILEPATH/{sev_version_id}"

    # read draws provided by temperature team
    source_params = {
        "draw_dir": TEMPERATURE_DIR,
        "file_pattern": "{location_id}_{year_id}.csv",
    }
    source = DrawSource(source_params)
    df = source.content(filters={columns.LOCATION_ID: location_id, columns.YEAR_ID: year_id})

    # validate the provided draws, will raise if anything is wrong
    validate_sevs(df)

    # resample to desired number of draws
    df = dfutils.resample(df, n_draws)

    # save all-cause temperature SEVs
    index_cols = [
        columns.REI_ID,
        columns.LOCATION_ID,
        columns.YEAR_ID,
        columns.AGE_GROUP_ID,
        columns.SEX_ID,
        columns.MEASURE_ID,
        columns.METRIC_ID,
    ]
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    save_draws(
        df=df.loc[df[columns.CAUSE_ID] == cause.ALL_CAUSE],
        index_cols=index_cols,
        draw_cols=draw_cols,
        draw_dir=f"{base_dir}/draws"
    )

    # save temperature SEVs at risk-cause level if requested
    if by_cause:
        index_cols.insert(index_cols.index(columns.REI_ID) + 1, columns.CAUSE_ID)
        save_draws(
            df=df.loc[df[columns.CAUSE_ID] != cause.ALL_CAUSE],
            index_cols=index_cols,
            draw_cols=draw_cols,
            draw_dir=f"{base_dir}/risk_cause/draws"
        )
