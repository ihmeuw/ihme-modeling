import argparse
import os
from functools import partial
from typing import List, Tuple

import pandas as pd

from aggregator.aggregators import AggMemEff
from aggregator.operators import WtdSum
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import standard_write_func
from hierarchies.dbtrees import loctree


def parse_arguments() -> Tuple:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sev_version_id", type=int)
    parser.add_argument("--rei_id", type=int)
    parser.add_argument("--location_set_id", type=int, nargs="+")
    parser.add_argument("--n_draws", type=int)
    parser.add_argument("--gbd_round_id", type=int)
    parser.add_argument("--decomp_step", type=str)
    parser.add_argument("--by_cause", type=int)

    args = parser.parse_args()
    sev_version_id = args.sev_version_id
    rei_id = args.rei_id
    location_set_id = args.location_set_id
    n_draws = args.n_draws
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    by_cause = bool(args.by_cause)

    return (
        sev_version_id,
        rei_id,
        gbd_round_id,
        decomp_step,
        n_draws,
        location_set_id,
        by_cause,
    )


def run_location_aggregation(
    sev_version_id: int,
    rei_id: int,
    gbd_round_id: int,
    decomp_step: int,
    n_draws: int,
    location_set_id: List[int],
    by_cause: bool,
) -> None:

    base_dir = f"FILEPATH/{sev_version_id}"
    draw_dir = f"{base_dir}/risk_cause/draws" if by_cause else f"{base_dir}/draws"

    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    index_cols = ["rei_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id"]
    if by_cause:
        index_cols.insert(index_cols.index("rei_id") + 1, "cause_id")

    # set up source and sink
    source_params = {"draw_dir": draw_dir, "file_pattern": "{rei_id}/{location_id}.csv"}
    source = DrawSource(source_params)
    sink_params = {"draw_dir": draw_dir, "file_pattern": "{rei_id}/{location_id}.csv"}
    sink = DrawSink(sink_params, write_func=partial(standard_write_func, index=False))
    for lsid in location_set_id:

        population = pd.read_csv(os.path.join(base_dir, f"population_{lsid}.csv"))

        # aggregation operator
        operator = WtdSum(
            index_cols=index_cols,
            value_cols=draw_cols,
            weight_df=population,
            weight_name="population",
            merge_cols=["location_id", "year_id", "age_group_id", "sex_id"],
        )
        # run aggregation
        aggregator = AggMemEff(
            draw_source=source,
            draw_sink=sink,
            index_cols=index_cols,
            aggregate_col="location_id",
            operator=operator,
        )

        if lsid == 40:
            loc_trees = loctree(
                location_set_id=lsid,
                gbd_round_id=gbd_round_id,
                decomp_step=decomp_step,
                return_many=True,
            )
            for tree in loc_trees:
                aggregator.run(tree, draw_filters={"rei_id": rei_id})
        else:
            loc_tree = loctree(
                location_set_id=lsid, gbd_round_id=gbd_round_id, decomp_step=decomp_step
            )
            aggregator.run(loc_tree, draw_filters={"rei_id": rei_id})


if __name__ == "__main__":
    (
        sev_version_id,
        rei_id,
        gbd_round_id,
        decomp_step,
        n_draws,
        location_set_id,
        by_cause,
    ) = parse_arguments()

    run_location_aggregation(
        sev_version_id=sev_version_id,
        rei_id=rei_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        n_draws=n_draws,
        location_set_id=location_set_id,
        by_cause=False,
    )
    if by_cause:
        run_location_aggregation(
            sev_version_id=sev_version_id,
            rei_id=rei_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            n_draws=n_draws,
            location_set_id=location_set_id,
            by_cause=True,
        )
