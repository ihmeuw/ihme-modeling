from functools import partial

import db_tools_core
from aggregator.aggregators import AggMemEff
from aggregator.operators import WtdSum
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import standard_write_func
from gbd import conn_defs
from hierarchies import dbtrees

from ihme_cc_sev_calculator.lib import constants, logging_utils, parameters

logger = logging_utils.module_logger(__name__)

INDEX_COLS = ["rei_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id"]
BY_CAUSE_INDEX_COLS = [
    "rei_id",
    "cause_id",
    "year_id",
    "age_group_id",
    "sex_id",
    "measure_id",
    "metric_id",
]


def run_location_aggregation(
    rei_id: int, by_cause: bool, params: parameters.Parameters
) -> None:
    """Run location aggregation for SEVs.

    SEVs are aggregated up the location hierarchy as the population-weighted sum.
    """
    draw_dir = (
        constants.SEV_DRAW_DIR.format(root_dir=params.output_dir)
        if not by_cause
        else constants.SEV_DRAW_CAUSE_SPECIFIC_DIR.format(root_dir=params.output_dir)
    )
    draw_pattern = constants.SEV_DRAW_FILE_PATTERN.replace("{rei_id}", str(rei_id))
    index_cols = INDEX_COLS if not by_cause else BY_CAUSE_INDEX_COLS

    # set up source and sink
    source_params = {"draw_dir": draw_dir, "file_pattern": draw_pattern}
    source = DrawSource(source_params)
    sink_params = {"draw_dir": draw_dir, "file_pattern": draw_pattern}
    sink = DrawSink(sink_params, write_func=partial(standard_write_func, index=False))

    # Read population for all location sets
    population = params.read_population()

    for location_set_id in params.location_set_ids:
        logger.info(f"Aggregating location set ID {location_set_id}")

        # Aggregation operator
        operator = WtdSum(
            index_cols=index_cols,
            value_cols=params.draw_cols,
            weight_df=population,
            weight_name="population",
            merge_cols=["location_id", "year_id", "age_group_id", "sex_id"],
        )
        # Run aggregation
        aggregator = AggMemEff(
            draw_source=source,
            draw_sink=sink,
            index_cols=index_cols,
            aggregate_col="location_id",
            operator=operator,
        )

        with db_tools_core.session_scope(conn_defs.SHARED_VIP) as session:
            loc_trees = dbtrees.loctree(
                location_set_id=location_set_id,
                release_id=params.release_id,
                return_many=True,
                session=session,
            )
            for tree in loc_trees:
                logger.info(f"Aggregating tree with root location_id {tree.root}")
                aggregator.run(tree)
