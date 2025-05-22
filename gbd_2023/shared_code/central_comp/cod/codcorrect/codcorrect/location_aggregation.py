import logging
import numpy as np
import pandas as pd
from typing import List

from aggregator.aggregators import AggMemEff
from aggregator.operators import Sum
import db_queries
from hierarchies.dbtrees import loctree

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import Columns, LocationSetId
from codcorrect.lib.utils.helpers import add_measure_id_to_sink

logger = logging.getLogger(__name__)


def aggregate_locations(
        aggregation_type: str,
        measure_id: int,
        release_id: int,
        location_set_id: int,
        year_id: int,
        version: MachineParameters
) -> None:
    """
    Uses a AggMemEff aggregator to aggregate locations for deaths and YLLs.

    Arguments:
        aggregation_type (str): the type of data to be aggregated up a
            location hierarchy. One of 'rescaled', 'unscaled', or 'shocks'.
        measure_id (int): measure ID for deaths or YLLs
        release_id (int): release ID for this CodCorrect run
        location_set_id (int): location set ID with which to aggregate
        year_id (int): draws year ID
        version: the machinery parameters set for the run

    Raises:
        ValueError: if measure_id is not deaths (1) or YLLs (4)
    """
    # Set up DrawSource and DrawSink.
    draw_source = version.file_system.get_location_aggregation_draw_source(
        aggregation_type, measure_id, year_id
    )
    draw_sink = version.file_system.get_location_aggregation_draw_sink(
        aggregation_type, measure_id, year_id
    )
    regional_scalars = version.file_system.read_regional_scalars()

    draw_sink.add_transform(
        _apply_regional_scalars,
        regional_scalars,
        release_id,
        location_set_id,
        version.draw_cols,
    )
    draw_sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)

    # Clean up old files we plan on writing (if they exist)
    version.file_system.delete_existing_location_aggregates(
        aggregation_type, measure_id, location_set_id, year_id, release_id
    )

    # Set up aggregator and location tree.
    index_cols = (
        [col for col in Columns.INDEX if col != Columns.LOCATION_ID]
    )
    operator = Sum(index_cols, version.draw_cols)

    agg = AggMemEff(
        draw_source, draw_sink, index_cols, Columns.LOCATION_ID, operator, chunksize=2
    )
    trees = loctree(
        location_set_id=location_set_id,
        release_id=release_id,
        return_many=True,  # Always return list of location sets in case there's multiple
    )

    logger.info(f"Aggregating locations, location_set_id: {location_set_id}")
    for tree in trees:
        agg.run(tree, draw_filters={}, n_processes=10)


def _apply_regional_scalars(
        df: pd.DataFrame,
        regional_scalars: pd.DataFrame,
        release_id: int,
        location_set_id: int,
        draw_cols: List[str]
) -> pd.DataFrame:
    """
    Transform to apply regional scalars to draws.

    Note: Regional scalars are only applied to regions
        if they are NOT most-detailed locations in the location set.
    """
    # Fetch list of regions and read regional scalars for those regions.
    regions = set(
        db_queries.get_location_metadata(
            release_id=release_id,
            location_set_id=location_set_id
        ).query('level == 2').location_id.unique()
    )
    regional_scalars = regional_scalars.query('location_id in @regions')

    # Apply scalars where applicable.
    # Multiply by 1.0 when scalars aren't present.
    join_cols = [Columns.YEAR_ID, Columns.LOCATION_ID]
    return (
        pd.merge(df, regional_scalars, how='left', on=join_cols)
        .fillna(1.0)
        .pipe(_apply_regional_scalars_helper, draw_cols=draw_cols)
        .drop(Columns.MEAN, axis=1)
    )


def _apply_regional_scalars_helper(
        df: pd.DataFrame,
        draw_cols: List[str]
) -> pd.DataFrame:
    """Helper function that multiplies draw columns by a regional scalar."""
    df[draw_cols] = df[draw_cols].mul(df[Columns.MEAN], axis=0)
    return df
