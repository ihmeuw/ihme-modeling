import logging
import numpy as np
from os.path import join
import pandas as pd
from typing import Tuple

from aggregator.aggregators import AggMemEff
from aggregator.operators import Sum
from db_queries import get_location_metadata
from draw_sources.draw_sources import DrawSource, DrawSink
import gbd.constants as gbd
from hierarchies.dbtrees import loctree

from fauxcorrect.queries.regional_scalars import get_all_regional_scalars
from fauxcorrect.utils.constants import (
    Columns,
    FilePaths,
    LocationAggregation,
    LocationSetId,
    Keys
)
from fauxcorrect.utils.file_cleanup import clean_aggregation_directory
from fauxcorrect.utils.io import (
    add_measure_id_to_sink,
    cache_hdf,
    read_cached_hdf
)


def cache_regional_scalars(parent_dir: str, gbd_round_id: int) -> None:
    """
    Cache regional scalars for a fauxcorrect run.
    Save in h5 table format queryable by location_id, sex_id, and year_id
    since regional scalar application will be parallelized along those values.

    Arguments:
        parent_dir: parent fauxcorrect directory
            e.g. PATH{version}
        gbd_round_id: GBD round ID
    """
    scalars_path = _get_regional_scalars_path(parent_dir)
    scalars = get_all_regional_scalars(gbd_round_id)
    data_cols = [Columns.LOCATION_ID, Columns.SEX_ID, Columns.YEAR_ID]
    cache_hdf(scalars, scalars_path, Keys.REGIONAL_SCALARS, data_cols)


def aggregate_locations(
        aggregation_type: str,
        parent_dir: str,
        measure_id: int,
        gbd_round_id: int,
        location_set_id: int,
        year_id: int
) -> None:
    """
    Uses a AggMemEff aggregator to aggregate locations for deaths and
    YLLs.

    Arguments:
        aggregation_type (str): the type of data to be aggregated up a
            location hierarchy. One of 'aggregated/rescaled',
            'aggregated/shocks', 'aggregated/unscaled'
            'scaled', or 'unaggregated/shocks'.
        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}
        measure_id (int): measure ID for deaths or YLLs
        gbd_round_id (int): GBD round ID for this fauxcorrect run
        location_set_id (int): location set ID with which to aggregate
        year_id (int): draws year ID

    Raises:
        ValueError: if measure_id is not deaths (1) or YLLs (4)
    """
    # Set up DrawSource and DrawSink.
    source_dir, sink_dir = _get_draw_source_sink_dirs(
        parent_dir,
        aggregation_type,
        measure_id
    )
    source, draw_filters = _get_draw_source_and_filters(
        aggregation_type,
        source_dir,
        year_id,
        measure_id
    )

    sink = DrawSink({
        'draw_dir': sink_dir,
        'file_pattern': FilePaths.LOCATION_AGGREGATE_FILE_PATTERN.format(
            year_id=year_id
        ),
        'h5_tablename': Keys.DRAWS
    })
    sink.add_transform(
        _apply_regional_scalars, parent_dir, gbd_round_id, location_set_id
    )
    sink.add_transform(add_measure_id_to_sink, measure_id=measure_id)

    # clean up old files we plan on writing
    clean_aggregation_directory(
        root_dir=sink.params['draw_dir'],
        file_pattern=sink.params['file_pattern'],
        location_set_id=location_set_id,
        gbd_round_id=gbd_round_id
    )

    # Set up aggregator and location tree.
    index_cols = (
        [col for col in Columns.INDEX if col != Columns.LOCATION_ID]
    )
    operator = Sum(index_cols, Columns.DRAWS)

    agg = AggMemEff(
        source, sink, index_cols, Columns.LOCATION_ID, operator, chunksize=2
    )
    is_sdi_set = location_set_id == LocationSetId.SDI
    trees = loctree(
        location_set_id=location_set_id,
        gbd_round_id=gbd_round_id,
        return_many=is_sdi_set
    )

    logging.info(f"Aggregating locations, location_set_id: {location_set_id}")
    for tree in np.atleast_1d(trees):
        agg.run(tree, draw_filters=draw_filters, n_processes=10)


def _get_draw_source_sink_dirs(
        parent_dir: str,
        aggregation_type: str,
        measure_id: int
) -> Tuple[str, str]:
    """Gets DrawSource and DrawSink directories for location aggregation."""
    if measure_id != gbd.measures.DEATH and measure_id != gbd.measures.YLL:
        raise ValueError(
            f'measure_id must be {gbd.measures.DEATH} or {gbd.measures.YLL}, '
            f'got {measure_id}'
        )

    source_dir = join(parent_dir, aggregation_type)

    if aggregation_type == LocationAggregation.Type.UNAGGREGATED_SHOCKS:
        sink_dir = join(
            parent_dir,
            aggregation_type,
            FilePaths.LOCATION_AGGREGATES
        )
    else:
        sink_dir = join(parent_dir, aggregation_type)

    if measure_id == gbd.measures.YLL:
        return (
            join(source_dir, FilePaths.YLLS_DIR),
            join(sink_dir, FilePaths.YLLS_DIR)
        )

    return (
        join(source_dir, FilePaths.DEATHS_DIR),
        join(sink_dir, FilePaths.DEATHS_DIR)
    )


def _get_draw_source_and_filters(
        aggregation_type: str,
        source_dir: str,
        year_id: int,
        measure_id: int
) -> DrawSource:
    if aggregation_type == LocationAggregation.Type.UNAGGREGATED_SHOCKS:
        source = DrawSource(
            params={
                'draw_dir': source_dir,
                'file_pattern': FilePaths.UNAGGREGATED_SHOCKS_FILE_PATTERN
            }
        )
        draw_filters = {
            Columns.MEASURE_ID: measure_id,
            Columns.YEAR_ID: year_id
        }
    else:
        source = DrawSource({
            'draw_dir': source_dir,
            'file_pattern': FilePaths.LOCATION_AGGREGATE_FILE_PATTERN.format(
                year_id=year_id)
        })
        draw_filters = {}
    return source, draw_filters


def _get_regional_scalars_path(parent_dir: str) -> str:
    """Get path to cached regional scalars"""
    return join(
        parent_dir,
        FilePaths.INPUT_FILES_DIR,
        FilePaths.REGIONAL_SCALARS
    )


def _read_regional_scalars(parent_dir: str) -> pd.DataFrame:
    """
    Read all regional scalars.
    There aren't many of them, so it's fine to keep all of them in memory.
    """
    return read_cached_hdf(
        _get_regional_scalars_path(parent_dir),
        Keys.REGIONAL_SCALARS,
        columns=[Columns.YEAR_ID, Columns.LOCATION_ID, Columns.MEAN]
    )


def _apply_regional_scalars(
        df: pd.DataFrame,
        parent_dir: str,
        gbd_round_id: int,
        location_set_id: int
) -> pd.DataFrame:
    """Transform to apply regional scalars to draws."""
    # Fetch list of regions and read regional scalars for those regions.
    regions = set(
        get_location_metadata(
            gbd_round_id=gbd_round_id, location_set_id=location_set_id
        ).query('level == 2').location_id.unique()
    )
    regional_scalars_df = _read_regional_scalars(parent_dir).query(
        'location_id in @regions')

    # Apply scalars where applicable.
    # Multiply by 1.0 when scalars aren't present.
    join_cols = [Columns.YEAR_ID, Columns.LOCATION_ID]
    return (
        pd.merge(df, regional_scalars_df, how='left', on=join_cols)
          .fillna(1.0)
          .pipe(_apply_regional_scalars_helper)
          .drop(Columns.MEAN, axis=1)
    )


def _apply_regional_scalars_helper(df: pd.DataFrame) -> pd.DataFrame:
    """Helper function that multiplies draw columns by a regional scalar."""
    df[Columns.DRAWS] = df[Columns.DRAWS].mul(df[Columns.MEAN], axis=0)
    return df
