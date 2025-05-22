import logging
import os
import time
from typing import List

import pandas as pd

from aggregator.aggregators import AggMemEff
from aggregator.operators import Sum
from dataframe_io import READ_LOGGER_NAME, WRITE_LOGGER_NAME
from draw_sources.draw_sources import DrawSink, DrawSource
from gbd import constants as gbd
from hierarchies.tree import Tree

from dalynator import get_input_args, logging_utils
from dalynator import makedirs_safely as mkds
from dalynator.data_container import DataContainer, remove_unwanted_stars
from dalynator.lib.utils import draw_column_list

SUCCESS_LOG_MESSAGE = "DONE write DF"
DEFAULT_INDEX_COLUMNS = [
    gbd.columns.MEASURE_ID,
    gbd.columns.METRIC_ID,
    gbd.columns.SEX_ID,
    gbd.columns.CAUSE_ID,
    gbd.columns.REI_ID,
    gbd.columns.YEAR_ID,
    gbd.columns.AGE_GROUP_ID,
]

logger = logging_utils.module_logger(__name__)
logging.getLogger(WRITE_LOGGER_NAME).setLevel(logging.DEBUG)
logging.getLogger(READ_LOGGER_NAME).setLevel(logging.DEBUG)


def apply_regional_scalars(
    df: pd.DataFrame,
    regional_scalar_path: str,
    region_locs: List[int],
    value_cols: List[str],
    year_ids: List[int],
) -> pd.DataFrame:
    """Apply regional multiplicative scalars.

    Args:
        df: the input DataFrame, with only a single location ID value
            and at least "location_id" and "year_id" columns in addition
            to 'value_cols'
        regional_scalar_path: the parent directory containing 'scalars.h5'
            containing regional scalars by location_id and year_id
        region_locs: a list of location_ids for which to seek and apply
            the scalar multiplication
        value_cols: the list of column labels which should be multiplied
        year_ids: the year_ids for which the multiplication should take place

    Returns:
        df: A DataFrame where rows matching year_id have been scaled
           if (1) the location_id unique value is in region_locs or (2) the
           'scalars.h5' has a matching entry for (location_id, year_id)

    """
    unique_locations = df.location_id.unique()
    if unique_locations.size != 1:
        raise ValueError(f"Expected 1 unique location_id and got {unique_locations.size}")
    current_loc = unique_locations[0]

    if current_loc not in region_locs:
        return df

    merge_cols = ["location_id", "year_id"]
    scalars = pd.read_hdf(
        path_or_buf=f"{regional_scalar_path}/scalars.h5",
        key="scalars",
        where=([f"'location_id'=={current_loc} & 'year_id' in ({[str(x) for x in year_ids]})"]),
    )
    df = df.merge(scalars, on=merge_cols, how="left")
    df["scaling_factor"].fillna(1.0, inplace=True)
    df[value_cols] = df[value_cols].mul(df["scaling_factor"], axis=0)
    df.drop("scaling_factor", axis=1, inplace=True)
    return df


class LocationAggregator():
    """Holds state and executes a single location aggregation computation."""

    def __init__(
        self,
        year_ids: List[int],
        rei_id: int,
        sex_id: int,
        measure_id: int,
        n_draws: int,
        data_root: str,
        region_locs: List[int],
        write_out_star_ids: bool,
        index_cols: List[str] = DEFAULT_INDEX_COLUMNS,
    ):
        """Initialize location aggregation object."""
        self.year_ids = year_ids
        self.rei_id = rei_id
        self.sex_id = sex_id
        self.measure_id = measure_id
        self.n_draws = n_draws
        self.data_root = data_root
        self.region_locs = region_locs

        self.in_dir = os.path.join(self.data_root, "draws")
        self.out_dir = os.path.join(self.data_root, "loc_agg_draws/burden")
        mkds.makedirs_safely(self.out_dir)
        self.write_out_star_ids = write_out_star_ids

        self.index_cols = index_cols
        self.value_cols = draw_column_list(n_draws=self.n_draws)

        self.draw_filters = {
            gbd.columns.METRIC_ID: gbd.metrics.NUMBER,
            gbd.columns.REI_ID: self.rei_id,
            gbd.columns.SEX_ID: self.sex_id,
            gbd.columns.MEASURE_ID: self.measure_id,
            gbd.columns.YEAR_ID: self.year_ids,
        }

        self.draw_source = self.get_draw_source()
        self.draw_sink = self.get_draw_sink()

    def clean_cached_location_aggregates(self, loctree_list: List[Tree]) -> None:
        """Remove old aggregates in case jobs failed in the middle."""
        for loctree in loctree_list:
            aggregates = [n.id for n in loctree.nodes if n not in loctree.leaves()]
            for loc in aggregates:
                filename = (
                    f"{self.out_dir}/{loc}/{self.measure_id}/"
                    f"{self.measure_id}_{loc}_{self.rei_id}_{self.sex_id}.h5"
                )
                if os.path.exists(filename):
                    logger.debug(f"Deleting pre-existing file {filename}.")
                    os.remove(filename)

    def get_draw_source(self) -> DrawSource:
        """Establishes the source for location aggregation draws."""
        in_pattern = f"{{location_id}}/{self.measure_id}_{{location_id}}_{{year_id}}.h5"
        draw_source = DrawSource(
            params={
                "draw_dir": self.in_dir,
                "file_pattern": in_pattern,
                "h5_tablename": f"{self.n_draws}_draws",
                "data_cols": draw_column_list(n_draws=self.n_draws),
                "index_cols": self.index_cols,
            }
        )
        return draw_source

    def get_draw_sink(self) -> DrawSink:
        """Establishes the sink for location aggregation draws."""
        out_pattern = (
            "{location_id}/{measure_id}/"
            "{measure_id}_{location_id}_"
            "{rei_id}_{sex_id}.h5"
        )
        draw_sink = DrawSink(
            params={
                "draw_dir": self.out_dir,
                "file_pattern": out_pattern,
                "h5_tablename": f"{self.n_draws}_draws",
            }
        )
        draw_sink.add_transform(
            apply_regional_scalars,
            regional_scalar_path=os.path.join(self.data_root, "cache"),
            region_locs=self.region_locs,
            value_cols=self.value_cols,
            year_ids=self.year_ids,
        )
        draw_sink.add_transform(
            remove_unwanted_stars, write_out_star_ids=self.write_out_star_ids
        )
        return draw_sink

    def run_aggregation(self, loctree_list: List[Tree]) -> None:
        """Run the aggregation."""
        start_time = time.time()
        logger.info("START aggregate locations, time = {start_time}")

        self.clean_cached_location_aggregates(loctree_list)

        location_aggregator = AggMemEff(
            draw_source=self.draw_source,
            draw_sink=self.draw_sink,
            index_cols=self.index_cols,
            aggregate_col="location_id",
            operator=Sum(index_cols=self.index_cols, value_cols=self.value_cols),
            chunksize=2,
        )

        for loctree in loctree_list:
            logger.info(
                f"Starting aggregation on tree with root location_id {loctree.root.id}"
            )
            location_aggregator.run(
                tree=loctree,
                include_leaves=False,
                n_processes=8,
                draw_filters=self.draw_filters,
            )
        end_time = time.time()
        logger.info(f"location aggregation complete, time = {end_time}")
        elapsed = end_time - start_time
        logger.info(f"DONE location agg pipeline at {end_time}, elapsed seconds= {elapsed}")
        logger.info(f"{SUCCESS_LOG_MESSAGE}")


def main() -> None:
    """Main function for executing a location aggregation task."""
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_burdenator_loc_agg()
    args = get_input_args.get_args_burdenator_loc_agg(parser)
    # access the location hierarchies from the cache
    data_container = DataContainer(
        {
            "location_set_id": args.location_set_id,
            "year_id": args.years,
            "sex_id": args.sex_id,
        },
        n_draws=args.n_draws,
        release_id=args.release_id,
        cache_dir=os.path.join(args.data_root, "cache"),
    )
    loctree_list = data_container[f"location_hierarchy_{args.location_set_id}"]
    logger.debug(f"Location Set ID: {args.location_set_id} and LocTree: {loctree_list}")

    loc_agg = LocationAggregator(
        year_ids=args.years,
        rei_id=args.rei_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        n_draws=args.n_draws,
        data_root=args.data_root,
        region_locs=args.region_locs,
        write_out_star_ids=args.write_out_star_ids,
    )
    loc_agg.run_aggregation(loctree_list)


if __name__ == "__main__":
    main()
