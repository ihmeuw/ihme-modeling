"""
save_birth_estimates.py is a module that pulls and saves
covariate estimates for live births. It filters covariate estimates,
aggregates them for location_sets (specified in mmr_constants),
and saves them, by key='year_{year_id}', in file location_set_id.h5,
in the constants dir.
"""
import argparse
import logging
import pathlib
from typing import List, Set, Tuple

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from draw_sources.draw_sources import DrawSource, DrawSink
from db_queries import get_covariate_estimates, get_location_metadata
from db_tools import ezfuncs
from gbd import decomp_step as gbd_decomp_step
from gbd import constants as gbd_constants
from hierarchies import dbtrees
import pandas as pd

import mmr_constants


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--process-vers', type=str, required=True)
    parser.add_argument('--gbd-round-id', type=int, required=True)
    parser.add_argument('--decomp-step', type=str, required=True)
    return parser.parse_args()


def main(
        gbd_round_id: int,
        decomp_step: str,
        process_vers: str,
        args: argparse.Namespace,
        location_set_ids: List[int] = mmr_constants.AGGREGATE_LOCATION_SET_IDS,
) -> None:
    """main method for creation of location aggregated live births"""

    constants_path = (
        pathlib.Path(mmr_constants.OUTDIR) / process_vers / 'constants')

    cov_estimate_filename_list = []
    for location_set_id in mmr_constants.AGGREGATE_LOCATION_SET_IDS:
        res = (location_aggregate_birth_counts(
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            constants_path=constants_path,
            location_set_id=location_set_id))
        cov_estimate_filename_list += res

    collate_and_save_birth_counts(
        cov_estimate_filename_list=cov_estimate_filename_list,
        constants_path=constants_path)


def location_aggregate_birth_counts(
        gbd_round_id: int,
        decomp_step: str,
        constants_path: pathlib.PosixPath,
        location_set_id: int
) -> None:
    """
    for given gbd_round, decomp_step, location_set_id, get a complete
    set of location-aggregated live births
    """

    logger.info(f'aggregating for location_set_id {location_set_id}')
    multiple_tree_flag = (
        location_set_id in mmr_constants.MULTIPLE_ROOT_LOCATION_SET_IDS)

    scalars = get_regional_scalars(gbd_round_id, decomp_step)
    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']

    cov_estimate_filename = (
        mmr_constants.COV_ESTIMATES_FORMAT_FILENAME.format(
            location_set_id))
    
    region_locs, most_detailed_locs = get_location_level_sets(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        location_set_id=location_set_id)

    save_birth_count_estimates(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        cov_estimate_filepath=constants_path / cov_estimate_filename,
        location_set_id=location_set_id,
        most_detailed_locs=most_detailed_locs)

    loc_trees = dbtrees.loctree(
        location_set_id=location_set_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        return_many=multiple_tree_flag)
    if not multiple_tree_flag:
        loc_trees = [loc_trees]

    draw_source = DrawSource(
        params={
            'draw_dir': str(constants_path),
            'file_pattern': cov_estimate_filename})

    i = 1
    output_filenames = []
    for loc_tree in loc_trees:
        output_filename = f'{location_set_id}_{i}.h5'
        i += 1
        draw_sink = DrawSink(
            params={
                'draw_dir': str(constants_path),
                'file_pattern': output_filename})
        draw_sink.add_transform(
            _apply_regional_scalars,
            regional_scalars_df=scalars.query('location_id in @region_locs'),
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step)

        op = Sum(
            index_cols=[s for s in index_cols if s != 'location_id'],
            value_cols=[mmr_constants.Columns.LIVE_BIRTH_VALUE_COL])

        AggSynchronous(
            draw_source=draw_source,
            draw_sink=draw_sink,
            index_cols=[s for s in index_cols if s != 'location_id'],
            aggregate_col='location_id',
            operator=op
        ).run(
            loc_tree,
            include_leaves=True
        )

        output_filenames.append(output_filename)

    return output_filenames


def collate_and_save_birth_counts(
        cov_estimate_filename_list: pathlib.PosixPath,
        constants_path: pathlib.PosixPath
):
    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']

    df = pd.concat(
        [pd.read_hdf(constants_path / filename)
         for filename in cov_estimate_filename_list]
    ).drop_duplicates(subset=index_cols)

    if (constants_path / mmr_constants.ALL_LIVE_BIRTHS_FILENAME).exists():
        (constants_path / mmr_constants.ALL_LIVE_BIRTHS_FILENAME).unlink()

    for year_id in mmr_constants.OUTPUT_YEARS:
        df.query('year_id == @year_id').to_hdf(
            constants_path / mmr_constants.ALL_LIVE_BIRTHS_FILENAME,
            index=False,
            key=mmr_constants.ALL_LIVE_BIRTHS_FORMAT_KEY.format(year_id))


def save_birth_count_estimates(
        gbd_round_id: int,
        decomp_step: str,
        cov_estimate_filepath: pathlib.PosixPath,
        location_set_id: int,
        most_detailed_locs: Set[int]
) -> None:
    """
    we need to pull covariate estimates for each unique location_id,
    that's where save_birth_count_estimates comes in
    """

    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']

    df = get_covariate_estimates(
        mmr_constants.LIVE_BIRTHS_COVARIATE_ID,
        decomp_step=decomp_step,
        gbd_round_id=gbd_round_id,
        year_id=mmr_constants.OUTPUT_YEARS,
        age_group_id=mmr_constants.ALL_MOST_DETAILED_AGE_GROUP_IDS,
        location_set_id=location_set_id
    )

    # Because covariate is tagged to sex of baby but MMR numerator is only
    # females, we want aggregate sex and assign to female sex_id
    df.loc[:, 'sex_id'] = 2
    df = df.loc[
        :, index_cols + [mmr_constants.Columns.LIVE_BIRTH_VALUE_COL]
    ].groupby(index_cols).sum().reset_index()

    df = _filter_to_most_detailed_locs(df, most_detailed_locs)
    df.to_csv(cov_estimate_filepath, index=False)


def get_regional_scalars(
        gbd_round_id: int,
        decomp_step: str,
) -> pd.DataFrame:
    """
    Previous iterations of scalars produced sex- and age-specific
    results. Starting GBD2017 scalars are produced with
    location/year-specific detail, at the both-sex and all-age level.
    """
    q = """
        SELECT * FROM mortality.upload_population_scalar_estimate 
        WHERE run_id = (
            SELECT run_id
            FROM mortality.vw_decomp_process_version
            WHERE process_id = 23
            AND gbd_round_id = :gbd_round_id
            AND decomp_step_id = :decomp_step_id
            AND year_id IN :year_ids
            AND is_best = 1)
        """
    params = {
        "gbd_round_id": gbd_round_id,
        "decomp_step_id": gbd_decomp_step.decomp_step_id_from_decomp_step(
            decomp_step, gbd_round_id),
        "year_ids": mmr_constants.OUTPUT_YEARS
    }
    scalars = ezfuncs.query(q, parameters=params, conn_def='mortality')
    scalars = scalars.rename(columns={'mean': 'scaling_factor'})
    scalars = scalars[['location_id', 'year_id', 'scaling_factor']]
    return scalars


def get_location_level_sets(
        gbd_round_id: int,
        decomp_step: str,
        location_set_id: int,
) -> Tuple[Set[int], Set[int]]:
    locs = get_location_metadata(
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        location_set_id=location_set_id
    )
    regions = set(
        locs.query('level == 2').location_id.unique()
    )
    most_detailed = (
        locs.query('most_detailed == 1').location_id.unique().tolist()
    )

    return (regions, set(most_detailed))


def _filter_to_most_detailed_locs(
        df: pd.DataFrame,
        most_detailed_locs: Set[int]
):
    return df.query('location_id in @most_detailed_locs')


def _apply_regional_scalars(
        df: pd.DataFrame,
        regional_scalars_df: pd.DataFrame,
        gbd_round_id: int,
        decomp_step: str,
) -> pd.DataFrame:
    """Transform function to apply regional scalars to draws."""

    # Apply scalars where applicable.
    # Multiply by 1.0 when scalars aren't present.

    join_cols = ['year_id', 'location_id']
    return (
        pd.merge(df, regional_scalars_df, how='left', on=join_cols)
        .fillna(1.0)
        .pipe(_apply_regional_scalars_helper)
        .drop('scaling_factor', axis=1)
    )


def _apply_regional_scalars_helper(df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function that multiplies covariate value column by a
    regional scalar.
    """
    df.loc[:, mmr_constants.Columns.LIVE_BIRTH_VALUE_COL] = (
        df[
            mmr_constants.Columns.LIVE_BIRTH_VALUE_COL
        ].mul(df['scaling_factor'], axis=0))
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    args = parse_args()
    main(args.gbd_round_id, args.decomp_step, args.process_vers, args)
