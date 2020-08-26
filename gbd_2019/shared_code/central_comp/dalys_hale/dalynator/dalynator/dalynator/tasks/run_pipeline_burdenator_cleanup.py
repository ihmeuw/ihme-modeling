import glob
import logging
import os
import time
import sys

import gbd.constants as gbd
import pandas as pd

from cluster_utils.pandas_utils import get_index_columns

from dalynator import get_input_args
from dalynator import write_summaries as write_sum
from dalynator.age_aggr import AgeAggregator
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_summaries import MetricConverter
from dalynator.data_container import DataContainer, add_star_id, \
    remove_unwanted_stars
from dalynator.data_sink import HDFDataSink
from dalynator.sex_aggr import SexAggregator
from dalynator.tasks.run_pipeline_burdenator_most_detailed import \
    back_calc_pafs, MPGlobals

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

logger = logging.getLogger("dalynator.tasks.run_pipeline_burdenator_cleanup")


def run_burdenator_cleanup(out_dir, location_id, year_id, n_draws, measure_id,
                           cod_dir, cod_pattern, epi_dir,
                           turn_off_null_and_nan_check, gbd_round_id,
                           decomp_step, write_out_star_ids,
                           cache_dir, dual_upload):
    """Take a set of aggregated results and reformat them into draws consistent
    with the most-detailed location draws.

    Args:
        out_dir (str): the root directory for this burdenator run
        location_id (int): location_id of the aggregate location
        year_id (int): year of the aggregate location
        n_draws (int): the number of draw columns in the H5 data frames,
            greater than zero
        measure_id (int): measure_id of the aggregate location
        cod_dir (str): directory where the cause-level envelope for
            cod (CoDCorrect) files are stored
        cod_pattern (str): file pattern for accessing CoD-or-FauxCorrect
            draws.  Example: '{measure_id}_{location_id}.h5'
        epi_dir (str): directory where the cause-level envelope for
            epi (COMO) files are stored
        turn_off_null_and_nan_check (bool): Disable checks for NaNs and Nulls
        write_out_star_ids (bool): If true, include star_ids in output
        draw files and CSV upload files
        dual_upload (bool): If True upload to column store as well
            as the gbd database.  Currently not implemented.
    """
    MPGlobals.logger = logger
    start_time = time.time()
    logger.info("START pipeline burdenator cleanup at {}".format(start_time))
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.warning('is when this event was logged.')

    # Get aggregated draws
    logger.info("start append files, time = {}".format(time.time()))
    draw_dir = os.path.join(out_dir, 'draws')
    aggregated_draw_dir = os.path.join(out_dir, 'loc_agg_draws')
    # df contains Attribute Burden, which is in Number space.
    # It is a subset of the total count for the parent metric,
    # ie AB of YLL's for a cause attributable to a risk
    # (or to all known & unknown risks, ie rei_id == 0)

    # df is a list of data frames
    df = []
    for metric in ['burden']:
        input_file_pattern = ('FILEPATH')
        logger.debug("Cleanup file pattern {}".format(
            input_file_pattern.format(root=aggregated_draw_dir, metric=metric,
                                      location_id=location_id, year_id=year_id,
                                      measure_id=measure_id)))
        draw_files = glob.glob(input_file_pattern.format(
            root=aggregated_draw_dir, metric=metric, location_id=location_id,
            year_id=year_id, measure_id=measure_id))
        for f in draw_files:
            logger.info("appending {}".format(f))
            this_df = pd.read_hdf('{}'.format(f))
            dups = this_df[this_df.filter(like='_id').columns
                           ].duplicated().any()
            if dups:
                msg = ("Duplicates found in location aggregate output "
                       "file {}. Failing this cleanup job".format(f))
                logger.error(msg)
                raise RuntimeError(msg)
            df.append(this_df)
    df = pd.concat(df)
    logger.info("append files complete, time = {}".format(time.time()))
    logger.info("columns appended df {}".format(get_index_columns(df)))
    add_star_id(df)

    # Get cause envelope
    data_container = DataContainer(
        {'location_id': location_id,
         'year_id': year_id},
        n_draws=n_draws,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        cod_dir=cod_dir,
        cod_pattern=cod_pattern,
        epi_dir=epi_dir,
        turn_off_null_and_nan_check=turn_off_null_and_nan_check,
        cache_dir=cache_dir)
    MPGlobals.data_container = data_container

    # cause_env_df has all-cause mortality/whatever, without risks
    if measure_id == gbd.measures.DEATH:
        cause_env_df = data_container['death']
    elif measure_id == gbd.measures.YLL:
        cause_env_df = data_container['yll']
    elif measure_id == gbd.measures.YLD:
        cause_env_df = data_container['yld']
    elif measure_id == gbd.measures.DALY:
        # Get YLLs and YLDs
        yll_df = data_container['yll']
        yld_df = data_container['yld']
        yld_df = yld_df.loc[yld_df.measure_id == gbd.measures.YLD]
        # Compute DALYs
        draw_cols = list(yld_df.filter(like='draw').columns)
        index_cols = list(set(yld_df.columns) - set(draw_cols))
        daly_ce = ComputeDalys(yll_df, yld_df, draw_cols, index_cols)
        cause_env_df = daly_ce.get_data_frame()

    cause_env_df['rei_id'] = gbd.risk.TOTAL_ATTRIBUTABLE
    cause_env_df['star_id'] = gbd.star.ANY_EVIDENCE_LEVEL

    # Concatenate cause envelope with data

    most_detailed_age_groups = MetricConverter.get_detailed_ages()
    df = pd.concat([df, cause_env_df], sort=True)
    df = df.loc[((df['sex_id'].isin([gbd.sex.MALE, gbd.sex.FEMALE])) &
                (df['age_group_id'].isin(most_detailed_age_groups)) &
                (df['metric_id'] == gbd.metrics.NUMBER))]

    # Do sex aggregation
    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))
    logger.info("start aggregating sexes, time = {}".format(time.time()))
    my_sex_aggr = SexAggregator(df, draw_cols, index_cols)
    df = my_sex_aggr.get_data_frame()
    logger.info("aggregating ages sexes, time = {}".format(time.time()))

    # Do age aggregation
    logger.info("start aggregating ages, time = {}".format(time.time()))
    my_age_aggr = AgeAggregator(df, draw_cols, index_cols,
                                data_container=data_container)
    df = my_age_aggr.get_data_frame()
    logger.info("aggregating ages complete, time = {}".format(time.time()))

    # Convert to rate space
    logger.info("start converting to rates, time = {}".format(time.time()))
    df = MetricConverter(df, to_rate=True,
                         data_container=data_container).get_data_frame()
    logger.info("converting to rates complete, time = {}".format(time.time()))

    # df does not contain AB's any more, because they are RATES

    # Back-calculate PAFs
    logger.info("start back-calculating PAFs, time = {}".format(time.time()))
    to_calc_pafs = ((df['metric_id'] == gbd.metrics.NUMBER) |
                    (df['age_group_id'] == gbd.age.AGE_STANDARDIZED))
    pafs_df = df.loc[to_calc_pafs].copy(deep=True)

    # back_calc_pafs is part of the most detailed pipeline, reused from here.
    pafs_df = back_calc_pafs(pafs_df, n_draws)
    df = pd.concat([df, pafs_df], sort=True)
    logger.info("back-calculating PAFs complete, time = {}"
                .format(time.time()))

    # Calculate and write out summaries as CSV files
    csv_dir = "FILEPATH".format(draw_dir, location_id)
    write_sum.write_summaries(location_id, year_id, csv_dir, df, index_cols,
                              do_risk_aggr=True,
                              write_out_star_ids=write_out_star_ids,
                              dual_upload=dual_upload)

    # Save draws
    df = df.loc[((df['sex_id'].isin([gbd.sex.MALE, gbd.sex.FEMALE])) &
                (df['age_group_id'].isin(most_detailed_age_groups)) &
                (df['metric_id'].isin([gbd.metrics.NUMBER,
                                      gbd.metrics.PERCENT])))]
    logger.info("start saving draws, time = {}".format(time.time()))
    output_file_pattern = ('FILEPATH')
    output_file_path = output_file_pattern.format(
        location_id=location_id, year_id=year_id, measure_id=measure_id)
    filename = "FILEPATH".format(draw_dir, output_file_path)
    remove_unwanted_stars(df, write_out_star_ids=write_out_star_ids)
    sink = HDFDataSink(filename,
                       complib="zlib",
                       complevel=1)
    sink.write(df)
    logger.info("saving output draws complete, time = {}".format(time.time()))

    # End log
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE cleanup pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("FILEPATH".format(SUCCESS_LOG_MESSAGE))


def main():
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_burdenator_cleanup()
    args = get_input_args.construct_args_burdenator_cleanup(parser)

    run_burdenator_cleanup(args.out_dir, args.location_id, args.year_id,
                           args.n_draws, args.measure_id, args.cod_dir,
                           args.cod_pattern, args.epi_dir,
                           args.turn_off_null_and_nan_check,
                           args.gbd_round_id, args.decomp_step,
                           args.write_out_star_ids, args.cache_dir,
                           args.dual_upload)


if __name__ == "__main__":
    main()
