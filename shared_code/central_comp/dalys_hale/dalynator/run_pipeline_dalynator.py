import logging
import time
import gbd.constants as gbd

import dalynator.write_summaries as write_sum
import dalynator.get_input_args as get_input_args
from dalynator.age_aggr import AgeAggregator
from dalynator.compute_dalys import ComputeDalys
from dalynator.data_container import DataContainer
from dalynator.compute_summaries import MetricConverter
from dalynator.data_sink import HDFDataSink
from dalynator.sex_aggr import SexAggregator

# Other programs look for this string.
# It should only be written ONCE to the log file, as the absolute last step
SUCCESS_LOG_MESSAGE = "DONE pipeline complete"


# example run : python run_pipeline_dalynator.py --cod 43 --epi 96 -p 178 -l 168 -y 2005 -o. -n -v

# location_ids = [35639, 571]  # Random choice of leaves
#    year_ids = [2005, 2010]


def run_pipeline(args):

    """
    Run the entire dalynator pipeline. Typically called from run_all->qsub->run_remote_pipeline->here

    Will throw ValueError if input files are not present.

    TBD Refactor as a ComputationElement followed by a DataSink at the end
    :param args
    :return:
    """

    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info("START location-year pipeline at {}".format(start_time))

    # Create a DataContainer
    data_container = DataContainer(
        location_id=args.location_id,
        year_id=args.year_id,
        n_draws=args.n_draws,
        gbd_round_id=args.gbd_round_id,
        epi_dir=args.epi_dir,
        cod_dir=args.cod_dir,
        cache_dir=args.cache_dir,
        turn_off_null_and_nan_check=args.turn_off_null_and_nan_check)
    yll_df = data_container['yll']
    yld_df = data_container['yld']

    # Compute DALYs
    draw_cols = list(yll_df.filter(like='draw').columns)
    index_cols = list(set(yll_df.columns) - set(draw_cols))
    computer = ComputeDalys(yll_df, yld_df, draw_cols, index_cols)
    df = computer.get_data_frame()

    logger.info("DALY computation complete, df shape {}".format((df.shape)))
    logger.info(" input DF age_group_id {}".format(df['age_group_id'].unique()))

    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))
    existing_age_groups= df['age_group_id'].unique()

    logger.info("Preparing for sex aggregation")

    # Do sex aggregation
    my_sex_aggr = SexAggregator(df, draw_cols, index_cols)
    df = my_sex_aggr.get_data_frame()
    logger.info("Sex aggregation complete")

    # Do age aggregation
    my_age_aggr = AgeAggregator(df, draw_cols, index_cols,
                                data_container=data_container)
    df = my_age_aggr.get_data_frame()
    logger.info("Age aggregation complete")

    # Convert to rate and % space
    df = MetricConverter(df, to_rate=True, to_percent=True,
                         data_container=data_container).get_data_frame()

    logger.debug("new  DF age_group_id {}".format(df['age_group_id'].unique()))
    logger.info("  FINAL dalynator result shape {}".format(df.shape))

    # Calculate and write out the year summaries as CSV files
    draw_cols = list(df.filter(like='draw').columns)
    index_cols = list(set(df.columns) - set(draw_cols))

    csv_dir = args.out_dir + '/upload/'
    write_sum.write_summaries(args.location_id, args.year_id, csv_dir, df, index_cols, False, args.gbd_round_id)

    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE location-year pipeline at {}, elapsed seconds= {}".format(end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))

    # Adding any index-like column to the HDF index for later random access
    filename = get_input_args.calculate_output_filename(args.out_dir, gbd.measures.DALY, args.location_id, args.year_id)
    if args.no_sex_aggr:
        df = df[df['sex_id'] != gbd.sex.BOTH]

    if args.no_age_aggr:
        df = df[df['age_group_id'].isin(existing_age_groups)]

    sink = HDFDataSink(filename,
                       data_columns=[col for col in df if col.endswith("_id")],
                       complib="zlib", complevel=1)
    sink.write(df)
    logger.info("DONE write DF {}".format(time.time()))

    return df.shape


if __name__ == "__main__":

    parser = get_input_args.construct_args_dalynator()
    args = get_input_args.get_args_and_create_dirs(parser)

    logger = logging.getLogger(__name__)
    logger.debug("no_sex_aggr={}".format(args.no_sex_aggr))
    logger.debug("no_age_aggr={}".format(args.no_age_aggr))
    logger.debug("year_id= {}, location_id= {}".format(args.location_id, args.year_id))

    shape = run_pipeline(args)
    logger.debug(" shape= {}".format(shape))
