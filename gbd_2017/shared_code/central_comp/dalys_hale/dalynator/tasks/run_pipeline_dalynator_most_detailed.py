import argparse
import os.path
import logging
import time
from gbd import constants as gbd

from dalynator.write_summaries import write_summaries
from dalynator.age_aggr import AgeAggregator
from dalynator.app_common import best_version, strictly_positive_integer
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_summaries import MetricConverter
from dalynator.get_yld_data import get_como_folder_structure
from dalynator.data_container import DataContainer
from dalynator.data_sink import HDFDataSink
from dalynator.makedirs_safely import makedirs_safely
from dalynator.setup_logger import create_logger_in_memory
from dalynator.sex_aggr import SexAggregator

# Other programs look for this string.
# It should only be written ONCE to the log file, as the absolute last step
SUCCESS_LOG_MESSAGE = "DONE pipeline complete"

logger = logging.getLogger(
    "dalynator.tasks.run_pipeline_dalynator_most_detailed")


class DalynatorMostDetailed(object):
    """
    This is the first phase of the dalynator - calculate Dalys for
    most-detailed locations.
    """

    @classmethod
    def from_parser(cls, cli_args=None):
        """
        Factory method to create a DalynatorMostDetailed instance from an
        argument string.
        Uses the argparse package.

        Args:
            cli_args: If none then reads from sysargs

        Returns:

        """
        parser = DalynatorMostDetailed._construct_parser()
        args = parser.parse_args(cli_args)
        if args.cod_version is None:
            args.cod_version = best_version('cod', args.gbd_round_id)
        if args.epi_version is None:
            args.epi_version = best_version('como', args.gbd_round_id)
        args.output_draws_dir, args.log_dir, args.cache_dir, args.cod_dir, \
            args.epi_dir, args.output_file_name = \
            DalynatorMostDetailed._construct_extra_paths(
                input_data_root=args.input_data_root,
                out_dir=args.out_dir,
                cod_version=args.cod_version,
                epi_version=args.epi_version,
                location_id=args.location_id,
                year_id=args.year_id)
        return cls(
            output_draws_dir=args.output_draws_dir,
            output_file_name=args.output_file_name,
            log_dir=args.log_dir,
            cache_dir=args.cache_dir,
            cod_dir=args.cod_dir,
            epi_dir=args.epi_dir,
            gbd_round_id=args.gbd_round_id,
            location_id=args.location_id,
            year_id=args.year_id,
            n_draws=args.n_draws,
            version=args.version,
            no_sex_aggr=args.no_sex_aggr,
            no_age_aggr=args.no_age_aggr,
            turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,
            verbose=args.verbose)

    def __init__(self, output_draws_dir=None, output_file_name=None,
                 log_dir=None, cache_dir=None,
                 cod_dir=None, epi_dir=None, gbd_round_id=None,
                 location_id=None, year_id=None, n_draws=None, version=None,
                 no_sex_aggr=None, no_age_aggr=None,
                 turn_off_null_and_nan_check=None, verbose=None):
        self.output_draws_dir = output_draws_dir
        self.output_file_name = output_file_name
        self.log_dir = log_dir
        self.cache_dir = cache_dir
        self.cod_dir = cod_dir
        self.epi_dir = epi_dir
        self.gbd_round_id = gbd_round_id
        self.location_id = location_id
        self.year_id = year_id
        self.n_draws = n_draws
        self.version = version
        self.no_sex_aggr = no_sex_aggr
        self.no_age_aggr = no_age_aggr
        self.turn_off_null_and_nan_check = turn_off_null_and_nan_check
        self.verbose = verbose
        self.tool_name = "dalynator"

    @staticmethod
    def _construct_parser():
        parser = argparse.ArgumentParser(description='Dalynator most detailed')
        parser.add_argument('--input_data_root',
                            default='FILEPATH',
                            type=str, action='store',
                            help='The root directory of all data, '
                                 'useful for testing')

        parser.add_argument('-o', '--out_dir', type=str,
                            action='store',
                            help='The root directory for the output files, '
                                 'version will be appended.')

        parser.add_argument('--log_dir', type=str,
                            action='store',
                            help='The root directory for the log files, '
                                 'overrides the usual location '
                                 'Default is <out_dir>/log')

        parser.add_argument('-n', '--turn_off_null_and_nan_check',
                            action='store_true',
                            help='No input restriction for nulls and NaNs. '
                                 'Dangerous but necessary for older GBD years.'
                            )

        parser.add_argument('-v', '--verbose', action='store_true',
                            default=False,
                            help='print many debugging messages')

        parser.add_argument('--cod',
                            default=None,
                            type=int, action='store',
                            dest='cod_version',
                            help='The version of the cod results to use, '
                                 'an integer')

        parser.add_argument('--epi',
                            default=None,
                            type=int, action='store',
                            dest='epi_version',
                            help='The version of the epi/como results to use, '
                                 'an integer')

        parser.add_argument('-l', '--location_id',
                            type=int, action='store', required=True,
                            help='The location_id, an integer')

        parser.add_argument('-y', '--year_id',
                            type=int,
                            required=True,
                            help='The year_id, an integer')

        parser.add_argument('--n_draws', default=1000,
                            type=strictly_positive_integer, action='store',
                            help='The number of draw columns for all input '
                                 'and output draw files')

        parser.add_argument('--version', required=True,
                            type=int, action='store',
                            help='burdenator version number, used for default '
                                 'output dir and dbs meta-data')

        gbd_round_group = parser.add_mutually_exclusive_group(required=True)
        gbd_round_group.add_argument('-G', '--gbd_round',
                                     type=int, action='store',
                                     help='The gbd_round as a year, eg 2013')
        gbd_round_group.add_argument('-g', '--gbd_round_id',
                                     type=int, action='store',
                                     help='The gbd_round_id as a database ID, '
                                          'eg 4 (==2016)')

        parser.add_argument('--no_sex',
                            action='store_true', default=False,
                            help='Do not write sex aggregates to draw files '
                                 '(they will be computed and included in '
                                 'summaries)',
                            dest="no_sex_aggr")

        parser.add_argument('--no_age',
                            action='store_true', default=False,
                            help='Do not write age aggregates to draw files '
                                 '(they will be computed and included in '
                                 'summaries)',
                            dest="no_age_aggr")

        # Needed by mock_framework
        valid_tool_names = ["dalynator", "burdenator"]
        parser.add_argument('--tool_name',
                            type=str,
                            required=False,
                            choices=valid_tool_names,
                            help='The tool name')

        return parser

    @staticmethod
    def _construct_extra_paths(input_data_root=None, out_dir=None,
                               cod_version=None, epi_version=None,
                               location_id=None, year_id=None):
        """
        Create the paths to various files
        This just computes the paths, no directories or files are actually
        created.

        Args:
            input_data_root: root for cod and como files
            out_dir: the root directory for output, including the version
                number
            cod_version:
            epi_version:
            location_id:

         Returns:
            output_draws_dir, log_dir, cache_dir, cod_dir, epi_dir,
            output_file_name; as strings
        """
        # Are they using the default output dir?
        # Always append the version number
        top_out_dir = out_dir
        cache_dir = '{}/cache'.format(out_dir)
        log_dir = os.path.join(top_out_dir, 'log', str(location_id))
        output_draws_dir = os.path.join(top_out_dir, 'draws', str(location_id))

        cod_dir = "DIRECTORY"

        epi_dir = get_como_folder_structure(os.path.join(input_data_root,
                                                         'como',
                                                         str(epi_version)))

        output_file_name = os.path.join(output_draws_dir,
                                        "{}_{}_{}.h5"
                                        .format(gbd.measures.DALY, location_id,
                                                year_id))
        return (output_draws_dir, log_dir, cache_dir, cod_dir, epi_dir,
                output_file_name)

    def _prepare_with_external_side_effects(self):
        """
        Creates output directories, loggers.

        Returns:
            Nothing
        """
        makedirs_safely(self.output_draws_dir)
        makedirs_safely(self.log_dir)

        log_level = logging.DEBUG if self.verbose else logging.INFO
        _ = create_logger_in_memory("dalynator", log_level,
                                    self.log_dir +
                                    "/daly_{}_{}.log".format(self.location_id,
                                                             self.year_id))

    def run_pipeline(self):
        """
        Run the entire dalynator pipeline, computation and file creation
        Will throw ValueError if input files are not present.

        Returns
            The final dataframe
        """
        self._prepare_with_external_side_effects()
        df, existing_age_groups = self._compute_most_detailed_df()
        self._write_output_files(df, existing_age_groups=existing_age_groups)
        return df

    def _compute_most_detailed_df(self):
        """
        Computations only, does not write files. Makes testing easier.
        """

        start_time = time.time()
        logger.info("START location-year pipeline at {}".format(start_time))

        # Create a DataContainer
        data_container = DataContainer(
            {'location_id': self.location_id,
             'year_id': self.year_id},
            n_draws=self.n_draws,
            gbd_round_id=self.gbd_round_id,
            epi_dir=self.epi_dir,
            cod_dir=self.cod_dir,
            cache_dir=self.cache_dir,
            turn_off_null_and_nan_check=self.turn_off_null_and_nan_check)
        yll_df = data_container['yll']
        yld_df = data_container['yld']

        # Compute DALYs
        draw_cols = list(yll_df.filter(like='draw').columns)
        index_cols = list(set(yll_df.columns) - set(draw_cols))
        computer = ComputeDalys(yll_df, yld_df, draw_cols, index_cols)
        df = computer.get_data_frame()

        logger.info("DALY computation complete, df shape {}"
                    .format((df.shape)))
        logger.info(" input DF age_group_id {}"
                    .format(df['age_group_id'].unique()))

        draw_cols = list(df.filter(like='draw').columns)
        index_cols = list(set(df.columns) - set(draw_cols))
        existing_age_groups = df['age_group_id'].unique()

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

        logger.debug("new  DF age_group_id {}"
                     .format(df['age_group_id'].unique()))
        logger.info("  FINAL dalynator result shape {}".format(df.shape))
        end_time = time.time()
        elapsed = end_time - start_time
        logger.info("DONE location-year pipeline at {}, elapsed seconds= {}"
                    .format(end_time, elapsed))
        logger.info("{}".format(SUCCESS_LOG_MESSAGE))

        return df, existing_age_groups

    def _write_output_files(self, df=None, existing_age_groups=None):
        """
        Writes the results to files, no computations (except filtering)
        """

        # Calculate and write out the year summaries as CSV files
        draw_cols = list(df.filter(like='draw').columns)
        index_cols = list(set(df.columns) - set(draw_cols))

        # Write the summaries BEFORE we (potentially) drop the age and sex
        # aggregates
        csv_dir = self.output_draws_dir + '/upload/'
        write_summaries(self.location_id, self.year_id, csv_dir, df,
                        index_cols,
                        do_risk_aggr=False,
                        write_out_star_ids=False)

        # Adding any index-like column to the HDF index for later random access
        if self.no_sex_aggr:
            df = df[df['sex_id'] != gbd.sex.BOTH]

        if self.no_age_aggr:
            df = df[df['age_group_id'].isin(existing_age_groups)]

        sink = HDFDataSink(self.output_file_name,
                           complib="zlib", complevel=1)
        sink.write(df)
        logger.info("DONE write DF {}".format(time.time()))


def main():
    pipeline = DalynatorMostDetailed.from_parser()
    logger.info("Arguments passed to run_pipeline_dalynator_most_detailed: {}"
                .format(vars(pipeline)))
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()
