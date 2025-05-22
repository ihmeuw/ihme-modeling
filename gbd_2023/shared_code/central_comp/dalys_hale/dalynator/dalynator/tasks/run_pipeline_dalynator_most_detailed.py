import os.path
import logging
import time
from gbd import constants as gbd

from cluster_utils.loggers import create_logger_in_memory

from dalynator.write_summaries import write_summaries
from dalynator.lib.aggregation import calculate_age_aggregates, aggregate_sexes
from dalynator.lib.metric_conversion import convert_number_to_percent, convert_number_to_rate
from dalynator.lib.utils import get_index_draw_columns
from dalynator.compute_dalys import ComputeDalys
from dalynator.get_yld_data import get_como_folder_structure
from dalynator.data_container import DataContainer
from dalynator.data_sink import HDFDataSink
from dalynator.makedirs_safely import makedirs_safely
from dalynator import get_input_args
from dalynator import tool_objects as to


SUCCESS_LOG_MESSAGE = "DONE pipeline complete"

logger = logging.getLogger(
    "dalynator.tasks.run_pipeline_dalynator_most_detailed")


class DalynatorMostDetailed(object):
    """
    This is the first phase of the dalynator - calculate Dalys for
    most-detailed locations.
    The class has built-in MVC. The from_wire method is a Controller or Factory
    Method - it constructs a complete object from command line arguments.
    It has a couple of helper methods which are static:
      _construct_parser()
      _construct_extra_paths()

    The __init__ method takes ALL the arguments that are needed to construct a
    fully functional pipeline, no other method sets any other initial
    arguments. The run_pipeline method only sets variables that it needs
    temporarily during computation
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
        parser = get_input_args.construct_parser_dalynator()
        args = parser.parse_args(cli_args)

        args.output_draws_dir, args.log_dir, args.cache_dir, args.cod_dir, \
            args.cod_pattern, args.epi_dir, args.output_file_name = \
            DalynatorMostDetailed._construct_extra_paths(
                input_data_root=args.input_data_root,
                out_dir=args.out_dir,
                codcorrect_version=args.codcorrect_version,
                fauxcorrect_version=args.fauxcorrect_version,
                epi_version=args.epi_version,
                location_id=args.location_id,
                year_id=args.year_id)
        return cls(
            output_draws_dir=args.output_draws_dir,
            output_file_name=args.output_file_name,
            log_dir=args.log_dir,
            cache_dir=args.cache_dir,
            cod_dir=args.cod_dir,
            cod_pattern=args.cod_pattern,
            epi_dir=args.epi_dir,
            release_id=args.release_id,
            age_group_ids=args.age_group_ids,
            location_id=args.location_id,
            year_id=args.year_id,
            n_draws=args.n_draws,
            output_version=args.output_version,
            turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,
            verbose=args.verbose,
            age_group_set_id=args.age_group_set_id,
        )

    def __init__(self, output_draws_dir=None, output_file_name=None,
                 log_dir=None, cache_dir=None,
                 cod_dir=None, cod_pattern=None, epi_dir=None, release_id=None,
                 location_id=None, year_id=None, n_draws=None, output_version=None,
                 turn_off_null_and_nan_check=None, verbose=None,
                 age_group_ids=None, age_group_set_id=None):
        self.output_draws_dir = output_draws_dir
        self.output_file_name = output_file_name
        self.log_dir = log_dir
        self.cache_dir = cache_dir
        self.cod_dir = cod_dir
        self.cod_pattern = cod_pattern
        self.epi_dir = epi_dir
        self.release_id = release_id
        self.age_group_ids = age_group_ids
        self.location_id = location_id
        self.year_id = year_id
        self.n_draws = n_draws
        self.output_version = output_version
        self.turn_off_null_and_nan_check = turn_off_null_and_nan_check
        self.verbose = verbose
        self.tool_name = "dalynator"
        self.age_group_set_id = age_group_set_id

    @staticmethod
    def _construct_extra_paths(input_data_root=None, out_dir=None,
                               codcorrect_version=None, fauxcorrect_version=None,
                               epi_version=None, location_id=None,
                               year_id=None):
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
        # Always append the version number
        top_out_dir = out_dir
        cache_dir = '{}/cache'.format(out_dir)
        log_dir = os.path.join(top_out_dir, 'log_most_detailed',
                               str(location_id))
        output_draws_dir = os.path.join(top_out_dir, 'draws', str(location_id))

        cod_object = to.cod_or_faux_correct(
            input_data_root,
            codcorrect_version=codcorrect_version,
            fauxcorrect_version=fauxcorrect_version)
        cod_dir = cod_object.abs_path_to_draws
        cod_pattern = cod_object.file_pattern

        epi_dir = get_como_folder_structure(os.path.join(input_data_root,
                                                         'como',
                                                         str(epi_version)))

        output_file_name = os.path.join(output_draws_dir,
                                        "{}_{}_{}.h5"
                                        .format(gbd.measures.DALY, location_id,
                                                year_id))
        return (output_draws_dir, log_dir, cache_dir, cod_dir, cod_pattern, epi_dir,
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
                                                             self.year_id),
                                    ['aggregator.aggregators', 'jobmon'])

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
        """Computations only, does not write files. Makes testing easier."""
        start_time = time.time()
        logger.info("START location-year pipeline at {}".format(start_time))

        # Create a DataContainer
        data_container = DataContainer(
            {'location_id': self.location_id,
             'year_id': self.year_id},
            n_draws=self.n_draws,
            release_id=self.release_id,
            age_group_ids=self.age_group_ids,
            epi_dir=self.epi_dir,
            cod_dir=self.cod_dir,
            cod_pattern=self.cod_pattern,
            cache_dir=self.cache_dir,
            turn_off_null_and_nan_check=self.turn_off_null_and_nan_check)
        yll_df = data_container['yll']
        yld_df = data_container['yld']

        # Compute DALYs
        index_cols, draw_cols = get_index_draw_columns(yll_df)
        computer = ComputeDalys(yll_df, yld_df, draw_cols, index_cols)
        df = computer.get_data_frame()

        logger.info("DALY computation complete, df shape {}"
                    .format((df.shape)))
        logger.info(" input DF age_group_id {}"
                    .format(df['age_group_id'].unique()))

        existing_age_groups = df['age_group_id'].unique()

        logger.info("Preparing for sex aggregation")

        # Do sex aggregation
        df = aggregate_sexes(df)
        logger.info("Sex aggregation complete")

        # Do age aggregation
        df = calculate_age_aggregates(
            data_frame=df,
            extra_aggregates=self.age_group_ids,
            data_container=data_container,
            age_group_set_id=self.age_group_set_id,
        )
        logger.info("Age aggregation complete")

        # Convert to rate and % space
        df = convert_number_to_rate(df=df, pop_df=data_container["pop"], include_pre_df=True)
        df = convert_number_to_percent(df=df, include_pre_df=True)

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
        """Writes the results to files, no computations (except filtering)"""
        # Calculate and write out the year summaries as CSV files
        index_cols, draw_cols = get_index_draw_columns(df)

        # Write the summaries BEFORE we (potentially) drop the age and sex
        # aggregates
        csv_dir = self.output_draws_dir + '/upload/'
        write_summaries(self.location_id, self.year_id, csv_dir, df,
                        index_cols,
                        do_risk_aggr=False,
                        write_out_star_ids=False)

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
