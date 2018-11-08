import argparse
import logging
import os
import time
import pickle

from dalynator import app_common as ac

from dalynator import DalynatorJobSwarm as djs
from dalynator import makedirs_safely as mkds
from dalynator.setup_logger import create_logger_in_memory


class DalynatorApplication(object):

    def _construct_base_parser(self, tool_name):
        self.parser = argparse.ArgumentParser(description='Compute DALYS')

        self.parser.add_argument('-r', '--resume', action='store_true',
                                 default=False,
                                 help='Resume an existing run - do not '
                                      'overwrite existing output files')

        self.parser.add_argument('-o', '--out_dir', type=str,
                                 action='store',
                                 dest="out_dir_without_version",
                                 help='The root directory for the output '
                                      'files, version will be appended.')

        self.parser.add_argument('--version', required=True,
                                 type=int, action='store',
                                 help='dalynator version number, used for '
                                      'default output dir and dbs meta-data')

        self.parser.add_argument('-v', '--verbose', action='store_true',
                                 default=False,
                                 help='print many debugging messages')

        if tool_name == 'dalynator':
            choices = ['most_detailed', 'pct_change', 'upload']
        else:
            choices = ['most_detailed', 'loc_agg', 'cleanup', 'pct_change',
                       'upload']
        self.parser.add_argument('--start_at', type=str,
                                 choices=choices,
                                 help='Which phase to start with: options {}'
                                 .format(choices))

        self.parser.add_argument('--end_at', type=str,
                                 choices=choices,
                                 help='Which phase to end with: options are {}'
                                 .format(choices))

        self.parser.add_argument('--raise_on_paf_error', action='store_true',
                            default=False,
                            help='Raise if aggregate causes are found in PAFs')

        self.parser.add_argument('-x', '--do_not_execute', action='store_true',
                                 default=False,
                                 help='Do not execute when flag raised')

    def _add_non_resume_args_to_parser(self):
        """
        The parse for run_all_dalynator, nothing shared with other parsers.
        However, this is reused (by explicit delegation) from
        run_all_burdenator.

        :return: parser
        """
        self.parser.add_argument('--input_data_root',
                                 default='DIRECTORY',
                                 type=str, action='store',
                                 help='The root directory of all input data, '
                                      'useful for testing')

        self.parser.add_argument('--cod',
                                 default=None,
                                 type=int, action='store',
                                 dest='cod_version',
                                 help='The version of the cod results to use, '
                                      'an integer')

        self.parser.add_argument('--epi',
                                 default=None,
                                 type=int, action='store',
                                 dest='epi_version',
                                 help='The version of the epi/como results to '
                                      'use, an integer')

        gbd_round_group = self.parser.add_mutually_exclusive_group(
            required=True)
        gbd_round_group.add_argument('-G', '--gbd_round',
                                     type=int, action='store',
                                     help='The gbd_round as a year, eg 2013')
        gbd_round_group.add_argument('-g', '--gbd_round_id',
                                     type=int, action='store',
                                     help='The gbd_round_id as a database ID, '
                                           'eg 4 (==2016)')

        self.parser.add_argument('--no_sex',
                                 action='store_true', default=False,
                                 help='Do not write sex aggregates to draw '
                                      'files (they will be computed and '
                                      'included in summaries)',
                                 dest="no_sex_aggr")

        self.parser.add_argument('--no_age',
                                 action='store_true', default=False,
                                 help='Do not write age aggregates to draw '
                                      'files (they will be computed and '
                                      'included in summaries)',
                                 dest="no_age_aggr")

        self.parser.add_argument('--log_dir', type=str,
                                 action='store',
                                 help='The root directory for the log files, '
                                      'overrides the usual location '
                                      'Default is <out_dir>/log')

        self.parser.add_argument('-n', '--turn_off_null_and_nan_check',
                                 action='store_true',
                                 help='No input restriction for nulls and '
                                      'NaNs. Dangerous but necessary for '
                                      'older GBD years.')

        self.parser.add_argument('--upload_to_test', action='store_true',
                                 default=False,
                                 help='Upload data to test environment')

        location_group = self.parser.add_mutually_exclusive_group(
            required=True)

        location_group.add_argument('--location_set_id',
                                    type=int, action='store',
                                    help='The location_set_id, an integer')

        location_group.add_argument('--location_set_version_id',
                                    type=int, action='store',
                                    help='The location_set_version_id, '
                                         'an integer')

        self.parser.add_argument('--add_agg_loc_set_ids', nargs='+',
                                 type=int, default=[],
                                 help=('EXPERTS ONLY. Additional location sets.'
                                       'DALYNATOR: The dalynator does NOT perform any location aggregation,'
                                       'this flag is used purely for upload.'
                                       'BURDENATOR: Additional location '
                                       'sets to apply to location aggregation '
                                       'phase onward. NOTE: If the sets have '
                                       'overlapping aggregate-level (i.e. '
                                       'non-leaf) locations, a race condition '
                                       'may be introduced where multiple '
                                       'aggregation jobs attempt to write to '
                                       'the same file. Further, additional '
                                       'aggregation sets are not used for the '
                                       'most detailed phase. Therefore all '
                                       'their detailed location data must '
                                       'have been created by the primary '
                                       'location set.'
                                 ))

        self.parser.add_argument('-y1', '--year_ids_1', type=int, nargs='+',
                                 default=[1990, 1995, 2000, 2005, 2010, 2016],
                                 action='store',
                                 help='The first set of year_ids, a '
                                      'space-separated list of integers')
        self.parser.add_argument('--n_draws_1', default=1000,
                                 type=ac.strictly_positive_integer,
                                 action='store',
                                 help='The number of draw columns in year-set '
                                      '1, with possible resampling')

        self.parser.add_argument('-y2', '--year_ids_2', type=int, nargs='+',
                                 default=[],
                                 action='store',
                                 help='The second set of year_ids, a '
                                      'space-separated list of integers')
        self.parser.add_argument('--n_draws_2', default=100,
                                 type=ac.strictly_positive_integer,
                                 action='store',
                                 help='The number of draw columns in year-set '
                                      '2, with possible resampling')

        self.parser.add_argument('-s', '--sge_project', type=str,
                                 action='store',
                                 help='The SGE project, default is '
                                      'proj_<tool_name>')

        self.parser.add_argument('-N', '--do_nothing', action='store_true',
                                 help="Report the jobs that would be qsub'ed, "
                                      "but do not qsub them")

        self.parser.add_argument('--start_years',
                                 type=int, nargs='+',
                                 default=[],
                                 dest="start_year_ids",
                                 help='The start years for pct change '
                                      'calculation, a space-separated list')
        self.parser.add_argument('--end_years',
                                 type=int, nargs='+',
                                 default=[],
                                 dest="end_year_ids",
                                 help='The end years for pct change '
                                      'calculation, a space-separated list')
        return self.parser

    def _construct_extra_paths(self, out_dir_without_version, log_dir,
                               tool_name, version):
        """
        Create the paths to out_dir, log_dir, cache_dir.
        This just computes the paths, no directories are actually created.

        Args:
            out_dir_without_version:   the root directory WITHOUT the version
                number
            log_dir:  The value of the --log_dir argument
            tool_name: dalynator or burdenator
            version: The dalynator or burdenator version

         Returns:
            out_dir, log_dir, cache_dir n as strings
        """
        # Always append the version number
        if not out_dir_without_version:
            out_dir = 'DIRECTORY'
        else:
            out_dir = '{}/{}'.format(out_dir_without_version, version)

        # Did they override the log directory?
        if not log_dir:
            log_dir = out_dir + "/log"

        cache_dir = '{}/cache'.format(out_dir)

        return out_dir, log_dir, cache_dir

    def _construct_directories(self, out_dir, log_dir, cache_dir, resume):
        """
        Create the output directory and the run_all logger. Used by both
        burdenator and dalynator.
        Check that both directories are empty. If they are not-empty then only
        continue if we are in resume mode.

        :param out_dir:  The root directory WITH the version number
        :param log_dir:  The path to the log directory
        :param cache_dir: The path to the cache directory
        :param resume: True if this is running in resume mode
        """
        if os.path.isdir(out_dir):
            if os.listdir(out_dir) and not resume:
                raise ValueError(
                    "Output directory {} contains files and NOT running in "
                    "resume mode".format(out_dir))

        if os.path.isdir(log_dir):
            if os.listdir(log_dir) and not resume:
                raise ValueError("Log directory {} contains files and NOT "
                                 "running in resume mode".format(log_dir))

        mkds.makedirs_safely(out_dir)
        mkds.makedirs_safely(log_dir)
        mkds.makedirs_safely(cache_dir)
        if resume:
            # If resuming then rotate (rename) the main log, daly_run_all.log
            self._rotate_logs(out_dir, log_dir)
        stderr_dir = os.path.join(out_dir, "stderr")
        mkds.makedirs_safely(stderr_dir)

    def _rotate_logs(self, out_dir, log_dir):
        """
        Move the existing daly_run_all.log and the stderr directories to be
        timestamped versions.
        Useful during resume, so that we don't keep appending to the same log.

        :param out_dir:  The root directory WITH the version number
        :param log_dir: The path to the log directory
        """
        t = time.localtime()
        time_stamp = "{}-{:02d}-{:02d}_{:02d}:{:02d}:{:02d}". \
            format(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min,
                   t.tm_sec)
        main_log = os.path.join(log_dir, "daly_run_all.log")
        if os.path.exists(main_log):
            os.rename(main_log, "{}.{}".format(main_log, time_stamp))

        stderr_dir = os.path.join(out_dir, "stderr")
        if os.path.exists(stderr_dir):
            os.rename(stderr_dir, "{}.{}".format(stderr_dir, time_stamp))
            # And re-recreate the normal stderr directory just to be sure
        mkds.makedirs_safely(stderr_dir)

    def _create_logger(self, out_dir, log_dir, verbose, resume):
        """
        Create the logger object, and rotate the logs
        :param out_dir:  The root directory WITH the version number
        :param log_dir: The path to the log directory
        :param verbose:  The verbose flag. If True, run the logger at
            DEBUG level
        :param resume: True if this is running in resume mode
        :return:
        """

        log_level = logging.DEBUG if verbose else logging.INFO
        create_logger_in_memory("dalynator", log_level,
                                log_dir + "/daly_run_all.log")

    def load_args_from_file(self, args, tool_name):
        if not args[0].out_dir_without_version:
            raise ValueError("In Resume Mode, must pass the root path to your "
                             "output directory")
        cache_file = ("{}/{}/cache/cli_args.pickle"
                      .format(args[0].out_dir_without_version,
                              args[0].version))
        if not os.path.exists(cache_file):
            raise RuntimeError("Nator has been run in --resume mode, but "
                               "no {} file exists".format(cache_file))
        with open(cache_file, "rb") as f:
            file_args = pickle.load(f)
        # overwrite the few defined arguments that can be different in a
        # resume case
        resume = args[0]
        if resume.start_at:
            file_args.start_at = resume.start_at
        if resume.end_at:
            file_args.end_at = resume.end_at
        if resume.verbose:
            file_args.verbose = resume.verbose
        file_args.resume = resume.resume
        return file_args

    def write_args_to_file(self, args):
        cache_file = "{}/cli_args.pickle".format(args.cache_dir)
        with open(cache_file, "wb") as f:
            pickle.dump(args, f)

    def set_phase_defaults(self, args):
        if not args.start_at:
                args.start_at = 'most_detailed'
        if not args.end_at:
            args.end_at = 'pct_change'
        return args

    def parse(self, tool_name="dalynator", cli_args=None):
        """
        Perform ALL argument parsing - argparse parsing, setting of defaults,
        checking of argument consistency. NO side effects in the file system.

        Args:
            tool_name: 'dalynator' or 'burdenator'
            cli_args: None or a list of strings. If None then read use sys.argv
        """
        self._construct_base_parser(tool_name)
        args = self.parser.parse_known_args(cli_args)
        if args[0].resume:
            print("Nator run in resume mode. Reading args from file")
            args = self.load_args_from_file(args, tool_name)
            args = self.set_phase_defaults(args)
            return args
        self._add_non_resume_args_to_parser()
        args = self.parser.parse_args(cli_args)
        args = self.set_phase_defaults(args)
        args.tool_name = tool_name

        args.gbd_round, args.gbd_round_id = ac.populate_gbd_round_args(
            args.gbd_round, args.gbd_round_id)

        if args.cod_version is None:
            args.cod_version = ac.best_version('cod', args.gbd_round_id)
        if args.epi_version is None:
            args.epi_version = ac.best_version('como', args.gbd_round_id)

        args.location_set_version_id = ac.location_set_to_location_set_version(
            args.location_set_id, args.location_set_version_id, args.gbd_round)
        args.sge_project = ac.create_sge_project(args.sge_project,
                                                 args.tool_name)
        year_n_draws_map = ac.construct_year_n_draws_map(
            args.year_ids_1, args.n_draws_1, args.year_ids_2, args.n_draws_2)
        ac.validate_multi_mode_years(
            year_n_draws_map, args.year_ids_1, args.n_draws_1,
            args.year_ids_2, args.n_draws_2, args.start_year_ids,
            args.end_year_ids)

        args.out_dir, args.log_dir, args.cache_dir = (
            self._construct_extra_paths(args.out_dir_without_version,
                                        args.log_dir, args.tool_name,
                                        args.version))
        return args

    def prepare_with_side_effects(self, out_dir=None, log_dir=None,
                                  cache_dir=None, verbose=None, resume=None):
        """
        Has side effects - creates files and directories, initializes loggers.
        No parsing or other manipulation of the arguments. You probably do not
        want to call this from a unit test.
        :return:
        """
        self._construct_directories(out_dir, log_dir, cache_dir, resume)
        self._create_logger(out_dir, log_dir, verbose, resume)

    def parse_and_initialize(self, cli_args=None):
        """
        Convenience for testing
        :param args_strings:  None or a list of strings. If None, use sys.argv
        :return:
        """
        args = self.parse(cli_args=cli_args)
        self.prepare_with_side_effects(args.out_dir, args.log_dir,
                                       args.cache_dir, args.verbose,
                                       args.resume)
        if not args.resume:
            self.write_args_to_file(args)
        return args


def main(cli_args=None):
    """
    Args:
        cli_args: If none then use sys.argv (the usual pattern except when
            testing)
    """
    da = DalynatorApplication()
    args = da.parse_and_initialize(cli_args=cli_args)

    swarm = djs.DalynatorJobSwarm(
        tool_name=args.tool_name,

        input_data_root=args.input_data_root,
        out_dir=args.out_dir,
        cod_version=args.cod_version,
        epi_version=args.epi_version,
        paf_version=None,
        cause_set_id=None,
        version=args.version,
        gbd_round_id=args.gbd_round_id,

        year_ids_1=args.year_ids_1,
        n_draws_1=args.n_draws_1,
        year_ids_2=args.year_ids_2,
        n_draws_2=args.n_draws_2,

        start_year_ids=args.start_year_ids,
        end_year_ids=args.end_year_ids,

        location_set_version_id=args.location_set_version_id,

        add_agg_loc_set_ids=args.add_agg_loc_set_ids,

        no_sex_aggr=args.no_sex_aggr,
        no_age_aggr=args.no_age_aggr,
        write_out_ylds_paf=None,
        write_out_ylls_paf=None,
        write_out_deaths_paf=None,
        write_out_dalys_paf=None,
        write_out_star_ids=False,

        start_at=args.start_at,
        end_at=args.end_at,
        upload_to_test=args.upload_to_test,

        turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,

        cache_dir=args.cache_dir,

        sge_project=args.sge_project,
        verbose=args.verbose,
        raise_on_paf_error=False,
        do_not_execute=args.do_not_execute
    )
    swarm.run("run_pipeline_dalynator.py")
    return swarm


if __name__ == "__main__":
    main()
