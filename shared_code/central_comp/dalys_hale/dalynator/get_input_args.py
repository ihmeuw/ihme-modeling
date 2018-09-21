import argparse
import logging
import os
import sys
import time

import gbd.constants as gbd
from db_tools import ezfuncs
from hierarchies.dbtrees import loctree

import dalynator.jobmon_config as jc
from dalynator.constants import FILE_PERMISSIONS, UMASK_PERMISSIONS
from dalynator.makedirs_safely import makedirs_safely
from dalynator.setup_logger import create_logger_in_memory
from dalynator.get_yld_data import get_folder_structure

# Contains all command line parsers for burdenator and dalynator.
# Functions named construct_parser_X return a new parser object
# Functions named add_to_parser_X  take a parser as input, add more arguments to it, and return that enhanced parser
# Functions named construct_args_X take a parser and a string (dUSERt is stdin) and return the parsed arguments

# There are four consumers of this file, the cross product of {pipeline, run_all}, and {dalynator, burdenator}
# The run_all functions take a superset of the pipeline arguments

# "shared" arguments are used by all four
# "run_all" arguments are only used by the run_all functions

# Valid DALYnator and burdenator measures
VALID_DALYNATOR_MEASURES = [gbd.measures.DALY]
VALID_BURDENATOR_MEASURES = [gbd.measures.DEATH, gbd.measures.YLD,
                             gbd.measures.YLL, gbd.measures.DALY]

# File permissions for output files, in python 3 octal syntax
os.umask(UMASK_PERMISSIONS)

# Linux user group to own the files
IHME_CENTRAL_COMP_GROUP = 'REDACTED'


def best_version(input_machine):
    """Find the 'best' model versions for como and cod from the database. Used as dUSERts"""
    if input_machine == 'como':
        return ezfuncs.query("""
            SELECT output_version_id FROM epi.output_version
            WHERE is_best=1""", conn_def='epi').squeeze()
    elif input_machine == 'cod':
        return ezfuncs.query("""
            SELECT output_version_id FROM cod.output_version
            WHERE is_best=1 AND code_version=4""", conn_def='cod').squeeze()


def calculate_filenames(output_dir, log_dir, measure_id, location_id, year_id):
    """Returns the output file name (just the basename), plus the full path to the log file"""
    output_file = calculate_output_filename(output_dir, measure_id, location_id, year_id)
    stdout_log = calculate_log_file_name(os.path.join(log_dir, str(location_id)), location_id, year_id)
    return output_file, stdout_log


def calculate_output_filename(output_dir, measure_id, location_id, year_id):
    output_file = os.path.join(output_dir, "{}_{}_{}.h5".format(measure_id, location_id, year_id))
    return output_file


def calculate_log_file_name(log_dir, location_id, year_id):
    return os.path.join(log_dir, 'daly_{}_{}.log'.format(location_id, year_id))


def set_folder_permissions(path, logger):
    """ Enforces permissions on the folder structure.

    These tools must be run by people in the same group, REDACTED
    will throw ValueError iof they are not. Sets group_id to same.

    When uploading to GBD outputs, the file and all upstream folders need to
    have open-enough permissions.  This function steps up the chain of folders
    and makes sure that they have the correct permissions

    NOTE: There are some folders which you will not have permission to change
    the permissions, especially the higher it goes.  This is ok.  Most of those
    folders have the correct permissions to be able to upload.
    """

    for root, dirs, files in os.walk(path):
        for d in dirs:
            chmod_quietly(root, d, logger)
        for f in files:
            chmod_quietly(root, f, logger)


def chmod_quietly(root, path, logger):
    p = os.path.join(root, path)
    try:
        logger.debug("chmod 775 on {}".format(p))
        os.chmod(p, FILE_PERMISSIONS)
    except Exception as e:
        logger.info("chmod failed to set {} permissions on {}: {}".
                    format(FILE_PERMISSIONS, p, e.message))
        pass


def to_list(v):
    if isinstance(v, (int, long)):
        return [v]
    else:
        return v


def construct_parser_run_all_dalynator():
    parser = construct_parser_shared()
    parser = add_to_parser_run_all_shared(parser)
    parser = add_to_parser_pct_start_end_years(parser)
    parser = add_to_parser_jobmon_connection(parser)
    return parser


def construct_args_run_all_dalynator(cli_args=None):
    """Used by run_all_dalynator"""
    parser = construct_parser_run_all_dalynator()
    args = parser.parse_args(cli_args)
    args.tool_name = 'dalynator'
    create_run_all_directories(args)
    args = expand_location_arguments(args)
    args = create_sge_project(args)
    args = construct_args_multi_mode_years(args)
    return args


def construct_args_multi_mode_years(args):
    # Check that:
    #  The two sets of years are disjoint
    #  They have different number of draws (if not - use one set)
    #  Percentage-change years have same number of draws
    #

    if not args.year_ids_2:
        args.n_draws_2 = 0

    if args.year_ids_1 and args.year_ids_2:
        if not set(args.year_ids_1).isdisjoint(args.year_ids_2):
            common = set(args.year_ids_1).intersection(set(args.year_ids_2))
            raise ValueError("The two sets of year_ids must be separate, common years: {}".format(list(common)))

    if args.n_draws_1 and args.n_draws_2:
        if args.n_draws_1 == args.n_draws_2:
            raise ValueError("The number of draws must be different for the two year sets, not both {}"
                             .format(args.n_draws_1))

    args.year_n_draws_map = {}
    if args.year_ids_1:
        for y in args.year_ids_1:
            args.year_n_draws_map[y] = args.n_draws_1
    if args.year_ids_2:
        for y in args.year_ids_2:
            args.year_n_draws_map[y] = args.n_draws_2

    if args.start_years and args.end_years:
        if len(args.start_years) != len(args.end_years):
            raise ValueError("start_years and end_years should have same length")

        for start_year, end_year in zip(args.start_years, args.end_years):
            if start_year >= end_year:
                raise ValueError("end_year need be greater than start_year")

            if not (start_year in args.year_ids_1 or start_year in args.year_ids_2):
                raise ValueError("percent start_year {} must be in --year_ids_1 or --year_ids_2"
                                 .format(start_year))
            if not (end_year in args.year_ids_1 or end_year in args.year_ids_2):
                raise ValueError("percent end_year {} must be in --year_ids_1 or --year_ids_2"
                                 .format(end_year))
            if args.year_n_draws_map[start_year] != args.year_n_draws_map[end_year]:
                raise ValueError("start and end_year have different number of draws: {} and {}"
                                 .format(start_year,end_year))

    return args


def construct_parser_run_all_burdenator():
    parser = construct_parser_shared()
    parser = add_to_parser_run_all_shared(parser)
    parser = add_to_parser_burdenator_specific(parser)
    parser = add_to_parser_pct_start_end_years(parser)
    parser = add_to_parser_jobmon_connection(parser)
    return parser


def construct_args_run_all_burdenator(cli_args=None):
    parser = construct_parser_run_all_burdenator()
    args = parser.parse_args(cli_args)
    args.tool_name='burdenator'
    create_run_all_directories(args)
    args = expand_location_arguments(args)
    args = create_sge_project(args)
    args = construct_args_multi_mode_years(args)

    if not any([args.write_out_ylls_paf, args.write_out_ylds_paf, args.write_out_deaths_paf]):
        raise ValueError("must choose at least one of --ylls_paf, --ylds_paf and --deaths_paf")

    if args.write_out_ylds_paf:
        if not args.epi_version:
            raise ValueError("An epi_version is needed if yld's are being burdenated, i.e. if --yld_pafs is set")

    return args


def add_run_all_directories_to_args(args):
    """Compute the draw output and log directories"""

    # Are they using the dUSERt output dir?
    # Always append the version number
    if not args.out_dir:
        args.out_dir = 'FILEPATH'.format(args.tool_name, args.version)
    else:
        args.out_dir = '{}/{}'.format(args.out_dir, args.version)
    args.cache_dir = '{}/cache'.format(args.out_dir)

    # Did they override the log directory?
    if not args.log_dir:
        args.log_dir = args.out_dir + "/log/"
    return args


def create_run_all_directories(args):
    """Create the output directory and the run_all logger. Used by both burdenator and dalynator."""

    add_run_all_directories_to_args(args)

    # Check that both directories are empty. If they are not-empty then only continue if we are in resume mode
    if os.path.isdir(args.out_dir):
        if os.listdir(args.out_dir) and not args.resume:
            raise ValueError("Output directory {} contains files and NOT running in resume mode".format(args.out_dir))

    if os.path.isdir(args.log_dir):
        if os.listdir(args.log_dir) and not args.resume:
            raise ValueError("Log directory {} contains files and NOT running in resume mode".format(args.log_dir))

    makedirs_safely(args.log_dir)
    makedirs_safely(args.out_dir)
    makedirs_safely(args.cache_dir)

    if args.resume:
        # If resuming then rotate (rename) the main log, daly_run_all.log
        rotate_logs(args.out_dir, args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = create_logger_in_memory("dalynator", log_level, args.log_dir + "/daly_run_all.log")


def create_sge_project(args):
    """Create the dUSERt SGE project name, which is 'proj_<tool_name>'"""
    if args.sge_project is None:
        if args.tool_name is None:
            raise ValueError(" Neither SGE project nor tool name is set. Tool name must be set")
        else:
            args.sge_project = "proj_{}".format(args.tool_name)
    return args


def expand_location_arguments(args):
    """Expand the various location arguments into a set of location ids"""
    logger = logging.getLogger("dalynator")
    if args.location_id is not None:
        location_id_list = to_list(args.location_id)
        if args.subtree_id is not None:
            raise ValueError("Cannot specify both a location_id and a subtree_id")
    else:
        if args.location_set_id:
            tree = loctree(None, args.location_set_id)
            location_id_list = [n.id for n in tree.nodes]
            logger.debug("  location_set_id original")
        elif args.location_set_version_id:
            tree = loctree(args.location_set_version_id)
            location_id_list = [n.id for n in tree.nodes]
            logger.debug("  location_set_version_id original")
        if args.subtree_id is not None:
            node_list = tree.get_node_by_id(args.subtree_id).all_descendants()
            location_id_list = [n.id for n in node_list]
            location_id_list.append(args.subtree_id)

    logger.debug("  location_id_list as list {}:{}".format(len(location_id_list), location_id_list))
    args.location_id_list = location_id_list

    return args


def add_to_parser_run_all_shared(parser):
    """Given a parser, add arguments used by all tools and return that parser"""
    # mutual exclusion - can only select location by one method
    location_group = parser.add_mutually_exclusive_group(required=True)
    location_group.add_argument('-l', '--location_id', nargs='+',
                                type=int, action='store',
                                help='The location_id, an integer')

    location_group.add_argument('--location_set_id',
                                type=int, action='store',
                                help='The location_set_id, an integer')

    location_group.add_argument('--location_set_version_id',
                                type=int, action='store',
                                help='The location_set_version_id, an integer')

    parser.add_argument('--subtree_id',
                        type=int, action='store',
                        help='all locations within this subtree, inclusive. \
                              Incompatible with --location_id, but requires --location_set_version_id')

    parser.add_argument('-y1', '--year_ids_1', type=int, nargs='+',
                        dUSERt=[1990, 1995, 2000, 2005, 2010, 2016],
                        action='store',
                        help='The first set of year_ids, a space-separated list of integers')
    parser.add_argument('--n_draws_1', dUSERt=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns in year-set 1, with possible resampling')

    parser.add_argument('-y2', '--year_ids_2', type=int, nargs='+',
                        dUSERt=[],
                        action='store',
                        help='The second set of year_ids, a space-separated list of integers')
    parser.add_argument('--n_draws_2', dUSERt=100,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns in year-set 2, with possible resampling')

    parser.add_argument('-s', '--sge_project', type=str,
                        action='store',
                        help='The SGE project, dUSERt is proj_<tool_name>')

    parser.add_argument('-N', '--do_nothing', action='store_true',
                        help="Report the jobs that would be qsub'ed, but do not qsub them")

    parser.add_argument('-r', '--resume', action='store_true',
                        dUSERt=False,
                        help='Resume an existing run - do not overwrite existing output files')

    parser.add_argument('--add_agg_loc_set_ids', nargs='+',
                        type=int, dUSERt=[],
                        help=('EXPERTS ONLY. Additional location sets to '
                              'apply to location aggregation phase onward. '
                              'NOTE: If the sets have overlapping '
                              'aggregate-level (i.e. non-leaf) locations, a '
                              'race condition may be introduced where multiple '
                              'aggregation jobs attempt to write to the same '
                              'file. Further, additional aggregation sets are '
                              'are not used for the most detailed phase. '
                              'Therefore all their detailed location data must '
                              'have been created by the primary location '
                              'set.'))

    return parser


def get_args_and_create_dirs(parser, cli_args=None):
    """Parses the command line using the parser and creates output directory and logger.
    Called by run_pipeline_* and remote_run_pipeline_*.
    Not used by run_all."""

    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    # Store all years for each location in one directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(top_out_dir, 'log', str(args.location_id))
    args.out_dir = os.path.join(top_out_dir, 'draws', str(args.location_id))

    makedirs_safely(args.out_dir)
    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory("dalynator", log_level,
                                          args.log_dir + "/daly_{}_{}.log".format(args.location_id, args.year_id))

    args.cod_dir = "{}/codcorrect/{}/draws/".format(args.input_data_root, args.cod_version)
    if hasattr(args, 'daly_version'):
        args.daly_dir = "{}/dalynator/{}/draws/".format(args.input_data_root, args.daly_version)
    else:
        args.daly_dir = None
    args.epi_dir = get_folder_structure(os.path.join(args.input_data_root,
                                                     'como',
                                                     str(args.epi_version)))

    if hasattr(args, 'paf_version'):
        # PAF directory structure has no "draws" sub-folder
        args.paf_dir = "{}/pafs/{}".format(args.input_data_root, args.paf_version)
    else:
        args.paf_dir = None

    return args


def rotate_logs(out_dir, log_dir):
    """Move the existing daly_run_all.log and the stderr directories to be timestamped versions.
    Useful during resume, so that
    we don't keep appending to the same log."""
    t = time.localtime()
    time_stamp = "{}-{:02d}-{:02d}_{:02d}:{:02d}:{:02d}".\
        format(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
    main_log = os.path.join(log_dir, "daly_run_all.log")
    if os.path.exists(main_log):
        os.rename(main_log, "{}.{}".format(main_log, time_stamp))

    stderr_dir = os.path.join(out_dir, "stderr")
    if os.path.exists(stderr_dir):
        os.rename(stderr_dir, "{}.{}".format(stderr_dir, time_stamp))
        # And re-recreate the normal stderr directory to be sure
        makedirs_safely(stderr_dir)


def add_to_parser_burdenator_specific(parser):
    """Arguments specific to the burdenator """
    parser.add_argument('-p', '--paf_version', required=True,
                        type=int, action='store',
                        help='The version of the paf results to use, an integer')

    parser.add_argument('--daly',
                        type=int, action='store',
                        dest='daly_version',
                        help='The version of the dalynator results to use, an integer')

    parser.add_argument('--ylls_paf',
                        action='store_true', dUSERt=False,
                        help='write_out_ylls_paf', dest="write_out_ylls_paf")

    parser.add_argument('--ylds_paf',
                        action='store_true', dUSERt=False,
                        help='Write out the back-calculated pafs for ylds', dest="write_out_ylds_paf")

    parser.add_argument('--deaths_paf',
                        action='store_true', dUSERt=False,
                        help='Write out the back-calculated pafs for deaths', dest="write_out_deaths_paf")

    parser.add_argument('--dalys_paf',
                        action='store_true', dUSERt=False,
                        help='Write out the back-calculated pafs for dalys', dest="write_out_dalys_paf")
    return parser


def construct_parser_burdenator():
    """Create a parser for all arguments used by burdenator from pipeline but not from run_all"""
    parser = construct_parser_shared()
    parser = add_to_parser_location_id(parser)
    parser = add_to_parser_pipeline_year_and_n_draws_group(parser)
    parser = add_to_parser_burdenator_specific(parser)
    # Needed when testing against a different job monitor, ie. not the permahost one defined in jobmon_config.py
    parser = add_to_parser_jobmon_connection(parser)
    return parser


def construct_args_dalynator():
    """No dUSERts for the demographic data - better to fail fast than run on the wrong data"""
    parser = construct_parser_shared()
    parser = add_to_parser_location_id(parser)
    parser = add_to_parser_pipeline_year_and_n_draws_group(parser)
    # Needed when testing against a different job monitor, ie. not the permahost one defined in jobmon_config.py
    parser = add_to_parser_jobmon_connection(parser)
    return parser


def add_to_parser_location_id(parser):
    # Notice that this is singular, so NOT a list
    parser.add_argument('-l', '--location_id',
                        type=int, action='store',
                        help='The location_id, an integer')
    return parser


def add_to_parser_pct_start_end_years(parser):
    parser.add_argument('--start_years',
                        type=int, nargs='+',
                        help='The start years for pct change calculation, a space-separated list')
    parser.add_argument('--end_years',
                        type=int, nargs='+',
                        help='The end years for pct change calculation, a space-separated list')

    return parser


def add_to_parser_jobmon_connection(parser):
    parser.add_argument("--monitor_host", dUSERt=jc.MONITOR_HOST, required=False)
    parser.add_argument("--monitor_port", dUSERt=jc.MONITOR_PORT, required=False, type=int)
    parser.add_argument("--request_timeout", dUSERt=jc.DEFAULT_TIMEOUT, required=False, type=int)
    parser.add_argument("--request_retries", dUSERt=jc.DEFAULT_RETRIES, required=False, type=int)

    parser.add_argument("--publisher_host", dUSERt=jc.PUBLISHER_HOST, required=False)
    parser.add_argument("--publisher_port", dUSERt=jc.PUBLISHER_PORT, required=False, type=int)

    return parser

def construct_parser_jobmon():
    parser = argparse.ArgumentParser(description='Parser for jobmon connection')
    add_to_parser_jobmon_connection(parser)
    return parser

def add_to_parser_pipeline_year_and_n_draws_group(parser):
    year_group = parser.add_mutually_exclusive_group(required=True)
    year_group.add_argument('-y', '--year_id',
                                type=int, action='store',
                                help='The year_id, an integer')
    year_group.add_argument('--y_list','--start_end_year', nargs='+',
                                type=int, action='store',
                                help='start_year, end_year, a list')
    parser.add_argument('--n_draws', dUSERt=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and output draw files')
    return parser


def strictly_positive_integer(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("Number must be strictly greater than zero, not {}".format(value))
    return ivalue


def construct_parser_shared():
    """Used by run_all AND the pipelines"""
    parser = argparse.ArgumentParser(description='Compute DALYS')
    parser.add_argument('--input_data_root',
                        dUSERt='FILEPATH',
                        type=str, action='store',
                        help='The root directory of all data, useful for testing')

    parser.add_argument('--cod',
                        dUSERt=best_version('cod'),
                        type=int, action='store',
                        dest='cod_version',
                        help='The version of the cod results to use, an integer')

    parser.add_argument('--epi',
                        dUSERt=best_version('como'),
                        type=int, action='store',
                        dest='epi_version',
                        help='The version of the epi/como results to use, an integer')

    parser.add_argument('--cause_set_id',
                        dUSERt=2,
                        type=int, action='store',
                        help='The cause_set_id to use for the cuase_hierarchy, an integer')

    parser.add_argument('-g', '--gbd_round_id',
                        dUSERt=4,
                        type=int, action='store',
                        help='The version of the gbd_round_id use, an integer')

    parser.add_argument('--no_sex',
                        action='store_true', dUSERt=False,
                        help='Do not write sex aggregates to draw files '
                             '(they will be computed and included in summaries)',
                        dest="no_sex_aggr")

    parser.add_argument('--no_age',
                        action='store_true', dUSERt=False,
                        help='Do not write age aggregates to draw files '
                             '(they will be computed and included in summaries)',
                        dest="no_age_aggr")

    parser.add_argument('-o', '--out_dir', type=str,
                        action='store',
                        help='The root directory for the output files, version will be appended. ')

    parser.add_argument('--log_dir', type=str,
                        action='store',
                        help='The root directory for the log files, overrides the usual location '
                             'DUSERt is <out_dir>/log')

    parser.add_argument('-n', '--turn_off_null_and_nan_check',
                        action='store_true',
                        help='No input restriction for nulls and NaNs. Dangerous but necessary for older GBD years.')

    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for dUSERt output dir and dbs meta-data')

    parser.add_argument('-v', '--verbose', action='store_true', dUSERt=False,
                        help='print many debugging messages')

    parser.add_argument('-u', '--upload', action='store_true', dUSERt=False,
                        help='Upload results to GBD Outputs DB')

    parser.add_argument('--upload_to_test', action='store_true', dUSERt=False,
                        help='Upload data to test environment')

    # Turn phases on and off. Useful for testing and operations
    parser.add_argument('--no_details', action='store_true', dUSERt=False,
                        help='DO NOT run the most-detailed phase, only for experts')

    parser.add_argument('--no_loc_agg', action='store_true', dUSERt=False,
                        help='DO NOT run the location-aggregation phase, only for experts')

    parser.add_argument('--no_cleanup', action='store_true', dUSERt=False,
                        help='DO NOT run the badly-named cleanup phase, only for experts')

    parser.add_argument('--no_pct_change', action='store_true', dUSERt=False,
                        help='DO NOT run the percentage-change phase, only for experts')

    # Needed by mock_framework
    valid_tool_names = ["dalynator", "burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=False,
                        choices=valid_tool_names,
                        help='The tool name')



    return parser


def construct_parser_burdenator_loc_agg():
    """Create parser for burdenator location aggregation"""
    parser = argparse.ArgumentParser(
        description='Run location aggregation after burdenation')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        required=True,
                        help='The root directory for the output files, version will be added. '
                             'DUSERt is FILEPATH')

    parser.add_argument('-y', '--year_id',
                        type=int,
                        required=True,
                        help='The year to aggregate')

    parser.add_argument('-s', '--sex_id',
                        type=int,
                        required=True,
                        choices=[1, 2],
                        help='The sex_id to aggregate')

    parser.add_argument('-r', '--rei_id',
                        type=int,
                        required=True,
                        help='The rei_id to aggregate')

    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=VALID_BURDENATOR_MEASURES,
                        help='The measure_id to aggregate')

    parser.add_argument('--location_set_id',
                        type=int,
                        required=True,
                        help='The location_set_id, an integer')

    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for dUSERt output dir and dbs meta-data')

    parser.add_argument('-v', '--verbose', action='store_true', dUSERt=False,
                        help='print many debugging messages')

    parser.add_argument('-g', '--gbd_round_id',
                        dUSERt=4,
                        type=int, action='store',
                        help='The version of the gbd_round_id use, an integer')

    valid_tool_names = ["burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')

    parser = add_to_parser_jobmon_connection(parser)

    return parser


def get_args_burdenator_loc_agg(parser, cli_args=None):
    """Creates arguments from parser for burdenator location aggregation"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    # Create log directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(top_out_dir, 'log_loc_agg',
                                str(args.year_id), str(args.measure_id))

    log_filename = "{}_{}_{}_{}.log".format(
        args.measure_id, args.rei_id, args.year_id, args.sex_id)

    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory(
        "dalynator", log_level, args.log_dir + "/" + log_filename)

    return args


def construct_parser_burdenator_cleanup():
    """Create parser for burdenator cleanup"""
    parser = argparse.ArgumentParser(description='Run burdenator cleanup')

    parser.add_argument('--input_data_root',
                        dUSERt='FILEPATH',
                        type=str, action='store',
                        help='The root directory of all data, useful for testing')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        required=True,
                        help=(
                            'The root directory for the output files, version will be added. '
                            'DUSERt is FILEPATH')
                        )

    parser.add_argument('-l', '--location_id', type=int, required=True,
                        action='store', help='The location_id, an integer')

    parser.add_argument('-y', '--year_id',
                        type=int,
                        required=True,
                        help='The year to aggregate')

    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=VALID_BURDENATOR_MEASURES,
                        help='The measure_id to aggregate')

    parser.add_argument('-g', '--gbd_round_id',
                        dUSERt=4,
                        type=int, action='store',
                        help='The version of the gbd_round_id use, an integer')

    parser.add_argument('--cod',
                        dUSERt=best_version('cod'),
                        type=int, action='store',
                        dest='cod_version',
                        help='The version of the cod results to use, an integer')

    parser.add_argument('--epi',
                        dUSERt=best_version('como'),
                        type=int, action='store',
                        dest='epi_version',
                        help='The version of the epi/como results to use, an integer')

    parser.add_argument('--daly',
                        type=int, action='store',
                        dest='daly_version',
                        help='The version of the dalynator results to use, an integer')

    parser.add_argument('-n', '--turn_off_null_and_nan_check',
                        action='store_true',
                        help='No input restriction for nulls and NaNs.')

    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for dUSERt output dir and dbs meta-data')

    parser.add_argument('-v', '--verbose', action='store_true', dUSERt=False,
                        help='print many debugging messages')

    valid_tool_names = ["burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')
    parser.add_argument('--n_draws', dUSERt=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and output draw files')

    parser = add_to_parser_jobmon_connection(parser)
    return parser


def construct_args_burdenator_cleanup(parser, cli_args=None):
    """Creates arguments from parser for rearranging the draw files at the end of the burdenator run"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.tool_name = 'burdenator'

    # Create log directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(top_out_dir, 'log_cleanup',
                                str(args.year_id), str(args.measure_id))

    log_filename = "{}_{}_{}.log".format(
        args.measure_id, args.location_id, args.year_id)

    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory(
        "dalynator", log_level, args.log_dir + "/" + log_filename)

    # Get cod/epi env directories
    args.cod_dir = (
        args.input_data_root + "/codcorrect/" + str(args.cod_version) +
        "/draws")
    args.epi_dir = get_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))
    return args


def construct_parser_burdenator_upload():
    """Create parser for burdenator upload"""
    parser = argparse.ArgumentParser(description='Run burdenator upload')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        dUSERt=(
                            'The root directory for the output files, version will be added. '
                            'DUSERt is FILEPATH')
                        )

    parser.add_argument('--gbd_process_version_id', type=int, required=True,
                        action='store', help='The gbd_process_version_id, an integer')

    parser.add_argument('-l', '--location_ids', type=int, nargs='+',
                        required=True, action='store',
                        help=('The location_ids to upload (space separated '
                              'list of integers)'))

    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=VALID_BURDENATOR_MEASURES,
                        help='The measure_id to aggregate')

    valid_table_types = ["single_year", "multi_year"]
    parser.add_argument('--table_type',
                        type=str,
                        required=True,
                        choices=valid_table_types,
                        help='The table type to upload to')

    parser.add_argument('--upload_to_test', action='store_true', dUSERt=False,
                        help='Upload data to test environment')

    parser.add_argument('-v', '--verbose', action='store_true', dUSERt=False,
                        help='print many debugging messages')

    valid_tool_names = ["burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')

    parser = add_to_parser_jobmon_connection(parser)

    return parser


def construct_args_burdenator_upload(parser, cli_args=None):
    """Creates arguments from parser for uploading burdenator data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.tool_name = 'burdenator'

    # Create log directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(
        top_out_dir, 'log_upload', args.table_type, str(args.measure_id))

    log_filename = "upload_{}_{}_{}.log".format(
        args.gbd_process_version_id, args.table_type, args.measure_id)

    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory(
        "dalynator", log_level, args.log_dir + "/" + log_filename)

    return args


def construct_parser_dalynator_upload():
    """Create parser for dalynator upload"""
    parser = argparse.ArgumentParser(description='Run dalynator upload')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        dUSERt=(
                            'The root directory for the output files, version will be added. '
                            'DUSERt is FILEPATH')
                        )

    parser.add_argument('--gbd_process_version_id', type=int, required=True,
                        action='store', help='The gbd_process_version_id, an integer')

    parser.add_argument('-l', '--location_ids', type=int, nargs='+',
                        required=True, action='store',
                        help=('The location_ids to upload (space separated '
                              'list of integers)'))

    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=VALID_DALYNATOR_MEASURES,
                        help='The measure_id to aggregate')

    valid_table_types = ["single_year", "multi_year"]
    parser.add_argument('--table_type',
                        type=str,
                        required=True,
                        choices=valid_table_types,
                        help='The table type to upload to')

    parser.add_argument('--upload_to_test', action='store_true', dUSERt=False,
                        help='Upload data to test environment')

    parser.add_argument('-v', '--verbose', action='store_true', dUSERt=False,
                        help='print many debugging messages')

    valid_tool_names = ["dalynator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')

    parser = add_to_parser_jobmon_connection(parser)
    return parser


def construct_args_dalynator_upload(parser, cli_args=None):
    """Creates arguments from parser for uploading dalynator data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.tool_name = 'dalynator'

    # Create log directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(
        top_out_dir, 'log_upload', args.table_type, str(args.measure_id))

    log_filename = "upload_{}_{}.log".format(args.table_type, args.measure_id)

    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory(
        "dalynator", log_level, args.log_dir + "/" + log_filename)

    return args


def construct_parser_pct_change():
    """Create parser for percentage change calculation"""
    parser = argparse.ArgumentParser(
        description='Run percentage change for DALYs')

    parser.add_argument('-o', '--out_dir', type=str,
                        help=('The root directory for the output files, '
                              'version will be added. DUSERt is '
                              'FILEPATH'))
    parser.add_argument('-l', '--location_id', type=int, required=True,
                        help='The location_id, an integer')
    parser.add_argument('-s', '--start_year',
                        type=int,
                        required=True,
                        help='The start year for pct change calculation')
    parser.add_argument('-e', '--end_year',
                        type=int,
                        required=True,
                        help='The end year for pct change calculation')

    valid_measures = [gbd.measures.DALY, gbd.measures.YLL, gbd.measures.YLD, gbd.measures.DEATH]
    parser.add_argument('-m', '--measure_id',
                        type=int, action='store',
                        required=True,
                        choices=valid_measures,
                        help='The measure_id for percentage-change calculations')
    valid_tool_names = ["dalynator", "burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')
    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='version number, used for dUSERt output dir and dbs meta-data')
    parser.add_argument('-g', '--gbd_round_id',
                        dUSERt=4,
                        type=int, action='store',
                        help='The version of the gbd_round_id use, an integer')
    parser.add_argument('--n_draws', dUSERt=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and output draw files')

    parser = add_to_parser_jobmon_connection(parser)

    return parser


def get_args_pct_change(parser, cli_args=None):
    """Creates arguments from parser for pct change calculation"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    args.log_dir = os.path.join(args.out_dir, 'log_pct_change',
                                str(args.location_id))
    makedirs_safely(args.log_dir)
    logfn = "pc_{}_{}.log".format(args.start_year, args.end_year)
    args.logger = create_logger_in_memory("dalynator", logging.DEBUG,
                                          "{}/{}".format(args.log_dir, logfn))

    return args


def construct_parser_outdir():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_dir', type=str)
    return parser
