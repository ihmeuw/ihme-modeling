import argparse
import logging
import os
import sys

import gbd.constants as gbd
from db_tools import ezfuncs

import dalynator.app_common as ac
from dalynator.constants import FILE_PERMISSIONS, UMASK_PERMISSIONS
from dalynator.makedirs_safely import makedirs_safely
from dalynator.setup_logger import create_logger_in_memory
from dalynator.get_yld_data import get_como_folder_structure

# Contains all command line parsers for burdenator and dalynator.
# Functions named construct_parser_X return a new parser object
# Functions named add_to_parser_X  take a parser as input, add more arguments
# to it, and return that enhanced parser
# Functions named construct_args_X take a parser and a string
# (default is stdin) and return the parsed arguments

# There are four consumers of this file, the cross product of
# {pipeline, run_all}, and {dalynator, burdenator}
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
IHME_CENTRAL_COMP_GROUP = 'USER'


def calculate_filenames(output_dir, log_dir, measure_id, location_id, year_id):
    """Returns the output file name (just the basename), plus the full path to
    the log file"""
    output_file = calculate_output_filename(output_dir, measure_id,
                                            location_id, year_id)
    stdout_log = calculate_log_file_name(
        os.path.join(log_dir, str(location_id)), location_id, year_id)
    return output_file, stdout_log


def calculate_output_filename(output_dir, measure_id, location_id, year_id):
    output_file = os.path.join(output_dir, "{}_{}_{}.h5".format(
        measure_id, location_id, year_id))
    return output_file


def calculate_log_file_name(log_dir, location_id, year_id):
    return os.path.join(log_dir, 'daly_{}_{}.log'.format(location_id, year_id))


def set_folder_permissions(path, logger):
    """ Enforces permissions on the folder structure.

    These tools must be run by people in the same group, USER,
    will throw ValueError iof they are not. Sets group_id to same.

    When uploading to GBD outputs, the file and all upstream folders need to
    have open-enough permissions.  This function steps up the chain of folders
    and makes sure that they have the correct permissions
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


def get_args_and_create_dirs(parser, cli_args=None):
    """Parses the command line using the parser and creates output directory
    and logger. Called by run_pipeline_*. Not used by run_all."""

    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    # resolve defaults for cod and epi versions
    args.gbd_round, args.gbd_round_id = ac.populate_gbd_round_args(
        args.gbd_round, args.gbd_round_id)
    if args.cod_version is None:
        args.cod_version = ac.best_version('cod', args.gbd_round_id)
    if args.epi_version is None:
        args.epi_version = ac.best_version('como', args.gbd_round_id)

    # Store all years for each location in one directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(top_out_dir, 'log', str(args.location_id))
    args.out_dir = os.path.join(top_out_dir, 'draws', str(args.location_id))

    makedirs_safely(args.out_dir)
    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory("dalynator", log_level,
                                          args.log_dir + "/daly_{}_{}.log"
                                          .format(args.location_id,
                                                  args.year_id))

    args.cod_dir = "{}/codcorrect/{}/draws/".format(args.input_data_root,
                                                    args.cod_version)
    if hasattr(args, 'daly_version'):
        args.daly_dir = "{}/dalynator/{}/draws/".format(args.input_data_root,
                                                        args.daly_version)
    else:
        args.daly_dir = None
    args.epi_dir = get_como_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))

    if hasattr(args, 'paf_version'):
        args.paf_dir = "{}/pafs/{}".format(args.input_data_root,
                                           args.paf_version)
    else:
        args.paf_dir = None

    return args


def add_to_parser_burdenator_specific(parser):
    """Arguments specific to the burdenator """
    parser.add_argument('-p', '--paf_version', required=True,
                        type=int, action='store',
                        help='The version of the paf results to use, '
                             'an integer')

    parser.add_argument('--cause_set_id',
                        default=2,
                        type=int, action='store',
                        help='The cause_set_id to use for the cause_hierarchy'
                             ', an integer')

    parser.add_argument('--daly',
                        type=int, action='store',
                        dest='daly_version',
                        help='The version of the dalynator results to use, '
                             'an integer')

    parser.add_argument('--ylls_paf',
                        action='store_true', default=False,
                        help='write_out_ylls_paf', dest="write_out_ylls_paf")

    parser.add_argument('--ylds_paf',
                        action='store_true', default=False,
                        help='Write out the back-calculated pafs for ylds',
                        dest="write_out_ylds_paf")

    parser.add_argument('--deaths_paf',
                        action='store_true', default=False,
                        help='Write out the back-calculated pafs for deaths',
                        dest="write_out_deaths_paf")

    parser.add_argument('--dalys_paf',
                        action='store_true', default=False,
                        help='Write out the back-calculated pafs for dalys',
                        dest="write_out_dalys_paf")

    parser.add_argument('--star_ids',
                        action='store_true', default=False,
                        help='Write out star_ids',
                        dest="write_out_star_ids")

    parser.add_argument('--raise_on_paf_error',
                        action='store_true', default=False,
                        help='Raise if aggregate causes are found in PAFs')
    return parser


def construct_parser_burdenator():
    """Create a parser for all arguments used by burdenator from pipeline but
    not from run_all"""
    parser = construct_parser_shared()
    parser = add_to_parser_location_id(parser)
    parser = add_to_parser_pipeline_year_and_n_draws_group(parser)
    parser = add_to_parser_burdenator_specific(parser)
    return parser


def construct_args_dalynator():
    """No defaults for the demographic data - better to fail fast than run on
    the wrong data"""
    parser = construct_parser_shared()
    parser = add_to_parser_location_id(parser)
    parser = add_to_parser_pipeline_year_and_n_draws_group(parser)
    return parser


def add_to_parser_location_id(parser):
    # Notice that this is singular, so NOT a list
    parser.add_argument('-l', '--location_id',
                        type=int, action='store',
                        help='The location_id, an integer')
    return parser


def add_to_parser_pct_start_end_year_ids(parser):
    parser.add_argument('--start_year_ids',
                        type=int, nargs='+',
                        help='The start years for pct change calculation, '
                             'a space-separated list')
    parser.add_argument('--end_year_ids',
                        type=int, nargs='+',
                        help='The end years for pct change calculation, '
                             'a space-separated list')

    return parser


def add_to_parser_pipeline_year_and_n_draws_group(parser):
    year_group = parser.add_mutually_exclusive_group(required=True)
    year_group.add_argument('-y', '--year_id',
                            type=int, action='store',
                            help='The year_id, an integer')
    year_group.add_argument('--y_list', '--start_end_year', nargs='+',
                            type=int, action='store',
                            help='start_year, end_year, a list')
    parser.add_argument('--n_draws', default=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and '
                             'output draw files, for the specific year being '
                             'run')
    return parser


def strictly_positive_integer(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("Number must be strictly greater "
                                         "than zero, not {}".format(value))
    return ivalue


def construct_parser_shared():
    """Used by the pipelines"""
    parser = argparse.ArgumentParser(description='Compute DALYS')
    parser.add_argument('--input_data_root',
                        default='DIRECTORY',
                        type=str, action='store',
                        help='The root directory of all data, '
                             'useful for testing')

    parser.add_argument('-o', '--out_dir', type=str,
                        action='store',
                        help='The root directory for the output files, '
                             'version will be appended.')

    parser.add_argument('--log_dir', type=str,
                        action='store',
                        help='The root directory for the log files, overrides '
                             'the usual location '
                             'Default is <out_dir>/log')

    parser.add_argument('-n', '--turn_off_null_and_nan_check',
                        action='store_true',
                        help='No input restriction for nulls and NaNs. '
                             'Dangerous but necessary for older GBD years.')

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
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


def construct_parser_burdenator_loc_agg():
    """Create parser for burdenator location aggregation"""
    parser = argparse.ArgumentParser(
        description='Run location aggregation after burdenation')

    parser.add_argument('-dr', '--data_root',
                        type=str,
                        required=True,
                        help='The root directory for where input files come '
                             'from and where output files will go, '
                             'version will be added.')

    parser.add_argument('-y', '--year_id',
                        type=int,
                        required=True,
                        help='The year to aggregate')

    parser.add_argument('-r', '--rei_id', type=int,
                        required=True, action='store',
                        help=('The rei to aggregate'))

    parser.add_argument('-s', '--sex_id', type=int,
                        required=True, action='store',
                        help=('The sex to aggregate'))

    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=VALID_BURDENATOR_MEASURES,
                        help='The measure_id to aggregate')

    parser.add_argument('--region_locs', type=int, nargs='*',
                        required=True, action='store', default=[],
                        help=('The list of region location_ids to multiply by '
                              'regional_scalars before saving'))

    parser.add_argument('--location_set_id',
                        type=int,
                        required=True,
                        help='The location_set_id, an integer')

    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for default '
                             'output dir and dbs meta-data')

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print many debugging messages')

    gbd_round_group = parser.add_mutually_exclusive_group(required=True)
    gbd_round_group.add_argument('-G', '--gbd_round',
                                 type=int, action='store',
                                 help='The gbd_round as a year, eg 2013')
    gbd_round_group.add_argument('-g', '--gbd_round_id',
                                 type=int, action='store',
                                 help='The gbd_round_id as a database ID, '
                                      'eg 4 (==2016)')

    parser.add_argument('--n_draws', required=True, type=int, action='store',
                        help='number of draws in dataframes to be operated on')

    parser.add_argument('--star_ids',
                        action='store_true', default=False,
                        help='Write out star_ids',
                        dest="write_out_star_ids")

    return parser


def get_args_burdenator_loc_agg(parser, cli_args=None):
    """Creates arguments from parser for burdenator location aggregation"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.gbd_round, args.gbd_round_id = ac.populate_gbd_round_args(
        args.gbd_round, args.gbd_round_id)

    # Create log directory
    top_out_dir = args.data_root
    args.cache_dir = '{}/cache'.format(args.data_root)
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
                        default='DIRECTORY',
                        type=str, action='store',
                        help='The root directory of all data, useful for '
                             'testing')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        required=True,
                        help=(
                            'The root directory for the output files, version '
                            'will be added.')
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

    gbd_round_group = parser.add_mutually_exclusive_group(required=True)
    gbd_round_group.add_argument('-G', '--gbd_round',
                                 type=int, action='store',
                                 help='The gbd_round as a year, eg 2013')
    gbd_round_group.add_argument('-g', '--gbd_round_id',
                                 type=int, action='store',
                                 help='The gbd_round_id as a database ID, '
                                      'eg 4 (==2016)')

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

    parser.add_argument('--daly',
                        type=int, action='store',
                        dest='daly_version',
                        help='The version of the dalynator results to use, '
                             'an integer')

    parser.add_argument('-n', '--turn_off_null_and_nan_check',
                        action='store_true',
                        help='No input restriction for nulls and NaNs. '
                             'Dangerous but necessary for older GBD years.')

    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for default '
                             'output dir and dbs meta-data')

    parser.add_argument('--star_ids',
                        action='store_true', default=False,
                        help='Write out star_ids',
                        dest="write_out_star_ids")

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print many debugging messages')

    valid_tool_names = ["burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')
    parser.add_argument('--n_draws', default=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and '
                             'output draw files')
    return parser


def construct_args_burdenator_cleanup(parser, cli_args=None):
    """Creates arguments from parser for rearranging the draw files at the end
    of the burdenator run"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.tool_name = 'burdenator'
    args.gbd_round, args.gbd_round_id = ac.populate_gbd_round_args(
        args.gbd_round, args.gbd_round_id)

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
    if args.cod_version is None:
        args.cod_version = ac.best_version('cod', args.gbd_round_id)
    if args.epi_version is None:
        args.epi_version = ac.best_version('como', args.gbd_round_id)
    args.cod_dir = (
        args.input_data_root + "/codcorrect/" + str(args.cod_version) +
        "/draws")
    args.epi_dir = get_como_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))
    return args


def construct_parser_upload():
    """Create parser for burdenator upload"""
    parser = argparse.ArgumentParser(description='Run upload')

    parser.add_argument('-o', '--out_dir',
                        type=str,
                        default=(
                            'The root directory for the output files, version '
                            'will be added.')
                        )

    parser.add_argument('--gbd_process_version_id', type=int, required=True,
                        action='store',
                        help='The gbd_process_version_id, an integer')

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

    valid_storage_engines = ["INNODB", "COLUMNSTORE"]
    parser.add_argument('--storage_engine',
                        type=str,
                        required=True,
                        choices=valid_storage_engines,
                        help='The storage engine to upload to')

    parser.add_argument('--upload_to_test', action='store_true', default=False,
                        help='Upload data to test environment')

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print many debugging messages')

    valid_tool_names = ["burdenator", "dalynator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')

    return parser


def construct_args_upload(parser, cli_args=None):
    """Creates arguments from parser for uploading data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

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


def construct_parser_pct_change():
    """Create parser for percentage change calculation"""
    parser = argparse.ArgumentParser(
        description='Run percentage change for DALYs')

    parser.add_argument('-o', '--out_dir', type=str,
                        help=('The root directory for the output files, '
                              'version will be added.'))
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

    valid_measures = [gbd.measures.DALY, gbd.measures.YLL, gbd.measures.YLD,
                      gbd.measures.DEATH]
    parser.add_argument('-m', '--measure_id',
                        type=int, action='store',
                        required=True,
                        choices=valid_measures,
                        help='The measure_id for percentage-change '
                             'calculations')
    valid_tool_names = ["dalynator", "burdenator"]
    parser.add_argument('--tool_name',
                        type=str,
                        required=True,
                        choices=valid_tool_names,
                        help='The tool name')
    parser.add_argument('--version', required=True,
                        type=int, action='store',
                        help='version number, used for default output dir and '
                             'dbs meta-data')
    gbd_round_group = parser.add_mutually_exclusive_group(required=True)
    gbd_round_group.add_argument('-G', '--gbd_round',
                                 type=int, action='store',
                                 help='The gbd_round as a year, eg 2013')
    gbd_round_group.add_argument('-g', '--gbd_round_id',
                                 type=int, action='store',
                                 help='The gbd_round_id as a database ID, '
                                      'eg 4 (==2016)')
    parser.add_argument('--n_draws', default=1000,
                        type=strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and '
                             'output draw files')

    parser.add_argument('--star_ids',
                        action='store_true', default=False,
                        help='Write out star_ids',
                        dest="write_out_star_ids")

    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print many debugging messages')

    return parser


def get_args_pct_change(parser, cli_args=None):
    """Creates arguments from parser for pct change calculation"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.gbd_round, args.gbd_round_id = ac.populate_gbd_round_args(
        args.gbd_round, args.gbd_round_id)

    args.log_dir = os.path.join(args.out_dir, 'log_pct_change',
                                str(args.location_id))
    makedirs_safely(args.log_dir)
    logfn = "pc_{}_{}.log".format(args.start_year, args.end_year)
    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory("dalynator", log_level,
                                          "{}/{}".format(args.log_dir, logfn))

    return args


def create_logging_directories():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_dir', type=str)
    parser.parse_known_args()
