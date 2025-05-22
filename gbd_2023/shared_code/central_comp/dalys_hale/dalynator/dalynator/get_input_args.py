import argparse
import logging
import os
import sys
import pickle
import time

import gbd.constants as gbd
from cluster_utils.loggers import create_logger_in_memory
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from dalynator.constants import (
    FILE_PERMISSIONS, UMASK_PERMISSIONS, NATOR_TABLE_TYPES
)
from dalynator.makedirs_safely import makedirs_safely
from dalynator.get_yld_data import get_como_folder_structure
import dalynator.argument_pool as arg_pool
import dalynator.tool_objects as to

# Contains all command line parsers for burdenator and dalynator.
# Functions named construct_parser_X return a new parser object
# Functions named add_to_parser_X  take a parser as input, add more arguments
# to it, and return that enhanced parser

# Valid DALYnator and burdenator measures
VALID_DALYNATOR_MEASURES = [gbd.measures.DALY]
VALID_BURDENATOR_MEASURES = [gbd.measures.DEATH, gbd.measures.YLD,
                             gbd.measures.YLL, gbd.measures.DALY]

# File permissions for output files, in python 3 octal syntax
os.umask(UMASK_PERMISSIONS)

# Linux user group to own the files
IHME_CENTRAL_COMP_GROUP = 'GROUP'


def calculate_filenames(output_dir, log_dir, measure_id, location_id, year_id):
    """Returns the output file name (just the basename), plus the full path to
    the log file
    """
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
    """Enforces permissions on the folder structure.
    """
    for root, dirs, files in os.walk(path):
        for d in dirs:
            chmod_quietly(root, d, logger)
        for f in files:
            chmod_quietly(root, f, logger)


def construct_extra_paths(out_dir_without_version, log_dir,
                          tool_name, output_version):
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
        out_dir = '/mnt/share/centralcomp/{}/{}'.format(tool_name, output_version)
    else:
        out_dir = '{}/{}'.format(out_dir_without_version, output_version)

    if not log_dir:
        log_dir = out_dir + "/log"

    cache_dir = '{}/cache'.format(out_dir)

    return out_dir, log_dir, cache_dir


def chmod_quietly(root, path, logger):
    p = os.path.join(root, path)
    try:
        logger.debug("chmod 775 on {}".format(p))
        os.chmod(p, FILE_PERMISSIONS)
    except Exception as e:
        logger.info("chmod failed to set {} permissions on {}: {}".
                    format(FILE_PERMISSIONS, p, e.message))
        pass


def rotate_logs(out_dir, log_dir):
    """
    Move the existing daly_run_all.log and the stderr directories to be
    timestamped versions.
    Useful during resume, so that we don't keep appending to the same log.

    :param out_dir: The root directory WITH the version number
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
    makedirs_safely(stderr_dir)


def construct_directories(out_dir, log_dir, cache_dir, resume):
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

    makedirs_safely(out_dir)
    makedirs_safely(log_dir)
    makedirs_safely(cache_dir)
    if resume:
        # If resuming then rotate (rename) the main log, daly_run_all.log
        rotate_logs(out_dir, log_dir)
    stderr_dir = os.path.join(out_dir, "stderr")
    makedirs_safely(stderr_dir)

def output_storage_metadata(out_dir: str) -> None:
    """
    Create output storage metadata json file.

    :param out_dir:  The root directory WITH the version number
    """
    storage_metadata = ModelStorageMetadata.from_dict(
        {
            "storage_pattern": "FILEPATH",
            "h5_tablename": "draws"
        }
    )
    storage_metadata.to_file(directory=out_dir)

def create_logger(out_dir, log_dir, verbose, resume):
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
                            log_dir + "/daly_run_all.log",
                            ['aggregator.aggregators', 'jobmon'])


def prepare_with_side_effects(out_dir=None, log_dir=None,
                              cache_dir=None, verbose=None, resume=None):
    """
    Creates files and directories, initializes loggers. No parsing or other
    manipulation of the arguments.
    """
    construct_directories(out_dir, log_dir, cache_dir, resume)
    output_storage_metadata(out_dir)
    create_logger(out_dir, log_dir, verbose, resume)


def get_args_and_create_dirs(parser, cli_args=None):
    """Parses the command line using the parser and creates output directory
    and logger. Called by run_pipeline_*. Not used by run_all.
    """
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    cod_object = to.cod_or_faux_correct(args.input_data_root,
                                       args.codcorrect_version,
                                       args.fauxcorrect_version)

    # Store all years for each location in one directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    makedirs_safely(os.path.join(top_out_dir, 'log_most_detailed'))
    args.log_dir = os.path.join(top_out_dir, 'log_most_detailed',
                                str(args.location_id))
    args.out_dir = os.path.join(top_out_dir, 'draws', str(args.location_id))

    makedirs_safely(args.out_dir)
    makedirs_safely(args.log_dir)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory(
        "dalynator", log_level,
        args.log_dir + "/daly_{}_{}.log".format(args.location_id,
                                                args.year_id),
        ['aggregator.aggregators', 'jobmon'])

    args.cod_dir = cod_object.abs_path_to_draws
    args.cod_pattern = cod_object.file_pattern

    if hasattr(args, 'tool_name') and args.tool_name == "dalynator":
        args.daly_dir = "{}/dalynator/{}/draws/".format(args.input_data_root,
                                                        args.output_version)
    else:
        args.daly_dir = None

    args.epi_dir = get_como_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))

    if hasattr(args, 'paf_version'):
        # PAF directory structure has no "draws" sub-folder
        args.paf_dir = "{}/pafs/{}".format(args.input_data_root,
                                           args.paf_version)
    else:
        args.paf_dir = None

    return args


def load_args_from_file(args, tool_name):
    if not args[0].out_dir_without_version:
        raise ValueError("In Resume Mode, must pass the root path to your "
                         "output directory.")
    cache_file = ("{}/{}/cache/cli_args.pickle"
                  .format(args[0].out_dir_without_version,
                          args[0].output_version))
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


def write_args_to_file(args):
    cache_file = "{}/cli_args.pickle".format(args.cache_dir)
    with open(cache_file, "wb") as f:
        pickle.dump(args, f)


def set_phase_defaults(args):
    if not args.start_at:
        args.start_at = 'most_detailed'
    if not args.end_at:
        args.end_at = 'pct_change'
    return args


def construct_parser_run_all_tool(tool_name):
    """ Used by run_all_burdenator and run_all_dalynator """
    parser = argparse.ArgumentParser(description='Run all {tool}'.format(tool=tool_name))
    parser = arg_pool.add_resume(parser)
    parser = arg_pool.add_out_dir_with_destination(parser, "out_dir_without_version")
    parser = arg_pool.add_output_version(parser)
    parser = arg_pool.add_verbose(parser)
    if tool_name == 'dalynator':
        choices = ['most_detailed', 'pct_change', 'upload']
    else:
        choices = ['most_detailed', 'loc_agg', 'cleanup', 'pct_change',
                   'upload']
    parser = arg_pool.add_internal_upload_concurrency(parser)
    parser = arg_pool.add_start_and_end_at(parser, choices)
    parser = arg_pool.add_raise_on_paf_error(parser)
    parser = arg_pool.add_do_not_execute(parser)
    parser = arg_pool.add_age_group_ids(parser)
    parser = arg_pool.add_age_group_set_id(parser)

    return parser


def add_to_parser_burdenator_specific(parser):
    """Arguments specific to the Burdenator for Most Detailed phase"""
    parser = arg_pool.add_paf_version(parser)
    parser = arg_pool.add_cause_set_ids(parser)
    parser = arg_pool.add_star_ids(parser)
    parser = arg_pool.add_raise_on_paf_error(parser)
    parser = arg_pool.add_measure_ids(parser)

    return parser


def add_non_resume_args_burdenator(parser, tool_name):
    """add non resume args from dalynator, then add burdenator specific non-resume args,
    for most detailed phase"""
    parser = add_non_resume_args(parser, tool_name)
    parser = arg_pool.add_paf_version(parser)
    parser = arg_pool.add_cause_set_ids(parser)
    parser = arg_pool.add_star_ids(parser)
    return parser


def add_non_resume_args(parser, tool_name):
    """
    The parse for run_all_dalynator, nothing shared with other parsers.

    :return: parser
    """
    parser = arg_pool.add_input_data_root(parser)
    parser = arg_pool.add_cod(parser)
    parser = arg_pool.add_epi(parser)
    parser = arg_pool.add_release_id(parser)
    parser = arg_pool.add_log_dir(parser)
    parser = arg_pool.add_turn_off_null_nan(parser)
    parser = arg_pool.add_upload_to_test(parser)
    parser = arg_pool.add_skip_cause_agg(parser)
    parser = arg_pool.add_read_from_prod(parser)
    parser = arg_pool.add_public_upload(parser)
    parser = arg_pool.add_loc_set_ids(parser)

    if tool_name == 'dalynator':
        default_measures = ['daly']
    else:
        default_measures = ['death', 'daly', 'yld', 'yll']
    parser = arg_pool.add_measures(parser, default_measures)
    parser = arg_pool.add_years(parser)
    parser = arg_pool.add_n_draws(parser)
    parser = arg_pool.add_cluster_project(parser)
    parser = arg_pool.add_do_nothing(parser)
    parser = arg_pool.add_start_and_end_years(parser)
    return parser


def construct_parser_burdenator():
    """Create a parser for all arguments used by burdenator from pipeline but
    not from run_all, used for Burdenator Most Detailed"""
    parser = construct_parser_shared('Burdenator most detailed')
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_year_and_n_draws_group(parser)
    parser = add_to_parser_burdenator_specific(parser)
    return parser


def construct_parser_dalynator():
    """Used for Dalynator Most Detailed"""
    parser = construct_parser_shared('Dalynator most detailed')
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_year_id(parser)
    parser = arg_pool.add_n_draws(parser)
    return parser


def construct_parser_shared(description):
    """Used by the pipelines"""
    parser = argparse.ArgumentParser(description=description)
    parser = arg_pool.add_input_data_root(parser)
    parser = arg_pool.add_out_dir(parser)
    parser = arg_pool.add_log_dir(parser)
    parser = arg_pool.add_turn_off_null_nan(parser)
    parser = arg_pool.add_verbose(parser)
    parser = arg_pool.add_cod(parser)
    parser = arg_pool.add_epi(parser)
    parser = arg_pool.add_output_version(parser)
    parser = arg_pool.add_release_id(parser)
    parser = arg_pool.add_age_group_ids(parser)
    parser = arg_pool.add_age_group_set_id(parser)

    # Needed by mock_framework
    valid_tool_names = ["dalynator", "burdenator"]
    parser = arg_pool.add_tool_names(parser, valid_tool_names, False)

    return parser


def construct_parser_burdenator_loc_agg():
    """Create parser for burdenator location aggregation"""
    parser = argparse.ArgumentParser(
        description='Run location aggregation after burdenation')
    parser = arg_pool.add_data_root(parser)
    parser = arg_pool.add_years(parser)
    parser = arg_pool.add_rei_id(parser)
    parser = arg_pool.add_sex_id(parser)
    parser = arg_pool.add_measure_id(parser, VALID_BURDENATOR_MEASURES)
    parser = arg_pool.add_region_locs(parser)
    parser = arg_pool.add_loc_set_id(parser)
    parser = arg_pool.add_output_version(parser)
    parser = arg_pool.add_release_id(parser)
    parser = arg_pool.add_n_draws(parser)
    parser = arg_pool.add_star_ids(parser)
    parser = arg_pool.add_sequential_num(parser)
    return parser


def get_args_burdenator_loc_agg(parser, cli_args=None):
    """Creates arguments from parser for burdenator location aggregation"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    args.cache_dir = '{}/cache'.format(args.data_root)

    return args


def construct_parser_burdenator_cleanup():
    """Create parser for burdenator cleanup"""
    parser = argparse.ArgumentParser(description='Run burdenator cleanup')
    parser = arg_pool.add_input_data_root(parser)
    parser = arg_pool.add_out_dir(parser)
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_years(parser)
    parser = arg_pool.add_measure_id(parser, VALID_BURDENATOR_MEASURES)
    parser = arg_pool.add_release_id(parser)
    parser = arg_pool.add_cod(parser)
    parser = arg_pool.add_epi(parser)
    parser = arg_pool.add_turn_off_null_nan(parser)
    parser = arg_pool.add_output_version(parser)
    parser = arg_pool.add_star_ids(parser)
    parser = arg_pool.add_skip_cause_agg(parser)
    parser = arg_pool.add_n_draws(parser)
    parser = arg_pool.add_age_group_ids(parser)
    parser = arg_pool.add_age_group_set_id(parser)
    return parser


def construct_args_burdenator_cleanup(parser, cli_args=None):
    """Creates arguments from parser for rearranging the draw files at the end
    of the burdenator run"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    args.tool_name = 'burdenator'

    args.cache_dir = '{}/cache'.format(args.out_dir)

    # Get cod/epi env directories
    args.epi_dir = get_como_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))
    cod_object = to.cod_or_faux_correct(
        args.input_data_root,
        codcorrect_version=args.codcorrect_version,
        fauxcorrect_version=args.fauxcorrect_version)
    args.cod_dir = cod_object.abs_path_to_draws
    args.cod_pattern = cod_object.file_pattern
    return args


def construct_parser_upload():
    """Create parser for burdenator upload"""
    parser = argparse.ArgumentParser(description='Run upload')
    parser = arg_pool.add_out_dir(parser)
    parser = arg_pool.add_gbd_process_version_id(parser)
    parser = arg_pool.add_loc_set_ids(parser)
    parser = arg_pool.add_table_types(parser, NATOR_TABLE_TYPES)
    parser = arg_pool.add_upload_to_test(parser)
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_measure_id(parser, VALID_BURDENATOR_MEASURES)
    parser = arg_pool.add_year_ids(parser)
    parser = arg_pool.add_internal_upload_concurrency(parser)

    return parser


def construct_args_upload(parser, cli_args=None):
    """Creates arguments from parser for uploading data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)

    # Create log directory
    top_out_dir = args.out_dir
    args.cache_dir = '{}/cache'.format(args.out_dir)
    args.log_dir = os.path.join(top_out_dir, 'log_upload', args.table_type)
    args.null_inf_json_dir = os.path.join(top_out_dir, 'null_inf_json')

    makedirs_safely(args.log_dir)
    makedirs_safely(args.null_inf_json_dir)

    return args


def construct_parser_pct_change():
    """Create parser for pct change calculation"""
    parser = argparse.ArgumentParser(
        description='Run pct change for DALYs')

    parser = arg_pool.add_input_data_root(parser)
    parser = arg_pool.add_out_dir(parser)
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_start_and_end_year(parser)

    valid_measures = [gbd.measures.DALY, gbd.measures.YLL, gbd.measures.YLD,
                      gbd.measures.DEATH]
    parser = arg_pool.add_measure_id(parser, valid_measures)
    parser = arg_pool.add_cod(parser)
    parser = arg_pool.add_epi(parser)
    valid_tool_names = ["dalynator", "burdenator"]
    parser = arg_pool.add_tool_names(parser, valid_tool_names)
    parser = arg_pool.add_output_version(parser)
    parser = arg_pool.add_release_id(parser)
    parser = arg_pool.add_n_draws(parser)
    parser = arg_pool.add_star_ids(parser)
    parser = arg_pool.add_verbose(parser)
    parser = arg_pool.add_age_group_ids(parser)

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
    log_level = logging.DEBUG if args.verbose else logging.INFO
    args.logger = create_logger_in_memory("dalynator", log_level,
                                          "{}/{}".format(args.log_dir, logfn),
                                          ['aggregator.aggregators', 'jobmon'])

    # Get cod/epi env directories
    args.epi_dir = get_como_folder_structure(os.path.join(
        args.input_data_root, 'como', str(args.epi_version)))
    cod_object = to.cod_or_faux_correct(
        args.input_data_root,
        codcorrect_version=args.codcorrect_version,
        fauxcorrect_version=args.fauxcorrect_version)
    args.cod_dir = cod_object.abs_path_to_draws
    args.cod_pattern = cod_object.file_pattern
    return args


def create_logging_directories():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_dir', type=str)
    parser.parse_known_args()


def construct_parser_public_sort():
    """Create parser for public upload"""
    parser = argparse.ArgumentParser(
        description='Consolidate summary files for CS upload')
    parser = arg_pool.add_gbd_process_id(parser)
    parser = arg_pool.add_table_types(parser, NATOR_TABLE_TYPES)
    parser = arg_pool.add_measure_id(parser, choices=VALID_BURDENATOR_MEASURES)
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_out_dir(parser, required=True)
    parser = arg_pool.add_upload_to_test(parser)

    return parser


def construct_args_public_sort(parser, cli_args=None):
    """Creates arguments from parser for uploading data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    return args


def construct_parser_public_sync():
    """ Creates parser for public db sync"""
    parser = argparse.ArgumentParser(
        description="Sync db metadata for public upload"
    )
    parser = arg_pool.add_upload_to_test(parser)
    parser = arg_pool.add_gbd_process_version_ids(parser)

    return parser


def construct_args_public_sync(parser, cli_args=None):
    """Creates arguments from parser for public db sync"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    return args


def construct_parser_public_upload():
    """Create parser for public upload"""
    parser = argparse.ArgumentParser(
        description='Upload to public public database')
    parser = arg_pool.add_gbd_process_id(parser)
    parser = arg_pool.add_table_types(parser, NATOR_TABLE_TYPES)
    parser = arg_pool.add_measure_id(parser, choices=VALID_BURDENATOR_MEASURES)
    parser = arg_pool.add_loc_id(parser)
    parser = arg_pool.add_gbd_process_version_id(parser)
    parser = arg_pool.add_out_dir(parser, required=True)
    parser = arg_pool.add_upload_to_test(parser)

    return parser


def construct_args_public_upload(parser, cli_args=None):
    """Creates arguments from parser for uploading data"""
    if cli_args is None:
        cli_args = sys.argv[1:]
    args = parser.parse_args(cli_args)
    return args
