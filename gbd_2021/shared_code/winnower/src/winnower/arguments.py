"""
Arguments for the winnower CLI.
"""
import configargparse
import functools
import logging
import os
from pathlib import Path
import re
import string

import attr
import yaml

from winnower import constants
from winnower import errors

# Constants
DEFAULT_TOPICS = ('design', 'demographics', 'geography')


# Batch functions - these add collections of arguments to a parser
def generate_extraction_hook_template(parser):
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help="Don't do anything except display output")
    links_key(parser)
    log_level(parser)
    winnower_id(parser)


def list_extraction_ids_arguments(parser):
    topics(parser)
    links_key(parser)
    log_level(parser)


def run_extract_arguments(parser):
    """
    Adds arguments to run_extract CLI.
    """
    ubcov_id(parser)
    topics(parser)
    log_level(parser)
    set_run_directory(parser)
    output_directories(parser)
    output_file(parser)
    links_key(parser)
    save_config_options(parser)
    output_file_type(parser)
    keep_non_indicator_columns(parser)
    remove_special_characters(parser)


# Utility functions relating to arguments
def set_arguments_log_level(args):
    """
    Set the log level on the root handler to what args specifies.
    """
    log_level = get_arguments_log_level(args)
    root_logger = logging.getLogger()
    for root_handler in root_logger.handlers:
        root_handler.setLevel(log_level)


def get_arguments_log_level(args):
    """
    Returns the effective log level requested by the user via log_level.
    """
    if args.fixed_loglevel:
        return args.fixed_loglevel

    levels = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO,
              logging.DEBUG]
    try:
        return levels[args.counter_log_level]
    except IndexError:
        return levels[-1]


def save_config(args, extractor):
    """
    Save configuration for extraction based on save_config_options

    Arguments:
        args: the args returned by ArgumentParser.parse_args()
        extractor: an UbcovExtractor instance.
    """
    data = {}
    data['universal'] = attr.asdict(extractor.universal)
    data['merges'] = [attr.asdict(merge) for merge in extractor.merges]
    for topic_name, topic_config in extractor.topic_config.items():
        data[topic_name] = attr.asdict(topic_config)
    # TODO: labels, vars, indicators

    with open(args.save_config_as, 'w') as outf:
        yaml.dump(data, outf)


# Individual argument methods
def ubcov_id(parser):
    parser.add_argument('ubcov_id', nargs='+', type=int,
                        help='The ubcov_id values for the surveys to process')


def winnower_id(parser):
    parser.add_argument('--winnower-id',
                        help='winnower_id value for the survey; dev only')


def topics(parser):
    # TODO: validate the provided topics
    parser.add_argument(
        '--topic',
        dest='topics',
        action='append',
        # TODO: support suppressing these topics
        default=list(DEFAULT_TOPICS),
        help="The topic to process; repeat for multiple topics")


def links_key(parser):
    parser.add_argument(
        '--links-key',
        default=constants.GDOCS_LINKS_KEY,
        help=("Key for 'links' spreadsheet. "
              "You probably don't need to change this"))


def log_level(parser):
    """
    Adds arguments to set the threshold log level for displayed messages.

    This adds multiple mutually exclusive options. Users can use -v thru -vvvv
    to specify the log level implicitly, or provide --debug to more clearly
    express -vvvv.
    """
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-v', '--verbose',
        action='count',
        dest='counter_log_level',
        default=1,
        help=('Increase log level. Default is CRITICAL, ERROR and WARNING '
              'messages but levels include CRITICAL -> ERROR -> WARNING -> '
              'INFO -> DEBUG (in that order). Each "-v" adds an additional '
              'message type from left to right. -vv enables all messages'))

    group.add_argument(
        '--debug',
        action='store_const',
        dest='fixed_loglevel',
        const=logging.DEBUG,
        default=None,
        help='Log all messages to output. Equivalent to -vv')


def output_directories(parser):
    parser.add_argument("--directory",
                        type=Path,
                        help="Dir to save non-LIMITED_USE extractions in")
    parser.add_argument("--lu-directory",
                        type=Path,
                        help="Dir to save LIMITED_USE extractions in")


def output_file(parser):
    fields = sorted(OutputFile.VALID_FIELDS)
    parser.add_argument(
        '-o', '--output-file',
        dest='output_file',
        action=OutputFile,
        # not using a format string because the interpolated vars below
        # don't exist yet. They are interpolated later.
        default=('{survey_name}_{nid}_{survey_module}_'
                 '{ihme_loc_id}_{year_start}_{year_end}'),
        help=('Output file name. Supports '
              'interpolated values e.g., "{iso3}_{year}_extraction". '
              f'Supported fields are {fields}. "year" is equivalent to '
              '"year_start" and iso3 is the first 3 letters of ihme_loc_id.'))

    parser.add_argument(
        '--no-make-output-dir',
        dest='make_output_dir',
        default=True,
        help=('Program attempts to make output directory by default. '
              'This disables that'))


class OutputFile(configargparse.Action):
    """
    Validates the output file specified can be correctly interpolated.
    """
    VALID_FIELDS = frozenset(['nid', 'survey_name', 'ihme_loc_id', 'iso3',
                              'year', 'year_start', 'year_end',
                              'survey_module'])

    fmtr = string.Formatter()

    def __call__(self, parser, namespace, values, option_string):
        fields = [X[1] for X in self.fmtr.parse(values) if X[1] is not None]
        bad = [X for X in fields if X not in self.VALID_FIELDS]

        if bad:
            msg = f"Invalid fields for naming: {bad}"
            raise errors.ValidationError(msg)

        setattr(namespace, self.dest, values)


# replaces all non-letter non-number values with an underscore
_fixer = functools.partial(re.compile(r'[^\w\d_]').sub, '_')


def format_dict(universal) -> dict:
    """
    Returns the format strings used for determining an OutputFile.
    """
    result = {
        # Values taken directly from the UniversalConfig
        'nid': universal.nid,
        'ihme_loc_id': universal.ihme_loc_id,
        'year_start': universal.year_start,
        'year_end': universal.year_end,
        'survey_name': _fixer(universal.survey_name),
        'survey_module': universal.survey_module,
        # convenience values
        'year': universal.year_start,
        'iso3': universal.ihme_loc_id[:3],
    }
    return result


def get_output_file(args, universal, merges, file_id):
    """
    Returns Path for the file to be saved, likely creating parent directory.

    Args:
        args: value returned by ArgumentParser().parse_args(). If
              args.make_output_dir is True, creates parent directory.
        universal: the universal configuration for an extraction.
        merges: sequence of merge configuration for an extraction.
                May be empty.
        file_id: file_id is a string extracted from basic_additional sheet.
    """
    root = get_output_root(args, universal, merges)

    fmt = format_dict(universal)

    p = root / args.output_file

    if file_id:
        p = "_".join([str(p), file_id])

    # formatting later adds support for directories to use interpolated values
    p = Path(str(p).format(**fmt))

    if args.make_output_dir:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            msg = f"Failed to make directory {p.parent} with error {e}"
            raise errors.Error(msg)

    return p


def get_output_root(args, universal, merges):
    """
    Returns Path root of output prior to any specialized path computation.
    """
    def path_is_lu(path: Path):
        if path.drive == "L:":
            return True
        return any(X.upper() == "LIMITED_USE"
                   for X in path.parts)

    is_lu = path_is_lu(universal.file_path) or any(path_is_lu(m.merge_file)
                                                   for m in merges)
    if is_lu:
        root = args.lu_directory or args.directory
    else:
        root = args.directory
    root = root or Path(".")

    return root


def save_config_options(parser):
    """
    Adds option to dump ubCov configuration to file.
    """
    parser.add_argument(
        '--save-config',
        action='store_true',
        default=False,
        help='Save configuration as YAML output')

    parser.add_argument(
        '--save-config-as',
        default='config.yaml',
        help='Name of file to save configuration to. Defaults to config.yaml')


def output_file_type(parser):
    """
    Allow users to designate output file type.
    """
    parser.add_argument(
        '--csv',
        action='store_true',
        default=False,
        help='Output as .csv')

    parser.add_argument(
        '--dta',
        action='store_true',
        default=False,
        help='Output as .dta')


def set_run_directory(parser):
    """
    Adds option to set the runtime directory for this command.

    WARNING: this uses a custom action that mutates the runtime environment.
    """
    parser.add_argument(
        '-C', '--runtime-directory',
        action=SetDirectory,
        default=os.curdir,
        help='Changes the directory before running the extraction.')


class SetDirectory(configargparse.Action):
    # https://docs.python.org/3/library/argparse.html#action-classes
    def __call__(self, parser, namespace, values, option_string):
        setattr(namespace, self.dest, values)
        os.chdir(values)


def keep_non_indicator_columns(parser):
    parser.add_argument(
        '--keep', '--keep-non-indicator-columns',
        dest='keep',
        action='store_true',
        default=False,
        help='Keep all columns; for debugging')


def remove_special_characters(parser):
    parser.add_argument(
        '--remove-special-characters',
        action='store_true',
        default=False,
        help='Remove special characters from output; "Mjolnir" not "Mjölnir"')
