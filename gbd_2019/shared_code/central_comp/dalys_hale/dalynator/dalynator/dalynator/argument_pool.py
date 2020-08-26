import argparse
import logging
import os
import sys

import gbd.constants as gbd

import dalynator.app_common as ac

# Valid DALYnator and burdenator measures
VALID_DALYNATOR_MEASURES = [gbd.measures.DALY]
VALID_BURDENATOR_MEASURES = [gbd.measures.DEATH, gbd.measures.YLD,
                             gbd.measures.YLL, gbd.measures.DALY]


def add_cause_set_ids(parser):
    parser.add_argument('--cause_set_ids', nargs='+',
                        default=[2],
                        type=int, action='store',
                        help='The cause_set_ids to use for the cause_hierarchy'
                             ', an integer list')
    return parser


def add_cod(parser):
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--codcorrect',
                       default=None,
                       type=int_or_best,
                       action='store',
                       dest='codcorrect_version',
                       help='The version of the codcorrect results to use, '
                            'an integer or best.')
    group.add_argument('--fauxcorrect',
                       default=None,
                       type=int_or_best,
                       action='store',
                       dest='fauxcorrect_version',
                       help='The version of the fauxcorrect results to use, '
                            'an integer or best.')
    return parser


def add_dalys_paf(parser):
    parser.add_argument('--dalys_paf',
                        action='store_true', default=False,
                        help='Write out the back-calculated pafs for dalys',
                        dest="write_out_dalys_paf")
    return parser


def add_data_root(parser):
    parser.add_argument('-dr', '--data_root',
                        type=str,
                        required=True,
                        help='The root directory for where input files come '
                             'from and where output files will go, '
                             'version will be added. Default is FILEPATH')
    return parser


def add_deaths_paf(parser):
    parser.add_argument('--deaths_paf',
                        action='store_true', default=False,
                        help='Write out the back-calculated pafs for deaths',
                        dest="write_out_deaths_paf")
    return parser


def add_do_not_execute(parser):
    parser.add_argument('-x', '--do_not_execute', action='store_true',
                        default=False,
                        help='Do not execute when flag raised')
    return parser


def add_do_nothing(parser):
    parser.add_argument('-N', '--do_nothing', action='store_true',
                        help="Report the jobs that would be qsub'ed, "
                             "but do not qsub them")
    return parser


def add_dual_upload(parser):
    parser.add_argument('--dual_upload', action='store_true',
                        default=False,
                        help='Upload data to viz '
                             'database as well as modeling database')
    return parser


def add_epi(parser):
    parser.add_argument('--epi',
                        default=None,
                        type=int, action='store',
                        dest='epi_version',
                        help='The version of the epi/como results to '
                             'use, an integer')
    return parser


def add_gbd_round_group(parser):
    gbd_round_group = parser.add_mutually_exclusive_group(required=True)
    gbd_round_group.add_argument('-G', '--gbd_round',
                                 type=int, action='store',
                                 help='The gbd_round as a year, eg 2013')
    gbd_round_group.add_argument('-g', '--gbd_round_id',
                                 type=int, action='store',
                                 help='The gbd_round_id as a database ID, '
                                      'eg 4 (==2016)')
    return parser


def add_gbd_round_id(parser):
    parser.add_argument('-g', '--gbd_round_id',
                        type=int, action='store',
                        help='The gbd_round_id as a database ID, '
                             'eg 4 (==2016)')
    return parser


def add_decomp_step(parser):
    parser.add_argument('--decomp_step',
                        type=str, action='store',
                        choices=list(gbd.decomp_step.values()),
                        help='The decomp_step as a string, '
                        '(see shared.decomp_step) eg iterative')
    return parser


def add_gbd_process_version_id(parser):
    parser.add_argument('--gbd_process_version_id', type=int, required=True,
                        action='store',
                        help='The gbd_process_version_id, an integer')
    return parser


def add_input_data_root(parser):
    parser.add_argument('--input_data_root',
                        default='FILEPATH',
                        type=str, action='store',
                        help='The root directory of all input data, '
                             'useful for testing')
    return parser


def add_loc_id(parser):
    # Notice that this is singular, so NOT a list
    parser.add_argument('-l', '--location_id',
                        type=int, action='store',
                        required=True,
                        help='The location_id, an integer')
    return parser


def add_loc_ids(parser):
    parser.add_argument('-l', '--location_ids', type=int, nargs='+',
                        required=True, action='store',
                        help=('The location_ids to upload (space separated '
                              'list of integers)'))
    return parser


def add_loc_set_id(parser):
    parser.add_argument('--location_set_id',
                        type=int,
                        required=True,
                        help='The location_set_id, an integer')
    return parser


def add_loc_set_ids(parser):
    parser.add_argument('--location_set_ids', type=int, nargs='+',
                        action='store',
                        help='The location_set_ids, an int list')
    return parser


def add_log_dir(parser):
    parser.add_argument('--log_dir', type=str,
                        action='store',
                        help='The root directory for the log files, '
                             'overrides the usual location '
                             'Default is FILEPATH')
    return parser


def add_measures(parser, default_measures):
    parser.add_argument('--measures', type=str, nargs='+',
                        action='store', default=default_measures,
                        help=('The measures, a str list of measures '
                              'in singular form, i.e. daly, not dalys'
                              ))
    return parser


def add_measure_id(parser, choices):
    parser.add_argument('-m', '--measure_id',
                        type=int,
                        required=True,
                        choices=choices,
                        help='The measure_id to aggregate')
    return parser


def add_measure_ids(parser, choices=None):
    parser.add_argument('-m', '--measure_ids',
                        type=int, nargs='+',
                        required=True,
                        choices=choices)
    return parser


def add_n_draws(parser, default=1000):
    parser.add_argument('--n_draws', default=default,
                        type=ac.strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and '
                             'output draw files, for the specific year being '
                             'run')
    return parser


def add_out_dir(parser, required=False):
    parser.add_argument('-o', '--out_dir', type=str,
                        action='store',
                        required=required,
                        help='The root directory for the output files, '
                             'version will be appended. Default is FILEPATH')
    return parser


def add_out_dir_with_destination(parser, destination):
    parser.add_argument('-o', '--out_dir', type=str,
                        action='store',
                        dest=destination,
                        help='The root directory for the output '
                             'files, version will be appended. '
                             'Default is FILEPATH')
    return parser


def add_output_version(parser):
    parser.add_argument('--output_version', required=True,
                        type=int, action='store',
                        help='burdenator version number, used for default '
                             'output dir and dbs meta-data')
    return parser


def add_paf_version(parser):
    parser.add_argument('-p', '--paf_version', required=True,
                        type=int, action='store',
                        help='The version of the paf results to use, '
                             'an integer')
    return parser


def add_raise_on_paf_error(parser):
    parser.add_argument('--raise_on_paf_error',
                        action='store_true', default=False,
                        help='Raise if aggregate causes are found in PAFs')
    return parser


def add_region_locs(parser):
    parser.add_argument('--region_locs', type=int, nargs='*',
                        required=True, action='store', default=[],
                        help=('The list of region location_ids to multiply by '
                              'regional_scalars before saving'))
    return parser


def add_rei_id(parser):
    parser.add_argument('-r', '--rei_id', type=int,
                        required=True, action='store',
                        help='The rei to aggregate')
    return parser


def add_resume(parser):
    parser.add_argument('-r', '--resume', action='store_true',
                        default=False,
                        help='Resume an existing run - do not '
                             'overwrite existing output files')
    return parser


def add_sex_id(parser):
    parser.add_argument('-s', '--sex_id', type=int,
                        required=True, action='store',
                        help='The sex to aggregate')
    return parser


def add_sge_project(parser):
    parser.add_argument('-s', '--sge_project', type=str,
                        action='store',
                        help='The SGE project, default is '
                             'proj_<tool_name>')
    return parser


def add_star_ids(parser):
    parser.add_argument('--star_ids',
                        action='store_true', default=False,
                        help='Write out star_ids',
                        dest="write_out_star_ids")
    return parser


def add_skip_cause_agg(parser):
    parser.add_argument('--skip_cause_agg',
                        action='store_true', default=False,
                        help='skip cause aggregation')
    return parser


def add_start_and_end_at(parser, choices):
    parser.add_argument('--start_at', type=str,
                        choices=choices,
                        help='Which phase to start with: options {}'
                        .format(choices))

    parser.add_argument('--end_at', type=str,
                        choices=choices,
                        help='Which phase to end with: options are {}'
                        .format(choices))
    return parser


def add_start_and_end_year_ids(parser):
    parser.add_argument('--start_year_ids',
                        type=int, nargs='+',
                        help='The start years for pct change calculation, '
                             'a space-separated list')
    parser.add_argument('--end_year_ids',
                        type=int, nargs='+',
                        help='The end years for pct change calculation, '
                             'a space-separated list')

    return parser


def add_start_and_end_year(parser):
    parser.add_argument('-s', '--start_year',
                        type=int,
                        required=True,
                        help='The start year for pct change calculation')
    parser.add_argument('-e', '--end_year',
                        type=int,
                        required=True,
                        help='The end year for pct change calculation')
    return parser


# one function for both start and end years because if you have one you have both
def add_start_and_end_years(parser):
    parser.add_argument('--start_years',
                        type=int, nargs='+',
                        default=[],
                        dest="start_year_ids",
                        help='The start years for pct change '
                             'calculation, a space-separated list')
    parser.add_argument('--end_years',
                        type=int, nargs='+',
                        default=[],
                        dest="end_year_ids",
                        help='The end years for pct change '
                             'calculation, a space-separated list')
    return parser


def add_storage_engines(parser, valid_storage_engines):
    parser.add_argument('--storage_engine',
                        type=str,
                        required=True,
                        choices=valid_storage_engines,
                        help='The storage engine to upload to')
    return parser


def add_table_types(parser, valid_table_types):
    parser.add_argument('--table_type',
                        type=str,
                        required=True,
                        choices=valid_table_types,
                        help='The table type to upload to')
    return parser


def add_tool_names(parser, valid_tool_names, required=True):
    parser.add_argument('--tool_name',
                        type=str,
                        required=required,
                        choices=valid_tool_names,
                        help='The tool name')
    return parser


def add_turn_off_null_nan(parser):
    parser.add_argument('-n', '--turn_off_null_and_nan_check',
                        action='store_true',
                        help='No input restriction for nulls and '
                             'NaNs. Dangerous but necessary for '
                             'older GBD years.')
    return parser


def add_upload_to_test(parser):
    parser.add_argument('--upload_to_test', action='store_true',
                        default=False,
                        help='Upload data to test environment')
    return parser


def add_read_from_prod(parser):
    parser.add_argument('--read_from_prod', action='store_true',
                        default=False,
                        help='Allows reading from prod for version '
                             'management even when testing.')
    return parser


def add_verbose(parser):
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print many debugging messages')
    return parser


def add_years(parser):
    # creates mutually exclusive group so that one year set and n_draws are not
    # used with mixed_draw_years
    years_group = parser.add_mutually_exclusive_group()
    years_group.add_argument('--years', type=int, nargs='+',
                             default=[1990, 1995, 2000, 2005, 2010, 2016],
                             action='store',
                             dest='years',
                             help='The first set of years when only one draw '
                                  ' and year set is being used, a'
                                  ' space-separated list of integers')
    years_group.add_argument('-mdy', '--mixed_draw_years',
                             type=ac.parse_json_to_dictionary,
                             dest='mixed_draw_years',
                             help='A string representation of a python'
                             ' dictionary to map different draw numbers to'
                             ' different year sets. There have to be multiple'
                             ' draws being used for this flag.')
    return parser


def add_year_and_n_draws_group(parser):
    year_group = parser.add_mutually_exclusive_group(required=True)
    year_group.add_argument('-y', '--year_id',
                            type=int, action='store',
                            help='The year_id, an integer')
    year_group.add_argument('--y_list', '--start_end_year', nargs='+',
                            type=int, action='store',
                            help='start_year, end_year, a list')
    parser.add_argument('--n_draws', default=1000,
                        type=ac.strictly_positive_integer, action='store',
                        help='The number of draw columns for all input and '
                             'output draw files, for the specific year being '
                             'run')
    return parser


def add_year_id(parser):
    parser.add_argument('-y', '--year_id',
                        type=int, action='store', required=True,
                        help='The year_id, an integer')
    return parser


def add_year_ids(parser):
    parser.add_argument('--year_ids',
                        type=int, nargs='+',
                        help='The start years for pct change calculation, '
                             'a space-separated list')
    return parser


def int_or_best(string):
    if string == 'best':
        value = string
    else:
        try:
            value = int(string)
        except ValueError:
            msg = "Must be an integer or 'best'. Passed {}".format(
                string)
            raise argparse.ArgumentTypeError(msg)
    return value
