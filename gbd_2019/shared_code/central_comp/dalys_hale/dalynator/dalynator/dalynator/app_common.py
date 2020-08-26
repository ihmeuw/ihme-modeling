import argparse
import logging
import json

from db_tools import ezfuncs
import gbd.gbd_round as gbd
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd import constants as c
from dalynator.type_checking import is_list_of_year_ids

import hierarchies.dbtrees as hdb

logger = logging.getLogger(__name__)


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
                        'output draw files')
    return parser


def best_version(input_machine, gbd_round_id, decomp_step):
    """Find the 'best' model versions for como and cod from the database.
    Used as defaults
    """
    if input_machine == 'como':
        gbd_process_id = c.gbd_process['EPI']
        metadata_type_id = c.gbd_metadata_type['COMO']
    elif input_machine == 'codcorrect':
        gbd_process_id = c.gbd_process['COD']
        metadata_type_id = c.gbd_metadata_type['CODCORRECT']
    elif input_machine == 'fauxcorrect':
        gbd_process_id = c.gbd_process['FAUXCORRECT']
        metadata_type_id = c.gbd_metadata_type['FAUXCORRECT']
    else:
        raise ValueError("app_common.best_version accepts 'como', "
                         "'codcorrect' or 'fauxcorrect' as "
                         "input_machine. Got {}".format(input_machine))
    decomp_step_id = decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id)
    q = """
            SELECT
            pvm.val as version_id from gbd.gbd_process_version gpv
            JOIN
            gbd.gbd_process_version_metadata pvm using (gbd_process_version_id)
            WHERE
            gbd_process_id =  {process_id}
            and metadata_type_id = {metadata_type_id}
            and gbd_round_id = {gbd_round_id}
            and decomp_step_id = {decomp_step_id}
            and gbd_process_version_status_id = 1
            order by gpv.date_inserted desc
            limit 1
            """.format(process_id=gbd_process_id,
                       metadata_type_id=metadata_type_id,
                       gbd_round_id=gbd_round_id,
                       decomp_step_id=decomp_step_id)
    return int(ezfuncs.query(q, conn_def='gbd').squeeze())


def create_sge_project(sge_project, tool_name):
    """
    Create the default SGE project name, which is 'proj_<tool_name>'

    Args:
        sge_project:  The project name, from the command line (possibly None)
        tool_name:  Dalynator or Burdenator, must be set

    Returns:
        The sge_project name, proj_dalynator or proj_burdenator

    Raises:
        ValueError if neither argument is set
    """
    if sge_project is None:
        if tool_name is None:
            raise ValueError(" Neither SGE project nor tool name is set. "
                             "Tool name must be set")
        else:
            sge_project = "proj_{}".format(tool_name)
    return sge_project


def construct_year_n_draws_map(n_draws_to_years_dict):
    """
    Return a dictionary mapping the year_id to the number of draws
    :param n_draws_to_years_dict: dictionary mapping number of draws to year_ids
    :return: the dictionary
    """
    year_n_draws_map = {}
    # take the keys and values and switch them to make the map
    for n_draws in n_draws_to_years_dict.keys():
        for y in n_draws_to_years_dict[n_draws]:
            year_n_draws_map[y] = n_draws

    return year_n_draws_map


def strictly_positive_integer(value):
    """
    Used by arg parser, hence it must raise that specific exception type.

    Args:
        value:

    Returns:
        The value, if it is an integer greater than zero

    Raises:
        ArgumentTypeError otherwise
    """
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("Number must be strictly greater "
                                         "than zero, not {}".format(value))
    return ivalue


def validate_start_end_flags(start_at, end_at, tool_name):
    start_at = start_at.lower()
    end_at = end_at.lower()
    if tool_name == 'burdenator':
        all_phases = ['most_detailed', 'loc_agg', 'cleanup', 'pct_change',
                      'upload']
    else:
        all_phases = ['most_detailed', 'pct_change', 'upload']
    assert start_at in all_phases
    assert end_at in all_phases
    if all_phases.index(start_at) > all_phases.index(end_at):
        raise ValueError("Start_at must be a phase that runs prior to the "
                         "end_at phase, or both start_at and end_at must be "
                         "the same phase. Got start_at of {} and end_at of {}"
                         .format(start_at, end_at))
    if start_at == end_at:
        run_phases = [start_at]
    else:
        run_phases = (
            all_phases[all_phases.index(start_at):
                       all_phases.index(end_at) + 1])
    return run_phases


def validate_multi_mode_years(year_n_draws_map, n_draws_years_dict,
                              start_year_ids, end_year_ids):
    """
    Check that:
      The two sets of years are disjoint
      They have different number of draws (if not - use one set)
      Percentage-change years have same number of draws

      Args:
        year_n_draws_map: The dictionary that maps each year to its number of
            draws
        n_draws_years_dict: The dictionary that maps each draw to its year set
        start_year_ids: as per CLI argument
        end_year_ids: as per CLI argument

      Returns:
           nothing, raises ValueError if there are validation errors
    """
    # if there are two draw-year sets, else assume one set
    if len(n_draws_years_dict.keys()) == 2:
        draw_1 = list(n_draws_years_dict.keys())[0]
        draw_2 = list(n_draws_years_dict.keys())[1]
        years = n_draws_years_dict[draw_1]
        years_2 = n_draws_years_dict[draw_2]
    else:
        years = list(n_draws_years_dict.values())[0]
        years_2 = []

    if not set(years).isdisjoint(years_2):
        common = set(years).intersection(set(years_2))
        raise ValueError("The two sets of year_ids must be separate, "
                         "common years: {}".format(list(common)))

    if start_year_ids and end_year_ids:
        if len(start_year_ids) != len(end_year_ids):
            raise ValueError("start_years and end_years should have same "
                             "length")

        for start_year, end_year in zip(start_year_ids, end_year_ids):
            if start_year >= end_year:
                raise ValueError("end_year need be greater than start_year")

            if not (start_year in years or start_year in years_2):
                raise ValueError("percent start_year {} must be in "
                                 "--year_ids_1 or --year_ids_2"
                                 .format(start_year))
            if not (end_year in years or end_year in years_2):
                raise ValueError("percent end_year {} must be in --year_ids_1 "
                                 "or --year_ids_2"
                                 .format(end_year))
            if year_n_draws_map[start_year] != year_n_draws_map[end_year]:
                raise ValueError("start and end_year have different number of "
                                 "draws: {} and {}"
                                 .format(start_year, end_year))


def parse_json_to_dictionary(value):
    """
    Used by arg parser, hence it must raise that specific exception type.

    Args:
        value:

    Returns:
        The value, if it resolves to a dictionary where

    Raises:
        ArgumentTypeError otherwise
    """
    try:
        # converts raw json string into a dictionary.
        # since keys have to be wrapped in double quotes to satisfy json string
        # format we have to convert the keys back into ints
        input_dict = json.loads(value,
                                object_hook=lambda d:
                                {int(k): v for k, v in d.items()})
    except Exception:
        raise argparse.ArgumentTypeError("String does not resolve to a "
                                         "dictionary: {}".format(value))

    return input_dict


def check_mixed_draw_years_format(mixed_draw_years):
    # has two keys
    num_keys = len(mixed_draw_years.keys())
    if num_keys != 2:
        raise argparse.ArgumentTypeError("--mixed_draw_years should have 2"
                                         " sets of draws. Instead has {}."
                                         .format(num_keys))
    # keys are positive ints
    for key in mixed_draw_years.keys():
        strictly_positive_integer(key)

    # values are lists
    for value in mixed_draw_years.values():
        if not isinstance(value, list):
            raise argparse.ArgumentTypeError("A list of years was expected as a"
                                             " value in --mixed_draw_years."
                                             " Instead got type {}"
                                             .format(type(value)))

    # each element in list is a list of year_ids
    for key in mixed_draw_years.keys():
        is_list_of_year_ids(mixed_draw_years[key], "Year set")


def to_list(v):
    """
    If the input var is a list, return it.
    If it is integer type, wrap it in a list.

    Args:
        v:

    Returns:
        a list of integer types
    """
    if isinstance(v, (int, long)):
        return [v]
    else:
        return v


def location_set_to_location_set_version(location_set_id=None,
                                         location_set_version_id=None,
                                         gbd_round=2017):
    """
    If location_set_id is defined then return the location_set_version_id for
    that gbd round.

    Args:
        location_set_id:

        location_set_version_id:

        gbd_round: a YEAR, not the id. e.g. 2016, not 4

    Returns
    """
    if location_set_id:
        lsv = hdb.get_location_set_version_id(location_set_id,
                                              gbd_round=gbd_round)
        if location_set_version_id and location_set_version_id != lsv:
            raise ValueError("location_set_id {ls} and "
                             "location_set_version_id {lsv} are inconsistent".
                             format(ls=location_set_id,
                                    lsv=location_set_version_id))
        else:
            return int(lsv)
    else:
        return None


def expand_and_validate_location_lists(tool_name, location_set_ids,
                                       gbd_round_id):
    """
    Create lists of most-detailed and aggregate location ids by expanding the
    location_set_ids.


    Returns:
     most_detailed_locs - the leaf locations with not children;
     aggregate_locs - the internal locations with children

    """
    most_detailed_locs = set()
    aggregate_locs = set()

    for loc_set in location_set_ids:
        tree_list = hdb.loctree(
            location_set_id=loc_set, gbd_round_id=gbd_round_id,
            return_many=True)
        for lt in tree_list:
            most_detailed_locs.update(set([l.id for l in lt.leaves()]))
            aggregate_locs.update(set([l.id for l in lt.nodes
                                  if l.id not in most_detailed_locs]))

    # Remove 44620 (non-standard Global location)
    aggregate_locs = set(aggregate_locs - {44620})

    return most_detailed_locs, aggregate_locs


def xor(a, b):
    """
    XOR - to avoid using Python's *bitwise* xor operator ^

    Args:
     this one:
     that one:

    Returns: xor of this one and that one
    """
    return (a and not b) or (not a and b)


def populate_gbd_round_args(gbd_round, gbd_round_id):
    """
    Ensures that the two fields are consistent.
    Exactly one must be set, will raise value error if that is not true.

    Args:
        gbd_round:
        gbd_round_id:

    Returns:
        Consistent gbd_round, gbd_round_id, or raises ValueError if both or
        neither set
    """
    if xor(gbd_round, gbd_round_id):
        if gbd_round:
            gbd_round_id = gbd.gbd_round_id_from_gbd_round(gbd_round)
        else:
            gbd_round = gbd.gbd_round_from_gbd_round_id(gbd_round_id)
        return gbd_round, gbd_round_id
    else:
        raise ValueError("Exactly one of gbd_round and gbd_round_id can be "
                         "set, not {r}, {id}"
                         .format(r=gbd_round, id=gbd_round_id))


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'
