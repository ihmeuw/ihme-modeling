
import argparse
import logging

from db_tools import ezfuncs
import gbd.gbd_round as gbd
from gbd import constants as c

import hierarchies.dbtrees as hdb

logger = logging.getLogger(__name__)


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
    i_value = int(value)
    if i_value <= 0:
        raise argparse.ArgumentTypeError("Number must be strictly greater "
                                         "than zero, not {}".format(value))
    return i_value


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


def best_version(input_machine, gbd_round_id=5):
    """Find the 'best' model versions for como and cod from the database.
    Used as defaults"""
    if input_machine == 'como':
        gbd_process_id = c.gbd_process['EPI']
        metadata_type_id = c.gbd_metadata_type['COMO']
    elif input_machine == 'cod':
        gbd_process_id = c.gbd_process['COD']
        metadata_type_id = c.gbd_metadata_type['CODCORRECT']
    else:
        raise ValueError("app_common.best_version accepts 'como' or 'cod' as "
                         "input_machine. Got {}".format(input_machine))
    q = """
            SELECT
            pvm.val as version_id from gbd.gbd_process_version gpv
            JOIN
            gbd.gbd_process_version_metadata pvm using (gbd_process_version_id)
            WHERE
            gbd_process_id =  {process_id}
            and metadata_type_id = {metadata_type_id}
            and gbd_round_id = {gbd_round_id}
            and gbd_process_version_status_id = 1
            order by gpv.date_inserted desc
            limit 1
            """.format(process_id=gbd_process_id,
                       metadata_type_id=metadata_type_id,
                       gbd_round_id=gbd_round_id)
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


def construct_year_n_draws_map(year_ids_1, n_draws_1, year_ids_2, n_draws_2):
    """
    Return a dictionary mapping the year_id to the number of draws
    :param year_ids_1: as per CLI argument --year_ids_1
    :param n_draws_1: as per CLI argument
    :param year_ids_2: as per CLI argument --year_ids_2
    :param n_draws_2: as per CLI argument
    :return: the dictionary
    """
    year_n_draws_map = {}
    if year_ids_1:
        for y in year_ids_1:
            year_n_draws_map[y] = n_draws_1
    if year_ids_2:
        for y in year_ids_2:
            year_n_draws_map[y] = n_draws_2
    return year_n_draws_map


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


def validate_multi_mode_years(year_n_draws_map, year_ids_1, n_draws_1,
                              year_ids_2, n_draws_2, start_year_ids,
                              end_year_ids):
    """
    Check that:
      The two sets of years are disjoint
      They have different number of draws (if not - use one set)
      Percentage-change years have same number of draws

      Args:
        year_n_draws_map: The dictionary that maps each year to its number of
            draws
        year_ids_1: as per CLI argument --year_ids_1
        n_draws_1: as per CLI argument
        year_ids_2: as per CLI argument --year_ids_2
        n_draws_2: as per CLI argument
        start_year_ids: as per CLI argument
        end_year_ids: as per CLI argument

      Returns:
           nothing, raises ValueError if there are validation errors
    """

    if year_ids_1 and year_ids_2:
        if not set(year_ids_1).isdisjoint(year_ids_2):
            common = set(year_ids_1).intersection(set(year_ids_2))
            raise ValueError("The two sets of year_ids must be separate, "
                             "common years: {}".format(list(common)))

    if year_ids_2 and n_draws_1 and n_draws_2:
        if n_draws_1 == n_draws_2:
            raise ValueError("The number of draws must be different for the "
                             "two year sets, not both {}".format(n_draws_1))

    if start_year_ids and end_year_ids:
        if len(start_year_ids) != len(end_year_ids):
            raise ValueError("start_years and end_years should have same "
                             "length")

        for start_year, end_year in zip(start_year_ids, end_year_ids):
            if start_year >= end_year:
                raise ValueError("end_year need be greater than start_year")

            if not (start_year in year_ids_1 or start_year in year_ids_2):
                raise ValueError("percent start_year {} must be in "
                                 "--year_ids_1 or --year_ids_2"
                                 .format(start_year))
            if not (end_year in year_ids_1 or end_year in year_ids_2):
                raise ValueError("percent end_year {} must be in --year_ids_1 "
                                 "or --year_ids_2"
                                 .format(end_year))
            if year_n_draws_map[start_year] != year_n_draws_map[end_year]:
                raise ValueError("start and end_year have different number of "
                                 "draws: {} and {}"
                                 .format(start_year, end_year))


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


def location_set_version_to_location_set(location_set_version_id=None):
    """
    If location_set_version_id is defined then return the location_set_id.

    Args
     location_set_version_id:

    Returns
    """
    if location_set_version_id:
        return hdb.get_location_set_id(location_set_version_id)
    else:
        return None


def expand_and_validate_location_lists(tool_name, location_set_version_id,
                                       add_agg_loc_set_ids, gbd_round_id):
    """
    Create lists of most-detailed and aggregate location ids by expanding the
    location_set_version.

    Splits into leaf and interior nodes (for aggregation). Adds the
    supplementary aggregation locations to the set of interior nodes.

    Returns:
     most_detailed_location_ids - the leaf locations with not children;
     aggregate_location_ids - the internal locations with children

    """

    lt = hdb.loctree(location_set_version_id=location_set_version_id)
    all_location_ids = [l.id for l in lt.nodes]
    most_detailed_location_ids = [l.id for l in lt.leaves()]
    aggregate_location_ids = list(
        set(all_location_ids) - set(most_detailed_location_ids))

    # Include aggregate locations from supplemental sets
    for supp_set in add_agg_loc_set_ids:
        supp_lt = hdb.loctree(location_set_id=supp_set,
                              gbd_round_id=gbd_round_id)
        supp_leaves = set([l.id for l in supp_lt.leaves()])
        supp_nonleaves = (
            set([l.id for l in supp_lt.nodes]) - set(supp_leaves))
        aggregate_location_ids = list(
            set(aggregate_location_ids) | supp_nonleaves)

    # Remove 44620 (non-standard Global location)
    aggregate_location_ids = list(set(aggregate_location_ids) - {44620})

    return most_detailed_location_ids, aggregate_location_ids


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
