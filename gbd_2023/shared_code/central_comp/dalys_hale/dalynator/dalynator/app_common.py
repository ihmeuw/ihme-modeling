import argparse
import logging
import json
from typing import List

from db_queries import get_age_spans, get_population
import hierarchies.dbtrees as hdb

from dalynator.type_checking import is_list_of_year_ids


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


def create_cluster_project(cluster_project, tool_name):
    """
    Create the default cluster project/account name

    Args:
        cluster_project:  The project name, from the command line
        tool_name:  Dalynator or Burdenator, must be set

    Returns:
        The cluster_project name

    Raises:
        ValueError if neither argument is set
    """
    if cluster_project is None:
        if tool_name is None:
            raise ValueError(" Neither cluster project nor tool name is set. "
                             "Tool name must be set")
        else:
            cluster_project = "proj_{}".format(tool_name)
    return cluster_project


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


def expand_and_validate_location_lists(tool_name, location_set_ids, release_id):
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
            location_set_id=loc_set, release_id=release_id, return_many=True
        )
        for lt in tree_list:
            most_detailed_locs.update(set([l.id for l in lt.leaves()]))
            aggregate_locs.update(set([l.id for l in lt.nodes
                                  if l.id not in most_detailed_locs]))

    # non-standard Global location
    aggregate_locs = set(aggregate_locs - {44620})

    return most_detailed_locs, aggregate_locs


def validate_age_group_ids(age_group_ids: List[int]) -> None:
    # Check for duplicates
    if len(age_group_ids) != len(set(age_group_ids)):
        raise ValueError(
            "List of age_group_ids contains duplicates"
        )

    # Make sure all age_group_ids are real age groups
    valid_age_groups = get_age_spans().age_group_id
    bad_age_groups = ", ".join(
        str(age) for age in set(age_group_ids).difference(set(valid_age_groups))
    )
    if bad_age_groups:
        raise ValueError(
            f"Age groups [{bad_age_groups}] are not valid age groups"
        )


def get_population_run_id(release_id: int) -> int:
    """The best population run_id for the given release."""
    return int(get_population(
        release_id=release_id,
        use_rotation=False
    ).run_id.iat[0])


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'
