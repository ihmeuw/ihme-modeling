import pandas as pd
import numpy as np

from db_queries import get_location_metadata, get_demographics
from db_tools.ezfuncs import query


def get_location_hierarchy(location_set_id, gbd_round_id=5):
    """Defaults to round 5 as of wave 1 update to shared functions."""
    result_df = get_location_metadata(location_set_id,
                                      gbd_round_id=gbd_round_id)
    return result_df[['location_id', 'location_name', 'path_to_top_parent',
                      'parent_id', 'level', 'is_estimate', 'most_detailed',
                      'sort_order']]


def get_data_rich_locations(gbd_round_id=4):
    """Hack for GBD 2017: Including Taiwan before new loc_set_id for round 5
    is ready. Need to use round id 4 in the meantime."""
    # Get location hierarchy
    location_hierarchy = get_location_hierarchy(43, gbd_round_id=gbd_round_id)
    taiwan = [8]
    loc_ids = location_hierarchy.loc[location_hierarchy['parent_id'] == 44640
                                     ].location_id.unique().tolist()
    return loc_ids + taiwan


def get_all_most_detailed(gbd_round_id):
    # Get location hierarchy
    location_hierarchy = get_location_hierarchy(89, gbd_round_id=gbd_round_id)
    return location_hierarchy.loc[location_hierarchy['most_detailed'] == 1
                                  ].location_id.unique().tolist()


def get_spacetime_restrictions(gbd_round):
    sql_query = """
        SELECT
            rv.cause_id,
            r.location_id,
            r.year_id
        FROM
            codcorrect.spacetime_restriction r
        JOIN
            codcorrect.spacetime_restriction_version rv
                USING (restriction_version_id)
        WHERE
            rv.is_best = 1 AND
            rv.gbd_round = {gbd_round};
    """.format(gbd_round=gbd_round)
    spacetime_restriction_data = query(sql_query, conn_def='codcorrect')
    return spacetime_restriction_data


def get_restricted_locations_for_cause(cause_id, gbd_round_id=5):
    round_map = {4: 2016, 5: 2017}
    # Get space-time restrictions
    st_restrictions = get_spacetime_restrictions(round_map[gbd_round_id])
    # Get data rich countries
    data_rich_countries = get_data_rich_countries()
    # Keep only causes tbat have space-time restrictions in data-rich countries
    st_restrictions = st_restrictions.loc[st_restrictions['location_id'].isin(
        data_rich_countries)]
    return st_restrictions.loc[st_restrictions['cause_id'] == cause_id,
                               'location_id'].drop_duplicates().tolist()


def get_cause_specific_locations(cause_id, gbd_round_id):
    round_map = {4: 2016, 5: 2017}
    st_restrictions = get_spacetime_restrictions(round_map[gbd_round_id])
    data_rich = get_data_rich_locations()
    st_restrictions = st_restrictions[st_restrictions.location_id.isin(
        data_rich)]
    return (st_restrictions[st_restrictions.cause_id == cause_id]
            .location_id.unique().tolist())


def generate_distribution(data):
    data = data.reset_index(drop=True)
    data['i'] = data.index
    temp = []
    for idx, row in data.iterrows():
        row_dict = row.to_dict()
        cf = row_dict['cf']
        sample_size = row_dict['sample_size']
        deaths = cf * sample_size
        other_deaths = sample_size - deaths
        if deaths < 1:
            draws = np.zeros(shape=(1000,))
        elif other_deaths <= 0:
            draws = np.zeros(shape=(1000,))
        else:
            draws = np.random.beta(deaths, other_deaths, 1000)
        draws = draws * sample_size
        t = pd.DataFrame([draws],
                         columns=['draw_{}'.format(x) for x in range(1000)])
        t['i'] = idx
        temp.append(t)
    temp = pd.concat(temp)
    data = pd.merge(data, temp, on='i')
    keep_cols = (['location_id', 'year_id', 'sex_id', 'age_group_id',
                  'cause_id'] + ['draw_{}'.format(x) for x in range(1000)])
    return data[keep_cols]


def expand_id_set(input_data, eligible_ids, id_name):
    """ Duplicates input data by the number of items in eligible ids and creates
        new column

        Returns: newly expanded DataFrame
    """
    result_df = []
    for i in eligible_ids:
        temp = input_data.copy(deep=True)
        temp[id_name] = i
        result_df.append(temp)
    return pd.concat(result_df)


def make_square_data(cause_id, age_groups, gbd_round_id):
    data = pd.DataFrame(get_all_most_detailed(gbd_round_id),
                        columns=['location_id'])
    data = expand_id_set(data, range(1980, 2018), 'year_id')
    data = expand_id_set(data, [1, 2], 'sex_id')
    data = expand_id_set(data, age_groups, 'age_group_id')
    data['cause_id'] = cause_id
    for x in range(1000):
        data['draw_{}'.format(x)] = 0
    return data
