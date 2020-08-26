import pandas as pd
import numpy as np

from db_queries import get_location_metadata, get_demographics
from db_tools.ezfuncs import query
import gbd.constants as gbd
from gbd.gbd_round import (
    gbd_round_from_gbd_round_id,
    gbd_round_from_gbd_round_id
)


def get_location_hierarchy(location_set_id, gbd_round_id=gbd.GBD_ROUND_ID):
    result_df = get_location_metadata(location_set_id,
                                      gbd_round_id=gbd_round_id)
    return result_df[['location_id', 'location_name', 'path_to_top_parent',
                      'parent_id', 'level', 'is_estimate', 'most_detailed',
                      'sort_order']]


def get_data_rich_locations(gbd_round_id=gbd.GBD_ROUND_ID):
    location_hierarchy = get_location_hierarchy(43, gbd_round_id=gbd_round_id)
    loc_ids = location_hierarchy.loc[location_hierarchy['parent_id'] == 44640
                                     ].location_id.unique().tolist()
    return loc_ids


def get_all_most_detailed(gbd_round_id):
    # Get location hierarchy
    return get_demographics('cod', gbd_round_id=gbd_round_id)['location_id']


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
    spacetime_restriction_data = query(sql_query, conn_def='SCHEMA')
    return spacetime_restriction_data

# FIXME - It doesn't look like this is used anywhere. The imported cases code
# doesn't use it and there is no get_data_rich_countries
# function in this module so it will break.
def get_restricted_locations_for_cause(cause_id, gbd_round_id=gbd.GBD_ROUND_ID):
    gbd_round = int(gbd_round_from_gbd_round_id(gbd_round_id))
    # Get space-time restrictions
    st_restrictions = get_spacetime_restrictions(gbd_round)
    # Get data rich countries
    data_rich_countries = get_data_rich_countries()
    # Keep only causes tbat have space-time restrictions in data-rich countries
    st_restrictions = st_restrictions.loc[st_restrictions['location_id'].isin(
        data_rich_countries)]
    return st_restrictions.loc[st_restrictions['cause_id'] == cause_id,
                               'location_id'].drop_duplicates().tolist()


def get_cause_specific_locations(cause_id, gbd_round_id):
    gbd_round = int(gbd_round_from_gbd_round_id(gbd_round_id))
    st_restrictions = get_spacetime_restrictions(gbd_round)
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
    final_year = int(gbd_round_from_gbd_round_id(gbd_round_id))
    data = pd.DataFrame(get_all_most_detailed(gbd_round_id),
                        columns=['location_id'])
    data = expand_id_set(data, range(1980, final_year+1), 'year_id')
    data = expand_id_set(data, [1, 2], 'sex_id')
    data = expand_id_set(data, age_groups, 'age_group_id')
    data['cause_id'] = cause_id
    for x in range(1000):
        data['draw_{}'.format(x)] = 0
    return data
