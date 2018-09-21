import pandas as pd
import numpy as np

from db_queries import get_location_metadata, get_demographics
from db_tools.ezfuncs import query


def get_best_model_version(gbd_round):
    """ Get the list of best models for a given GBD round

        Returns: DataFrame with gbd_round, model_version_id,
                 cause_id, sex_id, and is_best
    """
    sql_statement = """SELECT
                        gbd_round,
                        model_version_id,
                        cause_id,
                        sex_id,
                        model_version_type_id,
                        is_best
                       FROM
                        cod.model_version
                       JOIN shared.cause_set_version
                       USING (cause_set_version_id)
                       WHERE
                        is_best = 1 AND
                        gbd_round = {gbd_round}
                    """.format(gbd_round=gbd_round)
    result_df = query(sql_statement, conn_def='cod')
    return result_df


def get_age_groups():
    return get_demographics('cod')['age_group_ids']


def get_location_hierarchy(location_set_id):
    result_df = get_location_metadata(location_set_id)
    return result_df[['location_id', 'location_name', 'path_to_top_parent',
                      'parent_id', 'level', 'is_estimate', 'most_detailed',
                      'sort_order']]


def get_data_rich_countries():
    # Get location hierarchy
    location_hierarchy = get_location_hierarchy(43)
    return location_hierarchy.ix[location_hierarchy['parent_id'] == 44640
                                 ].location_id.unique().tolist()


def get_all_most_detailed():
    # Get location hierarchy
    location_hierarchy = get_location_hierarchy(35)
    return location_hierarchy.ix[location_hierarchy['most_detailed'] == 1
                                 ].location_id.unique().tolist()


def get_spacetime_restrictions():
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
            rv.gbd_round = 2016;"""
    spacetime_restriction_data = query(sql_query, conn_def='codcorrect')
    return spacetime_restriction_data


def get_data_rich_spacetime_restricted_causes():
    # Get space-time restrictions
    st_restrictions = get_spacetime_restrictions()
    # Get data rich countries
    data_rich_countries = get_data_rich_countries()
    # Keep only causes tbat have space-time restrictions in data-rich countries
    st_restrictions = st_restrictions.ix[st_restrictions['location_id'].isin(
        data_rich_countries)]
    # Get list of causes
    return st_restrictions['cause_id'].drop_duplicates().tolist()


def get_restricted_locations_for_cause(cause_id):
    # Get space-time restrictions
    st_restrictions = get_spacetime_restrictions()
    # Get data rich countries
    data_rich_countries = get_data_rich_countries()
    # Keep only causes tbat have space-time restrictions in data-rich countries
    st_restrictions = st_restrictions.ix[st_restrictions['location_id'].isin(
        data_rich_countries)]
    return st_restrictions.ix[st_restrictions['cause_id'] == cause_id,
                              'location_id'].drop_duplicates().tolist()


def get_cod_data_for_cause_location(cause_id, locations, value='raw'):
    ages = get_age_groups()
    sql_query = """
                    SELECT
                        location_id,
                        year_id,
                        sex_id,
                        age_group_id,
                        cause_id,
                        cf_{v} AS cf,
                        cf_{v} * sample_size AS deaths,
                        sample_size
                    FROM
                        cod.cm_data
                    WHERE
                        cause_id = {c} AND
                        is_outlier = 0 AND
                        age_group_id IN({ages}) AND
                        location_id IN ({loc}) AND
                        year_id >= 1980 AND
                        cf_{v} != 0;
                """.format(v=value,
                           c=cause_id,
                           ages=','.join([str(x) for x in ages]),
                           loc=','.join([str(x) for x in locations]))
    data = query(sql_query, conn_def='cod')
    return data


def generate_distribution(data):
    data = data.reset_index(drop=True)
    data['i'] = data.index
    temp = []
    for i in data.index:
        cf = data.ix[i, 'cf']
        sample_size = data.ix[i, 'sample_size']
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
                         columns=['draw_{}'.format(x) for x in xrange(1000)])
        t['i'] = i
        temp.append(t)
    temp = pd.concat(temp)
    data = pd.merge(data, temp, on='i')
    keep_cols = (['location_id', 'year_id', 'sex_id', 'age_group_id',
                  'cause_id'] + ['draw_{}'.format(x) for x in xrange(1000)])
    return data.ix[:, keep_cols]


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


def make_square_data(cause_id):
    data = pd.DataFrame(get_all_most_detailed(), columns=['location_id'])
    data = expand_id_set(data, range(1980, 2017), 'year_id')
    data = expand_id_set(data, [1, 2], 'sex_id')
    data['age_group_id'] = 20  # just need one age_group to ensure squareness
    data['cause_id'] = cause_id
    for x in xrange(1000):
        data['draw_{}'.format(x)] = 0
    return data
