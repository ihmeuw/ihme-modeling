"""
Functions to query the demographics information using shared functions for CODEm models.
"""

import pandas as pd
import logging
import numpy as np

from db_queries import get_envelope, get_population, get_location_metadata
from gbd.decomp_step import decomp_step_from_decomp_step_id

from codem.data.shared import get_location_info, create_age_df

logger = logging.getLogger(__name__)


def population_weights(df, location_df, age_group_id, sex_id, year_id,
                       decomp_step_id, gbd_round_id, pop_run_id,
                       location_set_version_id, standard_location_set_version_id):
    """
    Calculate population-weights for a non-standard location.
    :return:
    """
    sl = get_location_metadata(location_set_version_id=standard_location_set_version_id)
    sl = sl.loc[sl.is_estimate == 1]['location_id'].tolist()
    pp = get_location_metadata(location_set_version_id=location_set_version_id,
                               gbd_round_id=gbd_round_id)[['location_id',
                                                           'path_to_top_parent',
                                                           'level']]
    ldf = location_df.copy()
    ldf = ldf.merge(pp, on=['location_id'], how='left')
    if ldf['path_to_top_parent'].isnull().any():
        raise RuntimeError("There are locations in the location data frame that are not in the metadata. Fix!")
    # Get the parent ID that is standard location.
    # Might be at level 3, 4, or 5 of the hierarchy.
    ldf['standard_location_id'] = ldf['path_to_top_parent'].\
        apply(lambda x: np.array(x.split(','))[np.isin(x.split(','), sl)])
    for i in ldf['standard_location_id']:
        if len(i) > 1:
            raise RuntimeError("There is more than 1 level of the hierarchy for "
                               "this location listed as the standard location. Fix!")
        if len(i) == 0:
            raise RuntimeError("There is no standard location listed for this location. You"
                               "might have locations above the national level. Fix!")
    ldf['standard_location_id'] = ldf['standard_location_id'].apply(lambda x: x[0]).astype(int)

    sl_pop = get_population(age_group_id=age_group_id,
                            sex_id=sex_id,
                            year_id=year_id,
                            location_set_id=35,
                            location_id=sl,
                            gbd_round_id=gbd_round_id,
                            decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
                            run_id=pop_run_id)
    sl_pop.drop(['run_id'], inplace=True, axis=1)
    sl_pop.rename(columns={'population': 'standard_location_pop',
                           'location_id': 'standard_location_id'}, inplace=True)
    df = df.merge(ldf[['location_id', 'standard_location_id']],
                  how='left', on=['location_id'])
    df = df.merge(sl_pop,
                  on=['standard_location_id', 'age_group_id',
                      'year_id', 'sex_id'], how='left')

    if df['population'].isnull().any() or df['standard_location_pop'].isnull().any():
        raise ValueError("There are null values in the data frame. You would have null weights!")

    df['weight'] = df['population'].values / df['standard_location_pop'].values
    return df


def get_mortality_data(sex, start_year, start_age, end_age, location_set_version_id,
                       gbd_round_id, gbd_round, decomp_step_id, db_connection,
                       env_run_id, pop_run_id, standard_location_set_version_id):
    """
    strings indicating model parameters -> Pandas Data Frame

    Given a set of model parameters will query from the mortality database and
    return a pandas data frame. The data frame contains the base variables
    used in the CODEm process.

    Also calculates the weights for subnationals of standard locations.
    """
    logger.info("Querying mortality and population.")
    loc_df = get_location_info(location_set_version_id=location_set_version_id,
                               standard_location_set_version_id=standard_location_set_version_id,
                               db_connection=db_connection)
    loc_list = loc_df.location_id.values.tolist()

    age_df = create_age_df(db_connection)
    age_restrict = "all_ages >= {0} & all_ages <= {1}".format(start_age,
                                                              end_age)
    age_list = age_df.query(age_restrict).all_ages.values.tolist()
    env = get_envelope(age_group_id=age_list,
                       sex_id=sex,
                       year_id=list(range(start_year, gbd_round+1)),
                       location_set_id=35,
                       location_id=loc_list,
                       gbd_round_id=gbd_round_id,
                       decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
                       run_id=env_run_id)

    pop = get_population(age_group_id=age_list,
                         sex_id=sex,
                         year_id=list(range(start_year, gbd_round+1)),
                         location_set_id=35,
                         location_id=loc_list,
                         gbd_round_id=gbd_round_id,
                         decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
                         run_id=pop_run_id)

    df = pd.merge(env, pop, on=['age_group_id', 'location_id', 'year_id',
                                'sex_id'])
    df.drop(['upper', 'lower', 'run_id_x', 'run_id_y'], axis=1, inplace=True)

    df = population_weights(df, loc_df, age_group_id=age_list,
                            sex_id=sex, year_id=list(range(start_year, gbd_round + 1)),
                            decomp_step_id=decomp_step_id,
                            gbd_round_id=gbd_round_id,
                            pop_run_id=pop_run_id,
                            location_set_version_id=location_set_version_id,
                            standard_location_set_version_id=standard_location_set_version_id)

    df = df[['age_group_id', 'location_id', 'year_id', 'sex_id',
             'mean', 'population', 'weight']]
    df.rename(columns={'age_group_id': 'age', 'year_id': 'year',
                       'sex_id': 'sex', 'mean': 'envelope',
                       'population': 'pop'}, inplace=True)
    return df
