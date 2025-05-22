"""
Project: GBD Vision Loss
Purpose: Functions for assisting with the underlying processes of the
etiology specific adjustments.

Currently the functions in this script provide support for the following
processes:

- Applying geographic restrictions for Trachoma and Vitamin A Deficiency.
- Prevalence of vision loss due to Vitamin A under the age of 1 is set to zero
because it takes time for deficiency and damage to occur.
- Split out moderate or worse Meningitis and Encepalitis from mild or worse
Meningitis and Encepalitis.

"""

# ---IMPORTS-------------------------------------------------------------------

import logging
import numpy as np
import pandas as pd

from db_queries import (get_age_metadata)

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

# ---FUNCTIONS-----------------------------------------------------------------


def apply_grs(cause_df,
              cap_df,
              cap_col,
              age_restrictions,
              location_id,
              draw_col='draw_',
              subset_by_year=False,
              year_id=None,
              year_col=None,
              draw_count=1000):
    """
   
    Arguments:
        cause_df (dataframe): Pandas dataframe of estimates to apply
            geograpic restrictions to.
        cap_df (dataframe): Pandas dataframe representing a codebook of
            locations that must be zeroed out.
        cap_col (str): The column in "cap_df" containing binary values
            dictating whether the estimates should be zeroed out.
        age_restrictions (intlist): List of age groups that should also be
            zeroed out.
        location_id (int): The location_id of the data in "cause_df".
        draw_col (str): The name of the draw columns in "cause_df". Defaults to
            "draw_".
        subset_by_year (bool): Whether to subset the "cap_df"
            dataframe to use geographic retrictions from a specific
            year. Defaults to False.
        year_id (int): The year to subset the geographic restrictions by
            Defaults to None.
        year_col (str): The column in "cap_df" containing the year_id.
            Defaults to None.
        draw_count (int): The number of draws to run a specific
            transformation on. 

    Returns:
        Pandas dataframe of estimates where locations that are not estimated
        have been zeroed out.
    """

    if subset_by_year:
        cap_df = cap_df[cap_df[year_col] == year_id]
    cap_df = cap_df.rename(columns={cap_col: 'prev_0'})

    # Assume that locations with a value of 0 have a prevalence of zero
    logging.info(cap_df.shape)
    cap_prev = cap_df.query(
        "location_id == @location_id").prev_0.values[0]
    for i in range(draw_count):
        cause_df['{}{}'.format(draw_col,
            i)] = cause_df['{}{}'.format(draw_col, i)] * cap_prev

    # Age restrictions
    for i in range(draw_count):
        cause_df.loc[cause_df.age_group_id.isin(age_restrictions),
                     '{}{}'.format(draw_col, i)] = 0
    return cause_df


def cap_vita(df,
             location_id,
             age_ids_list):
    """
    Ensures that the prevalence of vitamin A deficiency does not exceed the
    prevalence of vitamin A deficiency for each sex, year, and draw.

    Arguments:
        df (pandas DataFrame): DataFrame with age_group_id (and a value for
            each age in age_group_id), location_id, sex_id, year_id.
        location_id (int): GBD location_id.
        age_ids_list (intlist): age_ids_list (intlist): The list of age groups
            to use.

    Returns:
        A dataframe where vitamin a is capped to be no greater than the age
        group 6 value within each draw, year, and sex.
    """

    for sex in df.sex_id.unique():
        logging.info("sex = {}".format(sex))
        for year in df.year_id.unique():
            logging.info("year = {}".format(year))
            df = df.set_index(["location_id",
                               "sex_id",
                               "year_id",
                               "age_group_id"]).sort_index()
            one_set_of_ages = df.loc[location_id, sex, year].copy()
            max_vals = one_set_of_ages.loc[6]
            for age in age_ids_list:
                if age in [ID]:
                    one_set_of_ages.loc[age] = one_set_of_ages.loc[age].clip(
                        upper=max_vals)
                    df.loc[location_id, sex, year, age] = one_set_of_ages.loc[
                       age]
                else:
                    pass
            df.reset_index(inplace=True)
    return df


def get_mod_plus_proportions(prop_path,
                             age_group_set_id,
                             release_id,
                             age_ids_list,
                             under_5):
    """

    Arguments:
        prop_path (str): The path to the meningitis and encepalitis
            proportion data used in the crosswalk.
        age_group_set_id (int): The age group set to use.
        release_id (int): The GBD release to pull estimates for.
        age_ids_list (intlist): The list of age groups to use.
        under_5 (intlist): A list of age groups representing age groups under 5
            years of age.

    Returns:
        A dataframe with draws of proportions of moderate or worse vision loss
    """

    mild_mod_xwalk = pd.read_stata(prop_path)

    # Get year start
    age_group_ids = get_age_metadata(age_group_set_id=age_group_set_id,
                                     release_id=release_id)

    # Filter age group column to ensure that only useable metadata is present.
    age_group_ids = age_group_ids[['age_group_id', 'age_group_years_start']]
    age_group_ids.rename(columns={'age_group_years_start': 'age_start'},
                         inplace=True)

    # Add birth prevalence (this is not included in get age metadata
    birth_prev = pd.Series([ID])
    birth_prev = pd.DataFrame([birth_prev])
    birth_prev = birth_prev.rename(columns={0: 'age_group_id', 1: 'age_start'})
    age_group_ids = pd.concat([birth_prev, age_group_ids], ignore_index=True)

    # Filter for age_group_ids that come from the config file list.
    age_group_ids = age_group_ids[
        age_group_ids.age_group_id.isin(age_ids_list)]
    mild_mod_x_walk = pd.merge(
        age_group_ids, mild_mod_xwalk, on='age_start', how='left')

    # Remove all proportions except for the mild or worse ones.
    mild_mod_x_walk = mild_mod_x_walk[['age_group_id',
                                       'mod_p_d_dmild_dmod_dsev_dvb',
                                       'mod_p_d_dmild_dmod_dsev_dvb_se']]
    under5_mean = mild_mod_x_walk.query("age_group_id == ID")[
        'mod_p_d_dmild_dmod_dsev_dvb'].values[0]
    under5_se = mild_mod_x_walk.query("age_group_id == ID")[
        'mod_p_d_dmild_dmod_dsev_dvb_se'].values[0]
    mild_mod_x_walk.loc[
        mild_mod_x_walk.age_group_id.isin(under_5),
        'mod_p_d_dmild_dmod_dsev_dvb'] = under5_mean
    mild_mod_x_walk.loc[
        mild_mod_x_walk.age_group_id.isin(under_5),
        'mod_p_d_dmild_dmod_dsev_dvb_se'] = under5_se
    if list(np.sort(mild_mod_x_walk.age_group_id.values)) != list(
            np.sort(age_ids_list)):
        logging.info(list(np.sort(mild_mod_x_walk.age_group_id.values)))
        logging.info(list(np.sort(age_ids_list)))
        raise ValueError(
            "all age group ids specified in the config file need to be valid")

 
    for i in range(1000):
        mild_mod_x_walk['draw_{}'.format(i)] = 0
    means = {}
    st_errs = {}
    for age_group in mild_mod_x_walk.age_group_id.values:
        # get mean and se
        means[age_group] = mild_mod_x_walk.query(
            "age_group_id == @age_group"
            ).mod_p_d_dmild_dmod_dsev_dvb.values[0]
        st_errs[age_group] = mild_mod_x_walk.query(
            "age_group_id == @age_group"
            ).mod_p_d_dmild_dmod_dsev_dvb_se.values[0]

    # drop columns that are no longer necessary
    mild_mod_x_walk.drop(['mod_p_d_dmild_dmod_dsev_dvb',
                          'mod_p_d_dmild_dmod_dsev_dvb_se'],
                         axis=1, inplace=True)
    for age_group in mild_mod_x_walk.age_group_id.values:
        # generate random draw values
        draw_values = list(np.random.normal(loc=means[age_group],
                           scale=st_errs[age_group], size=1000))
        # fill in mild_mod_x_walk row by row with draw values
        mild_mod_x_walk.loc[
            mild_mod_x_walk.age_group_id == age_group] = [
            age_group] + draw_values

    return mild_mod_x_walk
