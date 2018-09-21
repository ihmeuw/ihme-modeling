"""
This script calculates the proportions based off of mes read in frin the info
table. Namely, we
1. get_draws for all the mes we are forming proportions for (along with parent)
2. calculate the prevalence for the missing me
3. Save results as hdf
"""

import numpy as np
import argparse
from transmogrifier.gopher import draws
from db_queries import get_location_metadata


def get_most_detailed_locations(location_set={LOCATION SET ID}, gbd_round={GBD ROUND ID}):
    location_df = get_location_metadata(location_set_id=location_set,
                                        gbd_round_id=gbd_round)
    location_df = location_df[location_df['most_detailed'] == {MOST DETAILED}]
    location_list = location_df['location_id'].tolist()
    return location_list


def index_draws_by_demographics(df):
    # set index according to GBD demographics and keep draws
    draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]
    index = ['location_id', 'year_id', 'sex_id', 'age_group_id',
             'measure_id']
    good_stuff = draw_cols + index
    df = df[good_stuff]
    return_df = df.set_index(index)
    return return_df


def save_to_hdf(df, filepath):
    # save a file to hdf without its index set in order for save_results
    save_df = df.copy()
    index = list(save_df.index.names)
    save_df = save_df.reset_index()
    save_df.to_hdf(filepath, key="draws", format="table",
                   data_columns=index)


def grab_prevalence_draws(me_id, year, locations):
    # grabs prevalence draws for the given year, me_id, and locations
    gbd_round = {GBD ROUND ID}
    measure = {MEASURE ID}
    sexes = [{SEX ID}]
    ages = [{AGE GROUP IDS}]
    df = draws(source='epi', gbd_ids={"modelable_entity_ids": [me_id]},
               location_ids=locations, year_ids=year,
               age_group_ids=ages, sex_ids=sexes,
               status='best', measure_ids=[measure],
               gbd_round_id=gbd_round)
    return df


def calculate_other_group(parent_df, child_draws_list):
    # calculate the remaining group by subtracting draws from the parent_df
    other_df = parent_df.copy()
    for df in child_draws_list:
        other_df = other_df - df
    return other_df


def scale_draws_to_proportions(child_draws_list):
    """Scales child draws to proportions.

    Given a list of child dataframes each sharing an index and proper
    dimensions, divides each child by the parent (sum of the children) to get
    proportions.

    Args:
        child_draw_list (list): list of indexed dataframes all of the same
        size. Note, all children dataframes should not contain negative values.

    return:
        A new list of child_draws in the same order, now scaled to proportions

    """
    # define parent_df as the sum of the children.
    parent_df = 0
    for df in child_draws_list:
        parent_df = parent_df + df
    # scale children
    scaled_child_list = []
    for df in child_draws_list:
        scaled_df = df / parent_df
        scaled_child_list.append(scaled_df)

    return scaled_child_list


def remove_invalid_values(df):
    """Replaces infs and nans with 0.

    This function is important for both replacing negatives when subtracting
    off from the parent and for replacing infs with 0 in the case when the sum
    of the children is 0 (divide by 0 error).

    Args:
        df (a dataframe): any sort of dataframe

    return:
        The original dataframe with np.nan, np.inf, -np.inf, and negative
        numbers all replaced with 0.

    """
    new_df = df.copy()
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)

    def replace_neg_with_zero(x):
        if x < 0:
            return 0
        else:
            return x

    new_df = new_df.applymap(replace_neg_with_zero)
    return new_df


if __name__ == '__main__':
    # parse year and out directory
    parser = argparse.ArgumentParser()
    parser.add_argument("year", help="me_id subracted from", type=int)
    parser.add_argument("out_dir", help="output directory to save proportions",
                        type=str)
    args = parser.parse_args()
    year = args.year
    out_dir = args.out_dir
    # define id lists
    parent_id = {MODELABLE ENTITY ID}
    prev_me_ids = [{MODELABLE ENTITY IDS}]
    proportion_ids = [{MODELABLE ENTITY IDS}]
    locations = get_most_detailed_locations()

    # grab parent draws, then index after replacing prevalence with proportion
    parent_draws_df = grab_prevalence_draws(parent_id, year, locations)
    parent_draws_df['measure_id'] = {MEASURE ID}
    parent_draws_df = index_draws_by_demographics(parent_draws_df)

    # similarly for child me_ids
    child_draws_list = []
    for me_id in prev_me_ids:
        df = grab_prevalence_draws(me_id, year, locations)
        df['measure_id'] = {MEASURE ID}
        df = index_draws_by_demographics(df)
        child_draws_list.append(df)

    # calculate other group and move negative numbers to 0
    other_df = calculate_other_group(parent_draws_df, child_draws_list)
    other_df = remove_invalid_values(other_df)
    # add other group to children list and scale to proportions
    child_draws_list.append(other_df)
    child_proportion_list = scale_draws_to_proportions(child_draws_list)
    # zip proportion dfs and save_me_ids and save
    save_child_zipped = zip(child_proportion_list, proportion_ids)
    for df, me_id in save_child_zipped:
        # replace infs with 0
        save_df = remove_invalid_values(df)
        filepath = '{0}/{1}/{2}.h5'.format(out_dir, me_id, year)
        save_to_hdf(save_df, filepath)
