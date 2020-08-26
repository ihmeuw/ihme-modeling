"""
This script calculates the proportions based off of mes read in from the info
table. Namely, we
1. get_draws for all the mes we are forming proportions for (along with parent)
2. calculate the prevalence for the missing me
3. Save results as hdf
"""
from argparse import ArgumentParser, Namespace
import numpy as np
import os
import pandas as pd
from typing import List

from db_queries import get_demographics
from get_draws.api import get_draws


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--year_id', type=int)
    parser.add_argument('--anemia_cause', type=str)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--out_dir', type=str)
    return parser.parse_args()


def index_draws_by_demographics(df) -> pd.DataFrame:
    # set index according to GBD demographics and keep draws
    draw_cols = [f"draw_{i}" for i in range(0, 1000)]
    index = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'measure_id']
    df = df[draw_cols + index]
    return_df = df.set_index(index)
    return return_df


def save_to_hdf(df, filepath) -> None:
    # save a file to hdf without its index set in order for save_results
    save_df = df.copy()
    index = list(save_df.index.names)
    save_df = save_df.reset_index()
    save_df.to_hdf(filepath, key="draws", format="table", data_columns=index)


def save_to_csv(df, filepath) -> None:
    # save a file to hdf without its index set in order for save_results
    save_df = df.copy()
    index = list(save_df.index.names)
    save_df = save_df.reset_index()
    save_df.to_csv("FILEPATH", index=False)


def grab_prevalence_draws(me_id, year_id, gbd_round_id, decomp_step) -> pd.DataFrame:
    # grabs prevalence draws for the given year, me_id, and locations
    demo = get_demographics("epi", gbd_round_id=gbd_round_id)
    print(demo['age_group_id'], me_id)
    df = get_draws('modelable_entity_id', me_id, source='epi', measure_id=5,
                   location_id=demo['location_id'], year_id=year_id,
                   age_group_id=demo['age_group_id'], sex_id=demo['sex_id'],
                   gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    return df


def calculate_other_group(parent_df, child_draws_list) -> pd.DataFrame:
    # calculate the remaining group by subtracting draws from the parent_df
    other_df = parent_df.copy()
    for df in child_draws_list:
        other_df = other_df - df
    return other_df


def scale_draws_to_proportions(child_draws_list) -> List[pd.DataFrame]:
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


def remove_invalid_values(df) -> pd.DataFrame:
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

    def replace_neg_with_zero(x) -> int:
        if x < 0:
            return 0
        else:
            return x

    new_df = new_df.applymap(replace_neg_with_zero)
    return new_df


if __name__ == '__main__':
    # parse year and out directory
    args = parse_args()
    year_id = args.year_id
    out_dir = args.out_dir
    anemia_cause = args.anemia_cause
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step

    work_dir = os.path.dirname(os.path.realpath(__file__))
    info_df = pd.read_csv('FILEPATH')

    parent_id = info_df['parent_id'].tolist()[0]
    prev_me_ids = info_df['prev_me'].tolist()[:3]
    proportion_ids = info_df['proportion_me'].tolist()

    # grab parent draws, then index after replacing prevalence with proportion
    parent_draws_df = grab_prevalence_draws(
        me_id=parent_id, year_id=year_id, gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)
    parent_draws_df['measure_id'] = 18
    parent_draws_df = index_draws_by_demographics(parent_draws_df)

    # similarly for child me_ids
    child_draws_list = []
    for me_id in prev_me_ids:
        df = grab_prevalence_draws(
            me_id=me_id, year_id=year_id, gbd_round_id=gbd_round_id,
            decomp_step=decomp_step)
        df['measure_id'] = 18
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
        me_out_dir = "FILEPATH"
        if not os.path.isdir(me_out_dir):
            os.system(f'mkdir -m 775 -p {me_out_dir}')
        save_to_csv(save_df, "FILEPATH")
