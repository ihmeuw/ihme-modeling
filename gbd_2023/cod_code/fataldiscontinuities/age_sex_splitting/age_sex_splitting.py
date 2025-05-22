import numpy as np
import pandas as pd
import sys
import os
from db_tools import ezfuncs
import argparse
from db_queries import get_location_metadata
from db_queries import get_population
from db_queries import get_cause_metadata
from db_tools.ezfuncs import query
from db_queries import get_envelope_with_shock

import getpass
COD_DIR = FILEPATH
sys.path.append(COD_DIR)

from FILEPATH import AgeSexSplitter
from FILEPATH import get_current_location_hierarchy

def get_ages(row):
    if row['source_id'] == 0:
        return row['age_group_id_y']
    else:
        return row['age_group_id_x']
    
def get_sexes(row):
    if row['source_id'] == 0:
        return row['sex_id_y']
    else:
        return row['sex_id_x']


def get_best_prioritization_data(run_id, conn_def):
    best_prioritization_data_query = """
    QUERY
    """.format(run_id)

    df = ezfuncs.query(best_prioritization_data_query,
                       conn_def=conn_def)

    og_deaths = df.copy()['best'].sum()

    df['cause_id'] = df['cause_id'].replace(716, 729)
    df['cause_id'] = df['cause_id'].replace(985, 729)
    df['cause_id'] = df['cause_id'].replace(986, 729)
    df['cause_id'] = df['cause_id'].replace(987, 729)
    df['cause_id'] = df['cause_id'].replace(988, 729)
    df['cause_id'] = df['cause_id'].replace(989, 729)
    df['cause_id'] = df['cause_id'].replace(990, 729)
    df['cause_id'] = df['cause_id'].replace(855, 945)
    df['cause_id'] = df['cause_id'].replace(851, 945)
    df['cause_id'] = df['cause_id'].replace(689, 695)
    df['cause_id'] = df['cause_id'].replace(724, 727)
    df['cause_id'] = df['cause_id'].replace(303, 302)
    df['cause_id'] = df['cause_id'].replace(317, 302)
    
    df = df.groupby(['location_id','year_id','cause_id','sex_id','age_group_id','source_id'], as_index=False)['best','low','high'].sum()

    og_vr = pd.read_csv(FILEPATH)

    og_vr['total'] = og_vr.groupby(['location_id','cause_id','year_id'], as_index=False)['deaths'].transform(np.sum)

    og_vr['rate'] = og_vr['deaths'] / og_vr['total']
    

    assert np.isclose(og_vr.query('location_id == 8 & year_id == 1984 & cause_id == 707')['rate'].sum(), 1.0)

    og_vr['source_id'] = 0

    df = pd.merge(left=df, right=og_vr[['location_id','year_id','cause_id', 'rate', 'source_id','age_group_id','sex_id']], on=['location_id','cause_id','year_id', 'source_id'], how='outer')

    df['rate'] = df['rate'].fillna(1)

    df['best'] = df['rate'] * df['best']

    df['high'] = df['rate'] * df['high']

    df['low'] = df['rate'] * df['low']

    df['age_group_id'] = df.apply(get_ages, axis=1)

    df['sex_id'] = df.apply(get_sexes, axis=1)

    df = df[['location_id', 'year_id', 'cause_id', 'sex_id', 'age_group_id', 
         'source_id', 'best', 'low', 'high', 'rate']]

    df.dropna(subset=['best'], inplace=True)

    df['age_group_id'] = df['age_group_id'].fillna(22)

    df['sex_id'] = df['sex_id'].fillna(3)


    df = df.groupby(['location_id','year_id','cause_id','sex_id','age_group_id','source_id'], as_index=False)['best','low','high'].sum()

    df['cause_id'] = df['cause_id'].replace(335, 332)

    assert np.isclose(og_deaths, df.best.sum(), atol=10)

    return df


def run_cod_age_sex_splitting(df, conn_def, cause_set_version_id, pop_run_id):

    cause_metadata = get_cause_metadata(cause_set_version_id=cause_set_version_id, cause_set_id=4, release_id=16)
    possible_causes = cause_metadata['cause_id'].unique().tolist()
    for cause_id in df['cause_id'].unique().tolist():
        assert cause_id in possible_causes, "Cause ID {} not in hierarchy".format(cause_id)
    loc_meta = get_location_metadata(release_id=16, location_set_id=21)
    possible_locs = loc_meta['location_id'].tolist()
    df = df.loc[df['location_id'].isin(possible_locs), :]
    df = df.loc[df['best'] > 0, :]
    df['hi_best_ratio'] = df['high'] / df['best']
    df['lo_best_ratio'] = df['low'] / df['best']
    df = df.reset_index(drop=True)
    df['unique_join'] = df.index
    df_merge_later = df.loc[:, ['unique_join', 'hi_best_ratio', 'lo_best_ratio']]
    df = df.drop(labels=['high', 'low', 'hi_best_ratio', 'lo_best_ratio'], axis=1)

    splitter = AgeSexSplitter(cause_set_version_id=cause_set_version_id,
                              pop_run_id=pop_run_id,
                              distribution_set_version_id=77) 

    split_df = splitter.get_computed_dataframe(df=df, location_meta_df=loc_meta)
    original_shape = split_df.copy().shape[0]
    split_df = pd.merge(left=split_df,
                        right=df_merge_later,
                        on=['unique_join'],
                        how='left')
    assert(split_df.shape[0])
    split_df['low'] = split_df['best'] * split_df['lo_best_ratio']
    split_df['high'] = split_df['best'] * split_df['hi_best_ratio']
    split_df = split_df.drop(labels=['unique_join', 'lo_best_ratio',
                                     'hi_best_ratio'], axis=1)
    return split_df


def get_korean_war_locations():
    side_a = ["China", "Russian Federation"]
    locs = get_current_location_hierarchy()
    side_a = pd.DataFrame(data=side_a, columns={"location_name"})
    side_a = pd.merge(side_a, locs[['location_name', 'location_id']], how='left')
    side_a = list(locs[locs['parent_id'].isin(side_a['location_id'])]['location_id'])

    side_b_us = ['United States']
    side_b_us = pd.DataFrame(data=side_b_us, columns={"location_name"})
    side_b_us = pd.merge(side_b_us, locs[['location_name', 'location_id']], how='left')
    side_b_us = list(locs[locs['parent_id'].isin(side_b_us['location_id'])]['location_id'])

    side_b_uk = ['United Kingdom']
    side_b_uk = pd.DataFrame(data=side_b_uk, columns={"location_name"})
    side_b_uk = pd.merge(side_b_uk, locs[['location_name', 'location_id']], how='left')
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk['location_id'])]['location_id'])
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id'])
    # goes 3 levels deep for children
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id'])
    location_id = [16, 76, 18, 179, 125, 82, 80, 89, 101, 71,
                   155, 44850, 44851] + side_a + side_b_uk + side_b_us
    return location_id


def war_age_override(df, age_groups, location_id, year_id):
    cause_id = 855
    war_df = df[(df['cause_id'] == cause_id) &
                (df['location_id'].isin(location_id)) &
                (df['year_id'].isin(year_id))]
    final_df = df[~((df['cause_id'] == cause_id) &
                    (df['location_id'].isin(location_id)) &
                    (df['year_id'].isin(year_id)))]

    assert round(war_df['best'].sum() + final_df['best'].sum()) == round(df['best'].sum())

    war_df = war_df.groupby(['year_id', 'location_id', 'cause_id'],
                            as_index=False)['best', 'low', 'high'].sum()
    to_add = pd.DataFrame()
    for age_group in age_groups['age_group_id'].unique():
        percentage = age_groups[age_groups['age_group_id'] == age_group]['death_percentage'].iloc[0]
        war_df['age_group_id'] = age_group
        war_df['best_split'] = war_df['best'] * percentage
        war_df['high_split'] = war_df['high'] * percentage
        war_df['low_split'] = war_df['low'] * percentage
        to_add = to_add.append(war_df)
    to_add.drop(['best', 'low', 'high'], axis=1, inplace=True)
    to_add.rename(columns={'best_split': 'best',
                           'high_split': "high",
                           'low_split': "low"}, inplace=True)
    to_add['sex_id'] = 1
    final_df = final_df.append(to_add)
    assert round(final_df['best'].sum()) == round(df['best'].sum())
    return final_df


def catch_over_100_pop(df):
    total = round(df['best'].sum())
    pop = get_population(age_group_id=[2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 34, 235, 238, 388, 389],
                         location_id=-1,
                         year_id=list(range(1950, 2023)),
                         sex_id=[1, 2],
                         release_id=16)
    df_over = pd.merge(df, pop, how='left', on=['location_id', 'year_id',
                                                'age_group_id', 'sex_id'])
    df_over['percent'] = df_over['best'] / df_over['population']
    df_over = df_over.query("percent >= 1")
    df_over['over'] = 1
    df = pd.merge(df, df_over[['location_id', 'year_id', 'over', 'sex_id',
                               'age_group_id', 'cause_id']], how='left',
                  on=['cause_id', 'age_group_id', 'location_id', 'year_id', 'sex_id'])
    country_years = df.query("over == 1")
    country_years = country_years.groupby(['location_id', 'year_id', 'cause_id'],
                                          as_index=False)['over'].mean()
    df.drop(['over'], axis=1, inplace=True)
    df = pd.merge(df, country_years, how='left', on=['location_id', 'year_id', 'cause_id'])
    final = df.query("over != 1")
    df = df.query("over == 1")

    df_over_50 = df[df.age_group_id.isin([15, 16, 17, 18, 19, 20, 30, 31, 32, 235])]
    df_under_5 = df[df.age_group_id.isin([2,3,34, 238, 388, 389])]
    df_ok = df[df.age_group_id.isin([6, 7, 8, 9, 10, 11, 12, 13, 14])]
    final = final.append(df_ok)

    df_over_50 = pd.merge(df_over_50, pop, how='left',
                          on=['age_group_id', 'location_id', 'year_id', 'sex_id'])
    df_over_50['total_pop'] = df_over_50.groupby(
        ['location_id', 'year_id', 'cause_id', 'sex_id'])['population'].transform(sum)
    df_over_50['total_best'] = df_over_50.groupby(
        ['location_id', 'year_id', 'cause_id', 'sex_id'])['best'].transform(sum)
    df_over_50['rate'] = df_over_50['population'] / df_over_50['total_pop']
    df_over_50['best'] = df_over_50['total_best'] * df_over_50['rate']
    df_over_50.drop(
        ['over', 'population', 'run_id', 'total_pop', 'total_best', 'rate'], axis=1, inplace=True)
    final = final.append(df_over_50)

    df_under_5 = pd.merge(df_under_5, pop, how='left',
                          on=['age_group_id', 'location_id', 'year_id', 'sex_id'])
    df_under_5['total_pop'] = df_under_5.groupby(
        ['location_id', 'year_id', 'cause_id', 'sex_id'])['population'].transform(sum)
    df_under_5['total_best'] = df_under_5.groupby(
        ['location_id', 'year_id', 'cause_id', 'sex_id'])['best'].transform(sum)
    df_under_5['rate'] = df_under_5['population'] / df_under_5['total_pop']
    df_under_5['best'] = df_under_5['total_best'] * df_under_5['rate']
    df_under_5.drop(
        ['over', 'population', 'run_id', 'total_pop', 'total_best', 'rate'], axis=1, inplace=True)
    final = final.append(df_under_5)
    final.drop('over', axis=1, inplace=True)
    assert round(final['best'].sum()) == total
    return final

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--run_id", type=int,
                        help="The id of the prioritization run to be used")
    parser.add_argument("-s", "--current", type=str,
                        help="the main folder where the df will be saved")
    parser.add_argument("-a", "--archive", type=str,
                        help="the archive folder where the df will be saved")
    parser.add_argument("-d", "--conn_def", type=str,
                        help="the database to connect to")
    parser.add_argument("-c", "--cause_set_version_id", type=int,
                        help="the cause set version id to be used")
    parser.add_argument("-p", "--pop_run_id", type=int,
                        help="the population version id to be used")

    cmd_args = parser.parse_args()
    df = get_best_prioritization_data(cmd_args.run_id, cmd_args.conn_def)

    og_deaths = df.copy().best.sum()
    split_df = run_cod_age_sex_splitting(
        df, cmd_args.conn_def, cmd_args.cause_set_version_id, cmd_args.pop_run_id)

    vietnam_war_age_group_distribution = pd.read_csv((
        FILEPATH))
    korean_war_locations = get_korean_war_locations()
    korean_war_years = list(range(1950, 1954))
    split_df = war_age_override(split_df, vietnam_war_age_group_distribution,
                                korean_war_locations, korean_war_years)

    split_df = catch_over_100_pop(split_df)

    assert np.isclose(og_deaths, split_df.best.sum(), atol=100)

    split_df_current_filepath = FILEPATH
    split_df_archive_filepath = FILEPATH

    split_df_current_filepath = FILEPATH

    print(split_df_current_filepath)
    print(split_df_archive_filepath)
    split_df.to_csv(split_df_current_filepath, index=False, encoding='utf8')
    split_df.to_csv(split_df_archive_filepath, index=False, encoding='utf8')
