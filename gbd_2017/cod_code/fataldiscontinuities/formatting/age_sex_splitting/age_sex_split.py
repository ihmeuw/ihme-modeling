'''
AGE SEX SPLITTING
'''

import argparse
import numpy as np
import os
import pandas as pd
import sys
from os.path import join
from db_queries import get_location_metadata, get_population, get_cause_metadata
from db_tools.ezfuncs import query
import getpass
COD_DIR = 'FILEPATH'.format(getpass.getuser())
sys.path.append(COD_DIR)
from cod_prep.claude.age_sex_split import AgeSexSplitter
from cod_prep.downloaders import get_current_location_hierarchy
from db_queries import (get_demographics,
                        get_demographics_template as get_template,
                        get_location_metadata as lm,
                        get_population)

def run_cod_age_sex_splitting(db):
    # CHECK COMPLETENESS
    cause_set_version = 269 
    cm = get_cause_metadata(cause_set_version_id=cause_set_version)
    possible_causes = cm['cause_id'].unique().tolist()
    for cause_id in db['cause_id'].unique().tolist():
        assert cause_id in possible_causes, "Cause ID {} not in hierarchy".format(cause_id)
    loc_meta = get_location_metadata(gbd_round_id=5, location_set_id=21)
    possible_locs = loc_meta['location_id'].tolist()
    db = db.loc[db['location_id'].isin(possible_locs),:]
    db = db.loc[db['best'] > 0,:]
    db['hi_best_ratio'] = db['high'] / db['best']
    db['lo_best_ratio'] = db['low'] / db['best']
    db = db.reset_index(drop=True)
    db['unique_join'] = db.index
    db_merge_later = db.loc[:,['unique_join','hi_best_ratio','lo_best_ratio']]
    db = db.drop(labels=['high','low','hi_best_ratio','lo_best_ratio'],axis=1)
    id_cols = [i for i in db.columns if i not in ['best','age_group_id','sex_id']]
    cause_set_version_id = query("""SELECT cause_set_version_id
                                    FROM ADDRESS
                                    WHERE gbd_round_id=5 AND cause_set_id=4;""",
                                 conn_def='epi').iloc[0,0]
    pop_run_id = get_population(gbd_round_id=5, status="recent")['run_id'].iloc[0]
    splitter = AgeSexSplitter(
                     cause_set_version_id=cause_set_version,
                     pop_run_id=104,
                     distribution_set_version_id=29,
                     id_cols=['unique_join'],
                     value_column='best')
    split_db = splitter.get_computed_dataframe(
                          df=db,
                          location_meta_df=loc_meta)
    split_db = pd.merge(left=split_db,
                        right=db_merge_later,
                        on=['unique_join'],
                        how='left')
    split_db['low'] = split_db['best'] * split_db['lo_best_ratio']
    split_db['high'] = split_db['best'] * split_db['hi_best_ratio']
    split_db = split_db.drop(labels=['unique_join','lo_best_ratio',
                                     'hi_best_ratio'],axis=1)
    return split_db

def get_korean_war_locations():
    side_a = ["China","Russian Federation"]
    locs = get_current_location_hierarchy()
    side_a = pd.DataFrame(data = side_a, columns = {"location_name"})
    side_a = pd.merge(side_a, locs[['location_name','location_id']],how='left')
    side_a = list(locs[locs['parent_id'].isin(side_a['location_id'])]['location_id'])

    side_b_us = ['United States']
    side_b_us = pd.DataFrame(data = side_b_us, columns = {"location_name"})
    side_b_us = pd.merge(side_b_us, locs[['location_name','location_id']],how='left')
    side_b_us = list(locs[locs['parent_id'].isin(side_b_us['location_id'])]['location_id'])
    
    side_b_uk = ['United Kingdom']
    side_b_uk = pd.DataFrame(data = side_b_uk, columns = {"location_name"})
    side_b_uk = pd.merge(side_b_uk, locs[['location_name','location_id']],how='left')
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk['location_id'])]['location_id'])
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id'])
    side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id'])
    location_id = [16,76,18,179,125,82,80,89,101,71,155,44850,44851] + side_a + side_b_uk + side_b_us
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
    
    war_df = war_df.groupby(['year_id','location_id','cause_id','dataset'], as_index=False)['best','low','high'].sum()
    to_add = pd.DataFrame()
    for age_group in age_groups['age_group_id'].unique():
        percentage = age_groups[age_groups['age_group_id'] == age_group]['death_percentage'].iloc[0]
        war_df['age_group_id'] = age_group
        war_df['best_split'] = war_df['best'] * percentage
        war_df['high_split'] = war_df['high'] * percentage
        war_df['low_split'] = war_df['low'] * percentage
        to_add = to_add.append(war_df)
    to_add.drop(['best','low','high'],axis=1, inplace =True)
    to_add.rename(columns = {'best_split':'best',
                             'high_split':"high",
                             'low_split':"low"}, inplace=True)
    to_add['sex_id'] = 1
    final_df = final_df.append(to_add)
    assert round(final_df['best'].sum()) == round(df['best'].sum())
    return final_df

def split_by_pop(full_df, cause_id):
    total_b = round(full_df['best'].sum())
    total_h = round(full_df['high'].sum())
    total_l = round(full_df['low'].sum())
    
    final = full_df[full_df['cause_id'] != cause_id]
    df = full_df[full_df['cause_id'] == cause_id]

    if cause_id == 387:
        final.append(df.query("age_group_id == 2 | age_group_id == 3"))
        df = df.query("age_group_id != 2 & age_group_id != 3")

    locations = df.location_id.unique()
    years = df.year_id.unique()
    ages = df.age_group_id.unique()

    pop = get_population(age_group_id = list(ages),
                        location_id = list(locations),
                        year_id = list(years),
                        sex_id = [1,2],
                        run_id = 104)

    df = pd.merge(df,pop,how='left',on=['age_group_id', 'location_id','year_id','sex_id'])
    df['tpop']=  df.groupby(['location_id','year_id'])['population'].transform(sum)
    df['tbest'] = df.groupby(['location_id','year_id'])['best'].transform(sum)
    df['thigh'] = df.groupby(['location_id','year_id'])['high'].transform(sum)
    df['tlow'] = df.groupby(['location_id','year_id'])['low'].transform(sum)
    df['rate'] = df['population'] / df['tpop']
    df['best'] = df['rate'] * df['tbest']
    df['high'] = df['rate'] * df['thigh']
    df['low'] = df['rate'] * df['tlow']
    df.drop(['population','run_id','tpop','rate',"tbest"],axis=1,inplace=True)

    final = final.append(df)
    assert round(final['best'].sum()) == total_b
    assert round(final['high'].sum()) == total_h
    assert round(final['low'].sum()) == total_l
    
    return final


def catch_over_100_pop(df):
    total = round(df['best'].sum())
    pop = get_population(age_group_id = -1,
                        location_id = -1,
                        year_id = range(1950,2018),
                        sex_id = [1,2],
                        run_id = 104)

    df_over = pd.merge(df,pop, how='left', on = ['location_id','year_id','age_group_id','sex_id'])
    df_over['percent'] = df_over['best'] / df_over['population']
    df_over = df_over.query("percent >= 1")
    df_over['over'] = 1
    df = pd.merge(df,df_over[['location_id','year_id','over','sex_id','age_group_id','cause_id']], how='left', on =['cause_id','age_group_id','location_id','year_id','sex_id'])
    country_years = df.query("over == 1")
    country_years = country_years.groupby(['location_id','year_id','cause_id'], as_index=False)['over'].mean()
    df.drop(['over'],axis=1,inplace=True)
    df = pd.merge(df,country_years,how='left',on=['location_id','year_id','cause_id'])
    final = df.query("over != 1")
    df = df.query("over == 1")


    df_over_50 = df.query("age_group_id >= 15")
    df_under_5 = df.query("age_group_id <= 5")
    df_ok = df.query("age_group_id > 5 & age_group_id < 15")
    final = final.append(df_ok)

    df_over_50 = pd.merge(df_over_50,pop,how='left', on =['age_group_id','location_id','year_id','sex_id'])
    df_over_50['total_pop'] = df_over_50.groupby(['location_id','year_id','cause_id','sex_id'])['population'].transform(sum)
    df_over_50['total_best'] = df_over_50.groupby(['location_id','year_id','cause_id','sex_id'])['best'].transform(sum)
    df_over_50['rate'] = df_over_50['population'] / df_over_50['total_pop']
    df_over_50['best'] = df_over_50['total_best'] * df_over_50['rate']
    df_over_50.drop(['over','population','run_id','total_pop','total_best','rate'],axis=1, inplace=True)
    final = final.append(df_over_50)

    df_under_5 = pd.merge(df_under_5,pop,how='left', on =['age_group_id','location_id','year_id','sex_id'])
    df_under_5['total_pop'] = df_under_5.groupby(['location_id','year_id','cause_id','sex_id'])['population'].transform(sum)
    df_under_5['total_best'] = df_under_5.groupby(['location_id','year_id','cause_id','sex_id'])['best'].transform(sum)
    df_under_5['rate'] = df_under_5['population'] / df_under_5['total_pop']
    df_under_5['best'] = df_under_5['total_best'] * df_under_5['rate']
    df_under_5.drop(['over','population','run_id','total_pop','total_best','rate'],axis=1, inplace=True)
    final = final.append(df_under_5)
    final.drop('over',axis=1,inplace=True)
    assert round(final['best'].sum()) == total
    return final

if __name__=="__main__":
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--infile",type=str,
                        help="The CSV file that needs to be split")
    parser.add_argument("-o","--outfile",type=str,
                        help="The CSV file where age-sex split data will be "
                             "saved.")
    parser.add_argument("-n","--encoding",type=str,
                        help="Encoding for all CSV files")
    cmd_args = parser.parse_args()
    db = pd.read_csv(cmd_args.infile, encoding=cmd_args.encoding)
    db.loc[~db['age_group_id'].isin(range(1,22) + [30,31,32,235]),
           'age_group_id']=22
    split_db = run_cod_age_sex_splitting(db)

    
    vietnam_war_age_group_distribution = pd.read_csv("")
    korean_war_locations = get_korean_war_locations()
    korean_war_years = list(range(1950,1954))
    split_db = war_age_override(split_db,vietnam_war_age_group_distribution,korean_war_locations,korean_war_years)

    split_db = catch_over_100_pop(split_db)

    split_db.to_csv(cmd_args.outfile,index=False,encoding=cmd_args.encoding)
