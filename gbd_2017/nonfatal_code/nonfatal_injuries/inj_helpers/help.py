"""This is a file of reference functions that will be useful in all of the scripts. Will import these in the child scripts depending on
what is needed."""

import pandas as pd
import itertools
import os
from gbd_inj.inj_helpers import paths


GBD_ROUND = 5
LAST_YEAR = 2017


def convert_to_age_group_id(df, collapsed_0=False):
    df = df.copy()
    if collapsed_0:
        age_dict = {
            '0.0': 28, '1.0': 5, '5.0': 6, '10.0': 7, '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11,
            '35.0': 12, '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17, '65.0': 18,
            '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31, '90.0': 32, '95.0': 235
        }
    else:
        age_dict = {
            '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7, '15.0': 8, '20.0': 9, '25.0': 10,
            '30.0': 11, '35.0': 12, '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17, '65.0': 18,
            '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31, '90.0': 32, '95.0': 235
        }
    df['age'] = df['age'].astype(float).round(2).astype(str).replace(age_dict).astype(int)
    df.rename(columns={'age': 'age_group_id'}, inplace=True)
    return df


def convert_from_age_group_id(df):
    """Convert age group ID to age data that is used frequently. DisMod needs age start and age end and weird midpoint
    values instead of age group ID."""
    age_dict = {
        '2': 0.00, '3': 0.01, '4': 0.10, '5': 1.00,
        '6': 5.00, '7': 10.00, '8': 15.00, '9': 20.00,
        '10': 25.00, '11': 30.00, '12': 35.00, '13': 40.00,
        '14': 45.00, '15': 50.00, '16': 55.00, '17': 60.00,
        '18': 65.00, '19': 70.00, '20': 75.00, '30': 80.00,
        '31': 85.00, '32': 90.00, '235': 95.00
    }
    df["age_group_id"] = df["age_group_id"].astype(str).replace(age_dict)
    df.rename(columns = {"age_group_id": "age"}, inplace = True)
    return df


def convert_to_age_midpoint(df):
    df = df.copy()
    age_dict = {2: 3.5/365, 3: 17.5/365, 4: 196.5/365, 5: 3, 6: 7.5, 7: 12.5, 8: 17.5, 9: 22.5, 10: 27.5, 11: 32.5,
                12: 37.5, 13: 42.5, 14: 47.5, 15: 52.5, 16: 57.5, 17: 62.5, 18: 67.5, 19: 72.5, 20: 77.5, 30: 82.5,
                31: 87.5, 32: 92.5, 235: 97.5, 28: 0.5}
    df['age_group_id'] = df['age_group_id'].replace(age_dict)
    df.rename(columns={'age_group_id': 'age'}, inplace=True)
    return df


def append_age_mdpt(arr, coord_name='age_mid', single_years=False):
    age_dict = {2: 3.5/365, 3: 17.5/365, 4: 196.5/365, 5: 3, 6: 7.5, 7: 12.5, 8: 17.5, 9: 22.5, 10: 27.5, 11: 32.5,
                12: 37.5, 13: 42.5, 14: 47.5, 15: 52.5, 16: 57.5, 17: 62.5, 18: 67.5, 19: 72.5, 20: 77.5, 30: 82.5,
                31: 87.5, 32: 92.5, 235: 97.5,
                28: .5  # under-1 aggregate just in case
                }
    if single_years:
        # adjust standard age groups down slightly so that all values are unique, a requirement for xarray's interpolate
        age_dict.update({k: v - .00001 for k, v in age_dict.items() if v % 2.5 == 0})
        age_dict.update({a+48: a+.5 for a in range(1, 100)})  # single year ages
    age_map = pd.Series(age_dict).rename_axis('age_group_id').to_xarray()
    new_arr = arr.copy()
    new_arr[coord_name] = age_map
    return new_arr


def append_age_group(arr, coord_name='age_group'):
    """This method adds a coordinate with the corresponding age_group_id to an array with single years"""
    age_to_age_group = {1: 5, 5: 6, 10: 7, 15: 8, 20: 9, 25: 10, 30: 11, 35: 12, 40: 13, 45: 14, 50: 15, 55: 16, 60: 17,
                        65: 18, 70: 19, 75: 20, 80: 30, 85: 31, 90: 32, 95: 235}
    age_dict = {a+48: age_to_age_group[(a//5)*5] if a >= 5 else age_to_age_group[1] for a in range(1, 100)}
    age_map = pd.Series(age_dict).rename_axis('age_group_id').to_xarray()
    new_arr = arr.copy()
    new_arr[coord_name] = age_map
    return new_arr
    

def expand_under_1(df):
    """Replaces all age_group_id 28 (under 1) rows with one entry each for ids 2, 3, and 4
    
    IMPORTANT: only use for rate data, not count data (like if you have population in the df)"""
    age2 = df.loc[df['age_group_id'] == 28]
    age3 = age2.copy()
    age4 = age2.copy()
    age2['age_group_id'] = 2
    age3['age_group_id'] = 3
    age4['age_group_id'] = 4
    df = df[df['age_group_id']!=28]
    df = df.append(age2)
    df = df.append(age3)
    df = df.append(age4)
    return df

def expandgrid(*itrs):
    """ Define function from @username to expand grid... gets the
    Cartesian products."""
    product = list(itertools.product(*itrs))
    return({'Var{}'.format(i + 1):[x[i] for x in product] for i in range(len(itrs))})


def get_unique_level_values(df, level_name):
    """Get unique values of a df column with indices.

    :param df: data frame
    :param level_name: column for which you want levels:
    :returns: list of unique level values
    """
    return list(df.index.levels[df.index._get_level_number(level_name)])


def get_cause(ecode):
    """ Function to get the cause id for the ecode."""
    codes = pd.read_csv(os.path.join('FILEPATH.csv'))
    id = codes.ix[codes['ecode'] == ecode, 'cause_id'].iloc[0]
    return id


def get_me(ecode):
    """ Function to get the ME id for the ecode."""
    codes = pd.read_csv(os.path.join('FILEPATH.csv'))
    id = codes.ix[codes['ecode'] == ecode, 'modelable_entity_id'].iloc[0]
    return id


def get_final_me(ecode, ncode):
    mes = pd.read_csv(os.path.join('FILEPATH.csv'))
    me_id = mes.loc[(mes['cause_id']==get_cause(ecode)) & (mes['rei']==ncode)]['modelable_entity_id'].iloc[0]
    return me_id


def ihme_loc_id_dict(metaloc, location_ids):
    """ Returns a dictionary mapping location_id to ihme_loc_id"""
    return metaloc.loc[metaloc['location_id'].isin(location_ids),  # subset to the passed loc ids
                       ['location_id', 'ihme_loc_id']].set_index(  # grab just the location_id and ihme_loc_id
        'location_id')['ihme_loc_id'].astype(str).to_dict()        # series indexed by location_id, convert to dict


def drawcols(number=1000):
    drawcols = ["draw_{}".format(i) for i in range(number)]
    return drawcols


def start_timer():
    import time
    start = time.time()
    return start

def end_timer(start):
    import time
    end = time.time()
    total = end - start
    print("Total time was {} seconds".format(total))
