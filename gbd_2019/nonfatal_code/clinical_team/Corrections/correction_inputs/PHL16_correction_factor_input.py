"""
Format PHL health claims data for years 2013/2014 for use in the correction factors
beginning on decomp 2, gbd2019

"""
import warnings
import pandas as pd
import numpy as np
from db_queries import get_location_metadata
from datetime import datetime


def get_phl_data(files):
    """
    Read in a list of csv files and output a pd.DataFrame
    """
    df = pd.concat([pd.read_csv("{}".format(f)) for f in files],
                   sort=False, ignore_index=True)
    
    df['year_start'], df['year_end'] = [2016, 2016]
    df['code_system_id'] = 2

    # unnamed is an index, urbanicity isn't used by our team
    # C* are claims reimbursement info
    # rvs are procedure codes, not used in CFs
    df.drop(['Unnamed: 0', 'urbanicity', 'C1', 'C2',
             'rvscode1', 'rvscode2', 'rvscode3', 'rvscode4',
             'rvscode5'], inplace=True, axis=1)
    return df

def rename_phl(df):
    """
    rename PHL columns to match our system
    """
    hosp_wide_feat = {
        'sex': 'sex_id',
        'age_value': 'age',
        'age_unit': 'age_group_unit',
        'code_system_id': 'code_system_id',
        'outcome': 'outcome_id',
        # dates
        'date_dis': 'dis_date',
        'date_adm': 'adm_date',
        # diagnosis varibles
        'icdcode1': 'dx_1',
        'icdcode2': 'dx_2',
        'icdcode3': 'dx_3',
        'icdcode4': 'dx_4',
        'icdcode5': 'dx_5'}
    
    # Rename features using dictionary created above
    df.rename(columns=hosp_wide_feat, inplace=True)
    return df

def align_values(df):
    """
    make stuff like sex_id and age group unit and outcome id into the proper
    datatypes and values
    """
    # all sexes are either m or f
    df['sex'] = df['sex_id']
    assert set(['M', 'F']) == set(df.sex.unique()), "we did not expect other sex_id values"
    # all ages are years per codebook
    assert set(['Years']) == set(df.age_group_unit.unique()), "Some age units aren't years?"
    
    df['age_group_unit'] = 1
    df.loc[df['sex'] == 'M', 'sex_id'] = 1
    df.loc[df['sex'] == 'F', 'sex_id'] = 2
    # this doesn't matter for CFs
    df.loc[df['outcome_id'] == 'Expired', 'outcome_id'] = 'death'
    df.loc[df['outcome_id'] != 'death', 'outcome_id'] = 'discharge'

    # set dates to datetime object
    df['adm_date'] = pd.to_datetime(df['adm_date'])
    df['dis_date'] = pd.to_datetime(df['dis_date'])
    df.drop(['sex'], inplace=True, axis=1)
    return df 

data_dir=FILEPATH
files = [data_dir + PHIL_FILEPATH\
            format(i) for i in range(1, 5, 1)]
df = get_phl_data(files=files)
back = df.copy()

df = back.copy()

# drop los == 0 to match our inp data
df = df[df['los'] > 0]

df = rename_phl(df)
df = align_values(df)

df.shape[0] == 8647540

df.isnull().sum()
# 9,900 provinces are null
# 1,017,815 primary dx are null

# negative ages again, adding to email
neg_age = df[df.age < 0].copy()
warnings.warn("There are {} negative ages. This is {} of all rows".\
              format(neg_age.shape[0], neg_age.shape[0]/float(df.shape[0])))

locs = get_location_metadata(location_set_id=35)
loc_merge = locs[['location_ascii_name', 'location_id']].copy()
loc_merge.rename(columns={'location_ascii_name': 'location_name'}, inplace=True)
loc_merge['location_name'] = loc_merge['location_name'].str.lower()

df['location_name'] = df['province'].str.lower()
# manually adjust some locs in the data to match shared locs
df.loc[df['location_name'] == 'metro manila', 'location_name'] = 'national capital region'  # official name
df.loc[df['location_name'] == 'western samar', 'location_name'] = 'samar (western samar)'

pre = df.shape[0]

df = df.merge(loc_merge, how='left', on='location_name')
assert pre == df.shape[0]

test = df[df['location_id'].isnull()].copy()

data_locs = df.location_name.unique()
shared_locs = locs.query("parent_id == 16").location_ascii_name.str.lower().unique()

# present in data but not shared
set(data_locs) - set(shared_locs)
#[out] {nan, 'cotobato', 'north cotabato'}

# present in shared but not data
set(shared_locs) - set(data_locs)
#[out] {'cotabato (north cotabato)', 'davao occidental', 'dinagat islands'


assert df['province'].isnull().sum() == df['location_id'].isnull().sum()

## to look into

# long dx_* codes, this means they're still combining some diagnoses
df['dx_1_len'] = np.nan
df.loc[df['dx_1'].notnull(), 'dx_1_len'] = df.loc[df['dx_1'].notnull(), 'dx_1'].apply(len)
