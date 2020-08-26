"""
Created on Thu Jan  4 13:00:36 2018
updated Fri Jul 26 2019

@author: USERNAME

Clean the Norway ICPC data

Read the associated documentation here:
FILEPATH
"""

import pandas as pd
import getpass
import sys

from db_queries import get_location_metadata

user = getpass.getuser()


hosp_path = "FILEPATH".format(user)
sys.path.append(hosp_path)

import hosp_prep

df = pd.read_stata(
    "FILEPATH")

df.age = df.age.astype(str)

df['age_start'], df['age_end'] = df.age.str.split("-", 1).str

df.loc[df.age_start == "0", 'age_end'] = "0"
df.loc[df.age_start == "95+", 'age_end'] = "124"


df.loc[df.age_start == "95+", 'age_start'] = "95"

df['age_start'] = pd.to_numeric(df['age_start'])
df['age_end'] = pd.to_numeric(df['age_end'])


df['age_end'] = df['age_end'] + 1


df.sex = df.sex.replace(['male', 'female'], [1, 2])


null_county_percent = df.county.isnull().sum() / df.shape[0] * 100
print("{}% of county is null".format(null_county_percent))
df = df[df.county.notnull()]


df['county'] = df['county'].astype(str)


df.rename(columns={'sex': 'sex_id', 'year': 'year_start'}, inplace=True)

df.drop('age', axis=1, inplace=True)
df['year_end'] = df['year_start']




df.loc[df.county.isin(["Nord-Trondelag", "Sor-Trondelag"]),
       'county'] = "Trondelag"

locs = get_location_metadata(location_set_id=35)
loc_subnats = locs.loc[locs.parent_id == 90,
                       ['location_ascii_name', 'location_id']]

assert set(df.county.unique()) - \
    set(loc_subnats.location_ascii_name.unique()) == set()
assert set(loc_subnats.location_ascii_name.unique()) - \
    set(df.county.unique()) == set()


pre = df.shape[0]
df = df.merge(loc_subnats, how='left', left_on='county',
              right_on='location_ascii_name')
assert pre == df.shape[0]

assert df.location_id.notnull().all(), "location_id has nulls"

df.drop(['county', 'contacts', 'location_ascii_name'], axis=1, inplace=True)

df = df.rename(columns={"patients": "val", "diagnosis": "cause_code"})


nid_dictionary = {2006: 303732, 2007: 303746, 2008: 303747, 2009: 303749,
                  2010: 303750, 2011: 303752, 2012: 303753, 2013: 303754,
                  2014: 303755, 2015: 303756}
df = hosp_prep.fill_nid(df, nid_dictionary)


assert df.notnull().sum().sum()
df = df.groupby(by=[col for col in df.columns if col !=
                    'val'], as_index=False).val.sum()



df['facility_id'] = 'outpatient unknown'
df['source'] = "NOR_ICPC"
df['code_system_id'] = 5
df['metric_id'] = 1
df['representative_id'] = 3  
df['outcome_id'] = "case"
df['diagnosis_id'] = 1
df['age_group_unit'] = 1

write_path = "FILEPATH"

hosp_prep.write_hosp_file(df, write_path)
print("DONE!")
