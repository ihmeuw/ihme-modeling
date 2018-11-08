# -*- coding: utf-8 -*-
"""

"""

import pandas as pd
import getpass
import platform
import sys

user = getpass.getuser()

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
    hosp_path = r"FILEPATH/Functions".format(user)
else:
    root = "J:"
    hosp_path = r"FILEPATH/Functions".format(user)

sys.path.append(hosp_path)
import hosp_prep


df = pd.read_stata(r"FILEPATH/2006_"\
                          r"2015_primary_care_contacts_patients_icpc2.dta")

back = df.copy()

df.age = df.age.astype(str)

df['age_start'], df['age_end'] = df.age.str.split("-", 1).str

df.loc[df.age_start == "0", 'age_end'] = "0"
df.loc[df.age_start == "95+",'age_end'] = "124"
# fix this so we can cast to an int without raising an error, but still catch
# any other possible errors
df.loc[df.age_start == "95+", 'age_start'] = "95"

df['age_start'] = pd.to_numeric(df['age_start'])
df['age_end'] = pd.to_numeric(df['age_end'])

# add 1 to age end to fit our rubrik, age_start inclusive, age end exclusive
df['age_end'] = df['age_end'] + 1

# replace male and female with our codes
df.sex = df.sex.replace(['male', 'female'], [1, 2])

# convert county to str cause why not
df['county'] = df['county'].astype(str)

# rename sex to sex_id
df.rename(columns={'sex': 'sex_id', 'year': 'year_start'}, inplace=True)
# drop age
df.drop('age', axis=1, inplace=True)
df['year_end'] = df['year_start']

# set location id
df['location_id'] = 90

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2006: 303732, 2007: 303746, 2008: 303747, 2009: 303749,
                  2010: 303750, 2011: 303752, 2012: 303753, 2013: 303754,
                  2014: 303755, 2015: 303756}
df = hosp_prep.fill_nid(df, nid_dictionary)

write_path = "FILEPATH/formatted_NOR_ICPC.H5"
hosp_prep.write_hosp_file(df, write_path)
