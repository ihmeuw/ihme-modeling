"""
Created on Thu Jan  4 13:00:36 2018
updated Fri Jul 26 2019

@author: USERNAME

Clean the Norway ICPC data

Read the associated documentation here:
FILEPATH
"""

import getpass

import pandas as pd
from db_queries import get_location_metadata

user = getpass.getuser()

from crosscutting_functions import general_purpose
from crosscutting_functions.formatting-functions import formatting

df = pd.read_stata(FILEPATH
)

df.age = df.age.astype(str)

df["age_start"], df["age_end"] = df.age.str.split("-", 1).str

df.loc[df.age_start == "0", "age_end"] = "0"
df.loc[df.age_start == "95+", "age_end"] = "124"
# fix this so we can cast to an int without raising an error, but still catch
# any other possible errors
df.loc[df.age_start == "95+", "age_start"] = "95"

df["age_start"] = pd.to_numeric(df["age_start"])
df["age_end"] = pd.to_numeric(df["age_end"])

# add 1 to age end to fit our rubrik, age_start inclusive, age end exclusive
df["age_end"] = df["age_end"] + 1

# replace male and female with our codes
df.sex = df.sex.replace(["male", "female"], [1, 2])

# have to drop these - no way to recover
null_county_percent = df.county.isnull().sum() / df.shape[0] * 100
print("{}% of county is null".format(null_county_percent))
df = df[df.county.notnull()]

# convert county to str cause why not
df["county"] = df["county"].astype(str)

# rename sex to sex_id
df.rename(columns={"sex": "sex_id", "year": "year_start"}, inplace=True)
# drop age
df.drop("age", axis=1, inplace=True)
df["year_end"] = df["year_start"]

# location processing
# manually adjust the county names to fit the spelling in the IHME location table
location_name_dict = {
    "Ostfold": "Viken",
    "Akershus": "Viken",
    "Buskerud": "Viken",
    "Telemark": "Vestfold og Telemark",
    "Vestfold": "Vestfold og Telemark",
    "Rogaland": "Rogaland",
    "Sor-Trondelag": "Trondelag",
    "Nord-Trondelag": "Trondelag",
    "Oslo": "Oslo",
    "Hedmark": "Innlandet",
    "Oppland": "Innlandet",
    "Vest-Agder": "Agder",
    "Aust-Agder": "Agder",
    "Hordaland": "Vestland",
    "Sogn og Fjordane": "Vestland",
    "More og Romsdal": "More og Romsdal",
    "Troms": "Troms og Finnmark",
    "Finnmark": "Troms og Finnmark",
    "Nordland": "Nordland",
}
df["new_location_name"] = df.county.map(location_name_dict)
assert df.new_location_name.notnull().all(), "Location mapping didn't work"

locs = get_location_metadata(location_set_id=35, gbd_round_id=7)
loc_subnats = locs.loc[locs.parent_id == 90, ["location_ascii_name", "location_id"]]

assert (
    set(df.new_location_name.unique()) - set(loc_subnats.location_ascii_name.unique()) == set()
)
assert (
    set(loc_subnats.location_ascii_name.unique()) - set(df.new_location_name.unique()) == set()
)

# merge location_id on
pre = df.shape[0]
df = df.merge(
    loc_subnats, how="left", left_on="new_location_name", right_on="location_ascii_name"
)
assert pre == df.shape[0]

assert df.location_id.notnull().all(), "location_id has nulls"

df.drop(["county", "contacts", "location_ascii_name"], axis=1, inplace=True)

df = df.rename(columns={"patients": "val", "diagnosis": "cause_code"})

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {
    2006: 303732,
    2007: 303746,
    2008: 303747,
    2009: 303749,
    2010: 303750,
    2011: 303752,
    2012: 303753,
    2013: 303754,
    2014: 303755,
    2015: 303756,
}
df = formatting.fill_nid(df, nid_dictionary)

# collapse
assert df.notnull().sum().sum()
df = df.groupby(by=[col for col in df.columns if col != "val"], as_index=False).val.sum()

# fill columns
# 'facility_id', 'source', 'code_system_id', 'metric_id', 'representative_id', 'outcome_id', 'diagnosis_id', 'age_group_unit'
df["facility_id"] = "outpatient unknown"
df["source"] = "NOR_ICPC"
df["code_system_id"] = 5
df["metric_id"] = 1
df["representative_id"] = 3  # not representative
df["outcome_id"] = "case"
df["diagnosis_id"] = 1
df["age_group_unit"] = 1

write_path = FILEPATH

general_purpose.write_hosp_file(df, write_path)
print("DONE!")
