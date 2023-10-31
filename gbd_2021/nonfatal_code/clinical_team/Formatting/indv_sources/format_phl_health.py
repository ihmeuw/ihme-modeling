# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 13:24:34 2017

@author:

prepping and inspecting Phillipines claims data

"""
import pandas as pd
import platform

# %%

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# %%
# All at once
df_list = []
years = [2010, 2011, 2012]
#  filepath will change once data is catelogued
for year in years:
    df = pd.read_csv(root + r"FILEPATH", encoding="latin-1", sep="|",)
    df["year_start"] = year
    df["year_end"] = year
    df_list.append(df)
df = pd.concat(df_list)

# %%
# drop unneeded columns

df.drop(
    [
        "REG",
        "PROVINCE_NAME",
        "ICD_DESCRIPTION",
        "RVS",
        "RVS_DESCRIPTION",
        "AMT_ACTUAL",
        "TOTAL_AMOUNT",
        "YR",
    ],
    axis=1,
    inplace=True,
)
# %%
# count how many times patient ids show up (in any year)
counts = df.groupby("PATIENT_ID").size()
