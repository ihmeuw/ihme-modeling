# Author: Daniel Dicker
# coding: utf-8

"""
Script that adds LKA weights to the weights used in splitting.
This script will overwrite the weights file.
"""

# In[15]:


import itertools

import numpy as np
import pandas as pd

from db_tools.ezfuncs import query
from db_queries import get_covariate_estimates, get_age_metadata, get_population

def get_distribution_set_version(distribution_set_version_id):
    q = """
	QUERY
        """.format(distribution_set_version_id)
    
    return query(q, conn_def='cod')


age_sex_distribution_dir = ""


# In[16]:


# Get the pre-made Sri Lanka distribution
lka_distribution = get_distribution_set_version(53)
lka_distribution = lka_distribution.loc[(lka_distribution['cause_id'] == 294)]

# Reformat
keep_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']
lka_distribution.location_id = 17
lka_distribution_data = []
for year_id in range(1950, 2018):
    temp = lka_distribution.copy(deep=True)
    temp.year_id = year_id
    lka_distribution_data.append(temp)
    
lka_distribution = pd.concat(lka_distribution_data)
lka_distribution = lka_distribution[keep_cols]

# Get the original SDI distribution
sdi_distribution = pd.read_csv("".format(age_sex_distribution_dir))

# Reformat 
keep_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']
sdi_distribution = sdi_distribution.rename(columns={'mean': 'weight'})
sdi_distribution = sdi_distribution[keep_cols]


# Take SDI distribution and append on LKA distribution
sdi_distribution = sdi_distribution.loc[(sdi_distribution['location_id'] != 17)]
distribution = pd.concat([sdi_distribution, lka_distribution]).reset_index(drop=True)

# Save
print("".format(age_sex_distribution_dir))
distribution.to_csv("".format(age_sex_distribution_dir), index=False)

