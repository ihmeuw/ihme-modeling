import argparse
import getpass

import pandas as pd
import numpy as np
import sys

"""
Calculating data density:
    Add the following together to create a value data density for each location:
      - Years of complete VR
      - CBH sources (by location)
      - SBH sources (by location) * 0.25
      - Years of incomplete VR * 0.5
"""

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id to run')
parser.add_argument('--vr_death_cutoff', type=int, default=500,
                    action='store', help='The cutoff for VR deaths')
parser.add_argument('--gbd_year', type=int, default=2019,
                    action='store', help='the current gbd_year')
parser.add_argument('--emp_death_version', type=int, required=True,
                    action='store', help='death number empirical data version')
parser.add_argument('--code_dir', type=str,
                    action='store',
                    help = "Directory where child-mortality code is cloned")
parser.add_argument('--conda_env', type=str,
                    action='store',
                    help = 'Conda environment to use for py wrapper')
args = parser.parse_args()
version_id = args.version_id
emp_death_version = args.emp_death_version
vr_death_cutoff = args.vr_death_cutoff
gbd_year=args.gbd_year
code_dir = args.code_dir
conda_env = args.conda_env

# Need to manually specify to use wrappers
import os
os.environ['R_HOME'] = 'FILEPATH'
os.environ['R_USER'] = 'FILEPATH'

# Set directories
output_dir = "FILEPATH"
location_file = "FILEPATH"
input_5q0_data_file = "FILEPATH"
output_vr_dir = "FILEPATH"
input_vr_data_file = "FILEPATH"
output_file = "FILEPATH"

# Get location hierarchy
location_hierarchy = pd.read_csv(location_file)

# Get the VR file that the mort prep team uses
data_vr = pd.read_stata(input_vr_data_file)
data_vr = data_vr.loc[data_vr['outlier'] != 1]

data_vr = data_vr.loc[~((data_vr['ihme_loc_id'].str.match("IND_")) &
                      (data_vr['deaths_source'].isin(['SRS', 'SRS_REPORT', '165390#IND_DLHS4_2012_2014'])))]

data_vr = data_vr.loc[~((data_vr['ihme_loc_id'] == "GBR_4749") &
                      (data_vr['year'] < 1980))]

data_vr = data_vr.loc[~((data_vr['ihme_loc_id'].str.match("IRN")) &
                      (data_vr['deaths_source'] == "IRN_NOCR_allcause_VR"))]

data_vr = data_vr.loc[~((data_vr['ihme_loc_id'] == "NRU") &
                      (data_vr['deaths_source'] == "165208#NRU Census 2011"))]

# take only both sexes (not males and females) and extract only the necessary age variables for 5q0
data_vr = data_vr.loc[(data_vr['sex'] == "both") & (data_vr['year'] >= 1950)]

# Round year down
data_vr['year'] = data_vr['year'].astype('int64')

# Get age_group columns
under_5_cols = ['DATUM0to4', 'DATUM1to4']
for s in range(5):
    col = "DATUM{}to{}".format(s, s)
    under_5_cols.append(col)
    if col not in data_vr.columns:
        data_vr[col] = np.nan

# Keep just the columns we need
data_vr = data_vr[['ihme_loc_id', 'year', 'deaths_source'] + under_5_cols]

# Calculate under 5 deaths
data_vr.loc[data_vr['DATUM1to4'].isnull(), 'DATUM1to4'] = data_vr.loc[data_vr['DATUM1to4'].isnull(),
                                                                      ['DATUM{}to{}'.format(x, x) for x in range(1, 5)]
                                                                     ].sum(axis=1)
data_vr.loc[data_vr['DATUM0to4'].isnull(), 'DATUM0to4'] = data_vr.loc[data_vr['DATUM0to4'].isnull(),
                                                                      ['DATUM0to0', 'DATUM1to4']
                                                                     ].sum(axis=1)

# Set number of deaths under 5 to the data density
data_vr['vr_deaths'] = data_vr['DATUM0to4']

# Get 5q0 model input data
data_5q0 = pd.read_csv(input_5q0_data_file)

# Round year down
data_5q0['year'] = data_5q0['year'].astype('int64')

# Get a list of location-years with complete VR
data_complete_vr = data_5q0.loc[(data_5q0['category'].isin(['vr_no_overlap', 'vr_unbiased'])) &
                                (data_5q0['data'] == 1)].copy(deep=True)
data_complete_vr = data_complete_vr[['ihme_loc_id', 'year']].drop_duplicates()

data_complete_vr = pd.merge(data_complete_vr, data_vr, on=['ihme_loc_id', 'year'], how='left')
data_complete_vr = data_complete_vr.rename(columns={'vr_deaths': 'complete_vr_deaths'})


# Prioritize certain sources over others
data_complete_vr['source_sort'] = np.nan
data_complete_vr.loc[data_complete_vr['deaths_source'] == "WHO_causesofdeath", 'source_sort'] = 1
data_complete_vr.loc[data_complete_vr['deaths_source'] == "WHO", 'source_sort'] = 2
data_complete_vr.loc[data_complete_vr['deaths_source'] == "VR", 'source_sort'] = 3

#drop hh from complete vr
data_complete_vr = data_complete_vr[~((data_complete_vr.deaths_source == "IPUMS_HHDEATHS") & (data_complete_vr.ihme_loc_id=="MWI") & (data_complete_vr.year.isin([1998,2008])))]

# Get duplicated location-years
duplicated_years = data_complete_vr.loc[data_complete_vr[['ihme_loc_id', 'year']].duplicated(),
                                        ['ihme_loc_id', 'year']].drop_duplicates().copy(deep=True)
duplicated_years['duplicated'] = 1

# Get location-years that have been sourced
sourced_years = data_complete_vr.groupby(['ihme_loc_id', 'year'])['source_sort'].min().copy(deep=True)
sourced_years = sourced_years.reset_index()
sourced_years = sourced_years.loc[sourced_years['source_sort'].isnull()]
sourced_years['not_sourced'] = 1
sourced_years = sourced_years[['ihme_loc_id', 'year', 'not_sourced']]

duplicated_years = pd.merge(duplicated_years, sourced_years, on=['ihme_loc_id', 'year'], how='left')
assert len(duplicated_years.loc[duplicated_years['not_sourced'].notnull()]) == 0

# Take the source with the minimum source_sort
data_complete_vr.loc[data_complete_vr['source_sort'].isnull(), 'source_sort'] = 999999
data_complete_vr = data_complete_vr.sort_values(['ihme_loc_id', 'year', 'source_sort']).reset_index(drop=True)
data_complete_vr = data_complete_vr.loc[~(data_complete_vr[['ihme_loc_id', 'year']].duplicated())]

# Keep just the columns that we need
data_complete_vr = data_complete_vr[['ihme_loc_id', 'year', 'complete_vr_deaths']]

# Cap deaths at 500
data_complete_vr.loc[data_complete_vr['complete_vr_deaths'] >= vr_death_cutoff, 'complete_vr_deaths'] = vr_death_cutoff

# Divide by 1000
data_complete_vr['complete_vr_deaths'] = data_complete_vr['complete_vr_deaths'] / vr_death_cutoff

# Add up deaths by location
data_complete_vr = data_complete_vr.groupby(['ihme_loc_id'])['complete_vr_deaths'].sum().reset_index()

# Keep all data except for "complete" (vr_no_overlap, vr_unbiased) data
data_5q0 = data_5q0.loc[~(data_5q0['category'].isin(['vr_no_overlap', 'vr_unbiased'])) & (data_5q0['data'] == 1)]

# Get a count of the unique CBH source for each location
data_cbh_sources = data_5q0.loc[data_5q0['method'].isin(['CBH'])].copy(deep=True)
data_cbh_sources = data_cbh_sources[['ihme_loc_id', 'source']].drop_duplicates()
data_cbh_sources['cbh_sources'] = 1
data_cbh_sources = data_cbh_sources.groupby(['ihme_loc_id'])['cbh_sources'].sum().reset_index()

# Get a count of the unique SBH source for each location
data_sbh_sources = data_5q0.loc[data_5q0['method'].isin(['SBH'])].copy(deep=True)
data_sbh_sources = data_sbh_sources[['ihme_loc_id', 'source']].drop_duplicates()
data_sbh_sources['sbh_sources'] = 1
data_sbh_sources = data_sbh_sources.groupby(['ihme_loc_id'])['sbh_sources'].sum().reset_index()

# Get a count of the unique HH source for each location
data_hh_sources = data_5q0.loc[data_5q0['method'].isin(['HH'])].copy(deep=True)
data_hh_sources = data_hh_sources[['ihme_loc_id', 'source']].drop_duplicates()
data_hh_sources['hh_sources'] = 1
data_hh_sources = data_hh_sources.groupby(['ihme_loc_id'])['hh_sources'].sum().reset_index()

# Get a count of the unique incomplete VR sources for each location
data_incomplete_vr = data_5q0.loc[data_5q0['method'].isin(['VR/SRS/DSP'])].copy(deep=True)
data_incomplete_vr = data_incomplete_vr[['ihme_loc_id', 'year']].drop_duplicates()

data_incomplete_vr = pd.merge(data_incomplete_vr, data_vr, on=['ihme_loc_id', 'year'], how='left')
data_incomplete_vr = data_incomplete_vr.rename(columns={'vr_deaths': 'incomplete_vr_deaths'})
data_incomplete_vr.loc[data_incomplete_vr[['ihme_loc_id', 'year']].duplicated()]

# Prioritize certain sources over others
data_incomplete_vr['source_sort'] = np.nan
data_incomplete_vr['deaths_source'] = data_incomplete_vr['deaths_source'].fillna("")
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "WHO_causesofdeath", 'source_sort'] = 1
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "WHO", 'source_sort'] = 2
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "VR", 'source_sort'] = 3
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "CHN_DSP", 'source_sort'] = 4
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "CHINA_DSP_LOZANO", 'source_sort'] = 5
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "3rdNatlDSP", 'source_sort'] = 6
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "DYB", 'source_sort'] = 7
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "2010 SRS report", 'source_sort'] = 8
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "2011 SRS report", 'source_sort'] = 8
data_incomplete_vr.loc[data_incomplete_vr['deaths_source'] == "IND_CRS_allcause_VR", 'source_sort'] = 9


# Get duplicated location-years
duplicated_years = data_incomplete_vr.loc[data_incomplete_vr[['ihme_loc_id', 'year']].duplicated(),
                                          ['ihme_loc_id', 'year']].drop_duplicates().copy(deep=True)
duplicated_years['duplicated'] = 1

# Get location-years that have been sourced
sourced_years = data_incomplete_vr.groupby(['ihme_loc_id', 'year'])['source_sort'].min().copy(deep=True)
sourced_years = sourced_years.reset_index()
sourced_years = sourced_years.loc[sourced_years['source_sort'].isnull()]
sourced_years['not_sourced'] = 1
sourced_years = sourced_years[['ihme_loc_id', 'year', 'not_sourced']]

duplicated_years = pd.merge(duplicated_years, sourced_years, on=['ihme_loc_id', 'year'], how='left')

duplicated_years.loc[duplicated_years['not_sourced'].notnull()]
assert len(duplicated_years.loc[duplicated_years['not_sourced'].notnull()]) == 0

# Take the source with the minimum source_sort
data_incomplete_vr.loc[data_incomplete_vr['source_sort'].isnull(), 'source_sort'] = 999999
data_incomplete_vr = data_incomplete_vr.sort_values(['ihme_loc_id', 'year', 'source_sort']).reset_index(drop=True)
data_incomplete_vr = data_incomplete_vr.loc[~(data_incomplete_vr[['ihme_loc_id', 'year']].duplicated())]

# Keep just the columns that we need
data_incomplete_vr = data_incomplete_vr[['ihme_loc_id', 'year', 'incomplete_vr_deaths']]

# Cap deaths at 500
data_incomplete_vr.loc[data_incomplete_vr['incomplete_vr_deaths'] >= vr_death_cutoff, 'incomplete_vr_deaths'] = vr_death_cutoff

# Divide by 1000
data_incomplete_vr['incomplete_vr_deaths'] = data_incomplete_vr['incomplete_vr_deaths'] / vr_death_cutoff

# Add up deaths by location
data_incomplete_vr = data_incomplete_vr.groupby(['ihme_loc_id'])['incomplete_vr_deaths'].sum().reset_index()

# Merge everything together
data = pd.merge(data_complete_vr, data_cbh_sources, on=['ihme_loc_id'], how='outer')
data = pd.merge(data, data_sbh_sources, on=['ihme_loc_id'], how='outer')
data = pd.merge(data, data_hh_sources, on=['ihme_loc_id'], how='outer')
data = pd.merge(data, data_incomplete_vr, on=['ihme_loc_id'], how='outer')

# Clean up numbers
for c in data.columns:
    if c != 'ihme_loc_id':
        data[c] = data[c].fillna(0)

# Make score
data['data_density'] = (
    data['complete_vr_deaths'] + (2 * data['cbh_sources']) +
    (0.25 * data['sbh_sources']) + (0.5 * data['incomplete_vr_deaths']))

# Merge on locations
location_hierarchy = location_hierarchy.loc[(location_hierarchy['level'] >= 3) |
                                            (location_hierarchy['level'] == -1)]
data = pd.merge(location_hierarchy[['location_id', 'ihme_loc_id']], data, on='ihme_loc_id', how='left')
for c in ['data_density', 'complete_vr_deaths', 'cbh_sources', 'sbh_sources', 'hh_sources', 'incomplete_vr_deaths']:
    data.loc[data[c].isnull(), c] = 0

# Make hyperparameter categories
data.loc[data['data_density'] < 10, 'data_density_category'] = "0_to_10"
data.loc[(data['data_density'] >= 10) &
         (data['data_density'] < 20), 'data_density_category'] = "10_to_20"
data.loc[(data['data_density'] >= 20) &
         (data['data_density'] < 30), 'data_density_category'] = "20_to_30"
data.loc[(data['data_density'] >= 30) &
         (data['data_density'] < 50), 'data_density_category'] = "30_to_50"
data.loc[(data['data_density'] >= 50), 'data_density_category'] = "50_plus"

# Make manual adjustments
group_10_to_20 = ['NAM']
data.loc[(data['ihme_loc_id'].isin(group_10_to_20)), 'data_density_category'] = "10_to_20"

# Merge on hyperparameters
hyperparameters = pd.read_csv("FILEPATH")
data = pd.merge(data, hyperparameters, on='data_density_category', how='left')

# Add in legacy variables
data['best'] = 1
data['amp2x'] = 1

# Make manual adjustments to some places
data.loc[(data.ihme_loc_id == "AND"), 'lambda']  = 0.5
data.loc[(data.ihme_loc_id == "ARE"), 'lambda']  = 0.2
data.loc[(data.ihme_loc_id.str.contains("BRA")), 'lambda'] = 0.3
data.loc[(data.ihme_loc_id == "BRB"), 'lambda']  = 0.2
data.loc[(data.ihme_loc_id == "DMA"), 'lambda'] = 0.4
data.loc[(data.ihme_loc_id.str.contains("IND")), 'lambda'] = 0.3
data.loc[(data.ihme_loc_id == "KAZ"), 'lambda']  = 0.2
data.loc[(data.ihme_loc_id == "LBY"), 'lambda'] = 0.3
data.loc[(data.ihme_loc_id == "LSO"), 'lambda']  = 0.3
data.loc[(data.ihme_loc_id == "LUX"), 'lambda']  = 0.2
data.loc[(data.ihme_loc_id == "NER"), 'lambda'] = 0.2
data.loc[(data['ihme_loc_id'].isin(["RWA","UGA"])), 'lambda'] = 0.2
data.loc[(data.ihme_loc_id == "SSD"), 'lambda'] = 0.4
data.loc[(data.ihme_loc_id == "SLB"), 'lambda'] = 0.9
data.loc[(data.ihme_loc_id == "VCT"), 'lambda']  = 0.5
data.loc[(data.ihme_loc_id == "VEN"), 'lambda'] = 0.5
data.loc[(data.ihme_loc_id.str.contains("BRA")), 'scale'] = 5
data.loc[(data.ihme_loc_id == "COG"), 'scale'] = 5
data.loc[(data.ihme_loc_id == "LBR"), 'scale'] = 5
data.loc[(data.ihme_loc_id == "LSO"), 'scale']  = 10
data.loc[(data.ihme_loc_id == "RWA"), 'scale'] = 5
data.loc[(data['ihme_loc_id'] == "BFA"), 'zeta'] = 0.9
data.loc[(data.ihme_loc_id == "GBR_4636"), 'zeta'] = 0.9
data.loc[(data['ihme_loc_id'] == "GNQ"), 'zeta'] = 0.9
data.loc[(data.ihme_loc_id == "LBY"), 'zeta']  = 0.9
data.loc[(data.ihme_loc_id == "LSO"), 'zeta']  = 0.7
data.loc[(data['ihme_loc_id'] == "NER"), 'zeta'] = 0.9
data.loc[(data.ihme_loc_id == "SLB"), 'zeta'] = 0.9

ind_scale_change = data_5q0.loc[(data_5q0.ihme_loc_id.str.contains("IND")) &
                                (data_5q0['source'].str.contains("srs")) &
                                (data_5q0['year'] > 2000)].copy(deep=True)

data.loc[(data.ihme_loc_id.isin(ind_scale_change.ihme_loc_id)), 'scale'] = 5

# Save
data.to_csv(output_file, index=False)
