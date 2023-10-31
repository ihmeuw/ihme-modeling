import glob
import os
import math
import datetime
import argparse

import pandas as pd
import numpy as np
from sklearn import linear_model
from core_maths.summarize import get_summary

from gbdu5.io import get_sex_data, read_empirical_life_tables, get_finalizer_draws
from gbdu5.rescale import rescale_qx_conditional, convert_qx_to_days
from gbdu5.age_groups import MortAgeGroup, bin_five_year_ages, btime_to_wk
from gbdu5.transformations import reshape_wide, ready_to_merge, calculate_annualized_pct_change, back_calculate

"""
Read in empirical life tables
    These empirical life tables are by single-year ages
Calculate qx by single year ages (1q0, 1q1, 1q2, 1q3, 1q4, 1q5)
Calculate qx by 1q0, 1q1, 3q2
Predict log of single year qx values (1q2, 1q3, 1q4) using log of 3q2
    Run a separate regression for each sex and single year qx value (3 qx values x 2 sexes = 6 models)
Bring in results for age-sex splitting (specifically 3q2)
Apply regression results using the (log) 3q2 from age-sex splitting results
Rescale single year age qx values to match 3q2
Bring in qx values for ENN, PNN, and LNN

Convert qx values for ENN, LNN, PNA, PNB, 1, 2, 3, 4 to day qx values
    day_qx = 1 - (1 - qx)^(1/days_in_age_group)
"""

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--location_id', type=int, required=True,
                    action='store', help='location_id')
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='ihme_loc_id')
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='version_id')
parser.add_argument('--version_age_sex_id', type=int, required=True,
                    action='store', help='version_age_sex_id')
parser.add_argument('--births_draws_version', type=str, required=True,
                    action='store', help='versions for births draws')
parser.add_argument('--end_year', type=int, required=True,
                    action='store', help='last year to produce estimates for')
parser.add_argument('--with_shocks', type=str, required=True, default='False',
                    action='store', help="Whether to grab with shocks life tables or no shocks")
parser.add_argument('--conda_env', type = str)

args = parser.parse_args()
location_id = args.location_id
ihme_loc_id = args.ihme_loc_id
version_id = args.version_id
version_age_sex_id = args.version_age_sex_id
births_draws_version = args.births_draws_version
end_year = args.end_year
conda_env = args.conda_env
with_shocks = args.with_shocks

print(args)

if with_shocks == 'False':
    with_shocks = False
else:
    with_shocks = True

print("Shocks after conversion: ", with_shocks)

start_year = 1950

# Need to manually specify to use wrappers
os.environ['R_HOME'] = 'FILEPATH'.format(conda_env)
os.environ['R_USER'] = 'FILEPATH'.format(conda_env)

from mort_wrappers.call_mort_function import call_mort_function

""" Set input and output directories """
# Age-sex splitting
age_sex_sim_dir = "FILEPATH"
age_sex_sim_file = "FILEPATH"

# Births
births_filepath = "FILEPATH"

# Empirical life tables
empirical_input_dir = "FILEPATH"
male_empirical_input_dir = "FILEPATH"
female_empirical_input_dir = "FILEPATH"

# Output draws
output_dir = "FILEPATH"
output_file = "FILEPATH"
os.makedirs(output_dir, exist_ok=True)

""""""

# Get the sex DataFrame
df_sex = get_sex_data()

# Create list of age group objects
a_enn = MortAgeGroup('enn', 7, 0) # enn = day 0-6
a_lnn = MortAgeGroup('lnn', 21, a_enn.end_day) # lnn = day 7-27
a_pna = MortAgeGroup('pna', 155, a_lnn.end_day) # pna = 1-5 months (day 28-182)
a_pnb = MortAgeGroup('pnb', 182, a_pna.end_day) # 6-11 months (day 183-364)
a_1 = MortAgeGroup('1', 365, a_pnb.end_day)
a_2 = MortAgeGroup('2', 365, a_1.end_day)
a_3 = MortAgeGroup('3', 365, a_2.end_day)
a_4 = MortAgeGroup('4', 365, a_3.end_day)

ages = [a_enn, a_lnn, a_pna, a_pnb, a_1, a_2, a_3, a_4]

# Get the empirical life table files
male_files = glob.glob("FILEPATH")
female_files = glob.glob("FILEPATH")
data = []
data.append(read_empirical_life_tables(male_files, 1))
data.append(read_empirical_life_tables(female_files, 2))
data = pd.concat(data).reset_index(drop=True)

# Only keep years starting in 1950
data = data.loc[data['year']>=start_year]

"Calculate qx: qx = 1 - lx[_n+1]/lx"
# Calculate qx for the single-year age groups
data = data.sort_values(['iso3', 'sex_id', 'year', 'age']).reset_index(drop=True)
data['qx'] = data.groupby(['iso3', 'year', 'sex_id'])['lx'].apply(lambda x: 1 - x.shift(-1)/x)
data['age_five'] = data['age'].apply(lambda x: bin_five_year_ages(x))

# Make a copy of the single year ages
data_age_single = data.copy(deep=True)

# Keep the GBD 5 year age groups
data = data.loc[data['age']==data['age_five']]

# Calculate
data = data.sort_values(['iso3', 'sex_id', 'year', 'age']).reset_index(drop=True)
data['qxfive'] = data.groupby(['iso3', 'year', 'sex_id'])['lx'].apply(lambda x: 1 - x.shift(-1)/x)
data = data[['iso3', 'sex_id', 'year', 'age_five', 'qxfive']]

# Merge the single and five year DataFrames
data = pd.merge(data_age_single, data, on=['iso3', 'sex_id', 'year', 'age_five'])

# Keep just the under 5 values
data = data.loc[data['age']<5]

# Convert qx and qx_agg to log
for c in ['qx', 'qxfive']:
    data = data.loc[data[c]>0]
    data['ln{}'.format(c)] = data[c].apply(math.log)

# Run a simple regression to predict lnqx using lnqx_agg
pars = []
for sex_id in [1, 2]:
    for age in [2, 3, 4]:
        temp = data.loc[(data['sex_id']==sex_id)&(data['age']==age)].copy(deep=True)
        lm = linear_model.LinearRegression()
        model = lm.fit(temp[['lnqxfive']], temp['lnqx'])
        print(sex_id, age, lm.coef_, lm.intercept_)
        pars.append([sex_id, age, lm.coef_[0], lm.intercept_])

pars = pd.DataFrame(pars, columns=['sex_id', 'age', 'par_lnagg', 'par_cons'])


if with_shocks:
    parents = call_mort_function("get_proc_lineage", {"model_name" : "u5 envelope", "model_type" : "estimate", "run_id" : version_id})
    parents = dict(zip(parents.parent_process_name, parents.parent_run_id))
    with_shock_death_number = parents['with shock death number estimate']
    locs = pd.read_csv("FILEPATH")
    data, data_single = get_finalizer_draws(with_shock_death_number, location_id, locs)
else:
    # Get age-sex estimates
    data = pd.read_csv(age_sex_sim_file)
    data = data.rename(columns={'q_cha': 'qx_1', 'q_chb': 'q_3x2', 'simulation': 'sim'})
    data = pd.merge(data, df_sex, on=['sex'])
    data_single = data.copy(deep=True)
    data = []
    keep_columns = ['ihme_loc_id', 'sim', 'sex_id', 'year', 'q_3x2']
    for age in range(2, 5):
        temp = data_single.copy(deep=True)
        temp = temp[keep_columns]
        temp['age'] = age
        data.append(temp)
    data = pd.concat(data)

data = pd.merge(data, pars, on=['sex_id', 'age'])

data['lnq_3x2'] = data['q_3x2'].apply(math.log)

data['qx'] = np.exp(data['par_lnagg'] * data['lnq_3x2'] + data['par_cons'])

data = data[['ihme_loc_id', 'sim', 'sex_id', 'year', 'age', 'qx', 'q_3x2']]

# Reshape age wide on qx values
index_columns = ['ihme_loc_id', 'sim', 'sex_id', 'year', 'q_3x2']
data_columns = ['qx']
reshape_column = 'age'
data = reshape_wide(data, index_columns, data_columns, reshape_column)

# Rescale qx values to match 4q1
keep_cols = ['ihme_loc_id', 'sim', 'sex_id', 'year', 'qx_2', 'qx_3', 'qx_4']
data_rescaled = rescale_qx_conditional(data.copy(deep=True))
data_rescaled = data_rescaled[keep_cols]


data = pd.merge(data_rescaled, data_single, on=['ihme_loc_id', 'sim', 'sex_id', 'year'])

data = data.rename(columns={'q_enn': 'qx_enn',
                            'q_lnn': 'qx_lnn',
                            'q_pna': 'qx_pna',
                            'q_pnb': 'qx_pnb'})

data = data[['ihme_loc_id', 'sim', 'year', 'sex_id', 'qx_enn', 'qx_lnn', 'qx_pna', 'qx_pnb', 'qx_1', 'qx_2', 'qx_3', 'qx_4']]


for a in ages:
    data[f'day_qx_{a.name}'] = convert_qx_to_days(data[f'qx_{a.name}'], a.days)


data['year_id'] = data['year'].apply(math.floor).astype('int64')


data['location_id'] = location_id
for c in ['location_id', 'year_id', 'sex_id', 'sim']:
    data[c] = data[c].astype('int64')

data_qx = data.copy(deep=True)

# Create a DataFrame for the amount of time elapsed for each week in a year
data = []
for w in range(52):
    # Convert weeks to year space using the midpoint of the week (x + 0.5)
    week_time = (w + 0.5) * (1 / 52.0)
    for year_id in range(start_year, end_year + 1):
        data.append([year_id, year_id + week_time])
data = pd.DataFrame(data, columns=['year_id', 'btime'])

# Create timing DataFrame
for a in ages:
    # Calculate the start and end day of that age group for someone born at btime (birth time)
    data[f'start_time_{a.name}'] = data['btime'] + a.start_year
    data[f'end_time_{a.name}'] = data['btime'] + a.end_year

    # Calculate the start and end year of that age group for someone born at btime (birth time)
    data[f'start_year_{a.name}'] = data[f'start_time_{a.name}'].apply(math.floor).astype('int64')
    data[f'end_year_{a.name}'] = data[f'end_time_{a.name}'].apply(math.floor).astype('int64')

    # Calculate number of days in the start and end year
    sc = f'days_in_start_year_{a.name}'
    data[sc] = ((
        1 - (data[f'start_time_{a.name}'] - data[f'start_year_{a.name}'])) * 365)
    data[sc] = data[sc].apply(round)
    data.loc[data[sc] > a.days, sc] = float(a.days)

    # Calculate number of days in year after start year
    ec = f'days_in_end_year_{a.name}'
    data[ec] = float(a.days) - data[sc]

data_timing = data.copy(deep=True)
data_timing['week'] = data_timing['btime'].map(lambda x: btime_to_wk(x))


# Read in birth data
df_births = pd.read_csv(births_filepath)
df_births = df_births[df_births.age_group_id == 169]
df_births = df_births.loc[df_births['sex_id'].isin([1, 2])]
df_births = df_births.rename(columns={'draw': 'sim', 'value': 'births'})
df_births = df_births[['location_id', 'year_id', 'sex_id', 'sim', 'births']]
for c in ['location_id', 'year_id', 'sex_id', 'sim']:
    df_births[c] = df_births[c].astype('int64')

# Convert births to weeks
df_births['start_alive'] = df_births['births'] / 52
data_births = df_births[['location_id', 'year_id', 'sex_id', 'sim', 'start_alive']]

# Set up initial values
data_start_alive = {}
data_start_alive['0'] = pd.merge(data_timing[['year_id', 'btime', 'week']],
                                 data_births, on=['year_id'])

# Define output container and columns
output = []
output_cols = ['location_id', 'year_id', 'btime', 'week', 'sex_id', 'age', 'sim',
               'start_alive', 'start_deaths', 'mid_alive', 'end_deaths',
               'end_alive','start_person_years', 'end_person_years',
               'start_year', 'end_year']

# Loop through
previous_age = '0'
for a_name in [a.name for a in ages]:
    print(a_name)

    data_qx_cols = ['location_id', 'year_id', 'sex_id', 'sim', f'day_qx_{a_name}']

    # Define age-sepcific qx column
    qx_col = f'day_qx_{a_name}'

    # Define age-specific start and end columns
    sy_col = f'start_year_{a_name}'
    ey_col = f'end_year_{a_name}'
    dsy_col = f'days_in_start_year_{a_name}'
    dey_col = f'days_in_end_year_{a_name}'

    # Define timing columns
    timing_cols = ['year_id', 'week', sy_col, ey_col, dsy_col, dey_col]

    # Merge initial alive with timing data
    data = pd.merge(data_timing[timing_cols], data_start_alive[previous_age],
                on=['year_id', 'week'])

    # Merge on qx values for the start year
    temp_qx = data_qx[data_qx_cols].copy(deep=True)
    temp_qx = temp_qx.rename(columns={qx_col: 'qx_start', 'year_id': sy_col})
    data = pd.merge(data, temp_qx, on=['location_id', 'sex_id', 'sim', sy_col],
                    how='left')

    # Merge on qx values for the end year
    temp_qx = data_qx[data_qx_cols].copy(deep=True)
    temp_qx = temp_qx.rename(columns={qx_col: 'qx_end', 'year_id': ey_col})
    data = pd.merge(data, temp_qx, on=['location_id', 'sex_id', 'sim', ey_col],
                    how='left')

    # Calculate deaths in start year
    data['start_deaths'] = (1 - (1 - data['qx_start'])**data[dsy_col]) * data['start_alive']

    # Calculate number alive after start year
    data['mid_alive'] = ((1 - data['qx_start'])**data[dsy_col]) * data['start_alive']

    # Calculate deaths in end year
    data['end_deaths'] = (1 - (1 - data['qx_end'])**data[dey_col]) * data['mid_alive']

    # Calculate number alive after start year
    data['end_alive'] = ((1 - data['qx_end'])**data[dey_col]) * data['mid_alive']

    # Calculate person-years in start year
    data['start_person_years'] = (
        data['start_deaths'] * (data[dsy_col] / 2) / 365 + data['mid_alive'] * data[dsy_col] / 365)

    # Calculate person-years in end year
    data['end_person_years'] = (
        (1 - (1 - data['qx_end'])**data[dey_col]) * data['mid_alive'] * (data[dsy_col] / 2) / 365 +
        ((1 - data['qx_end'])**data[dey_col]) * data['mid_alive'] * (data[dey_col]/365)
    )

    # Add age to DataFrame
    data['age'] = a_name

    # Add data to output
    data['start_year'] = data[sy_col]
    data['end_year'] = data[ey_col]
    data = data[output_cols]
    output.append(data)

    # Create DataFrame for next age group
    data = data[['location_id', 'year_id', 'sex_id', 'sim', 'btime', 'week', 'end_alive']]
    data = data.rename(columns={'end_alive': 'start_alive'})
    data_start_alive[a_name] = data.copy(deep=True)

    previous_age = a_name

data_aged = pd.concat(output).reset_index(drop=True)

# Calculate all of the deaths and person years for the start years
start_year_index_cols = ['location_id', 'start_year', 'sex_id', 'age', 'sim']
start_year_rename_cols = {
    'start_year': 'year_id',
    'start_deaths': 'deaths',
    'start_person_years': 'person_years'
}
data_start_year = data_aged.groupby(start_year_index_cols)['start_deaths', 'start_person_years'].sum().copy(deep=True)
data_start_year = data_start_year.reset_index()
data_start_year = data_start_year.rename(columns=start_year_rename_cols)

# Get all of the deaths and person years for the end years
end_year_index_cols = ['location_id', 'end_year', 'sex_id', 'age', 'sim']
end_year_rename_cols = {
    'end_year': 'year_id',
    'end_deaths': 'deaths',
    'end_person_years': 'person_years'
}
data_end_year = data_aged.groupby(end_year_index_cols)['end_deaths', 'end_person_years'].sum().copy(deep=True)
data_end_year = data_end_year.reset_index()
data_end_year = data_end_year.rename(columns=end_year_rename_cols)

# Combine start year and end year deaths & person years
index_cols = ['location_id', 'year_id', 'sex_id', 'age', 'sim']
final_data = pd.concat([data_start_year, data_end_year])
final_data = final_data.groupby(index_cols)['deaths', 'person_years'].sum()
final_data = final_data.reset_index()

# Get annualized rate of change from 1955 to 1960
roc_data = calculate_annualized_pct_change(
    final_data, 1955, 1960, ['location_id', 'sex_id', 'age', 'sim'],
    ['deaths', 'person_years'], 'year_id')

# Use annualized rate of change to back calculate 1950 to 1954
backcalc_data = pd.merge(final_data, roc_data, on=['location_id', 'sex_id', 'age', 'sim'], how='left')
backcalc_data = back_calculate(backcalc_data, 1955, list(range(1950, 1955)), ['deaths', 'person_years'])

# Combine pre and post 1955
final_data = final_data.loc[(final_data['year_id'] >= 1955)]
backcalc_data = backcalc_data[['location_id', 'year_id', 'sex_id', 'age', 'sim', 'deaths', 'person_years']]
final_data = pd.concat([final_data, backcalc_data])

# Final formatting
new_names = {
    'year_id': 'year',
    'person_years': 'pys'
}
final_data['ihme_loc_id'] = ihme_loc_id
final_data.loc[final_data['sex_id'] == 1, 'sex'] = "male"
final_data.loc[final_data['sex_id'] == 2, 'sex'] = "female"
final_data = final_data.rename(columns=new_names)
final_data = final_data.loc[final_data['year'] <= end_year]

output_data = ready_to_merge(final_data, "enn")
for a in final_data.loc[final_data['age'] != "enn", "age"].drop_duplicates():
    output_data = pd.merge(output_data, ready_to_merge(final_data, a),
                           on=['ihme_loc_id', 'year', 'sim', 'sex'],
                           how='outer')

# add pnn from pna and pnb
output_data['deathspnn'] = output_data['deathspna'] + output_data['deathspnb']
output_data['pyspnn'] = output_data['pyspna'] + output_data['pyspnb']

# Assert that output data has all years
assert(max(output_data['year']) == end_year)

# Save
output_data.to_csv(output_file, index=False)

# Validate the write
check = pd.read_csv(output_file)
row_diff = output_data.shape[0] - check.shape[0]

assert row_diff==0, f"There are {row_diff} rows being dropped in write. Retry."
