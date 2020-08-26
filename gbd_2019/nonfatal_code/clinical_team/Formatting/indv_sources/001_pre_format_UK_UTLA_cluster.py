"""
Created on Tue Jan 17 12:07:25 2017

@author: USER & USER

formatting UK UTLA data

"""
print("need to incorporate injuries data which are stored in separate files")
import pandas as pd
import numpy as np
import platform
import sys

sys.path.append(r"FILENAME")
from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

utla = pd.read_csv(root + r"FILENAME"
                   r"GBR_ENGLAND_HES_2001_2014_BY_AGE_SEX_CAUSE_"
                   r"FILEPATH")






reg = pd.read_csv(root + r"FILEPATH")


weights = pd.read_excel(root + "FILEPATH")

weights = weights[['FYEAR', 'Inpatient', 'DiagCode3', 'value']]

weights.rename(columns = {'Inpatient': 'inpatient_id',
                          'DiagCode3': 'cause_code'}, inplace=True)

weights["denominator"] = weights.groupby(['FYEAR', 'cause_code'])['value'].transform("sum")

weights = weights[weights['inpatient_id'] == 1]


weights['weight'] = weights['value'] / weights['denominator']
assert weights.weight.max() <= 1

weights = weights[['FYEAR', 'cause_code', 'weight']]




reg_dict = {'E12000001': 'North East',
'E12000002': 'North West',
'E12000003': 'Yorkshire and The Humber',
'E12000004': 'East Midlands',
'E12000005': 'West Midlands',
'E12000006': 'East of England',
'E12000007': 'London',
'E12000008': 'South East',
'E12000009': 'South West'}

reg['region_name'] = reg['REGION'].map(reg_dict)





reg.loc[reg['AgeBand'] == '90+', 'AgeBand'] = '9099'
reg.loc[reg['AgeBand'] == '0000', 'AgeBand'] = '0001'
reg.dropna(subset=['AgeBand'], inplace=True)

reg['age_start'] = reg['AgeBand'].str[0:2]
reg['age_end'] = reg['AgeBand'].str[2:]
reg['age_start'] = pd.to_numeric(reg['age_start'], errors='raise')
reg['age_end'] = pd.to_numeric(reg['age_end'], errors='raise')

reg.drop(["AgeBand", "REGION"], axis=1, inplace=True)





reg['FYEAR'] = reg['FYEAR'].astype(str)
reg['year_end'] = reg['FYEAR'].str[-2:].astype(np.int)
reg['year_end'] = reg['year_end'] + 2000
reg['year_start'] = reg['year_end'] - 1
reg['fiscal_year'] = (reg['year_start'] + reg['year_end']) / 2





reg.rename(columns={'SEX': 'sex_id', 'DiagCode3': 'cause_code', 'value': 'reg_value'}, inplace=True)


reg.loc[reg.region_name=="North East", "region_name"] = "North East England"
reg.loc[reg.region_name=="North West", "region_name"] = "North West England"
reg.loc[reg.region_name=="South East", "region_name"] = "South East England"
reg.loc[reg.region_name=="South West", "region_name"] = "South West England"
reg.loc[reg.region_name=="Yorkshire and The Humber", "region_name"] = "Yorkshire and the Humber"
reg.loc[reg.region_name=="London", "region_name"] = "Greater London"


weights['FYEAR'] = weights['FYEAR'].astype(str)
reg = reg.merge(weights, how='left', on=['FYEAR', 'cause_code'])

reg['reg_value'] = reg['reg_value'] * reg['weight']
reg.drop('weight', axis=1, inplace=True)







utla_name_map = pd.read_excel(root + r"FILENAME"
                              r"FILENAME"
                              r"GBR_ENGLAND_HES_2001_2014_METADATA_"
                              r"FILEPATH",
                              sheetname="England_UTLA_codes")
utla_name_map.rename(columns={'Code': 'utla_code', 'Name': 'utla_name'},
                     inplace=True)


utla = utla.dropna(subset=['UTLA11'])


utla.rename(columns={"UTLA11": "utla_code"}, inplace=True)
utla = utla.merge(utla_name_map, how='left', on="utla_code")





utla.loc[utla['AgeBand'] == '90+', 'AgeBand'] = '9099'
utla.loc[utla['AgeBand'] == '0000', 'AgeBand'] = '0001'
utla.dropna(subset=['AgeBand'], inplace=True)

utla['age_start'] = utla['AgeBand'].str[0:2]
utla['age_end'] = utla['AgeBand'].str[2:]
utla['age_start'] = pd.to_numeric(utla['age_start'], errors='raise')
utla['age_end'] = pd.to_numeric(utla['age_end'], errors='raise')

utla.drop("AgeBand", axis=1, inplace=True)



utla['FYEAR'] = utla['FYEAR'].astype(str)
utla['year_end'] = utla['FYEAR'].str[-2:].astype(np.int)
utla['year_end'] = utla['year_end'] + 2000
utla['year_start'] = utla['year_end'] - 1
utla['fiscal_year'] = (utla['year_start'] + utla['year_end']) / 2


utla.rename(columns={'SEX': 'sex_id', 'DiagCode3': 'cause_code'}, inplace=True)


utla = utla.merge(weights, how='left', on=['FYEAR', 'cause_code'])


supressed = utla[utla.value == '*'].shape[0]
utla['value'] = pd.to_numeric(utla['value'], errors='coerce')
assert utla[utla.value.isnull()].shape[0] == supressed,            "Some supressed values were lost"
utla['value'] = utla['value'] * utla['weight']
utla.drop('weight', axis=1, inplace=True)


assert set(utla.age_start.unique()).symmetric_difference(set(reg.age_start.unique())) == set()
assert set(utla.age_end.unique()).symmetric_difference(set(reg.age_end.unique())) == set()
assert set(reg.fiscal_year.unique()).symmetric_difference(set(utla.fiscal_year.unique())) == set()
assert set(reg.cause_code.unique()).symmetric_difference(set(utla.cause_code.unique())) == set()










pop = pd.read_csv(root + r"FILEPATH")
age_df = get_hospital_age_groups()
pre = pop.shape[0]
pop = pop.merge(age_df, how='left', on='age_group_id')
assert pre == pop.shape[0]












pop.rename(columns={'year_id': 'year_start'}, inplace=True)
pop.drop(['age_group_id', 'run_id'], axis=1, inplace=True)

pop_sum = pop[pop.age_start.notnull()].population.sum()







pop.loc[pop.age_start > 89, ['age_start', 'age_end']] = [90, 99]

pop = pop.groupby(['location_id', 'sex_id', 'age_start', 'age_end',
                   'year_start']).agg({'population': 'sum'}).\
                   reset_index()
assert round(pop.population.sum(), 3) == round(pop_sum, 3),    "groupby has changed pop counts"



pop['start_pop'] = pop['population']/2
pop['end_pop'] = pop['population']/2
pop['year_start'] = pop['year_start'] + .5
pop['year_end'] = pop['year_start'] + 1

pop.drop(['population'], axis=1, inplace=True)

pop = pop.set_index(['location_id', 'year_start', 'year_end', 'sex_id',
                     'age_start', 'age_end']).stack().reset_index()



pop.loc[pop['level_6'] == 'start_pop', 'year_end'] =    pop.loc[pop['level_6'] == 'start_pop', 'year_start']
pop.loc[pop['level_6'] == 'end_pop', 'year_start'] =    pop.loc[pop['level_6'] == 'end_pop', 'year_end']
pop.rename(columns={0: 'population'}, inplace=True)


pop = pop.groupby(['location_id', 'sex_id', 'age_start', 'age_end',
                   'year_start', 'year_end']).agg({'population': 'sum'}).\
                   reset_index()
pop['fiscal_year'] = pop['year_start']
pop.drop(['year_start', 'year_end'], axis=1, inplace=True)



loc = pd.read_csv(root + r"FILEPATH")
loc.loc[loc.location_name.str.contains("hackney", case=False), 'location_name'] = "Hackney & City of London"
loc.loc[loc.location_name.str.contains("helens", case=False), 'location_name'] = "St. Helens"
assert set(utla.utla_name) - set(loc.location_name) == set()







pre = utla.shape[0]
utla = utla.merge(loc, how='left', left_on='utla_name',
                  right_on='location_name')
assert pre == utla.shape[0]
assert utla.location_id.isnull().sum() == 0


pre = utla.shape[0]
utla = utla.merge(pop, how='left', on=['location_id', 'fiscal_year', 'sex_id',
                                       'age_start', 'age_end'])
assert pre == utla.shape[0]






pre = reg.shape[0]
reg = reg.merge(loc, how='left', left_on='region_name',
                  right_on='location_name')
assert pre == reg.shape[0]
assert reg.location_id.isnull().sum() == 0


pre = reg.shape[0]


reg = reg.merge(pop, how='left', on=['location_id', 'fiscal_year', 'sex_id',
                                       'age_start', 'age_end'])


assert set(utla[utla.population.isnull()].sex_id).symmetric_difference(set([9, 0])) == set()
assert set(reg[reg.population.isnull()].sex_id).symmetric_difference(set([9, 0])) == set()












assert pre == reg.shape[0]

utla['in_utla_data'] = True

utla['utla_log_rate'] = np.log(utla['value'] / utla['population'])
utla = utla[['cause_code', 'location_id', 'location_parent_id', 'population', 'age_start', 'age_end', 'sex_id', 'fiscal_year', 'value', 'utla_log_rate', 'in_utla_data']]



reg = reg[['cause_code', 'location_id', 'age_start', 'age_end', 'sex_id', 'fiscal_year', 'reg_value', 'population']]
reg = reg.groupby(['cause_code', 'location_id', 'age_start', 'age_end', 'sex_id', 'fiscal_year', 'population']).agg({'reg_value': 'sum'}).reset_index()

reg['reg_log_rate'] = np.log(reg['reg_value'] / reg['population'])





utla = utla.query("sex_id == 1 | sex_id == 2")
reg = reg.query("sex_id == 1 | sex_id == 2")


reg.rename(columns={'location_id': 'location_parent_id', 'population': 'reg_populuation'}, inplace=True)
utla.rename(columns={'population': 'utla_population'}, inplace=True)






both = utla.merge(reg, how='left', on=['location_parent_id', 'sex_id', 'age_start', 'age_end', 'fiscal_year', 'cause_code'])


both['log_rate'] = both['utla_log_rate']
both.loc[both.log_rate.isnull(), 'log_rate'] = both.loc[both.log_rate.isnull(), 'reg_log_rate']



both = both[both['reg_value'].notnull()]

assert both['log_rate'].isnull().sum() == 0
assert both['age_start'].unique().size == 20
assert both['fiscal_year'].unique().size == 14


both.to_csv(r"FILEPATH",compression='gzip', index=False)







