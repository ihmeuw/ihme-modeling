
# coding: utf-8

# ## Bring in and format hospitla data from four sources
# 1. Hospital data team (only the data with complete coverage)
#     Sources: ECU, CHL, USA_NHDS, USA_Hosp, NZL
#     New: CHN, AUT, BRA
# 2. HMDB
# 3. Health for all
# 4. OECD

# In[ ]:

## where your code lives
home_dir = 'FILEPATH'
## you project
proj = 'PROJECT'
## where you want data written
write_dir ='FILEPATH'


# In[ ]:

import pandas as pd
import numpy as np
import os as os

import requests

from StringIO import StringIO

import copy as copy

from multiprocessing import Pool

os.chdir(home_dir+'code')
from extractor import gdoc_query, extractor
from db_queries import get_population, get_covariate_estimates, get_model_results
from functions_to_prep_extractions import *
from cluster_utilities import *
from elmo import run


# In[ ]:

def prep_upload(final):
    #final = final.loc[(final['sex'] == 'Both') & (final['sample_size'] >30)]
    print final['mean'].min() 
    assert final['mean'].min() > 0, 'why are there zeros here'

    #### set cross walks

    temp = final[['nid', 'location_name', 'location_id', 'sex', 'sex_id', 'year_start', 'cv_sick', 'year_end', 'age_start', 'age_end','mean',                   'cases', 'sample_size', 'visit_type', 'recall_type_value', 'cv_1_month_recall', 'cv_12_month_recall']].reset_index(drop=True)
    final['recall']=temp['recall_type_value'].values
    final['recall_type_value'] =np.round(temp['recall_type_value'].values)  
    final['source_type'] = 'Facility - inpatient'
   
    final['site_memo'] = 'UHC'
    final['sex_issue']= 0
    final['year_issue']=0
    final['age_issue']=0
    final['measure'] ='continuous'
    final['unit_type'] = 'Person'

    final['measure_issue'] =0
    final['measure_adjustment']=0
    final['representative_name']= 'Nationally and subnationally representative'
    final['urbanicity_type']= 'Mixed/both'
    final['recall_type'] = 'Period: weeks'
    final['extractor'] = ''
    final['is_outlier']=0
    final['seq']=''
    final = final.fillna('')
    final['row_num'] = ""
    
    ## fix these
    #temp.loc[temp.year_end =='n', 'year_end'] =  temp.loc[temp.year_end =='n', 'year_start']

    add_cols = [x not in final.columns.tolist() for x in  template.columns]
    add_cols = template.columns[add_cols] 
                     
                     
    add_cols_test = [x for x in add_cols if x not in final.columns.tolist()]
    for b in add_cols_test:
        final[b] = ''
    return final


# In[ ]:

## age group map by age_start, age_end, age_group_id
map_age_group =pd.read_csv('FILEPATH')


# In[ ]:

template = pd.read_excel('FILEPATH')
from db_queries import get_population, get_location_metadata


# ## Hospital Data teams data

# ### Bring in new ecuador data 

# In[ ]:

ecu =pd.read_hdf('FILEPATH')

ecu= ecu.loc[ecu.diagnosis_id==1] ## only want one diagnosis

ecu = ecu.groupby(['age_start', 'age_end', 'sex_id', 'year_start', 'year_end', 'location_id', 'nid']).sum().reset_index()

ecu.age_end.replace(1, .99, inplace=True)

ecu = ecu.merge(map_age_group, on=['age_start', 'age_end'], how='left')


# In[ ]:

ecu.head()


# ### new zealand data because I need to drop day cases

# In[ ]:

## thsi takes forever....
nzl = pd.read_stata('FILEPATH')
nzl =nzl.loc[nzl.metric_day_cases==0]
nzl = nzl.loc[nzl.dx_ecode_id==1]
nzl.rename(columns={'NID':'nid'}, inplace=True)
nzl = nzl.groupby(['age', 'sex', 'year', 'nid']).sum().reset_index()
nzl['age_end'] = nzl['age']+4
mask = (nzl.age_end==4) & (nzl.age==0)
nzl.loc[mask, 'age_end']=.99
nzl.rename(columns={'age':'age_start'}, inplace=True)
nzl = nzl.merge(map_age_group, on=['age_start', 'age_end'], how='left')


# ## Try Austria

# In[ ]:

## Load in dataset
aut = pd.read_hdf('FILEPATH', key = 'df')


# In[ ]:

bra = pd.read_hdf('FILEPATH', key = 'df')


# In[ ]:

## going to need to sort for facility_id, outcome_id, and diagnosis_id
## Check if you need to sort out non-inpatient data

aut.facility_id.unique()


# In[ ]:

aut = aut.loc[aut.diagnosis_id == 1] ## only want one diagnosis


# In[ ]:

## Want to actually merge and get age_start and age_end back
aut = aut.merge(map_age_group, on=['age_group_id'], how = 'left')
aut =aut[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id', 'age_group_id', 'val', 'nid']]


# In[ ]:

## Get total admissions by age and sex and year (and location if subnational)
aut = aut.groupby(['age_group_id', 'age_start', 'age_end', 'sex_id', 'year_start', 'year_end', 'location_id', 'nid']).sum().reset_index()


# In[ ]:

pop = get_population(QUERY)
aut = aut.merge(pop[['age_group_id', 'sex_id', 'location_id', 'population', 'year_id']], 
                          left_on =['age_group_id', 'sex_id', 'year_start', 'location_id'], 
                          right_on =['age_group_id', 'sex_id', 'year_id', 'location_id'],how='left')


# In[ ]:

## Calculate mean
aut['mean'] = aut['val']/aut['population']


# In[ ]:

aut.head


# In[ ]:

## Convert to run through other code (should really make this a single function)
all_hosp = aut


# In[ ]:


## custom formatting
all_hosp.rename(columns={'population': 'sample_size'}, inplace=True)

## for uncertianty
all_hosp['cases'] = all_hosp['mean'] * all_hosp['sample_size']
all_hosp['cases'] = map(np.round, all_hosp['cases'])
all_hosp['sample_size'] = map(np.round, all_hosp['sample_size'])

## some  formatting
all_hosp['cv_sick']=0
all_hosp['visit_type'] = 'ip'
all_hosp['recall_type_value'] =52
all_hosp['cv_med_contact']= 0
all_hosp['cv_1_month_recall']= 0 
all_hosp['cv_12_month_recall']= 0
all_hosp['cv_survey'] =0 
all_hosp['cv_whs']=0
all_hosp['cv_marketscan']=0
#all_hosp.sex_id.replace(1, 'Male', inplace=True)
#all_hosp.sex_id.replace(2, 'Female', inplace=True)
#all_hosp.sex_id.replace(3, 'Both', inplace = True)

all_hosp = all_hosp.merge(get_location_metadata(22), on='location_id', how='left')## ohh well

all_hosp = all_hosp.loc[all_hosp.sex_id!=9]


# In[ ]:

## Figure out sex because I keep going back and forth
## Should be correct stop messing with it

all_hosp['sex'] = all_hosp['sex_id']
all_hosp.sex.replace(1, 'Male', inplace=True)
all_hosp.sex.replace(2, 'Female', inplace=True)
all_hosp.sex.replace(3, 'Both', inplace = True)


# In[ ]:

all_hosp['standard_error'] = ''


# In[ ]:

## Use prep_uplaod function
hosp =prep_upload(all_hosp)
hosp['unit_value_as_published']=1

hosp = hosp[[x for x in hosp.columns.tolist() if x in template.columns.tolist()]]
hosp['seq']=''


# In[ ]:

hosp['measure'] ='continuous'


# In[ ]:

hosp = hosp.merge(map_age_group, on=['age_start', 'age_end'], how = 'left')


# In[ ]:

hosp.age_group_id.unique()


# ### Stage two with "collect" file

# In[ ]:

collect = hosp
## Moving functions to here
## convert nids to numerics
collect['location_id'] = map(np.float, collect['location_id'])
collect['nid'] = map(np.float, collect['nid'])
## wierd dismod quirk for under 1
collect.age_end.replace(.99, .999, inplace=True)
collect.age_end.replace(1, .999, inplace=True)


# In[ ]:

## Set up columns
collect['nid'] = map(np.float, collect['nid'])
collect['is_outlier']=0
collect['visit_type']='ip'

collect['cv_survey']=0
collect['cv_1_month_recall'] =0
collect['cv_12_month_recall']=0
collect['cv_marketscan']=0
collect['cv_whs']=0
collect['true_recall'] = ''
collect['group_review'] = ''
collect['specificity'] = ''
collect['file_path'] = ''
collect['cv_sick'] = ''
collect['variable'] = ''
collect['group'] = ''
collect['data_sheet_filepath'] = ''
collect['cv_mics'] = ''
collect['data_file'] = 'fac_data'
collect['unique_id'] = 'FILEPATH'


# In[ ]:

## Get only the columns I want
cols  =pd.read_excel('FILEPATH').columns
cols= [ x for x in cols if '.1' not in x]
cols.extend(['cv_12_month_recall', 'cv_survey', 'cv_1_month_recall', 'data_file', 'cv_sick','cv_mics', 'cv_whs', 'cv_marketscan', 'unique_id', 'true_recall', 'age_group_id'])#,              
cols =list(set(cols))
collect = collect[cols]

## make sure these are numerics...
collect['year_start'] = map(np.float, collect['year_start'])
collect['year_end'] = map(np.float, collect['year_end'])
## fix odd years.
collect.loc[collect.year_start>=collect.year_end, 'year_start'] = collect.loc[collect.year_start>=collect.year_end, 'year_end']-1
collect['unit_value_as_published']=1


# In[ ]:

collect['sex_id'] = collect['sex']
collect.sex_id.replace('Male', 1, inplace=True)
collect.sex_id.replace('Female', 2, inplace=True)
collect.sex_id.replace('Both', 3, inplace = True)


# In[ ]:

collect['mean'].mean()


# In[ ]:

## Calculate standard error
collect = calc_se(collect.copy()).calc_se()


# In[ ]:

## Wuick glance at data:
get_ipython().magic(u'matplotlib inline')
collect['standard_error'].hist()
## low se should make sense with just hospital data?


# In[ ]:

## check to make sure There are not wierd things going on
print collect['mean'].isnull().sum()
print collect['standard_error'].isnull().sum()
try:
    print (collect['mean']=='').sum()
except:
    pass
try:
    print (collect['standard_error']=='').sum()
except:
    pass
print len(collect)
print collect['mean'].min()
print collect['standard_error'].min()
print collect['mean'].mean()
print collect['standard_error'].mean()


# In[ ]:

## clear up somethings before loading to the epi uplaoder
## these columns are required and added on late should be added on in an earlier step

collect['input_type']=''
collect['location_name']=''
collect['lower'] =''
collect['upper'] =''
collect['response_rate']=''
# somehow some means became character double check thios ## NO IT"S NOT
#collect['mean'] = map(np.float, collect['mean'])
#collect = collect.fillna('')
## epi uplaoder require round numbers....
collect['recall_type_value'] =np.round(collect['recall_type_value'],0)


# ## Write data and upload, should be ready

# In[ ]:

write_dir = 'FILEPATH'

## weird dismod quirk 
collect.age_end.replace(1, .99999, inplace=True)

collect.to_csv(write_dir +'aut.csv')

collect.to_excel(write_dir +'aut.xlsx',
                        sheet_name ='extraction', index=False)


# In[ ]:

## Check length one more time to make sure it looks correct
len(collect)


# In[ ]:

## Check age trend too
collect.groupby(['age_group_id', 'sex_id'], as_index = False)['mean'].mean()


# In[ ]:

## Upload data
## Should be ready!

run.upload_epi_data(3125,write_dir +'aut.xlsx')


# # Bring in China Hospital data and process

# In[ ]:

chn = pd.read_hdf('FILEPATH', key = 'df')
## already in age_groups


# In[ ]:

chn = chn[['location_id', 'year_start', 'year_end']]


# In[ ]:

chn.drop_duplicates()


# In[ ]:

chn = chn.loc[chn.diagnosis_id == 1] ## only want one diagnosis


# In[ ]:

## Want to actually merge and get age_start and age_end back
chn = chn.merge(map_age_group, on=['age_group_id'], how = 'left')
chn =chn[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id', 'age_group_id', 'val', 'nid']]


# In[ ]:

chn = chn.groupby(['age_group_id', 'age_start', 'age_end', 'sex_id', 'year_start', 'year_end', 'location_id', 'nid']).sum().reset_index()
#chn.age_end.replace(1, .99, inplace = True)


pop = get_population(QUERY)
chn = chn.merge(pop[['age_group_id', 'sex_id', 'location_id', 'population', 'year_id']], 
                          left_on =['age_group_id', 'sex_id', 'year_start', 'location_id'], 
                          right_on =['age_group_id', 'sex_id', 'year_id', 'location_id'],how='left')


# In[ ]:

## Calculate mean
chn['mean'] = chn['val']/chn['population']


# In[ ]:

## Check means by sex and age
## See that Both group looks bad
#chn.groupby(['age_group_id', 'sex_id'], as_index = False)['mean'].mean()


# In[ ]:

## custom formatting
all_hosp.rename(columns={'population': 'sample_size'}, inplace=True)

## for uncertianty
all_hosp['cases'] = all_hosp['mean'] * all_hosp['sample_size']
all_hosp['cases'] = map(np.round, all_hosp['cases'])
all_hosp['sample_size'] = map(np.round, all_hosp['sample_size'])

## some annoying formatting
all_hosp['cv_sick']=0
all_hosp['visit_type'] = 'ip'
all_hosp['recall_type_value'] =52
all_hosp['cv_med_contact']= 0
all_hosp['cv_1_month_recall']= 0 
all_hosp['cv_12_month_recall']= 0
all_hosp['cv_survey'] =0 
all_hosp['cv_whs']=0
all_hosp['cv_marketscan']=0
#all_hosp.sex_id.replace(1, 'Male', inplace=True)
#all_hosp.sex_id.replace(2, 'Female', inplace=True)
#all_hosp.sex_id.replace(3, 'Both', inplace = True)

all_hosp = all_hosp.merge(get_location_metadata(22), on='location_id', how='left')## ohh well

all_hosp = all_hosp.loc[all_hosp.sex_id!=9]


# In[ ]:

## Figure out sex because I keep going back and forth
## Should be correct stop messing with it

all_hosp['sex'] = all_hosp['sex_id']
all_hosp.sex.replace(1, 'Male', inplace=True)
all_hosp.sex.replace(2, 'Female', inplace=True)
all_hosp.sex.replace(3, 'Both', inplace = True)


# In[ ]:

all_hosp['standard_error'] = ''


# In[ ]:

## Use prep_uplaod function
hosp =prep_upload(all_hosp)
hosp['unit_value_as_published']=1

hosp = hosp[[x for x in hosp.columns.tolist() if x in template.columns.tolist()]]
hosp['seq']=''



# In[ ]:

hosp['measure'] ='continuous'

# In[ ]:

## and merge back to get age_group_id

hosp = hosp.merge(map_age_group, on=['age_start', 'age_end'], how = 'left')


# In[ ]:

hosp.age_group_id.unique()


# # Need to make sure sex_id, sex, age_group_id, age_start, age_end are all correct

# # Data is now ready to move to other sheet

# In[ ]:

collect = hosp

## convert nids to numerics
collect['location_id'] = map(np.float, collect['location_id'])
collect['nid'] = map(np.float, collect['nid'])
## wierd dismod quirk for under 1
collect.age_end.replace(.99, .999, inplace=True)
collect.age_end.replace(1, .999, inplace=True)


# In[ ]:

collect['nid'] = map(np.float, collect['nid'])
collect['is_outlier']=0
collect['visit_type']='ip'


# In[ ]:

collect['cv_survey']=0
collect['cv_1_month_recall'] =0
collect['cv_12_month_recall']=0
collect['cv_marketscan']=0
collect['cv_whs']=0


# In[ ]:

## manually add in data for missing columns (necessary, how did Mark get around this?)
collect['true_recall'] = ''
collect['group_review'] = ''
collect['specificity'] = ''
collect['file_path'] = ''
collect['cv_sick'] = ''
collect['variable'] = ''
collect['group'] = ''
collect['data_sheet_filepath'] = ''
collect['cv_mics'] = ''
collect['data_file'] = 'fac_data'
collect['unique_id'] = 'FILEPATH'


# In[ ]:

## Get only the columns I want
cols  =pd.read_excel('FILEPATH').columns
cols= [ x for x in cols if '.1' not in x]
cols.extend(['cv_12_month_recall', 'cv_survey', 'cv_1_month_recall', 'data_file', 'cv_sick','cv_mics', 'cv_whs', 'cv_marketscan', 'unique_id', 'true_recall', 'age_group_id'])#,              
cols =list(set(cols))
collect = collect[cols]

## make sure these are numerics...
collect['year_start'] = map(np.float, collect['year_start'])
collect['year_end'] = map(np.float, collect['year_end'])
## fix odd years.
collect.loc[collect.year_start>=collect.year_end, 'year_start'] = collect.loc[collect.year_start>=collect.year_end, 'year_end']-1
collect['unit_value_as_published']=1


# In[ ]:

## add sex_id back (this should be the last time this is needed)
collect['sex_id'] = collect['sex']
collect.sex_id.replace('Male', 1, inplace=True)
collect.sex_id.replace('Female', 2, inplace=True)
collect.sex_id.replace('Both', 3, inplace = True)


# In[ ]:

len(collect[])


# In[ ]:

collect['mean'].mean()


# ## Calc SE from sample size using formula

# In[ ]:

collect = calc_se(collect.copy()).calc_se()


# ## Split all age data points based off dismod model

## Wuick glance at data:
get_ipython().magic(u'matplotlib inline')
collect['standard_error'].hist()
## low se should make sense with just hospital data?


# In[ ]:

## check to make sure There are not wierd things going on
print collect['mean'].isnull().sum()
print collect['standard_error'].isnull().sum()
try:
    print (collect['mean']=='').sum()
except:
    pass
try:
    print (collect['standard_error']=='').sum()
except:
    pass
print len(collect)
print collect['mean'].min()
print collect['standard_error'].min()
print collect['mean'].mean()
print collect['standard_error'].mean()


# In[ ]:

## clear up somethings before loading to the epi uplaoder
## these columns are required and added on late should be added on in an earlier step

collect['input_type']=''
collect['location_name']=''
collect['lower'] =''
collect['upper'] =''
collect['response_rate']=''
#collect['mean'] = map(np.float, collect['mean'])
#collect = collect.fillna('')
## epi uplaoder require round numbers....
collect['recall_type_value'] =np.round(collect['recall_type_value'],0)


# In[ ]:

print(collect['mean'].mean())


# In[ ]:

## China specific
collect = collect.loc[collect.sex_id != 3] ## eliminating both group because mean's are insanely low


# In[ ]:

print(collect['mean'].mean())

# In[ ]:

write_dir = 'FILEPATH'


# In[ ]:

## weird dismod quirk 
collect.age_end.replace(1, .99999, inplace=True)

collect.to_csv(write_dir +'chn.csv')

collect.to_excel(write_dir +'chn.xlsx',
                        sheet_name ='extraction', index=False)


# In[ ]:

collect.nid.unique()


# In[ ]:

## Mean is same as after first download, which should make sense

print(collect['mean'].mean())
print(len(collect))


# In[ ]:

collect.groupby(['age_group_id', 'sex_id'], as_index = False)['mean'].mean()


# In[ ]:

## Upload data
## Should be ready!

run.upload_epi_data(3125,write_dir +'chn.xlsx')


# ### Data with complete coverage only
# 

# In[ ]:

other_hosp = pd.read_csv('FILEPATH')

hold = other_hosp.copy() ## large file didn't want to reload it

other_hosp = hold.copy()

other_hosp = other_hosp.loc[other_hosp.diagnosis_id==1,]
other_hosp = other_hosp.loc[other_hosp.source.isin(['CHL_MOH','USA_NHDS_79_10', 'USA_HCUP_SID', 'ECU_INEC_97_11'])]

other_hosp = other_hosp.merge(map_age_group, on=['age_start', 'age_end'], how='left')

other_hosp.loc[other_hosp.age_group_id.isnull(), 'age_group_id']=28

other_hosp = other_hosp.loc[other_hosp.facility_id.isin(['inpatient unknown'])]

other_hosp = other_hosp.groupby(['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'age_group_id', 'sex_id', 'nid']).sum().reset_index()


# ### Combine all hospital data together

# In[ ]:

other_hosp =other_hosp[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id', 'age_group_id', 'val', 'nid']]

nzl.rename(columns={'metric_discharges': 'val',
                   'sex':'sex_id',
                     'year':'year_start'}, inplace=True)
nzl['year_end'] = nzl['year_start']+1
nzl['location_id'] =72
nzl =nzl[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id', 'age_group_id', 'val', 'nid']]

ecu =ecu[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id', 'age_group_id', 'val', 'nid']]

all_hosp = pd.concat((other_hosp, ecu, nzl))
all_hosp.age_group_id.replace(33, 235, inplace=True)

#3 pull in pop to get in rate space
pop = get_population(QUERY)

all_hosp = all_hosp.merge(pop[['age_group_id', 'sex_id', 'location_id', 'population', 'year_id']], 
                          left_on =['age_group_id', 'sex_id', 'year_start', 'location_id'], 
                          right_on =['age_group_id', 'sex_id', 'year_id', 'location_id'],how='left')

all_hosp['mean'] = all_hosp['val']/all_hosp['population']

## custom formatting
all_hosp.rename(columns={'population': 'sample_size'}, inplace=True)

## for uncertianty
all_hosp['cases'] = all_hosp['mean'] * all_hosp['sample_size']
all_hosp['cases'] = map(np.round, all_hosp['cases'])
all_hosp['sample_size'] = map(np.round, all_hosp['sample_size'])

## some annoying formatting
all_hosp['cv_sick']=0
all_hosp['visit_type'] = 'ip'
all_hosp['recall_type_value'] =52
all_hosp['cv_med_contact']= 0
all_hosp['cv_1_month_recall']= 0 
all_hosp['cv_12_month_recall']= 0
all_hosp['cv_survey'] =0 
all_hosp['cv_whs']=0
all_hosp['cv_marketscan']=0
all_hosp.sex_id.replace(1, 'Male', inplace=True)
all_hosp.sex_id.replace(2, 'Female', inplace=True)

all_hosp = all_hosp.merge(get_location_metadata(22), on='location_id', how='left')## ohh well

all_hosp = all_hosp.loc[all_hosp.sex_id!=9]
all_hosp['sex'] = all_hosp['sex_id']
hosp =prep_upload(all_hosp)
hosp['unit_value_as_published']=1

hosp = hosp[[x for x in hosp.columns.tolist() if x in template.columns.tolist()]]
hosp['seq']=''

hosp = hosp.loc[hosp['location_name']!='Acre']
hosp =hosp.loc[hosp['location_name']!='Alaska']
hosp =hosp.loc[hosp['location_name']!='Mato Grosso do Sul']

hosp['measure'] ='continuous'


# ## hmdb data

# In[ ]:


df = pd.read_pickle("FILEPATH")
template = pd.read_excel('FILEPATH')
coverage= pd.read_excel('FILEPATH')

## correct for hmdb coverage
coverage['estCoverage'] = [str(x).replace('%', '').replace(' (e)', '') for x in coverage.estCoverage.tolist()]
coverage['estCoverage'] = coverage['estCoverage'].astype(np.float)
coverage['estCoverage'] /=100
df = df.merge(coverage, right_on=['Country', 'yId'], left_on=['Country', 'year_id'], how='left')
df = df.loc[(df.estCoverage>=.95) & (df.estCoverage<=1.05)] ## make sure there is decent coverage 
df['mean'] /= df['estCoverage'] ## correct for coverage

## custom dropping based on data
df = df.loc[df.location_name != 'Slovakia']#don't seperate day patients
df = df.loc[df.location_name !='Bulgaria']
df= df.loc[~((df.location_name == 'Macedonia') &(df.yId<2008))]
df= df.loc[ df.location_name != 'Turkey']
df= df.loc[ df.location_name != 'France']

### custom formatting stuff
df['sex'] = df['sex_id'].copy()
df['year_start'] = df['year_id']
df['year_end'] = df['year_id']-1
df.rename(columns={'population': 'sample_size'}, inplace=True)
df['cases'] = df['mean'] * df['sample_size']
df['cases'] = map(np.round, df['cases'])
df['sample_size'] = map(np.round, df['sample_size'])
df['nid'] =3822
df.sex.replace(1, 'Male', inplace=True)
df.sex.replace(2, 'Female', inplace=True)
df['cv_sick']=0
df.rename(columns={'average_cases': 'mean'}, inplace=True)
df['visit_type'] = 'ip'
df['recall_type_value'] =52
df['cv_med_contact']= 0
df['cv_1_month_recall']= 0 
df['cv_12_month_recall']= 0
df['cv_survey'] =0 

final = prep_upload(df)


# ## Prep health for all

# In[ ]:

h4all_ip = pd.read_csv("FILEPATH")
## do some quick formatting and prepping
h4all_ip = pd.melt(h4all_ip, id_vars='country', value_vars=h4all_ip.columns.tolist()[1:])
h4all_ip.rename(columns={'variable': 'year_start',
                     'value': 'ip'}, inplace=True)
h4all_ip.replace('...', np.nan, inplace=True)
h4all_ip = h4all_ip.loc[~h4all_ip.ip.isnull(),]
h4all_ip['location_name'] = h4all_ip['country'].apply(lambda x :str(x)[4:])

h4all= h4all_ip.copy()
h4all['ip'] = map(np.float,h4all['ip'] )
h4all['year_start'] = map(np.float, h4all['year_start'])

h4all.loc[h4all.location_name=='MKD*', 'location_name']='Macedonia'
h4all.loc[h4all.location_name=='Republic of Moldova', 'location_name']='Moldova'
h4all.loc[h4all.location_name=='Russian Federation', 'location_name']='Russia'

convert = pd.read_csv("FILEPATH")

## I don't know why I started calling this health for all 2.
h4all2 = pd.merge(h4all, convert[['location_name', 'location_id', 'ihme_loc_id']], on='location_name', how='left')

#3 drop these random countries becuas eof incomplete coverage based on metadata documentation
h4all2 = h4all2.loc[~h4all2.location_id.isnull()] ## drop these odds ones
## drop observations that include day patients.
h4all2 = h4all2.loc[~h4all2.location_name.isin(['Cyprus'])] 
h4all2 = h4all2.loc[~h4all2.location_name.isin(['Malta'])] 
h4all2 = h4all2.loc[~h4all2.location_name.isin(['Czech Republic'])]
h4all2 = h4all2.loc[~((h4all2.location_name=='Iceland') & (h4all2.year_start<1988))]
## drop certain countries
h4all2 = h4all2.loc[~((h4all2.location_name=='Macedonia') & (h4all.year_start<2002))]
h4all2 = h4all2.loc[(h4all2.location_name != 'Lithuania')]
h4all2 = h4all2.loc[~((h4all2.location_name=='Germnay') & (h4all.year_start<2005))]
h4all2 = h4all2.loc[h4all2.location_name != 'Turkmenistan']
h4all2 = h4all2.loc[h4all2.location_name!='Bosnia and Herzegovina']
h4all2 = h4all2.loc[h4all2.location_name!='France']
h4all2 = h4all2.loc[h4all2.location_name!='Belarus']
h4all2.loc[h4all2.ihme_loc_id=='USA_533' , 'location_id'] =35
h4all2.loc[h4all2.location_id==35 , 'ihme_loc_id'] = 'GEO'

## bring in pop
pop =get_population(QUERY)

## more formatting
h4all2['year_start'] = map(np.float, h4all2['year_start'])
h4all2 =h4all2.merge(pop, left_on=['location_id', 'year_start'], right_on=['location_id', 'year_id'], how='left')
h4all2['mean'] = h4all2.ip/h4all2.population
h4all2['sample_size'] = h4all2['population'].copy()
del h4all2['population']
h4all2['year_end'] = h4all2['year_start']+1
h4all2['age_start']=0
h4all2['age_end']=100
h4all2['nid'] = 231762
h4all2['sex'] ='Both'
h4all2['cases'] = map(np.round, h4all2['ip'])
h4all2['sample_size'] = map(np.round, h4all2['sample_size'])
h4all2['cv_sick']=0

h4all2['visit_type'] = 'ip'
h4all2['recall_type_value'] =52
h4all2['cv_med_contact']= 0
h4all2['cv_1_month_recall']= 0 
h4all2['cv_12_month_recall']= 0
h4all2['cv_survey']=0

h4all2 = prep_upload(final =h4all2)
country_list =h4all2.ihme_loc_id.unique().tolist()

## merge on nids
nids = pd.read_csv('FILEPATH')
mask =np.char.isnumeric(nids['underlying NID'].values.astype('unicode')).tolist()
nids.loc[[ not x for x in mask], 'underlying NID'] =nids.loc[[ not x for x in mask], 'parent NID']
nids =nids.loc[nids['HFA_DB output']=='Inpatient care discharges per 100']
nids['nid'] =nids['underlying NID'].map(np.float)

del h4all2['nid']
h4all2 = h4all2.merge(nids[['iso code', 'year', 'nid']], right_on=['iso code', 'year'],
             left_on=['ihme_loc_id', 'year_start'], how='left')

## fix this one add hoc
h4all2.loc[(h4all2['nid'].isnull() )& (h4all2.ihme_loc_id=='DEU'), 'nid']=299692
h4all2.loc[(h4all2['nid'].isnull() )& (h4all2.ihme_loc_id=='BGR'), 'nid']= 299692
h4all2.loc[h4all2.nid.isnull(), 'year_id'].unique()


# ## look for overalp between data sources

# In[ ]:

hosp['type'] = 'hosp'
final['type'] ='hmdb'
h4all2['type']='h4all'

hosp['id'] = hosp['location_name'].astype(str) +'_'+ hosp['year_start'].astype(int).astype(str)
final['id'] =  final['location_name'].astype(str) +'_'+ final['year_start'].astype(int).astype(str)
h4all2['id'] =  h4all2['location_name'].astype(str) +'_'+ h4all2['year_start'].astype(int).astype(str)

final = final.loc[~final['id'].isin(hosp['id'].unique().tolist())]

final = pd.concat((final, hosp))

h4all2 = h4all2.loc[~h4all2['id'].isin(final['id'].unique().tolist())]

final_hosp= pd.concat((h4all2, final))


# ## look at oecd
# 

# In[ ]:

oecd= pd.read_excel('FILEPATH')
oecd['age_start']=0
oecd['age_end']=100
oecd['sex']='Both'
oecd['type']='oecd'


## drop based on coverage
oecd = oecd.loc[oecd.location_name!='Czech Republic']
oecd = oecd.loc[~((oecd.location_name =='Denmark') & (oecd.year_start<2005))]
oecd = oecd.loc[oecd.location_name != 'Iceland']
oecd = oecd.loc[oecd.location_name != 'Hungary']
oecd = oecd.loc[oecd.location_name != 'Slovakia']

oecd['id'] = oecd['location_name'].astype(str) +'_'+ oecd['year_start'].astype(int).astype(str)

oecd2 = oecd.loc[[x not in final_hosp['id'].unique().tolist() for x in oecd['id']]]


# In[ ]:

oecd= pd.read_excel('FILEPATH')


# In[ ]:

oecd


# ### write to file

# In[ ]:

final_hosp= pd.concat((final_hosp, oecd2))

## drop some suspicious data
os.chdir('FILEPATH')

final_hosp[template.columns.tolist()].to_csv('FILEPATH')

