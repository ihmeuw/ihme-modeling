
# coding: utf-8

# In[1]:

## where your code lives
home_dir = 'FILEPATH'
## you project
proj = 'PROJECT'
## where you want data written
write_dir ='FILEPATH'


# In[70]:

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


# ## read in survey data and submit parallelized extraction
# * read in from google doc
# * subset things I want
# * submit jobs

# In[24]:

df_reader =gdoc_query()
### indonesia subnational
idn_sub= gdoc_query(url='FILEPATH')
## inidia subnational
ind_sub= gdoc_query(url='FILEPATH')
### WHO MCCS
mccs = gdoc_query(url='FILEPATH')
# world health survey
whs = gdoc_query(url='FILEPATH')
## world bank
wb = gdoc_query(url ='FILEPATH')
## mexico subnational
mex_2006 = gdoc_query(url ='FILEPATH')
## brazil data
bra = gdoc_query(url='FILEPATH')
## new data
#n = gdoc_query(url="FILEPATH")

##testing dismod effect
nk = gdoc_query(url="FILEPATH")


# In[25]:

##compile all data
df_reader =pd.concat((bra, wb,ind_sub, mccs, df_reader, idn_sub,  whs,  mex_2006, nk))


# In[26]:

############################################################
##3 ## create folders for data
##########################################################
#are you sure you wantot o do this????
get_ipython().system(u'rm -r J')

get_ipython().system(u' mkdir J')


# In[27]:

df_reader =df_reader.loc[df_reader.type.isin(['ip'])]
df_reader= df_reader.loc[~df_reader.unique_id.duplicated()]
df_reader = df_reader.reset_index(drop=True)


# In[ ]:

## Trying to change 130979 survey

special_nid = 130979
df_reader_subset = df_reader.query("nid != @special_nid")
df_reader_subset['filepaths'] = df_reader_subset['filepaths'].apply(lambda x :x.replace('.p', '.csv'))

df_reader_remainder = df_reader.query("nid == @special_nid")
df_reader = pd.concat([df_reader_subset, df_reader_remainder])


# In[30]:

df_reader.query("nid == @special_nid")["filepaths"].head().values[0]


# In[31]:

df_reader_remainder["filepaths"].head().values[0]


# In[32]:

df_reader.reset_index(drop=True).to_csv(write_dir+'inpatient_template_reader.csv')


# ## Parallelize survey extractions

# In[33]:

os.chdir(home_dir+'code')
for i in np.arange((len(df_reader))):
    call  =qsub(QSUB)
    # my qsub function is broken for 2.7 so just take the returned list and put it into the command line
    os.system(" ".join(call))


#Error finding workflow:
    #1) Run df_reader.loc[df_reader.unique_id=='[insert file here]
        # Where file is one from long list below
    #2) Get index # from the table that df_reader.loc reads
    #3) Put in index to cell with i= below
    #4) Take that output in brackets, put it in as sys.argv in next tab (which is ip_extraction_code)
    #5) Run each cell with setargv correct
    #6) Third to last cell gives an error like "over 40% respondents had wrong sex variable)
    #7) next cell: func.reader to find the variables, open the csv too to check
    #8) change on google doc for all surveys
    # rinse and repeate for all easy ones

# In[88]:

## Find specific file

df_reader.loc[df_reader.unique_id=="IND_43908_5243_India_National_Sample_Survey_Round_52_1995-1996_1995"]


# In[89]:

df_reader.loc[df_reader.unique_id.contains['DHS']]


# In[35]:

## check to make sure I got all of them 
files = os.listdir(write_dir)
files = [x for x in files if ('num_visit' in x) | ('util_var' in x)]

## check to see which surveys failed. can use my check run function for the jid but I like to see the name of the survey
for y in df_reader.unique_id.unique():
    if len([x for x in files if str(y).lower() in x]) !=0:
        pass
    else:
        print y


# In[ ]:

## Can put in specific index and run it specifically. Look at output file to see if it's breaking.

os.chdir(home_dir+'code')
i = 414
call= qsub(QSUB)

os.system(" ".join(call))


# ### compile surveys

# In[37]:

num_visit, fraction= compile_survey_data().compile_survey_data(df_reader=df_reader, 
             recall_rules={4.3: [0,29.1],
                      52. :[29.1, 999]},
       path=write_dir)


# In[38]:

data={'num_visit':num_visit,
     'fraction':fraction}

for i in data.keys():
    temp = data[i]
    temp.loc[temp.location_id==95, 'location_id']=4749
    temp.rename(columns={'filepath': 'unique_id'}, inplace=True)
    # replace zero with tiny number
    temp.loc[temp['mean']==0, 'mean']= .001
    ## print number of values below zero
    print len(temp[temp['mean']<0])
    temp = prep_upload(temp.copy(), recall_rules={'cv_1_month_recall':[0, 29.1],
                                                'cv_12_month_recall' :[29.1, 999.]})
    temp['cv_marketscan']=0
    temp['cv_whs']=0
    temp['is_outlier']=0
    temp['cv_survey']=1
    temp['cv_whs']=0
    temp['cv_mics']=0
    data[i] = temp.copy()
fraction = data['fraction']
num_visit = data['num_visit']


# ### Read in other data (i.e. hospital data, data from facility surveys and tabs)

# In[39]:

# hmdb
hmdb = pd.read_csv('FILEPATH', low_memory=True)

hmdb['cv_survey']=0
hmdb['cv_1_month_recall'] =0
hmdb['cv_12_month_recall']=0
hmdb['cv_marketscan']=0
hmdb['cv_whs']=0

hmdb['nid'] = map(np.float, hmdb['nid'])
hmdb['is_outlier']=0
hmdb['visit_type']='ip'
hmdb = hmdb.loc[['BRA' not in x for x in hmdb.ihme_loc_id.tolist()]] ## drop Brazil


# In[40]:

## remove state databases that are inappropriately aggregated to national
hmdb= hmdb.loc[~((hmdb.nid==90319 )&(hmdb.location_id==102))]
hmdb= hmdb.loc[~((hmdb.nid==90318 )&(hmdb.location_id==102))]
hmdb= hmdb.loc[~((hmdb.nid==90317 )&(hmdb.location_id==102))]
hmdb= hmdb.loc[~((hmdb.nid==90316 )&(hmdb.location_id==102))]
hmdb= hmdb.loc[~((hmdb.nid==90315 )&(hmdb.location_id==102))]


# In[41]:

## facility data
fac_data = pd.read_csv('FILEPATH')
fac_data['is_outlier']=0
fac_data['cv_survey']=0
fac_data['cv_1_month_recall'] =0
fac_data['cv_12_month_recall']=0
fac_data['cv_marketscan']=0
fac_data['cv_whs']=0
fac_data['visit_type']='ip'


# In[42]:

## reported tabs
tabs  = pd.read_csv('FILEPATH')
tabs = tabs.loc[tabs.visit_type.isin(['ip_avg', 'ip_frac'])]
tabs['cv_marketscan']=0
tabs['cv_whs']=0
tabs['is_outlier']=0
tabs['cv_survey']=1


# In[43]:

len(tabs.index)


# ## combine all data types

# In[44]:

all_cols = [] 
collect = pd.DataFrame()
dict_of_dfs = {'fracs_survey':fraction,
              'avg_survey' : num_visit,
               'tabs': tabs,
              'hmdb': hmdb,
                 'fac_data': fac_data}

for i in dict_of_dfs.keys():
    print i
    print len(dict_of_dfs[i])
    
    all_cols.extend(dict_of_dfs[i].columns.tolist())
    dict_of_dfs[i]['data_file'] = i
    dict_of_dfs[i]['sex'] = dict_of_dfs[i]['sex'].str.lower()
    collect =collect.append(dict_of_dfs[i])
collect.loc[collect.visit_type=='ip', 'visit_type']='ip_avg' # there are two types ip_fractions and ip_average


# In[60]:

#### FOR BUNDLE TROUBLESHOOTING ####
### SEE THE DATA THAT ACTUALLY GOES IN ##### 



# ## custom modifications and custom outliers

# In[61]:

## add dummy crosswalk to whs and mccs data to test in dismod
collect.loc[collect.title=='World health survey', 'cv_whs']=1
collect.loc[collect.title=='WHO_MCCs', 'cv_mics']=1


# In[62]:

## wrong iso code for romania
collect.loc[collect.ihme_loc_id=='ROM', 'location_id'] =52
collect.loc[collect.ihme_loc_id=='ROM', 'ihme_loc_id'] ='ROU'
collect['is_outlier']=0
collect['measure'] ='continuous'
## check to make sure no blanks
try:
    print len(collect.loc[collect.location_id==''])
    collect = collect.loc[collect.location_id!='']
except:
    pass
## convert nids to numerics
collect['location_id'] = map(np.float, collect['location_id'])
collect['nid'] = map(np.float, collect['nid'])
## wierd dismod quirk for under 1
collect.age_end.replace(.99, .999, inplace=True)
collect.age_end.replace(1, .999, inplace=True)
collect.loc[collect['location_name']=='Alaska', 'is_outlier']=1


# In[63]:

## cutom outlier not consistent with other data
collect.loc[(collect.nid==112332) &(collect.sex=='Female'), 'is_outlier']=1
collect.loc[(collect.nid==20596) &(collect.sex=='Female'), 'is_outlier']=1
             
## BIH data is wacky here  TODO: investigate it
collect.loc[(collect.ihme_loc_id=='BIH') & (collect['mean'] >.5) & (collect.year_start==2004), 'is_outlier']=1
# collect.loc[collect.location_id==170, 'is_outlier']=1## the congo
# collect.loc[(collect.location_id==195) & (collect.year_start==2002), 'is_outlier']=1 # namibia 


# In[64]:

## GBR correct duplciate in lower uk levels
uk = collect.loc[collect.location_id==95]
collect= collect.loc[collect.location_id!=95]
uk1 = uk.copy()
uk2 = uk.copy()
uk3 = uk.copy()
uk4 = uk.copy()
uk1['location_id']=4749
uk2['location_id']=4636
uk3['location_id']=434
uk4['location_id']=433
uk = pd.concat((uk1, uk2, uk3, uk4))
collect= pd.concat((collect, uk))


# In[65]:

## get only the columns I want
cols  =pd.read_excel('FILEPATH').columns
cols= [ x for x in cols if '.1' not in x]
cols.extend(['cv_12_month_recall', 'cv_survey', 'cv_1_month_recall', 'data_file', 'cv_sick','cv_mics', 'cv_whs', 'cv_marketscan', 'unique_id', 'true_recall'])#,              
cols =list(set(cols))
collect = collect[cols]

## make sure these are numerics...
collect['year_start'] = map(np.float, collect['year_start'])
collect['year_end'] = map(np.float, collect['year_end'])
## fix odd years.
collect.loc[collect.year_start>=collect.year_end, 'year_start'] = collect.loc[collect.year_start>=collect.year_end, 'year_end']-1
collect['unit_value_as_published']=1


# In[ ]:

len(collect)


# ## calc se from sample size using formula
# 

# In[66]:

collect = calc_se(collect.copy()).calc_se()


# ## split all age data points data based off dismod model
# * turn off when running for age pattern

# In[67]:

split= sweet_age_split(collect.copy(), 154802)

## step thru each part in case something is broken
split.prep_input_data()
split.get_dismod_model()
split.prep_dismod_results()
split.add_age_group_id()      
split.add_year_id()
## just in case uk is wierd ion dismod
if 95 in split.df.location_id.unique().astype(np.int):
    print  'United kingdom' + str(len(split.df.loc[split.df.location_id==95]))                                                                                           


split.all_age = split.df.loc[(split.df.age_group_id==22)] 
## don't need to adjust these
split.df =split.df[split.df.age_group_id!=22]
split.adjust_all_age()
split.all_age['tabs_split']=1
collect = pd.concat((split.df, split.all_age))
## dismod sex code
collect['sex'] = collect['sex_id']
collect.sex.replace(1, 'Male', inplace=True)
collect.sex.replace(2, 'Female', inplace=True)
collect.sex.replace(3, 'Both', inplace=True)


# ### age weighting

# In[ ]:

# weighted_mean = collect.loc[(collect.cv_survey ==1)].reset_index(drop=True)
# non_weighted_mean =collect.loc[(collect.cv_survey !=1)]
# weighted_mean= create_age_weight(weighted_mean.copy())

# assert len(weighted_mean) + len(non_weighted_mean) == len(collect), 'check the lengths'
# collect = pd.concat((weighted_mean , non_weighted_mean))


# In[68]:

len_check = len(collect.loc[((collect.visit_type=='ip_avg') & (collect.cv_1_month_recall==1))])
print 'I beleive there is only one survey with this info {}'.format(len_check)
collect= collect.loc[~((collect.visit_type=='ip_avg') & (collect.cv_1_month_recall==1))] 

## save all fractions.. I drop fraction where I have avg visits
collect.loc[(collect.is_outlier==0) & (collect.visit_type=='ip_frac')].to_csv(write_dir+ 'preped_ip_fractions.csv')
## When surveys reprot average I also take fractions drop fractions when we have average 
num_visit_loc_ids = num_visit.unique_id.unique().tolist()
print len(collect.loc[((collect.data_file=='fracs_survey') & (collect.unique_id.isin(num_visit_loc_ids)))])
collect= collect.loc[~((collect.data_file=='fracs_survey') & (collect.unique_id.isin(num_visit_loc_ids)))]



get_ipython().magic(u'matplotlib inline')
collect['standard_error'].hist()


# In[70]:

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


# ## clean up somethings

# In[71]:

## clear up somethings before loading to the uplaoder
## these columns are required and added on late should be added on in an earlier step

collect['input_type']=''
collect['location_name']=''
collect['lower'] =''
collect['upper'] =''
collect['response_rate']=''
# somehow some means became character double check thios
collect['mean'] = map(np.float, collect['mean'])
collect = collect.fillna('')
## epi uplaoder require round numbers....
collect['recall_type_value'] =np.round(collect['recall_type_value'],0)


# ## upload results for age pattern

# In[ ]:


collect['age_mean'] = (collect['age_start'] + collect['age_end'])/2. # close enough

## save data for pre-dismod cross walk. Pre dismod crosswalk to be done in R because python does not have the library support
## couldc hange this to feather
collect.loc[(collect.cv_12_month_recall==1 ) & (collect.visit_type.isin(['ip_avg']))].to_csv(write_dir+'for_cw_ip_12_month_avg.csv')
print len(collect.loc[(collect.cv_12_month_recall==1 ) & (collect.visit_type.isin(['ip_avg']))])
collect.loc[(collect.cv_12_month_recall==1 ) & (collect.visit_type.isin(['ip_frac']))].to_csv(write_dir+ 'for_cw_ip_12_month_frac.csv')
print len(collect.loc[(collect.cv_12_month_recall==1 ) & (collect.visit_type.isin(['ip_frac']))])
collect.loc[(collect.cv_1_month_recall==1 ) & (collect.visit_type.isin(['ip_frac']))].to_csv(write_dir + 'for_cw_ip_1_month_frac.csv')
print len(collect.loc[(collect.cv_1_month_recall==1 ) & (collect.visit_type.isin(['ip_frac']))])
collect.loc[collect.cv_survey==0].to_csv(write_dir + 'for_cw_ip_admin.csv')
print len(collect.loc[collect.cv_survey==0])



# In[73]:

## submit crosswalk job
call = qsub(QSUB)

# In[ ]:

print(write_dir)


# In[74]:


## read in crosswalked values
avg_12 = pd.read_csv(write_dir +'cwd_ip_12_month_avg.csv')
frac_12 = pd.read_csv(write_dir + 'cwd_ip_frac.csv')
frac_1 = pd.read_csv(write_dir + 'cwd_ip_1_month_avg.csv')


# In[75]:

## concat the dfs
crosswalked =pd.concat((avg_12, frac_12, frac_1))

## these are now like admin records ... fingers crossed
crosswalked['cv_1_month_recall']=0
crosswalked['cv_12_month_recall']=0
crosswalked['cv_survey']=0
crosswalked['cv_pre_dismod_crosswalk']=1


# In[76]:

## concat and drop surveys as we have crosswalked them
collect = pd.concat((crosswalked,collect.loc[collect.cv_survey==0]))

## clean these thing up... not neccessary but may cause issues 
collect['location_name']=''
collect['lower']=''
collect['upper']=''

## check what is up with this
collect.loc[(collect.ihme_loc_id=='BIH' )& (collect.year_start==2004), 'is_outlier']=1
#collect.loc[(collect.ihme_loc_id=='BLR' )& (collect.year_start==2009), 'is_outlier']=1


## weird dismod quirk 
collect.age_end.replace(1, .99999, inplace=True)

collect.to_csv(write_dir +'ip_upload.csv')

collect.to_excel(write_dir +'ip_upload.xlsx',
                        sheet_name ='extraction', index=False)


# In[100]:

list(collect)


# In[77]:

write_dir


# In[78]:

## For troubleshooting
collect.to_csv('FILEPATH')


# In[92]:

len(collect)


# In[93]:

## upload data
run.upload_epi_data(3125,write_dir +'ip_upload.xlsx')


# # outlier/ifd specifics

# In[65]:

outlier = pd.read_csv('FILEPATH')


# In[66]:

outlier.head()


# In[67]:

outlier.to_excel('FILEPATH',
                        sheet_name ='extraction', index=False)


# In[ ]:

run.upload_epi_data(3125, 'FILEPATH')


# ## Outpatient envelope uploads

# In[72]:

op = pd.read_csv('FILEPATH')


# In[73]:

op.to_excel('FILEPATH',
                        sheet_name ='extraction', index=False)

