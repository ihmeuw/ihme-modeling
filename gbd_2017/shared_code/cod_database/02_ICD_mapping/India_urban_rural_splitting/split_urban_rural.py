
import pandas as pd
import numpy as np
import matplotlib as mp
import sqlalchemy as sa
import time
import logging
import sys
import os
# set the database connection string
db = "ADDRESS"
# set the location set version id that we will use
location_set_version_id = 38

prefix = ''
if os.name=='nt':
    prefix = 'J:'
elif os.name=='':
    prefix = 'FILEPATH'
    
def setLogger(fname):
    # get logger ready
    logger = logging.getLogger(__name__)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def queryToDF(query):
    engine = sa.create_engine(db)
    conn = engine.connect()

    # for a given source, get all the cause fractions from this round and last
    query = sa.text(query)

    # execute, store and close connection
    result = conn.execute(query) 
    conn.close()

    logger.info('FETCHING RESULT FOR QUERY')
    # convert the result to a DataFrame object
    df = pd.DataFrame(result.fetchall()) # rename your dataframe as desired
    if len(df)==0:
        logger.warn('NO DATA RETURNED FROM QUERY, FAILING OUT')
    df.columns = result.keys()
    assert(result.rowcount >0)
    logger.info('successfully queried the database')

    return df

def getIndiaPops():
    '''
    Returns a dataframe containing pop and deaths by year and location. Result has only all ages and both sexes.
    
    Parameters
    ----------
    location_filter: list of location_ids, optional. Filters result to only the given location_ids
    '''
    
    pop_query = '''
    SELECT
        o.year_id as year,
        o.location_id,
        o.sex_id as sex,
        o.mean_pop as pop,
        o.mean_env as env,
        loc.location_ascii_name as location_name
    FROM "ADDRESS:" o
    INNER JOIN "ADDRESS" ov
        ON o.output_version_id = ov.output_version_id and ov.is_best = 1
    INNER JOIN "ADDRESS" ag ON o.age_group_id = ag.age_group_id
    INNER JOIN "ADDRESS" lhh USING(location_id)
    INNER JOIN "ADDRESS" loc USING(location_id)
    WHERE 
        ((lhh.location_set_version_id = {lsvid} AND lhh.path_to_top_parent LIKE '%,163,%')
            OR lhh.location_id IN(4637, 4638))
        AND ag.age_group_id=22
        AND o.year_id BETWEEN 1970 and 2013
    '''.format(lsvid=location_set_version_id)

    return queryToDF(pop_query)

def getData(fname):
    '''
    GET THE RAW DATA
    
    will use only the CRS data to split by urbanicity
    '''
    raw = pd.read_stata(fname)
    # This was being read in as a float for some reason
    raw['location_id'] = raw.location_id.astype(int)
    assert(len(raw)>0)
    logger.info('successfully read data file')
    return raw

def describeData(raw):
    # DESCRIBE THE RAW DATA
    #
    # run if desired
    logger.info("Adult Age formats in dataset: " + str(raw.frmat.unique()))
    logger.info("Infant Age formats in dataset: " + str(raw.im_frmat.unique()))
    logger.info("Sex in dataset: " + str(raw[raw['year']<=1987].sex.unique()))
    logger.info("Number of deaths in dataset: " + str(raw.deaths1.sum()))
    logger.info("Locations in the dataset: " + str(len(raw[raw['year']<=1987].location_id.unique())))
    
def getUrbanicity(location_name):
    if "Urban" in location_name:
        return 1
    elif "Rural" in location_name:
        return 0
    else:
        return 3


# In[2]:

# GET THE MOST RECENTLY GENERATED URBAN RURAL WEIGHTS
#
# contains rates-per-1 of deaths by each cause, urbanicity, state
location_cause_weights = pd.read_csv("FILEPATH")



# In[3]:

# GET THE RAW DATA
#
# will use only the CRS data to split by urbanicity
logger = setLogger('')
data_location ="FILEPATH"
raw = getData(data_location)
pop = getIndiaPops()
locations = pop[['location_id', 'location_name']].drop_duplicates()


# In[4]:

# MERGE RAW WITH LOCATIONS
#
# merge location ids to location names
raw_with_loc = pd.merge(raw, locations, how='inner', on='location_id')
try:
    assert(len(raw)==len(raw_with_loc))
except AssertionError:
    logging.critical('FAILURE: The locations query did not return all the locations that are in the dataset.')

    assert(False)


# In[5]:

# STORE DATA FROM YEARS WITH URBAN RURAL SPLIT FOR LATER VERIFICATION STEPS
# filter years with urban rural split
# determine the urbanicity of each location
raw_with_loc['urbanicity'] = raw_with_loc.apply(lambda x: getUrbanicity(x['location_name']), axis=1)
# drop all-state observations
weight_input = raw_with_loc[raw_with_loc['urbanicity']!=3]
# state determined by the content of location_ascii_name
weight_input['location_name'] = weight_input.apply(lambda x: x['location_name'][0:x['location_name'].index(",")], axis=1)

weight_input.drop('urbanicity', axis=1, inplace=True)


# In[6]:

# PREPARE THE DATA TO SPLIT FOR MERGE WITH WEIGHTS
data_to_split = raw_with_loc[raw_with_loc['urbanicity']==3]
# location weights have state and location_id_state columns that we can merge on for the weights by urbanicity
data_to_split = data_to_split.rename(columns={'location_id': 'location_id_state'})
# get rid of urbanicity variable for now
data_to_split.drop('urbanicity', axis=1, inplace=True)


# In[7]:

# MERGE THE DATA WITH WEIGHTS
#
# notice that this uses a left join - that's because there are states that just weren't in the training data at all
# the next step will take those unmerged observations and force them to duplicate so that national weights can be multiplied
weights_merge_with_nulls = pd.merge(data_to_split, location_cause_weights, on=['location_id_state', 'acause', 'location_name'], how='left')


# In[8]:

# DUPLICATE THOSE OBSERVATIONS WITH NO URBAN RURAL IN TRAINING DATA SO THAT NATIONAL WEIGHTS CAN BE APPLIED
# THEN FILL THEM IN WITH NATIONAL WEIGHTS
#
# some of the states have no urban rural in the whole CRS dataset. Need to duplicate all the deaths observations
# so that there is an urban deaths observation and a rural deaths observation
urban_deaths = weights_merge_with_nulls[pd.isnull(weights_merge_with_nulls['weight'])]
rural_deaths = weights_merge_with_nulls[pd.isnull(weights_merge_with_nulls['weight'])]
urban_deaths['urbanicity'] = 1
rural_deaths['urbanicity'] = 0

urban_deaths['location_name'] = urban_deaths['location_name'] + ", Urban"
rural_deaths['location_name'] = rural_deaths['location_name'] + ", Rural"
fake_urban_rural = urban_deaths.append(rural_deaths)

# get urban/rural location_ids
fake_urban_rural_merge = pd.merge(fake_urban_rural, locations, on='location_name', how='inner')
assert len(fake_urban_rural_merge)==len(fake_urban_rural)
# now replace location_id_urban with the location_id from the merge
fake_urban_rural_merge['location_id_urban']= fake_urban_rural_merge['location_id']
fake_urban_rural_merge = fake_urban_rural_merge.drop(['location_id'], axis=1)
fake_urban_rural_merge['weight']=0

weights_merge = weights_merge_with_nulls[~pd.isnull(weights_merge_with_nulls['weight'])].append(fake_urban_rural_merge)


# In[9]:

# NOW MERGE THE WEIGHTED DATA WITH POPULATION
#
pop = pop.rename(columns={'location_id':'location_id_urban'})
weights_merge_pop = pd.merge(weights_merge, pop.drop('location_name', axis=1), how='inner', on=['location_id_urban', 'year', 'sex'])
# make sure that every observation got a population
try:
    assert(len(weights_merge)==len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: Some of the location-year-sexes did not get a population.')
    assert(False)


# In[10]:

# sum the total deaths and weights in each grouping 
weights_merge_pop['total_weight'] = weights_merge_pop.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).weight.transform('sum')
weights_merge_pop['total_death'] = weights_merge_pop.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).deaths1.transform('sum')

missing_weights = weights_merge_pop[(weights_merge_pop['total_weight']==0) & (weights_merge_pop['total_death']>0)]
nonmissing_weights =  weights_merge_pop[(weights_merge_pop['total_weight']>0) | (weights_merge_pop['total_death']==0)]

missing_weights['location_name']="India"
missing_weights = missing_weights.drop(['weight', 'total_weight'], axis=1)

national_weights = location_cause_weights[['location_name', 'acause', 'urbanicity', 'weight']]

missing_weights_merge = pd.merge(missing_weights, national_weights, on=['urbanicity', 'acause', 'location_name'], how='inner')


missing_weights_merge['location_name'] = missing_weights_merge['subdiv']
missing_weights_merge['total_weight'] = missing_weights_merge.groupby(
    ['location_id_state', 'acause', 'sex', 'year', 'cause']).weight.transform('sum')

# assert that there are no missing weights in the filled missing weights
try:
    assert(len(missing_weights_merge[(missing_weights_merge['total_weight']<=0) & (missing_weights_merge['total_death']>0)]))==0
except AssertionError:
    logging.critical('FAILURE: There are no weights for some datapoints;         we are about to lose deaths because they will be multiplied by a zero weight.')
    assert(False)


# In[11]:

#
# ready for split!
ready_to_split = nonmissing_weights.append(missing_weights_merge)
try:
    assert(len(ready_to_split) == len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: The dataset that we are about to split is not as long as expected based on the input dataset.')
    assert(False)


# In[12]:

ready_to_split['WN']=ready_to_split['weight']*ready_to_split['pop']


# In[13]:

# GENERATE K DENOMINATOR (the sum of the WNs for each grouping)
ready_to_split['Kdenom'] = ready_to_split.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).WN.transform('sum')


# In[14]:

# GENERATE K (the number of deaths divided by the K denominator)

ready_to_split['K'] = ready_to_split['deaths1']/ready_to_split['Kdenom']


# In[15]:

# SPLIT UP THE DEATHS!
# Multiply the total deaths adjuster (K) by the deaths estimate (WN)
ready_to_split['split_deaths'] = ready_to_split['K']*ready_to_split['WN']


# In[17]:

# COPY FOR VERIFICATION STEPS
split = ready_to_split.copy()
split.drop(['deaths1', 'deaths26'])
split['deaths1'] = split['split_deaths']
split['deaths26'] = split['split_deaths']

# In[21]:

# now append the years back together with their new urban rural divides
split_clean = split.rename(columns={'location_id_urban':'location_id'})
split_clean.ix[split_clean['urbanicity']==1,'subdiv2'] = ": Urban"
split_clean.ix[split_clean['urbanicity']==0,'subdiv2'] = ": Rural"
split_clean['subdiv'] = split_clean['subdiv'] + split_clean['subdiv2']
split_clean = split_clean.drop(
    ['total_death', 'total_weight', 'weight', 'location_id_state', 'subdiv2', 'WN', 'K', 'pop', 'Kdenom', 'split_deaths', 'urbanicity', 'env'], axis=1)
unsplit_clean = weight_input


# In[23]:

final_split = unsplit_clean.append(split_clean)


# In[25]:

deaths_raw = pd.pivot_table(raw, index=["acause", "sex", "year"], values=["deaths1"], aggfunc=sum)
deaths_final = pd.pivot_table(final_split, index=["acause", "sex", "year"], values=["deaths1"], aggfunc=sum)
sameness = abs(deaths_raw.fillna(0) - deaths_final.fillna(0)) <.0001


# In[28]:

timestamp = time.strftime("%m_%d_%y_%H_%M")
out_dir = "FILEPATH"
archive_path_s = "FILEPATH"
archive_path_f = "FILEPATH"
sameness.to_csv("FILEPATH")
sameness.to_csv("FILEPATH")
final_split.to_csv("FILEPATH")
final_split.to_csv("FILEPATH")


# In[29]: