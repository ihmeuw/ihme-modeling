## Purpose: The India CRS data needs to be split into urban/rural locations ##

# coding: utf-8

import pandas as pd
import numpy as np
import matplotlib as mp
import sqlalchemy as sa # comes with anaconda
import time
import logging
import sys
import os
# set the database connection string
db = "strConnction//strUser:strPassword@strHost:strPort"
# set the location set version id that we will use
location_set_version_id = 38

prefix = ''
if os.name=='nt':
    prefix = 'J:'
elif os.name=='posix':
    prefix = '/home/j'
    
def setLogger(fname):
    # get logger ready
    logger = logging.getLogger(__name__)
    #logger.setLevel(logging.INFO)
    # set logging format
    #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # create the logging file handler
    #handler = logging.FileHandler(fname)
    #handler.setLevel(logging.INFO)
    #handler.setFormatter(formatter)
    #logger.addHandler(handler)
    # also log to stdout
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
    
    Requires a global variable location_set_version_id that determines the location_set to return populations for.
        Note: overrides the location_set_version_id to include India, rural and India, urban.
    '''
    
    pop_query = '''
    SELECT
        o.year_id as year,
        o.location_id,
        o.sex_id as sex,
        o.mean_pop as pop,
        o.mean_env as env,
        loc.location_ascii_name as location_name
    FROM mortality.output o
    INNER JOIN mortality.output_version ov
        ON o.output_version_id = ov.output_version_id and ov.is_best = 1
    INNER JOIN shared.age_group ag ON o.age_group_id = ag.age_group_id
    INNER JOIN shared.location_hierarchy_history lhh USING(location_id)
    INNER JOIN shared.location loc USING(location_id)
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



# GET THE MOST RECENTLY GENERATED URBAN RURAL WEIGHTS
#
# contains rates-per-1 of deaths by each cause, urbanicity, state
location_cause_weights = pd.read_csv("{prefix}/WORK/03_cod/01_database/02_programs/urban_rural_splitting/weights/urban_rural_weights.csv".format(prefix=prefix))
#location_cause_weights = location_cause_weights.rename(columns={'location_name':'state'})


# GET THE RAW DATA
#
# will use only the CRS data to split by urbanicity
logger = setLogger('')
data_location = "{prefix}/WORK/03_cod/01_database/03_datasets/India_CRS/data/intermediate/01_mapped.dta".format(prefix=prefix)
raw = getData(data_location)
pop = getIndiaPops()
locations = pop[['location_id', 'location_name']].drop_duplicates()

# MERGE RAW WITH LOCATIONS
#
# merge location ids to location names
raw_with_loc = pd.merge(raw, locations, how='inner', on='location_id')
# make sure we didn't lose any data from that
try:
    assert(len(raw)==len(raw_with_loc))
except AssertionError:
    logging.critical('FAILURE: The locations query did not return all the locations that are in the dataset.')
    # ok, now that it's been logged, fail out.
    assert(False)


# STORE DATA FROM YEARS WITH URBAN RURAL SPLIT FOR LATER VERIFICATION STEPS
# filter years with urban rural split
# determine the urbanicity of each location
raw_with_loc['urbanicity'] = raw_with_loc.apply(lambda x: getUrbanicity(x['location_name']), axis=1)
# drop all-state observations
weight_input = raw_with_loc[raw_with_loc['urbanicity']!=3]
# state determined by the content of location_ascii_name
weight_input['location_name'] = weight_input.apply(lambda x: x['location_name'][0:x['location_name'].index(",")], axis=1)
# get rid of urbanicity variable for now
weight_input.drop('urbanicity', axis=1, inplace=True)


# PREPARE THE DATA TO SPLIT FOR MERGE WITH WEIGHTS
# these are the years to split (Note: a generalized program would learn this as opposed to having it hard-coded)
data_to_split = raw_with_loc[raw_with_loc['urbanicity']==3]
# location weights have state and location_id_state columns that we can merge on for the weights by urbanicity
data_to_split = data_to_split.rename(columns={'location_id': 'location_id_state'})
# get rid of urbanicity variable for now
data_to_split.drop('urbanicity', axis=1, inplace=True)


# MERGE THE DATA WITH WEIGHTS
#
# notice that this uses a left join - that's because there are states that just weren't in the training data at all
# the next step will take those unmerged observations and force them to duplicate so that national weights can be multiplied
weights_merge_with_nulls = pd.merge(data_to_split, location_cause_weights, on=['location_id_state', 'acause', 'location_name'], how='left')


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
#pd.pivot_table(weights_merge[pd.isnull(weights_merge['weight'])], index=["year"], values=["deaths1"], aggfunc=np.sum)
#weights_merge = weights_merge[['weight', '].fillna(0)


# NOW MERGE THE WEIGHTED DATA WITH POPULATION
#
# couldn't add populations earlier because population values are urban/rural specific
pop = pop.rename(columns={'location_id':'location_id_urban'})
weights_merge_pop = pd.merge(weights_merge, pop.drop('location_name', axis=1), how='inner', on=['location_id_urban', 'year', 'sex'])
# make sure that every observation got a population
try:
    assert(len(weights_merge)==len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: Some of the location-year-sexes did not get a population.')
    # ok, now that it's been logged, fail out.
    assert(False)


# MAKE SURE THAT THERE ARE NO MISSING WEIGHTS WHERE THERE ARE DEATHS IN THE TO-SPLIT DATA; FILL WITH NATIONAL WHERE NEEDED
#  (This is possible if there is a cause that has no deaths in the training data but deaths in the to-split data)
#  In the case that the weights are missing, fill in national level weights

# sum the total deaths and weights in each grouping 
weights_merge_pop['total_weight'] = weights_merge_pop.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).weight.transform('sum')
weights_merge_pop['total_death'] = weights_merge_pop.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).deaths1.transform('sum')

# count the number of state-acause-sex-year-cause groups that are at 100%, at 0, or above 0 and not 100%
missing_weights = weights_merge_pop[(weights_merge_pop['total_weight']==0) & (weights_merge_pop['total_death']>0)]
nonmissing_weights =  weights_merge_pop[(weights_merge_pop['total_weight']>0) | (weights_merge_pop['total_death']==0)]
#bad_weights = weights_merge[(weights_merge['total_weight']!=0) & (weights_merge['total_weight']!=1)]

# fill in missing weights with the national level weights
missing_weights['location_name']="India"
missing_weights = missing_weights.drop(['weight', 'total_weight'], axis=1)
# make a copy of location_cause_weights without location_id_state because it isn't necessary to 
# merge correctly and I want to maintain the correct location_id_state for the states with missing weights
national_weights = location_cause_weights[['location_name', 'acause', 'urbanicity', 'weight']]
# this merge depends on an observation existing in location_cause_weights that has the 'state' equal to 'India'
# the merge is an inner merge, so it will only add the India observation
missing_weights_merge = pd.merge(missing_weights, national_weights, on=['urbanicity', 'acause', 'location_name'], how='inner')

# now bring back values that make sense for append (make state equal to the actual state, not India)
missing_weights_merge['location_name'] = missing_weights_merge['subdiv']
missing_weights_merge['total_weight'] = missing_weights_merge.groupby(
    ['location_id_state', 'acause', 'sex', 'year', 'cause']).weight.transform('sum')

# assert that there are no missing weights in the filled missing weights
try:
    assert(len(missing_weights_merge[(missing_weights_merge['total_weight']<=0) & (missing_weights_merge['total_death']>0)]))==0
except AssertionError:
    logging.critical('FAILURE: There are no weights for some datapoints;         we are about to lose deaths because they will be multiplied by a zero weight.')
    # ok, now that it's been logged, fail out.
    assert(False)



# APPEND THE NATIONAL-FILLED WEIGHTS TO THE GOOD ONES
#
# ready for split!
ready_to_split = nonmissing_weights.append(missing_weights_merge)
# assure ourselves that we didn't just lose any data from adding national weights
try:
    assert(len(ready_to_split) == len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: The dataset that we are about to split is not as long as expected based on the input dataset.')
    # ok, now that it's been logged, fail out.
    assert(False)



# GENERATE WN= WEIGHT*POPULATION (what the estimated deaths should be based on weights and population)
#   (I call it WN to stick to naming convention in age sex splitting)
ready_to_split['WN']=ready_to_split['weight']*ready_to_split['pop']



# GENERATE K DENOMINATOR (the sum of the WNs for each grouping)
ready_to_split['Kdenom'] = ready_to_split.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).WN.transform('sum')



# GENERATE K (the number of deaths divided by the K denominator)
# This is the real deaths over the number of deaths estimated by the weights for the state-acause-sex-year-cause
# K is functionally used to adjust down the number of estimated deaths from the death rates to the number of deaths in the source
ready_to_split['K'] = ready_to_split['deaths1']/ready_to_split['Kdenom']


# SPLIT UP THE DEATHS!
# Multiply the total deaths adjuster (K) by the deaths estimate (WN)
ready_to_split['split_deaths'] = ready_to_split['K']*ready_to_split['WN']


# Show a table view that makes some sense of what just happened
# Multiplying K by WN should make the total deaths in the split source equal to the total in the raw
#ready_to_split[['acause', 'cause', 'location_name', 'sex', 'year','deaths1', 'weight', 'pop', 'WN', 'K', 'Kdenom', 'split_deaths', 'urbanicity']].sort(
#    ['location_name', 'acause', 'sex', 'year', 'cause']).tail(20)



# COPY FOR VERIFICATION STEPS
split = ready_to_split.copy()
split.drop(['deaths1', 'deaths26'])
split['deaths1'] = split['split_deaths']
split['deaths26'] = split['split_deaths']


# # VERIFY THAT WE DIDN'T JUST BREAK THE YEARLY DEATHS TOTALS
# deaths_by_year_split = pd.pivot_table(split, index=["year"], values=["deaths1", "deaths26", "deaths2"], aggfunc='sum')
# deaths_by_year_raw = pd.pivot_table(data_to_split, index=["year"], values=["deaths1", "deaths26", "deaths2"], aggfunc='sum')
# abs(deaths_by_year_split.fillna(0) - deaths_by_year_raw) <.0001


# # DO AN EVEN BETTER VERIFICATION - THAT ALL CAUSE_YEAR_SEX ARE STILL THE SAME
# deaths_by_cause_year_sex1 = pd.pivot_table(split, index=["cause", "sex"], columns=["year"], values=["deaths1", "deaths26"], aggfunc='sum')
# deaths_by_cause_year_sex2 = pd.pivot_table(data_to_split, index=["cause", "sex"], columns=["year"], values=["deaths1", "deaths26"], aggfunc='sum')
# # compare the total deaths in each cause/sex/year before and after weights were applied. Ensure no difference
# # Minute precision differences and NaN values will fail this so adjust for that; everything should read 'True'
# abs(deaths_by_cause_year_sex1.fillna(0) - deaths_by_cause_year_sex2.fillna(0)) <.0001


# now append the years back together with their new urban rural divides
#weight_input_comp = weight_input[['iso3', 'subdiv', 'location_id', 'sex', 'year', 'acause', 'cause', 'deaths1']]
split_clean = split.rename(columns={'location_id_urban':'location_id'})
split_clean.ix[split_clean['urbanicity']==1,'subdiv2'] = ": Urban"
split_clean.ix[split_clean['urbanicity']==0,'subdiv2'] = ": Rural"
split_clean['subdiv'] = split_clean['subdiv'] + split_clean['subdiv2']
split_clean = split_clean.drop(
    ['total_death', 'total_weight', 'weight', 'location_id_state', 'subdiv2', 'WN', 'K', 'pop', 'Kdenom', 'split_deaths', 'urbanicity', 'env'], axis=1)
unsplit_clean = weight_input

final_split = unsplit_clean.append(split_clean)

deaths_raw = pd.pivot_table(raw, index=["acause", "sex", "year"], values=["deaths1"], aggfunc=sum)
deaths_final = pd.pivot_table(final_split, index=["acause", "sex", "year"], values=["deaths1"], aggfunc=sum)
sameness = abs(deaths_raw.fillna(0) - deaths_final.fillna(0)) <.0001


timestamp = time.strftime("%m_%d_%y_%H_%M")
out_dir = "{prefix}/WORK/03_cod/01_database/02_programs/urban_rural_splitting".format(prefix=prefix)
archive_path_s = "{out_dir}/diagnostics/_archive/urban_rural_split_deaths_check_{ts}.csv".format(out_dir=out_dir, ts=timestamp)
archive_path_f = "{out_dir}/outputs/_archive/urban_rural_split_deaths_{ts}.csv".format(out_dir=out_dir, ts=timestamp)
sameness.to_csv("{out_dir}/diagnostics/urban_rural_split_deaths_check.csv".format(out_dir=out_dir), index=True)
sameness.to_csv(archive_path_s, index=True)
final_split.to_csv("{out_dir}/outputs/urban_rural_split_deaths.csv".format(out_dir=out_dir), index=False)
final_split.to_csv(archive_path_f, index=False)




