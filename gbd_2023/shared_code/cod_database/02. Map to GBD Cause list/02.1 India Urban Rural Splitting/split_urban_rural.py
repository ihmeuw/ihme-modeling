import pandas as pd
import numpy as np
import matplotlib as mp
import sqlalchemy as sa
import time
import logging
import sys
import os
db = "ADDRESS"
location_set_version_id = 38

prefix = ''
if os.name=='nt':
    prefix = 'J:'
elif os.name=='posix':
    prefix = 'FILEPATH'
    
def setLogger(fname):
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

    query = sa.text(query)

    result = conn.execute(query) 
    conn.close()

    logger.info('FETCHING RESULT FOR QUERY')
    df = pd.DataFrame(result.fetchall())
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
    raw['location_id'] = raw.location_id.astype(int)
    assert(len(raw)>0)
    logger.info('successfully read data file')
    return raw

def describeData(raw):
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

location_cause_weights = pd.read_csv("{prefix}FILEPATH".format(prefix=prefix))

logger = setLogger('')
data_location = "{prefix}FILEPATH".format(prefix=prefix)
raw = getData(data_location)
pop = getIndiaPops()
locations = pop[['location_id', 'location_name']].drop_duplicates()

raw_with_loc = pd.merge(raw, locations, how='inner', on='location_id')
try:
    assert(len(raw)==len(raw_with_loc))
except AssertionError:
    logging.critical('FAILURE: The locations query did not return all the locations that are in the dataset.')
    assert(False)

raw_with_loc['urbanicity'] = raw_with_loc.apply(lambda x: getUrbanicity(x['location_name']), axis=1)
weight_input = raw_with_loc[raw_with_loc['urbanicity']!=3]
weight_input['location_name'] = weight_input.apply(lambda x: x['location_name'][0:x['location_name'].index(",")], axis=1)
weight_input.drop('urbanicity', axis=1, inplace=True)

data_to_split = raw_with_loc[raw_with_loc['urbanicity']==3]
data_to_split = data_to_split.rename(columns={'location_id': 'location_id_state'})
data_to_split.drop('urbanicity', axis=1, inplace=True)

weights_merge_with_nulls = pd.merge(data_to_split, location_cause_weights, on=['location_id_state', 'acause', 'location_name'], how='left')

urban_deaths = weights_merge_with_nulls[pd.isnull(weights_merge_with_nulls['weight'])]
rural_deaths = weights_merge_with_nulls[pd.isnull(weights_merge_with_nulls['weight'])]
urban_deaths['urbanicity'] = 1
rural_deaths['urbanicity'] = 0

urban_deaths['location_name'] = urban_deaths['location_name'] + ", Urban"
rural_deaths['location_name'] = rural_deaths['location_name'] + ", Rural"
fake_urban_rural = urban_deaths.append(rural_deaths)

fake_urban_rural_merge = pd.merge(fake_urban_rural, locations, on='location_name', how='inner')
assert len(fake_urban_rural_merge)==len(fake_urban_rural)
fake_urban_rural_merge['location_id_urban']= fake_urban_rural_merge['location_id']
fake_urban_rural_merge = fake_urban_rural_merge.drop(['location_id'], axis=1)
fake_urban_rural_merge['weight']=0

weights_merge = weights_merge_with_nulls[~pd.isnull(weights_merge_with_nulls['weight'])].append(fake_urban_rural_merge)

pop = pop.rename(columns={'location_id':'location_id_urban'})
weights_merge_pop = pd.merge(weights_merge, pop.drop('location_name', axis=1), how='inner', on=['location_id_urban', 'year', 'sex'])
try:
    assert(len(weights_merge)==len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: Some of the location-year-sexes did not get a population.')
    assert(False)

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

try:
    assert(len(missing_weights_merge[(missing_weights_merge['total_weight']<=0) & (missing_weights_merge['total_death']>0)]))==0
except AssertionError:
    logging.critical('FAILURE: There are no weights for some datapoints;         we are about to lose deaths because they will be multiplied by a zero weight.')
    assert(False)

ready_to_split = nonmissing_weights.append(missing_weights_merge)
try:
    assert(len(ready_to_split) == len(weights_merge_pop))
except AssertionError:
    logging.critical('FAILURE: The dataset that we are about to split is not as long as expected based on the input dataset.')
    assert(False)

ready_to_split['WN']=ready_to_split['weight']*ready_to_split['pop']

ready_to_split['Kdenom'] = ready_to_split.groupby(['location_id_state', 'acause', 'sex', 'year', 'cause']).WN.transform('sum')

ready_to_split['K'] = ready_to_split['deaths1']/ready_to_split['Kdenom']

ready_to_split['split_deaths'] = ready_to_split['K']*ready_to_split['WN']

split = ready_to_split.copy()
split.drop(['deaths1', 'deaths26'])
split['deaths1'] = split['split_deaths']
split['deaths26'] = split['split_deaths']

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
out_dir = "{prefix}FILEPATH".format(prefix=prefix)
archive_path_s = "{out_dir}FILEPATH".format(out_dir=out_dir, ts=timestamp)
archive_path_f = "{out_dir}FILEPATH".format(out_dir=out_dir, ts=timestamp)
sameness.to_csv("{out_dir}FILEPATH".format(out_dir=out_dir), index=True)
sameness.to_csv(archive_path_s, index=True)
final_split.to_csv("{out_dir}FILEPATH".format(out_dir=out_dir), index=False)
final_split.to_csv(archive_path_f, index=False)
