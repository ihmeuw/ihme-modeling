
import pandas as pd
import numpy as np
import matplotlib as mp
import sqlalchemy as sa # comes with anaconda
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
elif os.name=='posix':
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
    
    Requires a global variable location_set_version_id that determines the location_set to return populations for.
        Note: overrides the location_set_version_id to include India, rural and India, urban.
    '''
    
    pop_query = '''
    SELECT
        o.year_id as year,
        o.location_id,
        o.mean_pop as pop,
        o.mean_env as env,
        loc.location_ascii_name as location_name
    FROM "ADDRESS" o
    INNER JOIN "ADDRESS" ov
        ON o.output_version_id = ov.output_version_id and ov.is_best = 1
    INNER JOIN "ADDRESS" ag ON o.age_group_id = ag.age_group_id
    INNER JOIN "ADDRESS" lhh USING(location_id)
    INNER JOIN "ADDRESS" loc USING(location_id)
    WHERE 
        ((lhh.location_set_version_id = {lsvid} AND lhh.path_to_top_parent LIKE '%,163,%')
            OR lhh.location_id IN(4637, 4638))
        AND ag.age_group_id=22
        AND o.sex_id=3
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
    

def prepTrainingData():
    '''
        uses two dataframes assumed to be global: raw and pop
        
        separates urban rural data from all data, collapses deaths and pops to location/urbanicity pairs
    '''
    # this gets us pop and location_name
    with_pops = pd.merge(raw, pop, how='inner', on=['location_id', 'year'])

    if not len(with_pops)==len(raw):
        no_pops = set(raw.location_id.unique()) - set(pop.location_id.unique())
        logger.critical("NOT ALL LOCATIONS IN RAW HAVE POPULATIONS: {locations}".format(locations=no_pops))
        sys.exit()
        
    # determine the urbanicity of each location
    with_pops['urbanicity'] = with_pops.apply(lambda x: getUrbanicity(x['location_name']), axis=1)
    # drop all-state observations
    weight_input = with_pops[with_pops['urbanicity']!=3]
    
    # check that input years aren't outside of what's expected at time of writing this code
    input_years = set(weight_input.year.unique())
    if set(input_years) != set([1987, 1986, 1984, 1983]):
        logger.warn("GOT UNEXPECTED YEARS IN INPUT DATA: {yrs}".format(yrs=input_years))
        
    # get the state name without the urban/rural designation
    weight_input['state'] = weight_input.apply(lambda x: x['location_name'][0:x['location_name'].index(",")], axis=1)
    
    # COLLAPSE POP AND DEATHS SEPARATELY THEN MERGE
    # For both, make a national/urban rural set of observations as well for calculating national weights by urbanicity
    
    # POPULATIONS 
    # the pop for calculating rates should be summed population of all the location_id-years in the input data
    # will later merge this onto the collapsed deaths
    pop_in_data = weight_input[['location_id', 'year', 'pop', 'urbanicity']].drop_duplicates()
    pop_state_urb = pop_in_data[['location_id', 'urbanicity', 'pop']].groupby(['location_id', 'urbanicity']).agg('sum').reset_index()
    pop_nat_urb = pop_in_data[['urbanicity', 'pop']].groupby('urbanicity').agg('sum').reset_index()
    # 4637 is the location_id for national rural India, 4638 is location_id for national urban India
    pop_nat_urb['location_id'] = pop_nat_urb.apply(lambda x: 4637 if x['urbanicity']==0 else 4638, axis=1)
    pop_for_rates = pop_state_urb.append(pop_nat_urb)
    
    # DEATHS
    deaths_state_urb = pd.pivot_table(weight_input, 
                             index=['state', 'location_id', 'urbanicity', 'acause'],
                             values=['deaths1'],
                             aggfunc=np.sum).reset_index()
    deaths_nat_urb = pd.pivot_table(weight_input,
                                  index=['urbanicity', 'acause'],
                                  values=['deaths1'],
                                  aggfunc=np.sum).reset_index()
     # 4637 is the location_id for national rural India, 4638 is location_id for national urban India
    deaths_nat_urb['location_id'] = deaths_nat_urb.apply(lambda x: 4637 if x['urbanicity']==0 else 4638, axis=1)
    deaths_nat_urb['state']='India'
    deaths_for_rates = deaths_state_urb.append(deaths_nat_urb)
    
    weight_data = pd.merge(deaths_for_rates, pop_for_rates, on=['location_id', 'urbanicity'], how='inner')
    assert len(weight_data)==len(deaths_for_rates)
    return weight_data
    
    

# do everything here
logger = setLogger('')
pop = getIndiaPops()
data_location = "FILEPATH"
raw = getData(data_location)
weight_data = prepTrainingData()

# ACTUAL WEIGHTS GENERATED HERE
#  (weights are defined by the rate of summed deaths to summed population)
#
# now generate a column that is the sum of deaths by state-acause, and divide the value of deaths in the state-urbanicity-acause into that
weight_data['weight'] = weight_data.apply(lambda x: (x['deaths1']*1.0)/x['pop'], axis=1)

# SLICE AND DICE TO MAKE IT NICE
# make a copy and grab relevant columns
location_cause_weights = weight_data.copy()[['state', 'location_id', 'acause', 'weight', 'urbanicity']]
# rename the state to location_ascii_name temporarily so we can get the state location ids too
location_cause_weights.rename(columns = {'state': 'location_name', 'location_id':'location_id_urban'}, inplace=True)
# use the locations map (location_id and location_ascii_name in there) to get the state location_id
location_cause_weights = pd.merge(location_cause_weights, pop[['location_id', 'location_name']].drop_duplicates(), 
                                  how='left', on='location_name')
# Just hardcode the national observation's location_id
location_cause_weights.ix[location_cause_weights['location_name']=='India', 'location_id']=163
# now we have the location_id of the state, its urbanicity, its urban/rural location id, and its state id. Should be the most
# helpful setup.
location_cause_weights.rename(columns = {'location_id':'location_id_state', 'location_ascii_name': 'state'}, inplace=True)

# archive old weights and save new ones
time_string = time.strftime("%m_%d_%y_%H_%M")
weights_dir = "FILEPATH"
archive_path = "FILEPATH"
location_cause_weights.to_csv("FILEPATH")
location_cause_weights.to_csv("FILEPATH")
