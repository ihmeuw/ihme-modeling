"""
Purpose: Calculates the percentage of maternal deaths that move to hiv for CoD.
"""

# imports
import pandas as pd, numpy as np
import sys
import os
from datetime import date, datetime
import sqlalchemy as sa

def queryToDF(query, host='strHost', db='', user='strUsername', pwd='strPassword', select=True):
    if db=='':
        conn_string = "mysql://{user}:{pwd}@{host}.strHost".format(user=user, pwd=pwd, host=host)
    else:
        conn_string = "mysql://{user}:{pwd}@{host}.strHost{db}".format(user=user, pwd=pwd, host=host, db=db)
    engine = sa.create_engine(conn_string)
    conn = engine.connect()

    # for a given source, get all the cause fractions from this round and last
    query = sa.text(query)

    # execute, store and close connection
    result = conn.execute(query) 

    if not select:
        conn.close()
        return result
        
    #logger.info('FETCHING RESULT FOR QUERY')
    # convert the result to a DataFrame object
    df = pd.DataFrame(result.fetchall()) # rename your dataframe as desired
    conn.close()
    if len(df)==0:
        return pd.DataFrame()
        #logger.warn('NO DATA RETURNED FROM QUERY, FAILING OUT')
    df.columns = result.keys()
    assert(result.rowcount >0)
    #logger.info('successfully queried the database')

    return df

def get_most_recent_date(paf_dir):
    """Return the name of the pafs file that is most recent."""
    fdict = {}
    for filename in os.listdir(paf_dir):
        d = filename.split('_')
        dt = datetime(year=int(d[0]), month=int(d[1]),
                      day=int(d[2]), hour=int(d[3]))
        fdict[dt] = filename
    ord_dates = fdict.keys()
    ord_dates.sort()
    # most recent file
    paf_fname = fdict[ord_dates[-1]]
    return paf_fname

def get_locations(location_set_id=35):
    """Return the locations for the best locations in the passed location_set"""
    locations = queryToDF('''
        SELECT *
        FROM shared.location_hierarchy_history
        WHERE location_set_version_id IN(
                SELECT max(location_set_version_id) AS location_set_version_id
                FROM shared.location_set_version
                WHERE location_set_id={lsid}
                GROUP BY location_set_id
            )
    '''.format(lsid=location_set_id))
    return locations

def get_subnationals_to_aggregate(locations, agg_list):
    """Return the subnationals for a list of countries that are estimates,
        and adds the location_id of the eventual aggregate, called 'agg_id'
    """
    # get the country id for each location
    locations.loc[:, 'agg_id'] = locations.loc[:, 'path_to_top_parent'].apply(
        lambda x: int(x.split(',')[3]) if len(x.split(','))>=4 else np.NaN)
    # get the list of country ids that are in the passed country_list
    agg_ids = list(locations.ix[locations.loc[:, 'ihme_loc_id'].isin(agg_list), 'location_id'])
    # filter to the locations that are in the country id list
    sub_locations = locations.ix[(locations['agg_id'].isin(agg_ids))&(locations['is_estimate']==1),]
    return sub_locations

def collapse_subnationals(pafs, subnationals):
    """Collapse the pafs to national level using the subnational locations 
    returned from get_subnationals_to_aggregate. Gets the mean of the subnationals for each draw."""
    m = pd.merge(pafs, subnationals, on='location_id', how='inner')
    m.loc[:, 'location_id'] = m.loc[:, 'agg_id']
    draw_cols = [col for col in m.columns if 'draw' in col]
    aggregated = m.groupby(['age_group_id', 'location_id', 'year'])[draw_cols].mean().reset_index()
    return aggregated

def read_paf_data(year):
    """Return PAFS data for given year."""
    # set the folder that we will pull pafs from
    paf_dir = '/ihme/centralcomp/maternal_mortality/pafs/'
    paf_fname = get_most_recent_date(paf_dir)

    # read pafs
    pafs = pd.read_hdf('{pd}/{pf}/pafs_{y}.h5'.format(pd=paf_dir,
                                                      pf=paf_fname,
                                                      y=year),
                       'pafs')
    return pafs

def clean_paf_data(pafs):
    """Make national aggregates and set index."""
    # create national pafs for certain locations
    locations = get_locations()
    country_list = ['KEN', 'ZAF', 'BRA', 'IND']
    subnationals = get_subnationals_to_aggregate(locations, country_list)[['location_id', 'agg_id']]
    aggregates = collapse_subnationals(pafs, subnationals)
    pafs = pafs.append(aggregates)

    # index so that draw columns are all that is left
    pafs = pafs.set_index(['age_group_id', 'location_id', 'year'])
    pafs = pafs[[col for col in pafs.columns if 'draw_' in col]]
    return pafs

def calc_props(pafs, prop_maternal):
    """Calculate the proportions needed 
    for CoD processing and return."""
    # calculate percent to hiv as PAF(1-prop_maternal)
    to_hiv = pafs*(1-prop_maternal)
    to_hiv['pct_hiv'] = to_hiv.mean(axis=1)
    to_hiv = to_hiv['pct_hiv'].reset_index()
    # calculate percent to maternal_hiv as PAF(prop_maternal)
    to_mat_hiv = pafs*(prop_maternal)
    to_mat_hiv['pct_maternal_hiv'] = to_mat_hiv.mean(axis=1)
    to_mat_hiv = to_mat_hiv['pct_maternal_hiv'].reset_index()
    # calculate percent to maternal_hiv for VR as
    # PAF(prop_maternal)/(1-PAF(prop_maternal))
    to_mat_hiv_vr = pafs*prop_maternal/(1-pafs*prop_maternal)
    to_mat_hiv_vr['pct_maternal_hiv_vr'] = to_mat_hiv_vr.mean(axis=1)
    to_mat_hiv_vr = to_mat_hiv_vr['pct_maternal_hiv_vr'].reset_index()

    # merge to make proportions
    props = pd.merge(to_hiv, to_mat_hiv,
                     on=['age_group_id', 'location_id', 'year'],
                     how='inner')
    props = pd.merge(props, to_mat_hiv_vr,
                     on=['age_group_id', 'location_id', 'year'],
                     how='inner')
    # sometimes pafs are missing; in this case, move nothing to hiv
    props = props.fillna(0)
    return props

def write_maternal_hiv_props(year):
    """Write maternal hiv props, 
    used as the model for the maternal_hiv proportion in dismod."""
    # directory upon which save results will be called
    out_dir = '/ihme/cod/prep/01_database/maternal_hiv_props/9015'
    # modelable_entity_id
    modelable_entity_id=9015
     # set the proportion of hiv+ deaths that are actually maternal deaths; this
    # number comes from a meta-analysis that is not
    # centrally stored.
    prop_maternal = .13/1.13

    # read the data for the year
    pafs = read_paf_data(year)

    # clean it up for calculating proportions
    pafs = clean_paf_data(pafs)

    props = (prop_maternal*pafs).reset_index()

    draw_cols = [col for col in props.columns if 'draw' in col]
    locations = props.location_id.unique()
    i=0
    num_locs = len(locations)
    # a tenth of the number locations; print status update every
    # additional ten percent
    a_tenth = num_locs/10
    print 'Writing {n} locations for {y}...'.format(n=num_locs, y=year)
    for loc in locations:
        ld = props.query('location_id==@loc')
        ld = ld[['age_group_id']+draw_cols]
        ld.to_csv('{od}/{measure}_{loc}_{yr}_{s}.csv'.format(
                                                    od=out_dir,
                                                    measure=18,
                                                    loc=int(loc),
                                                    yr=year,
                                                    s=2
                                                        ))
        if i%a_tenth==0:
            sys.stdout.write('\r')
            sys.stdout.write('[%-10s] %d%%' % ("="*(i/a_tenth), 10*(i/a_tenth)))
            sys.stdout.flush()
        i=i+1
    print '\n...Done'


def main(year):
    # set the proportion of hiv+ deaths that are actually maternal deaths; this
    # number comes from a meta-analysis 
    prop_maternal = .13/1.13

    # read the data for the year
    pafs = read_paf_data(year)

    # clean it up for calculating proportions
    pafs = clean_paf_data(pafs)

    props = calc_props(pafs, prop_maternal)

    # save
    props.to_csv(('/ihme/cod/prep/01_database/maternal_hiv_props/' +
                 'maternal_hiv_props_{y}.csv').format(y=year),
                 index=False)

if __name__=="__main__":

    # set year_id
    year = sys.argv[1]
    #years = range(1980,2016)
    main(year)
    #for year in years:
    #    write_maternal_hiv_props(year)
