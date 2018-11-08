import numpy as np
import pandas as pd
import os
# Imports (from GBD)
from db_queries import get_location_metadata as get_meta


def get_regions_dict(location_set_id=21, gbd_round_id=5):
    '''
    Generate a dictionary that maps the region ID associated with each GBD
    location ID.
    '''
    regions_df = (get_meta(location_set_id=location_set_id,
                           gbd_round_id=gbd_round_id)
                    .loc[:,['location_id','region_id']])
    regions_dict = dict(zip(regions_df['location_id'],regions_df['region_id']))
    return regions_dict


def fill_war_cis(war_db):
    '''
    Fill any missing confidence intervals for war data.
    '''
    # Use the UCDP regional-level confidence intervals to generate CIs for other
    #  types of sources
    ci_template = war_db.loc[(war_db['dataset']=="UCDP") & 
                             (war_db['best'].notnull()) & 
                             (war_db['high'].notnull()) & 
                             (war_db['low'].notnull()),
                             ['location_id','best','low','high']]
    ci_template = ci_template.loc[(ci_template['low'] < ci_template['best']) & (ci_template['high'] > ci_template['best'])]
    # Get regions for each of the location IDs listed
    regions_dict = get_regions_dict()
    ci_template['region_id'] = ci_template['location_id'].map(regions_dict)
    war_db['region_id'] = war_db['location_id'].map(regions_dict)
    # Aggregate CI template by region and find average ratios of high-best and 
    #  low-best for each region
    # ci_template['hi_best_ratio'] = ci_template['high'] / ci_template['best']
    # ci_template['lo_best_ratio'] = ci_template['low'] / ci_template['best']
    region_avg = (ci_template.loc[:,['region_id','best','high','low']]
                            .groupby(by=['region_id'])
                            .mean()
                            .reset_index(drop=False))
    region_avg['hi_best_ratio'] = region_avg['high'] / region_avg['best']
    region_avg['lo_best_ratio'] = region_avg['low'] / region_avg['best']
    region_avg = region_avg[['region_id', 'hi_best_ratio', 'lo_best_ratio']]
    # Merge onto the entire dataset and fill any areas with missing high and
    #  low values
    war_db = pd.merge(left=war_db,
                      right=region_avg,
                      on=['region_id'],
                      how='left')
    war_db['high_est'] = war_db['best'] * war_db['hi_best_ratio']
    war_db['low_est'] = war_db['best'] * war_db['lo_best_ratio']
    war_db.loc[war_db['low'].isnull(),
               'low'] = war_db.loc[war_db['low'].isnull(),'low_est']
    war_db.loc[war_db['high'].isnull(),
               'high'] = war_db.loc[war_db['high'].isnull(),'high_est']
    # Drop extra columns
    war_db = war_db.drop(labels=['low_est','high_est','hi_best_ratio','lo_best_ratio'],
                         axis=1,errors='ignore')
    # Return both the filled dataset and the regional average standard
    return (war_db, region_avg)

def fill_disaster_cis(disaster_db, cis):
    '''
    Fill any missing confidence intervals for disaster data.
    '''
    # Get information about the region ID associated with each location ID
    regions_dict = get_regions_dict()
    disaster_db['region_id'] = disaster_db['location_id'].map(regions_dict)
    # Merge the confidence interval template on the disaster DB data and use it
    #  to generate upper and lower confidence intervals for rows without
    #  current high and low estimates.
    disaster_db = pd.merge(left=disaster_db,
                           right=cis,
                           on=['region_id'],
                           how='left')
    disaster_db['high_est'] = disaster_db['best'] * disaster_db['hi_best_ratio']
    disaster_db['low_est'] = disaster_db['best'] * disaster_db['lo_best_ratio']
    disaster_db.loc[disaster_db['low'].isnull(),
                    'low'] = disaster_db.loc[disaster_db['low'].isnull(),'low_est']
    disaster_db.loc[disaster_db['high'].isnull(),
                    'high'] = disaster_db.loc[disaster_db['high'].isnull(),'high_est']
    # Drop extra columns
    disaster_db = disaster_db.drop(labels=['low_est','high_est','hi_best_ratio',
                                           'lo_best_ratio','region_id'],
                         axis=1,errors='ignore')
    # Return the filled dataset
    return disaster_db


def fill_ebola_cis(ebola_db):
    '''
    Fill any missing confidence intervals for ebola data.
    '''
    # *** FILL EBOLA CONFIDENCE INTERVALS ***
    # Ebola modeling is simple: "local" cases of ebola are multiplied by fixed
    #  estimates to obtain lower, middle, and upper estimates. Because imported
    #  ebola deaths are assumed to be recorded with 100% completeness, the
    #  upper and lower values are equal to the recorded 'best' estimates.
    is_local = ebola_db['dataset']=='ebola_local'
    # Ebola imported cases are known with no uncertainty
    for col in ['low','high']:
        ebola_db.loc[~is_local,col] = ebola_db.loc[~is_local,'best']
    # Ebola local cases are multiplied by fixed ratios
    ebola_db.loc[is_local,'low'] = ebola_db.loc[is_local,'best'] * 1.4580
    ebola_db.loc[is_local,'high'] = ebola_db.loc[is_local,'best'] * 2.5475
    ebola_db.loc[is_local,'best'] = ebola_db.loc[is_local,'best'] * 2.0027
    return ebola_db


def generate_upper_lower(db):
    '''
    This function takes the modeled shocks database with incomplete upper and lower
      confidence intervals, then adds those confidence intervals to all records.
    Inputs:
      db (pandas DataFrame): The input shocks database after source prioritization
        has occurred
    Outputs:
      with_uis (pandas DataFrame): The shocks database with 95% uncertainty
        bounds added to the 'upper' and 'lower' fields
    '''
    # Split into war, disaster, and ebola
    ebola_rows = db['dataset'].str.contains('ebola')
    ebola_db = db.loc[ebola_rows,:]
    non_ebola_db = db.loc[~ebola_rows,:]
    war_causes = [724,  # inj_homicide 
                  855,  # inj_war_war
                  851,  # inj_war_terrorism
                  854,  # inj_war_execution
                  945]  # inj_war_warterror
    war_db = non_ebola_db.loc[non_ebola_db['cause_id'].isin(war_causes),:]
    disaster_db = non_ebola_db.loc[~non_ebola_db['cause_id'].isin(war_causes),:]
    # Process confidence intervals separately
    ebola_db = fill_ebola_cis(ebola_db)
    (war_db, ci_ratio_standard) = fill_war_cis(war_db)
    disaster_db = fill_disaster_cis(disaster_db, cis=ci_ratio_standard)
    # Combine and check that all values for best, upper, and lower have been
    #  filled.
    filled = pd.concat([ebola_db, war_db, disaster_db])
    filled.loc[filled.low.isnull(), "low"] = filled.loc[filled.low.isnull(), "best"] * .9
    filled.loc[filled.high.isnull(), "high"] = filled.loc[filled.high.isnull(), "best"] * 1.1
    for col in ['best','high','low']:
        # Just drop for now
        # assert np.all(filled[col].notnull()), "Some NA values in column '{}'".format(col)
        filled = filled.loc[filled[col].notnull(),:]
        filled.loc[filled[col]<0,col] = 0
    # Rename to more conventional GBD parlance
    filled = filled.rename(columns={'best':'val','low':'lower','high':'upper'})
    return filled
