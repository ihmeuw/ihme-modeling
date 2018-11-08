'''
PRIORITIZE SOURCES

Author: Nat Henry, @nathenry
Date: January 8, 2017
Purpose: Functions in this file deduplicate shocks data across all the shocks 
  sources, including both VR and non-VR data.

'''
# Imports (general)
import numpy as np
import pandas as pd
import os
import warnings

################################################################################
# HELPER FUNCTIONS
################################################################################
def split_on_series(df, series):
    '''
    Split row-wise into two dataframes based on a boolean series
    Inputs:
      df (pandas DataFrame): dataframe to be split
      series (pandas Series): Series with all boolean (True/False) values
    Outputs:
      s_true (pandas DataFrame): Rows of the dataframe where the series was true
      s_false (pandas DataFrame): Rows of the dataframe where the series was
        false
    '''
    s_true = df.loc[series,:]
    s_false = df.loc[~series,:]
    return (s_true,s_false)

def get_closest_year(year, location, cause, noshock_df, rel):
    # Subset to the given country and cause
    noshock_df = noshock_df.loc[(noshock_df['cause_id']==cause) & 
                                (noshock_df['location_id']==location),:]
    # 'rel' is either the closest year above or below the argument year
    assert rel in ['above','below']
    if rel == 'above':
        data_sub = noshock_df.loc[noshock_df['year'] > year]
        if data_sub.shape[0] == 0:
            return np.nan
        else:
            return data_sub['year'].min()
    else:
        data_sub = noshock_df.loc[noshock_df['year'] < year]
        if data_sub.shape[0] == 0:
            return np.nan
        else:
            return data_sub['year'].max()

def b_get_priority(sub_df):
    # Set VR priority to 1 if it is highest
    if 'VR' in sub_df['dataset'].tolist():
        return sub_df.loc[sub_df['dataset']=='VR',:]
    else:
        min_priority = sub_df['priority'].min()
        return (sub_df.loc[sub_df['priority']==min_priority,:]
                      .iloc[[0],:])

# Get the difference between this year's results and the expected results
# based on other years
def get_shocks_diff(before, current, after):
    prev_exists = ~np.isnan(before)
    next_exists = ~np.isnan(after)
    # Case 1: both before and after exist
    # Return the difference between the average of the two and the current year
    if prev_exists and next_exists:
        diff = current - (1/2 * (before + after))
    # Case 2: either before or after exists
    # Take the difference between the extant other year and the current year
    elif prev_exists and not next_exists:
        diff = current - before
    elif next_exists and not prev_exists:
        diff = current - after
    else:
        # In the final case, neither before nor after exists
        # Return 0
        diff = 0
    # The difference cannot be less than 0
    diff = np.max([0,diff])
    return diff


def check_rows_needing_prioritization(no_dedupe):
    '''
    This function checks the rows that still need deduplication
    Inputs:
      no_dedupe (pandas DataFrame): All DB rows that were not used as input for
        a deduplication process
    Outputs: None (Writes warning message about missing rows to stderr)
    '''
    # Pass if there are no rows remaining in the no_dedupe dataframe
    if no_dedupe.shape[0] == 0:
        return None
    not_yet_deduped = (no_dedupe
                        .loc[:,['dataset','cause_id']]
                        .drop_duplicates()
                        .to_records())
    warnings.warn("WARNING: Deduplication was not performed for the following "
                  "dataset-cause combinations: {}".format(not_yet_deduped))
    return None

def assign_priority(db):
    ds_to_source = {
        "EMDAT":"EMDAT",
        "GED":"UCDP",
        "PRIO_BDD":"UCDP",
        "ACLED_Africa":"Strauss",
        "ACLED_Asia":"Strauss",
        "SCAD_Africa":"Strauss",
        "SCAD_Latin America":"Strauss",
        "ACLED_MiddleEast":"ACLED_MiddleEast",
        "IISS":"IISS",
        "VR":"VR",
        "GlobalTerrorDB":"GlobalTerrorDB",
        "CPOST_SAD":"CPOST_SAD",
        "war_supplement_2014a":"supplements_2014",
        "terrorism_scrape_2016":"supplements_2016",
        "disaster_supplement_2015":"supplements_2015",
        "disaster_supplement_2016":"supplements_2016",
        "phl_supp_2015":"supplements_2015",
        "pse_supp_2015":"supplements_2015",
        "war_supp_2015":"supplements_2015",
        "multi_supp_2015":"supplements_2015",
        "IRQ_IMS_IBC":"special_supplements",
        "USDoD":"special_supplements",
        "Shocks_2017":"supplements_2017",
        "Twitter_2017":"twitter_supplements",
        "iraq_iran_war":"special_supplements",
        "US_Archives_gov":"special_supplements",
        "yemen_cholera":"special_supplements",
        "Epidemics":"special_supplements",
        "Mina_crowd_collapse_2015":"dis_overrides",
        "TombStone":'dis_overrides',
        "war_exceptions_overrides_GBD2016": "war_overrides",
        "disaster_exceptions_overrides_GBD2016": "dis_overrides",
        "collaborator_overrides":"dis_overrides",
        "war_overrides":"war_overrides",
        "dis_overrides":"dis_overrides",
        "UCDP": "UCDP",
        "Strauss": "Strauss",     
        "special_supplements":"special_supplements",
        "supplements_2014": "supplements_2014",
        "supplements_2015": "supplements_2015",
        "supplements_2016": "supplements_2016",
        "supplements_2017": "supplements_2017",
        "twitter_supplements": "twitter_supplements",
        "current_year_shocks": "war_overrides"
    }
    # Generate a dictionary containing prioritization of data by relevant source
    # VR priority will be overridden to 1 if it is higher than any other source
    #  for a given cause/location/year grouping
    source_to_priority = {
        "war_overrides":0,
        "dis_overrides":1,
        "ACLED_MiddleEast":3,
        "EMDAT":4,
        "UCDP": 5,
        "IISS": 6,
        "VR": 7,
        "Strauss": 8,
        "GlobalTerrorDB":9,
        "CPOST_SAD":10,
        "special_supplements":11,
        "supplements_2014": 12,
        "supplements_2015": 13,
        "supplements_2016": 14,
        "supplements_2017": 15,
        "twitter_supplements": 16
    }
    db['source'] = db['dataset'].map(ds_to_source)
    db['priority'] =db['source'].map(source_to_priority)

    return db


def VR_is_highest(row):
    if row['dataset'] == "VR":
        if row['best'] == row['highest_deaths']:
            return 2
        else:
            return row['priority']
    else:
        return row['priority']

def police_remap_to_war(prioritized_db):
    remap = pd.read_excel("FILEPATH")
    remap['new_cause_id'] = remap['new_cause_id'].mask(remap['new_cause_id'] == "inj_war_warterror", 855)
    remap = remap.drop("year", axis = 1)
    remap = remap.rename(columns ={"year_id":"year"})
    prioritized_db = pd.merge(prioritized_db,remap[['cause_id','year','location_id','new_cause_id','best']],
            how = 'left',
            on = ['cause_id','year','location_id','best'])
    prioritized_db['cause_id'] = prioritized_db['cause_id'].mask(prioritized_db['new_cause_id'] == 855, 855)
    prioritized_db.drop('new_cause_id', axis=1, inplace=True)
    return prioritized_db

def interpersonal_violance_remap(db):
    #guatemala war remap
    db.loc[(db['location_id'] == 128) &
       (db['year'] >= 1980) &
       (db['year'] <= 1982) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    #Armenia war remap
    db.loc[(db['location_id'] == 33) &
       (db['year'] >= 1990) &
       (db['year'] <= 1993) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    #Azerbaijan war remap
    db.loc[(db['location_id'] == 34) &
       (db['year'] >= 1992) &
       (db['year'] <= 1994) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    return db

def police_remap_manual(db):

    #Russian Chechan war
    db.loc[(db['location_id'] == 44945) &
       (db['year'] >= 1994) &
       (db['year'] <= 2006) &
       (db['dataset'] == "UCDP") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Oromia war remap
    db.loc[(db['location_id'] == 44855) &
       (db['year'] >= 1976) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Myanmar war remap
    db.loc[(db['location_id'] == 15) &
       (db['year'] >= 1951) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855
    
    #Kurdistan war remap
    db.loc[(db['location_id'] == 44884) &
       (db['year'] >= 1979) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Kurdistan remap to Iraq. two step process
    db.loc[(db['location_id'] == 44884) &
       (db['year'] >= 1966) &
       (db['year'] <= 1978) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"location_id"] = 143

    #Kurdistan-Iraq remap to war
    db.loc[(db['location_id'] == 143) &
       (db['year'] >= 1966) &
       (db['year'] <= 1978) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Angola war remap
    db.loc[(db['location_id'] == 168) &
       (db['year'] >= 1999) &
       (db['year'] <= 2001) &
       (db['dataset'] == "ACLED_Africa") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Amahara war remap
    db.loc[(db['location_id'] == 44854) &
       (db['year'] >= 1976) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Amahara war remap 2016
    db.loc[(db['location_id'] == 44854) &
       (db['year'] == 2016) &
       (db['dataset'] == "ACLED_Africa") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #Afar war remap
    db.loc[(db['location_id'] == 44853) &
       (db['year'] == 1976) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    #set_honduras as override
    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"priority"] = 0

    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"event_type"] = 'D6'

    #Honduras war remap to interpersonal violance
    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    #Honduras war remap to interpersonal violance
    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "GED") &
       (db['cause_id'] == 724),"priority"] = 10

    #Guatemala war remap to interpersonal violance
    db.loc[(db['location_id'] == 128) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    #El Salvador war remap to interpersonal violance
    db.loc[(db['location_id'] == 127) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    #US remap VR to terrorism
    db.loc[(db['location_id'] >= 523) &
       (db['location_id'] <= 573) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 945),"cause_id"] = 851

    #remap Bahrain war to police
    db.loc[(db['location_id'] == 140) &
       (db['year'] == 2011) &
       (db['cause_id'] == 855),"cause_id"] = 854

    db = db.drop(db.query("location_id == 129 & dataset == 'GED' & year == 2012").index)
    db = db.drop(db.query("location_id == 129 & dataset == 'GED' & year == 2013").index)
    db = db.drop(db.query("location_id == 129 & dataset == 'GED' & year == 2014").index)
    db = db.drop(db.query("location_id == 129 & dataset == 'GED' & year == 2015").index)

    return db


################################################################################
# PRIORITIZATION FUNCTIONS
################################################################################
def prioritize_war(db, cause_map_df):
    '''
    Prioritize all war sources by country, year, and source type.
    Inputs:
      db: Database of shocks sources that have not yet been prioritized, with VR
      cause_map_df: Dataframe linking causes and event types
    Outputs:
      no_priority: Database of all rows that were excluded from this round of 
        prioritization
      war_prioritized: All prioritized sources for war
    '''
    # Causes to deduplicate are all war and conflict sources
    causes_modeled = [851,  # inj_war_terrorism war
                      855,  # inj_war_war war 
                      945]  # inj_war_warterror war 
    
    # Only use rows with the given sources and event types
    s_to_model = db['cause_id'].isin(causes_modeled)
                  #db['dataset'].isin(list(ds_to_source.keys())))
    (to_prioritize, no_priority) = split_on_series(df=db,
                                                   series=s_to_model)

    # Sum across location ID, year, cause, and source
    to_prioritize['nid'] = to_prioritize['nid'].fillna(999999)
    agg_source = (to_prioritize
                    .loc[:,['year','location_id','cause_id','source','nid','best','low','high',"priority","event_name"]]
                    .groupby(by=['year','location_id','cause_id','source','nid','priority','event_name'])
                    .sum(axis=0, skipna=False)
                    .reset_index(drop=False))

    # Function to determine the priority source within each year/location/cause
    #  grouping

    agg_source['vr_deaths'] = agg_source['best'] * (agg_source['source'] == 'VR')
    agg_source['vr_deaths'] = agg_source.groupby(['location_id', 'year', 'cause_id'])['vr_deaths'].transform(np.sum)
    agg_source['source_deaths'] = agg_source.groupby(['location_id', 'year', 'cause_id', 'source'])['best'].transform(np.sum)
    agg_source['max_source_deaths'] = agg_source.groupby(
        ['location_id', 'year', 'cause_id'])['source_deaths'].transform(np.max)

    is_vr_and_trumps = (agg_source['source'] == "VR") & (agg_source['vr_deaths'] >= (agg_source['max_source_deaths']) - .01)
    agg_source['calc_priority'] = agg_source['priority']
    agg_source.loc[is_vr_and_trumps, 'calc_priority'] = 2
    agg_source['lowest_priority'] = agg_source.groupby(['location_id', 'year', 'cause_id'])['calc_priority'].transform(np.min)
    war_prioritized = agg_source.query('calc_priority == lowest_priority')
    war_prioritized = war_prioritized.rename(columns = {"source":"dataset"})
    war_dropped = agg_source.query('calc_priority != lowest_priority')
    war_dropped = war_dropped.rename(columns = {"source":"dataset"})
    # Return the deduplicated data
    return (war_prioritized, no_priority, war_dropped)


def prioritize_non_war(db,cause_map_df):
    '''
    Prioritize all disaster sources by country, year, and source type.
    Inputs:
      db: Database of shocks sources that have not yet been prioritized, with VR
      cause_map_df: Dataframe linking causes and event types
    Outputs:
      no_priority: Database of all rows that were excluded from this round of 
        prioritization
      dis_prioritized: All prioritized sources for disaster
    '''
    # Keep only causes that are included in disaster sources
    # These are split into causes with and without CODEM models
    causes_no_model = [729, #inj_disaster
                       987, #inj_disaster_avalanch
                       985, #inj_disaster_earth
                       989, #inj_disaster_flood
                       988, #inj_disaster_storm
                       986, #inj_disaster_volcan
                       990] #inj_disaster_other
                    
    causes_modeled  = [317, #diarrhea_other
                       408, #infectious
                       699, #inj_fires
                       707, #inj_mech_other
                       703, #poisoning
                       842, #inj_non_disaster
                       716, #inj_othunintent
                       695, #inj_trans_other
                       689, #inj_trans_road
                       693, #inj_trans_road_4wheel
                       345, #malaria
                       341, #measles
                       335, #meningitis_meningo
                       387, #nutrition_pem
                       357, #dengue
                       711, #inj_animal
                       724, # inj_homicide
                       727, # inj_homicide_other
                       854, # inj_war_execution- police conflict
                       302, #diarrhea
                       303] #diarrhea_cholera

    all_disaster_causes = causes_modeled + causes_no_model
    s_to_model = db['cause_id'].isin(all_disaster_causes)

    
    # Split into data that will and won't be prioritized
    (to_prioritize, no_priority) = split_on_series(df=db,series=s_to_model)
    # Determine the list of locations with high and low VR star ratings

    PATH_TO_STAR_RATINGS = ("FILEPATH")
    vr_rich_locs = (pd.read_csv(PATH_TO_STAR_RATINGS,encoding='utf8')
                      .loc[:,'location_id']
                      .tolist())
    # Split into the four groups that will be prioritized separately:
    #  - Events in VR-rich versus VR-poor locations
    #  - Causes with and without CODEM models
    is_vr_rich = to_prioritize['location_id'].isin(vr_rich_locs)
    after_1980 = to_prioritize['year'] >= 1980
    has_codem_model = to_prioritize['cause_id'].isin(causes_modeled)
    # The four dataframes to model
    a_rich_modeled = to_prioritize.loc[(is_vr_rich & after_1980) & has_codem_model,:]
    b_rich_nomodel = to_prioritize.loc[(is_vr_rich & after_1980) & ~has_codem_model,:]
    c_poor_modeled = to_prioritize.loc[(~is_vr_rich | ~after_1980) & has_codem_model,:]
    d_poor_nomodel = to_prioritize.loc[(~is_vr_rich | ~after_1980) & ~has_codem_model,:]  

    ############################################################################
    # PRIORITIZE EACH DISASTER SUB-GROUP
    ############################################################################
    # *** High-quality VR locations and causes that DO have a CODEM model ***
    # For each location, cause, and year, get the VR estimates from the most recent
    # past year and the soonest upcoming year that do NOT have shocks in them
    # Subset to VR only in this group
    a_vr = a_rich_modeled.loc[a_rich_modeled['dataset']=="VR",:]
    a_vr_groupby_cols = ['cause_id','location_id','year','nid','event_name']
    a_vr = (a_vr.loc[:,a_vr_groupby_cols+['best']]
                .groupby(by=a_vr_groupby_cols)
                .sum()
                .reset_index())
    # Get the list of locations/causes/years that have disasters in them, which
    #  is indicated by all sources BESIDES VR
    a_disaster_indicator = (a_rich_modeled
                             .loc[(a_rich_modeled['dataset']!="VR") &
                                  (a_rich_modeled['best'] > 1),
                                  ['location_id','cause_id','year']]
                             .drop_duplicates())
    a_disaster_indicator['shock_year'] = 1
    # Merge the shock indicator onto the dataset
    # Only data with VR available is kept
    a_merged = pd.merge(left=a_vr,
                        right=a_disaster_indicator,
                        on=['cause_id','location_id','year'],
                        how='left')
    a_merged['shock_year'] = a_merged['shock_year'].fillna(0)
    # Estimate only for years with shocks
    a_merged_sub = (a_merged.loc[a_merged['shock_year']==1,:]
                            .drop(labels=['shock_year'],axis=1))
    a_noshock_df = (a_merged.loc[a_merged['shock_year']!=1,:]
                            .drop(labels=['shock_year'],axis=1))
    # Get the closest years above and below all shock years for a given country
    a_merged_sub['year_below'] = (a_merged_sub
                                    .apply(lambda row: get_closest_year(
                                             year=row['year'], location=row['location_id'],
                                             cause=row['cause_id'], noshock_df=a_noshock_df,
                                             rel='below'),axis=1))
    a_merged_sub['year_above'] = (a_merged_sub
                                    .apply(lambda row: get_closest_year(
                                             year=row['year'], location=row['location_id'],
                                             cause=row['cause_id'], noshock_df=a_noshock_df,
                                             rel='above'),axis=1))
    # Merge on shock_free VR estimates for the upper and lower years
    a_est = pd.merge(left=a_merged_sub,
                     right=a_noshock_df[['cause_id','location_id','year','best']]
                                      .rename(columns={'best':'best_below',
                                      'year':'year_below'}),
                     on=['cause_id','location_id','year_below'],
                     how='left')
    a_est = pd.merge(left=a_est,
                     right=a_noshock_df[['cause_id','location_id','year','best']]
                                      .rename(columns={'best':'best_above',
                                      'year':'year_above'}),
                     on=['cause_id','location_id','year_above'],
                     how='left')
    a_est['shocks_diff'] = a_est.apply(lambda x: get_shocks_diff(x['best_below'],
                                                  x['best'],x['best_above']),axis=1)
    # Rename and drop columns
    a_est = a_est.drop(labels=['best','best_below','best_above',
                               'year_below','year_above'],axis=1,errors='ignore')
    a_est = a_est.rename(columns={'shocks_diff':'best'})
    a_est = a_est.loc[a_est['best'] > 0,:]
    a_est['upper'] = np.nan
    a_est['lower'] = np.nan
    a_est['dataset'] = "VR"
    a_rich_modeled_good = a_est

    #fixed b model
    b_rich_nomodel['nid'] = b_rich_nomodel['nid'].fillna("99999")
    b_rich_nomodel = b_rich_nomodel.groupby(['cause_id','dataset','event_name','location_id','nid','year','source','priority'],
                          as_index=False)['best','high','low'].sum()
    b_rich_nomodel.loc[b_rich_nomodel['dataset'] == 'VR', "priority"] = 2
    b_rich_nomodel['lowest_priority'] = b_rich_nomodel.groupby(['location_id', 'year', 'cause_id'])['priority'].transform(np.min)
    b_rich_nomodel_good = b_rich_nomodel.query("priority == lowest_priority")

    # *** Low-quality VR countries (groupings C and D) ***
    # For causes where CODEM models ARE run, drop VR data before proceeding
    c_vr = c_poor_modeled.loc[c_poor_modeled['dataset'] == "VR",:]
    c_poor_modeled = c_poor_modeled.loc[c_poor_modeled['dataset'] != "VR",:]

    all_poor_good = pd.concat([c_poor_modeled,d_poor_nomodel])

    all_poor_good['source_deaths'] = all_poor_good.groupby(['location_id','year','dataset','cause_id',"priority"])['best'].transform(np.sum)
    all_poor_good['highest_deaths'] = all_poor_good.groupby(['location_id','year','cause_id'])['source_deaths'].transform(np.max)
    all_poor_good['priority'] = all_poor_good.apply(VR_is_highest, axis =1)
    all_poor_good['highest_priority'] = all_poor_good.groupby(['location_id','year','cause_id'])['priority'].transform(np.min)
    all_poor_bad = all_poor_good.query('priority != highest_priority')
    all_poor_good = all_poor_good.query('priority == highest_priority')

    ########################################################################
    # ** COMPILE ALL PRIORITIZED DATA **
    dis_prioritized = (pd.concat([a_rich_modeled_good,
                                 b_rich_nomodel_good,
                                 all_poor_good])
                         .loc[:,['year','location_id','cause_id','dataset',
                                  'low','best','high','nid','event_name']])
    dis_dropped = all_poor_bad[['year','location_id','cause_id','dataset','low','best','high','nid','event_name']]
    return (dis_prioritized,no_priority,dis_dropped)


def apply_overrides(db, overrides):
    '''
    Apply all age-sex-year-location-cause overrides generated for past rounds
      of the GBD.
    Inputs:
      db (pandas DataFrame): The prioritized shocks database, including VR data
      overrides (pandas DataFrame): All rows that should be used for overrides
    Outputs:
      overridden_db (pandas DataFrame): The shocks database with overrides applied
    '''
    # Format overrides data, summing across all sources
    overrides = (overrides.loc[:,['low','best','high','year','location_id',
                                  'cause_id','dataset']]
                          .groupby(by=['year','location_id','cause_id','dataset'])
                          .sum(axis=0,skipna=False)
                          .reset_index(drop=False))
    # Assert that there are no duplicates in year-location cause across sources
    assert ~np.any(overrides
                    .duplicated(subset=['year','location_id','cause_id'],keep=False)
                    .tolist()), ("There are year/location/cause overlaps in the "
                                 "overrides sources - resolve before proceeding")
    # Drop all year/location/causes that have been covered in the overrides
    main_df_drop_df = overrides.loc[:,['year','location_id','cause_id']]
    main_df_drop_df['to_drop'] = True
    sub_db = pd.merge(left=db,
                      right=main_df_drop_df,
                      on=['year','location_id','cause_id'],
                      how='left')
    sub_db['to_drop'] = sub_db['to_drop'].fillna(False)
    sub_db = (sub_db.loc[~sub_db['to_drop'],:]
                    .drop(labels=['to_drop'],axis=1))
    # Add the overrides to the pruned database
    overridden_db = pd.concat([sub_db,overrides])
    return overridden_db

def apply_overrides(prioritized_db, overrides):
    df = prioritized_db.append(overrides)
    df = assign_priority(df)
    df['lowest_priority'] = df.groupby(['location_id', 'year', 'cause_id'])['priority'].transform(np.min)
    final_df = df.query("priority == lowest_priority")
    dropped_df = df.query("priority != lowest_priority")
    return final_df, dropped_df


def prioritize_dedupe_all(db, cause_map_df):
    '''
    Deduplicates and prioritizes all sources by country, year, and source type,
      then applies overrides from past rounds of GBD.
    Inputs:
      db (pandas DataFrame): Database of shocks sources that have not yet been
        prioritized. At this point, this should include the all-age, both-sex
        data pulled from CoD database
      cause_map_df (pandas DataFrame): Dataframe linking causes and event types
    Outputs:
      prioritized_db (pandas DataFrame): The prioritized and deduplicated
        shocks database, with overrides applied
      needs_priority_db (pandas DataFrame): All rows that were not successfully
        prioritized in this process. These should be added back in later iterations
    '''
    # Exclude sources that do not need deduplication
    # Since ebola has already been excluded, this should only include 
    #   exception/override datasets
    db = assign_priority(db)
    db = police_remap_to_war(db)
    db = interpersonal_violance_remap(db)
    db = police_remap_manual(db) #second round, catches ones that were initially not caught
    needs_priority_db = db[db['best'] > 0]

    needs_priority_db['event_name'] = needs_priority_db['event_name'].fillna("")

    #split out overrides to be applied later
    overrides = needs_priority_db.query("priority == 0 | priority == 1")
    needs_priority_db = needs_priority_db.query('priority != 0 & priority != 1')

    # Deduplicate war data, continually shrinking the rows that need processing
    (war_prioritized,needs_priority_db,war_dropped) = prioritize_war(
                                            db=needs_priority_db,
                                            cause_map_df=cause_map_df)
    (disaster_prioritized,needs_priority_db,dis_dropped) = prioritize_non_war(
                                                 db=needs_priority_db,
                                                 cause_map_df=cause_map_df)
    # Check to make sure that all necessary rows have been deduplicated
    check_rows_needing_prioritization(needs_priority_db)
    # Compile the de-duplicated data, with the exception of overrides
    prioritized_db = pd.concat([war_prioritized,disaster_prioritized])

    (prioritized_db, dropped_by_overrides) = apply_overrides(prioritized_db,overrides)


    dropped_db = pd.concat([war_dropped,dis_dropped])

    dropped_db = dropped_db[['best','cause_id','dataset','high','location_id','low','year','nid','event_name']]
    dropped_db = dropped_db.groupby(['year',"location_id",'cause_id','dataset','nid','event_name'], as_index=False)['best','low','high'].sum()

    prioritized_db = prioritized_db[['best','cause_id','dataset','high','location_id','low','year','nid','event_name']]
    prioritized_db = prioritized_db.groupby(['year',"location_id",'cause_id','dataset','nid','event_name'], as_index=False)['best','low','high'].sum()
    return (prioritized_db, needs_priority_db, dropped_db)
