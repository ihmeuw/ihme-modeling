
# Imports (general)
import numpy as np
import pandas as pd
import os
import warnings

################################################################################
# HELPER FUNCTIONS
################################################################################
def split_on_series(df, series):

    s_true = df.loc[series,:]
    s_false = df.loc[~series,:]
    return (s_true,s_false)

def get_closest_year(year, location, cause, noshock_df, rel):
    noshock_df = noshock_df.loc[(noshock_df['cause_id']==cause) & 
                                (noshock_df['location_id']==location),:]
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


def get_shocks_diff(before, current, after):
    prev_exists = ~np.isnan(before)
    next_exists = ~np.isnan(after)

    if prev_exists and next_exists:
        diff = current - (1/2 * (before + after))

    elif prev_exists and not next_exists:
        diff = current - before
    elif next_exists and not prev_exists:
        diff = current - after
    else:

        diff = 0
    diff = np.max([0,diff])
    return diff


def check_rows_needing_prioritization(no_dedupe):

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
    db.loc[(db['location_id'] == 128) &
       (db['year'] >= 1980) &
       (db['year'] <= 1982) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    db.loc[(db['location_id'] == 33) &
       (db['year'] >= 1990) &
       (db['year'] <= 1993) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    db.loc[(db['location_id'] == 34) &
       (db['year'] >= 1992) &
       (db['year'] <= 1994) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 724),"cause_id"] = 855

    return db

def police_remap_manual(db):

    db.loc[(db['location_id'] == 44945) &
       (db['year'] >= 1994) &
       (db['year'] <= 2006) &
       (db['dataset'] == "UCDP") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44855) &
       (db['year'] >= 1976) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 15) &
       (db['year'] >= 1951) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44884) &
       (db['year'] >= 1979) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44884) &
       (db['year'] >= 1966) &
       (db['year'] <= 1978) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"location_id"] = 143

    db.loc[(db['location_id'] == 143) &
       (db['year'] >= 1966) &
       (db['year'] <= 1978) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 168) &
       (db['year'] >= 1999) &
       (db['year'] <= 2001) &
       (db['dataset'] == "ACLED_Africa") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44854) &
       (db['year'] >= 1976) &
       (db['year'] <= 1988) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44854) &
       (db['year'] == 2016) &
       (db['dataset'] == "ACLED_Africa") &
       (db['cause_id'] == 854),"cause_id"] = 855

    db.loc[(db['location_id'] == 44853) &
       (db['year'] == 1976) &
       (db['dataset'] == "PRIO_BDD") &
       (db['cause_id'] == 854),"cause_id"] = 855

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

    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    db.loc[(db['location_id'] == 129) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "GED") &
       (db['cause_id'] == 724),"priority"] = 10

    db.loc[(db['location_id'] == 128) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    db.loc[(db['location_id'] == 127) &
       (db['year'] >= 2012) &
       (db['year'] <= 2015) &
       (db['dataset'] == "IISS") &
       (db['cause_id'] == 855),"cause_id"] = 724

    db.loc[(db['location_id'] >= 523) &
       (db['location_id'] <= 573) &
       (db['dataset'] == "VR") &
       (db['cause_id'] == 945),"cause_id"] = 851

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

    causes_modeled = [851, 
                      855,
                      945]  
    
    s_to_model = db['cause_id'].isin(causes_modeled)

    (to_prioritize, no_priority) = split_on_series(df=db,
                                                   series=s_to_model)

    to_prioritize['nid'] = to_prioritize['nid'].fillna(999999)
    agg_source = (to_prioritize
                    .loc[:,['year','location_id','cause_id','source','nid','best','low','high',"priority","event_name"]]
                    .groupby(by=['year','location_id','cause_id','source','nid','priority','event_name'])
                    .sum(axis=0, skipna=False)
                    .reset_index(drop=False))

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

    return (war_prioritized, no_priority, war_dropped)


def prioritize_non_war(db,cause_map_df):

    causes_no_model = [729, 
                       987, 
                       985, 
                       989, 
                       988,
                       986,
                       990]
                    
    causes_modeled  = [317, 
                       408, 
                       699, 
                       707, 
                       703, 
                       842, 
                       716, 
                       695, 
                       689, 
                       693, 
                       345, 
                       341, 
                       335, 
                       387, 
                       357, 
                       711, 
                       724, 
                       727, 
                       854, 
                       302, 
                       303] 

    all_disaster_causes = causes_modeled + causes_no_model
    s_to_model = db['cause_id'].isin(all_disaster_causes)

    
    (to_prioritize, no_priority) = split_on_series(df=db,series=s_to_model)

    PATH_TO_STAR_RATINGS = ("FILEPATH")
    vr_rich_locs = (pd.read_csv(PATH_TO_STAR_RATINGS,encoding='utf8')
                      .loc[:,'location_id']
                      .tolist())

    is_vr_rich = to_prioritize['location_id'].isin(vr_rich_locs)
    after_1980 = to_prioritize['year'] >= 1980
    has_codem_model = to_prioritize['cause_id'].isin(causes_modeled)

    a_rich_modeled = to_prioritize.loc[(is_vr_rich & after_1980) & has_codem_model,:]
    b_rich_nomodel = to_prioritize.loc[(is_vr_rich & after_1980) & ~has_codem_model,:]
    c_poor_modeled = to_prioritize.loc[(~is_vr_rich | ~after_1980) & has_codem_model,:]
    d_poor_nomodel = to_prioritize.loc[(~is_vr_rich | ~after_1980) & ~has_codem_model,:]  


    a_vr = a_rich_modeled.loc[a_rich_modeled['dataset']=="VR",:]
    a_vr_groupby_cols = ['cause_id','location_id','year','nid','event_name']
    a_vr = (a_vr.loc[:,a_vr_groupby_cols+['best']]
                .groupby(by=a_vr_groupby_cols)
                .sum()
                .reset_index())

    a_disaster_indicator = (a_rich_modeled
                             .loc[(a_rich_modeled['dataset']!="VR") &
                                  (a_rich_modeled['best'] > 1),
                                  ['location_id','cause_id','year']]
                             .drop_duplicates())
    a_disaster_indicator['shock_year'] = 1

    a_merged = pd.merge(left=a_vr,
                        right=a_disaster_indicator,
                        on=['cause_id','location_id','year'],
                        how='left')
    a_merged['shock_year'] = a_merged['shock_year'].fillna(0)
    a_merged_sub = (a_merged.loc[a_merged['shock_year']==1,:]
                            .drop(labels=['shock_year'],axis=1))
    a_noshock_df = (a_merged.loc[a_merged['shock_year']!=1,:]
                            .drop(labels=['shock_year'],axis=1))
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


    b_rich_nomodel['nid'] = b_rich_nomodel['nid'].fillna("99999")
    b_rich_nomodel = b_rich_nomodel.groupby(['cause_id','dataset','event_name','location_id','nid','year','source','priority'],
                          as_index=False)['best','high','low'].sum()
    b_rich_nomodel.loc[b_rich_nomodel['dataset'] == 'VR', "priority"] = 2
    b_rich_nomodel['lowest_priority'] = b_rich_nomodel.groupby(['location_id', 'year', 'cause_id'])['priority'].transform(np.min)
    b_rich_nomodel_good = b_rich_nomodel.query("priority == lowest_priority")

    c_vr = c_poor_modeled.loc[c_poor_modeled['dataset'] == "VR",:]
    c_poor_modeled = c_poor_modeled.loc[c_poor_modeled['dataset'] != "VR",:]

    all_poor_good = pd.concat([c_poor_modeled,d_poor_nomodel])

    all_poor_good['source_deaths'] = all_poor_good.groupby(['location_id','year','dataset','cause_id',"priority"])['best'].transform(np.sum)
    all_poor_good['highest_deaths'] = all_poor_good.groupby(['location_id','year','cause_id'])['source_deaths'].transform(np.max)
    all_poor_good['priority'] = all_poor_good.apply(VR_is_highest, axis =1)
    all_poor_good['highest_priority'] = all_poor_good.groupby(['location_id','year','cause_id'])['priority'].transform(np.min)
    all_poor_bad = all_poor_good.query('priority != highest_priority')
    all_poor_good = all_poor_good.query('priority == highest_priority')

    dis_prioritized = (pd.concat([a_rich_modeled_good,
                                 b_rich_nomodel_good,
                                 all_poor_good])
                         .loc[:,['year','location_id','cause_id','dataset',
                                  'low','best','high','nid','event_name']])
    dis_dropped = all_poor_bad[['year','location_id','cause_id','dataset','low','best','high','nid','event_name']]
    return (dis_prioritized,no_priority,dis_dropped)


def apply_overrides(db, overrides):

    overrides = (overrides.loc[:,['low','best','high','year','location_id',
                                  'cause_id','dataset']]
                          .groupby(by=['year','location_id','cause_id','dataset'])
                          .sum(axis=0,skipna=False)
                          .reset_index(drop=False))

    assert ~np.any(overrides
                    .duplicated(subset=['year','location_id','cause_id'],keep=False)
                    .tolist()), ("There are year/location/cause overlaps in the "
                                 "overrides sources - resolve before proceeding")

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

    db = assign_priority(db)
    db = police_remap_to_war(db)
    db = interpersonal_violance_remap(db)
    db = police_remap_manual(db) 
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
