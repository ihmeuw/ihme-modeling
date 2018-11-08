import pandas as pd
import sys
from os import rename, path

from getpass import getuser
if getuser() == 'USER':
    SDG_REPO = 'FILEPATH'
sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.aggregate_age_standardize as agg

INDEX_COLS_PAST = ['location_id', 'year_id', 'age_group_id', 'sex_id']
INDEX_COLS_FUTURE = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'scenario']

PAST_FP_FILE = 'FILEPATH'
FUTURE_FP_FILE = 'FILEPATH'
ASFR_FILE = 'FILEPATH'

FUTURE_POP_FILE = 'FILEPATH'
POP_FILE = 'FILEPATH'
FHS_MORT_VERS = '20180608_new_inputs_squeezed_agg'
FP_FILE = 'FILEPATH'
DRAW_COLS = ['draw_' + str(i) for i in xrange(1000)]


def add_denom_rows(df, agg_var, agg_dim, agg_parent, agg_children=None):
    '''
    Create expanded demographics dataframe for denominators.
    '''
    index_cols = INDEX_COLS_FUTURE

    agg_df = df.copy()
    if agg_children is not None:
        agg_df = agg_df.loc[agg_df[agg_dim].isin(agg_children)]
    agg_df[agg_dim] = agg_parent
    agg_df = agg_df.groupby(
        index_cols,
        as_index=False
    )[agg_var].sum()
    df = df.append(agg_df[list(df)])

    return df


def load_births():
    '''
    Load time series for live births.
    '''
    df = qry.get_births()
    
    # add on global
    df = add_denom_rows(df, 'births', 'location_id', 1)
    # add on all age
    df = add_denom_rows(df, 'births', 'age_group_id', 22)
    # add on 10-24
    df = add_denom_rows(df, 'births', 'age_group_id', 159, [7, 8, 9])
    # add on total births for under-5
    df = add_denom_rows(df, 'births', 'age_group_id', 42, [22])
    # add on total births for under-5
    df = add_denom_rows(df, 'births', 'age_group_id', 1, [22])
    
    return df


def load_population():
    '''
    Load population DataArray into memory, return what we want as DataFrame.

    Use GBD 2016 for the past.
    '''
    df = pd.read_csv(POP_FILE)
    # add on global
    df = add_denom_rows(df, 'population', 'location_id', 1)

    # add on both sexes
    df = add_denom_rows(df, 'population', 'sex_id', 3)

    # add on all ages
    df = add_denom_rows(df, 'population', 'age_group_id', 22)

    # add on under-5
    df = add_denom_rows(df, 'population', 'age_group_id', 1, [2, 3, 4, 5])

    #add 10-19
    df = add_denom_rows(df, 'population', 'age_group_id', 162, [7,8])
    
    # add on 10-24
    df = add_denom_rows(df, 'population', 'age_group_id', 159, [7, 8, 9])
    
    # add on 10-plus
    df = add_denom_rows(df, 'population', 'age_group_id', 194, [7, 8, 9, 10, 11,12, 13, 14, 15,16, 17, 18, 19, 20, 30, 31, 32, 235])
    return df


def load_age_disagg_births():

    df = qry.get_asfr()
    db_pops = qry.get_pops()
    df = df.merge(db_pops, how='left')
    assert df['population'].notnull().values.all(), 'merge with population failed'

    df['asfr'] = df['asfr'] * df['population']
    df.drop('population', axis=1, inplace=True)
    df.rename(columns={'asfr': 'births'}, inplace=True)

    return df


########################################################################################
# model-specific functions
########################################################################################


def process_burdenator_draws(past_future, version = dw.BURDENATOR_VERS):
    if past_future == 'past':
        index_cols = dw.RISK_BURDEN_GROUP_COLS
        data_dir = dw.INPUT_DATA_DIR + 'risk_burden' + '/' + str(version)
        db_pops = qry.get_pops()
    elif past_future == 'future':
        index_cols = ['indicator_component_id'] + INDEX_COLS_FUTURE
        data_dir = dw.FORECAST_DATA_DIR + 'risk_burden' + '/' + str(version)
        db_pops = load_population()

    else:
        raise ValueError('The past_future arg must be set to "past" or "future".')

    dfs=[]

    component_ids = dw.RISK_BURDEN_COMPONENT_IDS + dw.RISK_BURDEN_DALY_COMPONENT_IDS
    for component_id in component_ids:
        print("pulling " + str(component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        df.loc[:,'indicator_component_id'] = component_id
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    
    # aggregate to both sexes but keep sex-split data as well
    df = df.merge(db_pops, how='left')
    df_sex_split = df.copy(deep=True)
    df['sex_id'] = 3
    df = agg.age_sex_aggregate(df, group_cols = index_cols)
    df = pd.concat([df, df_sex_split], axis = 0, ignore_index = True)

    # global
    df_global = agg.aggregate_locations_to_global(df, index_cols,
                                                  age_standardized=True,
                                                  age_group_years_start=0,
                                                  age_group_years_end=125,
                                                  age_group_id=27)
    
    # age-standardize
    df = agg.age_standardize(df, index_cols, 0, 125)
    
    # output
    df = df[index_cols + dw.DRAW_COLS]
    df_global = df_global[index_cols + dw.DRAW_COLS]

    file_dict = dw.RB_INPUT_FILE_DICT
    for component_id in file_dict.keys():

        path = data_dir + '/' + str(file_dict[component_id]) + '.feather'
        global_path = data_dir + '/' + str(file_dict[component_id]) + '_global'+ '.feather'

        # sdg
        print('outputting ' + str(file_dict[component_id]))
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(path)

        # global
        print('outputting ' + str(file_dict[component_id]) + ' global')
        df_id_global = df_global[df_global.indicator_component_id == component_id]
        df_id_global.reset_index(drop=True, inplace=True)
        df_id_global.to_feather(global_path)

    return df


def process_como_prev_draws(past_future, version = dw.COMO_VERS):

    if past_future == 'past':
        data_dir = dw.INPUT_DATA_DIR + 'como_prev' + '/' + str(version) # just como_prev for now
    elif past_future == 'future':
        data_dir = dw.FORECAST_DATA_DIR + 'como_prev' + '/' + str(version)
    else:
        raise ValueError('The past_future arg must be set to "past" or "future".')

    db_pops = qry.get_pops()

    dfs=[]

    # nonfatal and nema + fatal
    component_ids = [125, 128, 131, 1433, 149, 152, 140, 143, 146, 104, 107, 110, 113, 116, 119, 122, 134, 137]
    for component_id in component_ids:
        print("pulling " + str(component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather') 
        df['metric_id'] = 3
        df = df.merge(db_pops, how='left')
    
        # Keep sex-split
        df = df[df.sex_id != 3] # temporary for goalkeepers diagnostics
        df_sex_split = df.copy(deep=True)

        # aggregate sexes
        df = agg.aggregate_sexes(df, dw.COMO_GROUP_COLS)

        # age standardize
        print('appending sex split data')
        df = df.append(df_sex_split, ignore_index=True)
        df = agg.age_standardize(df, dw.COMO_GROUP_COLS, 0, 125)

        df = df[dw.COMO_GROUP_COLS + dw.DRAW_COLS]

        print("outputting " + str(component_id))
        df.to_feather(data_dir + '/' + str(component_id) + '_as.feather')


def process_demo_draws(past_future, version = dw.DEMO_VERS):
    '''process demographics draws'''

    if past_future == 'past':
        data_dir = dw.INPUT_DATA_DIR + 'demographics' + '/' + str(version)
    elif past_future == 'future':
        data_dir = dw.FORECAST_DATA_DIR + 'demographics' + '/' + str(version)
    else:
        raise ValueError('The past_future arg must be set to "past" or "future".')

    component_ids = dw.DEMO_COMPONENT_IDS
    dfs = []
    
    for component_id in component_ids:
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')

        dfs.append(df)

    print('concatenating')
    df = pd.concat(dfs, ignore_index=True)

    #temporary for gk
    locs = qry.get_sdg_reporting_locations(level_3 = True)
    df = df[df.location_id.isin(locs.location_id)]

    # get live births
    births = load_births()
    if past_future == 'past':
        births = births[births.scenario == 0]
        births.drop('scenario', inplace=True, axis=1)

    df = df.merge(births, how='left')

    # Keep sex-split
    df_sex_split = df.copy(deep=True)

    # aggregate sexes
    df['sex_id'] == 3
    df = agg.age_sex_aggregate(df, dw.DEMO_GROUP_COLS, denominator='births')
    df = df.append(df_sex_split, ignore_index=True)

    # global aggregation
    df_global = agg.aggregate_locations_to_global(df, dw.DEMO_GROUP_COLS, denominator='births')
    
    # output
    df = df[dw.DEMO_GROUP_COLS + dw.DRAW_COLS]
    df_global = df_global[dw.DEMO_GROUP_COLS + dw.DRAW_COLS]

    for component_id in component_ids:
        if component_id == 56:
            ind_id = '1040'
        else:
            ind_id = '1041'

        print("outputting " + ind_id)
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(data_dir + '/' + ind_id + '.feather')

        df_id_global = df_global[df_global.indicator_component_id == component_id]
        df_id_global.reset_index(drop=True, inplace=True)
        df_id_global.to_feather(data_dir + '/' + ind_id + '_' + 'global' + '.feather')


def process_uhc_intervention_draws(version=dw.COV_VERS):
    data_dir = dw.INPUT_DATA_DIR + 'covariate' + '/' + str(version)
    component_ids = dw.UHC_INTERVENTION_COMP_IDS

    # first rename components that don't need further prep
    rename_ids = [str(rid) for rid in component_ids if rid not in [206, 209]]

    for rid in rename_ids:
        if path.isfile(data_dir + '/' + rid + '.feather'):
            print('renaming' + rid)
            rename(data_dir + '/' + rid + '.feather', data_dir + '/' + rid + '_prepped' + '.feather')
    
    dfs = []
    for component_id in [206, 209]: # these are the interventions that require aggregation
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        dfs.append(df)

    print('concatenating')
    df = pd.concat(dfs, ignore_index=True)

    # merge populations
    db_pops = qry.get_pops()
    df = df.merge(db_pops, how='left')

    # age/sex aggregate
    df.loc[df.indicator_component_id == 209, 'sex_id'] = 3
    df.loc[df.indicator_component_id == 209, 'age_group_id'] = 29 # 15+
    df.loc[df.indicator_component_id == 206, 'age_group_id'] = 24 # 15-49
    df = agg.age_sex_aggregate(df, group_cols = dw.COV_GROUP_COLS, denominator = 'population')

    # output
    df = df[dw.COV_GROUP_COLS + dw.DRAW_COLS]

    for component_id in [206, 209]:
        print("outputting " + str(component_id))
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(data_dir + '/' + str(component_id) + '_prepped' + '.feather')


# non-UHC intervention covs (hrh goes into UHC later)
def process_covariate_draws(version=dw.COV_VERS):
    data_dir = dw.INPUT_DATA_DIR + 'covariate' + '/' + str(version)
    component_ids = dw.NON_UHC_COV_COMPONENT_IDS

    dfs = []
    for component_id in component_ids: # read in all components
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        dfs.append(df)

    print('concatenating')
    df = pd.concat(dfs, ignore_index=True)

    # merge populations
    db_pops = qry.get_pops()

    db_pops_adol_birth = db_pops[db_pops.age_group_id.isin([7,8])] # create adol birth age group
    db_pops_adol_birth['age_group_id'] = 162
    pop_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    db_pops_adol_birth = db_pops_adol_birth.groupby(pop_cols, as_index=False)['population'].sum()
    db_pops = db_pops.append(db_pops_adol_birth, ignore_index=True)
    df = df.merge(db_pops, how='left')

    # aggregate met need
    df_met_need_15_plus = df[df.indicator_component_id == 179]
    df_met_need_15_24 = df_met_need_15_plus[df_met_need_15_plus.age_group_id.isin([8,9])]
    df_met_need_15_plus['age_group_id'] = 24
    df_met_need_15_24['age_group_id'] = 149
    df_met_need = df_met_need_15_plus.append(df_met_need_15_24, ignore_index=True)
    df_met_need = agg.age_sex_aggregate(df_met_need, group_cols = dw.COV_GROUP_COLS, denominator = 'population')

    df = df[df.indicator_component_id != 179]
    df = df.append(df_met_need, ignore_index=True)

    # global aggregation
    df_global = df_global[~df_global.indicator_component_id.isin([1457, 1460, 1463, 1556])] # hrh aggregated later
    
    df_global = agg.aggregate_locations_to_global(df, dw.COV_GROUP_COLS, denominator='population')

    # output
    df = df[dw.COV_GROUP_COLS + dw.DRAW_COLS]
    df_global = df_global[dw.COV_GROUP_COLS + dw.DRAW_COLS]

    file_dict = dw.COV_FILE_DICT
    for component_id in file_dict.keys():
        if file_dict[component_id] == component_id:
            path = data_dir + '/' + str(component_id) + '_prepped' + '.feather'
            global_path = data_dir + '/' + str(component_id) + '_global' + '.feather'
        else:
            path = data_dir + '/' + str(file_dict[component_id]) + '.feather'
            global_path = data_dir + '/' + str(file_dict[component_id]) + '_global'+ '.feather'

        print('outputting ' + str(file_dict[component_id]))
        df_id = df[(df.indicator_component_id == component_id) & (df.age_group_id != 149)]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(path)

        if component_id not in [1457, 1460, 1463, 1556]: # save global dfs
            print('outputting ' + str(file_dict[component_id]) + ' global')
            df_id_global = df_global[df_global.indicator_component_id == component_id]
            df_id_global.reset_index(drop=True, inplace=True)
            df_id_global.to_feather(global_path)


def process_mmr_draws(version=dw.MMR_VERS):
    '''process maternal mortality ratio draws'''

    data_dir = dw.INPUT_DATA_DIR + 'mmr' + '/' + str(version)

    print("pulling MMR")
    df = pd.read_feather(data_dir + '/' + '47' + '.feather')

    # merge births
    births = load_age_disagg_births()
    df = df.merge(births, how='left')

    # create age groups and aggregate
    df_10_24 = df.loc[df.age_group_id.isin([7,8,9])]
    df.loc[:, 'age_group_id'] = 169
    df_10_24.loc[:, 'age_group_id'] = 159
    df = pd.concat([df, df_10_24], ignore_index=True)
    df = agg.age_sex_aggregate(df, group_cols = dw.MMR_GROUP_COLS, denominator='births')

    # aggregate to global and make goalkeepers units/age-groups
    df_global = agg.aggregate_locations_to_global(df, dw.MMR_GROUP_COLS, denominator='births')
    df_global_gk = df_global.copy(deep=True)
    df_global_gk.loc[:, dw.DRAW_COLS] = df_global_gk.loc[:, dw.DRAW_COLS].applymap(
                                        lambda x: x / 100) # want per 1000 live births for goalkeepers
    df_global = df_global[df_global.age_group_id != 159] # no 10-24 age group for sdgs
    df_global.loc[:, 'units'] = 'sdg'
    df_global_gk.loc[:, 'units'] = 'goalkeepers'
    df_global = pd.concat([df_global, df_global_gk], ignore_index=True)

    # output
    df = df[df.age_group_id != 159] # no 10-24 age group for sdgs
    df = df[dw.MMR_GROUP_COLS + dw.DRAW_COLS]
    df_global = df_global[dw.MMR_GROUP_COLS + dw.DRAW_COLS + ['units']]

    df.reset_index(drop=True, inplace=True)
    df.to_feather(data_dir + '/' + '1033' + '.feather')
    df_global.reset_index(drop=True, inplace=True)
    df_global.to_feather(data_dir + '/' + '1033'+ '_global' + '.feather')

    return df_global


def process_risk_exposure_draws(past_future, version=dw.RISK_EXPOSURE_VERS):

    if past_future == 'past':
        index_cols = dw.RISK_EXPOSURE_GROUP_COLS
        data_dir = dw.INPUT_DATA_DIR + 'risk_exposure' + '/' + str(version)
        db_pops = qry.get_pops()
    if past_future == 'future':
        index_cols = ['indicator_component_id'] + INDEX_COLS_FUTURE
        data_dir = dw.FORECAST_DATA_DIR + 'risk_exposure' + '/' + str(version)
        db_pops = load_population()

    component_ids = dw.RISK_EXPOSURE_COMPONENT_IDS
    
    dfs = []
    for component_id in component_ids:
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        df.loc[:,'indicator_component_id'] = component_id
        dfs.append(df)

    print("concatenating")
    df = pd.concat(dfs, ignore_index=True)

    # collapse sex/ages
    df = df.merge(db_pops, how='left')

    # set age-groups for aggregation now so it doesn't have to be done twice (for sex_split)
    df.loc[df.indicator_component_id.isin([35, 41]), 'age_group_id'] = 1 # Malnutrition
    df.loc[df.indicator_component_id == 44, 'age_group_id'] = 5 # Child Overweight

    # keep these for later
    df_sex_split = df[~df.indicator_component_id.isin([5, 227])]
    df_smoking = df[df.indicator_component_id == 227]
    df_smoking_sex_split = df_smoking.copy(deep=True)

    df = df[df.indicator_component_id != 227] # remove smoking from main df

    # age/sex aggregate
    df['sex_id'] = 3 # changes everything but Mean PM2.5 which is already aggregated
    print("concatenating") # concat sex-split data
    df = pd.concat([df, df_sex_split], ignore_index=True)
    df = agg.age_sex_aggregate(df, group_cols = index_cols, denominator='population')

    # sex aggregate smoking data before age-standardizing
    df_smoking['sex_id'] = 3
    df = agg.age_sex_aggregate(df, group_cols = index_cols, denominator='population')
    print("concatenating")
    df_smoking = pd.concat([df_smoking, df_smoking_sex_split], axis = 0)

    # aggregate all but smoking to global
    df_global = agg.aggregate_locations_to_global(df, index_cols)

    # aggregate smoking to global and age-standardize global and non-global
    df_smoking_global = agg.aggregate_locations_to_global(df_smoking, index_cols,
                                                          age_standardized=True,
                                                          age_group_years_start=10,
                                                          age_group_years_end=125,
                                                          age_group_id=194)
    # df_smoking_global['units'] = 'sdg'
    df_smoking = agg.age_standardize(df_smoking, index_cols, 10, 125, 194)

    # concat smoking
    df = pd.concat([df, df_smoking], axis = 0)
    df_global = pd.concat([df_global, df_smoking_global], axis=0)
    
    # output
    df = df[index_cols + dw.DRAW_COLS]
    df_global = df_global[index_cols + dw.DRAW_COLS]

    file_dict = dw.RE_FILE_DICT
    for component_id in file_dict.keys():

        path = data_dir + '/' + str(file_dict[component_id]) + '.feather'
        global_path = data_dir + '/' + str(file_dict[component_id]) + '_global'+ '.feather'

        # sdg
        print('outputting ' + str(file_dict[component_id]))
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(path)

        # global
        print('outputting ' + str(file_dict[component_id]) + ' global')
        df_id_global = df_global[df_global.indicator_component_id == component_id]
        df_id_global.reset_index(drop=True, inplace=True)
        df_id_global.to_feather(global_path)

    return df_global


def process_codcorrect_draws(version=dw.CC_VERS):

    index_cols = dw.CC_GROUP_COLS
    component_ids = dw.CC_ALL_AGE_COMPONENT_IDS + dw.CC_THIRTY_SEVENTY_COMPONENT_IDS + dw.CONF_DIS_COMPONENT_IDS
    data_dir = dw.INPUT_DATA_DIR + 'codcorrect' + '/' + str(version)
    
    dfs = []
    for component_id in component_ids:
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        df.loc[:,'indicator_component_id'] = component_id
        dfs.append(df)

    print("concatenating")
    df = pd.concat(dfs, ignore_index=True)

    # convert to numbers
    db_pops = qry.get_pops()
    df = df.merge(db_pops, how='left')
    
    # keep sex split for certain indicators (ncds, road mort, poisoning, homicide)
    df_keep_sex_split = df.loc[df['indicator_component_id'].isin(dw.CC_THIRTY_SEVENTY_COMPONENT_IDS +
        dw.CC_ALL_AGE_COMPONENT_IDS),:] # these age_groups get standardized later

    # collapse sex (and age for conlict and distaster mort)
    df['sex_id'] = 3
    df.loc[df['indicator_component_id'].isin(dw.CONF_DIS_COMPONENT_IDS), 'age_group_id'] = 22
    df = agg.age_sex_aggregate(df, group_cols = index_cols)
    
    # make sure it looks like we expect
    assert set(df.loc[df['cause_id'].isin(dw.CONF_DIS_CAUSES)].age_group_id) == set([22]), \
        'unexpected age group ids found'
    assert set(df.loc[~df['cause_id'].isin(dw.CONF_DIS_CAUSES)].age_group_id) == \
        set(range(2, 21) + range(30, 33) + [235]), \
        'unexpected age group ids found'
    assert set(df.sex_id) == set([3]), 'unexpected sex ids found'

    # concat sex-split data before age-standardizing
    df = pd.concat([df, df_keep_sex_split], axis = 0)

    # prepare for age-standardization
    # all age-standardized except for conflict and disaster mort
    df_conf_dis = df.loc[df['indicator_component_id'].isin(dw.CONF_DIS_COMPONENT_IDS)]
    df_ncds = df.loc[df['indicator_component_id'].isin(dw.CC_THIRTY_SEVENTY_COMPONENT_IDS)]
    df_all_ages = df.loc[df['indicator_component_id'].isin(dw.CC_ALL_AGE_COMPONENT_IDS)]

    # global aggregation
    df_cd_global = agg.aggregate_locations_to_global(df_conf_dis, index_cols)

    df_ncds_global = agg.aggregate_locations_to_global(df_ncds, index_cols,
                                                       age_standardized=True,
                                                       age_group_years_start=30,
                                                       age_group_years_end=70,
                                                       age_group_id=214)

    df_aa_global = agg.aggregate_locations_to_global(df_all_ages, index_cols,
                                                     age_standardized=True,
                                                     age_group_years_start=0,
                                                     age_group_years_end=125,
                                                     age_group_id=27)

    # age standardize
    df_ncds = agg.age_standardize(df_ncds, index_cols, 30, 70, 214)
    df_all_ages = agg.age_standardize(df_all_ages, index_cols, 0, 125, 27)

    # concat all
    print('concatenating')
    df = pd.concat([df_ncds, df_all_ages, df_conf_dis], axis = 0)
    df_global = pd.concat([df_ncds_global, df_aa_global, df_cd_global], axis = 0)

    # output
    df = df[index_cols + dw.DRAW_COLS]
    df_global = df_global[index_cols + dw.DRAW_COLS]

    file_dict = dw.CC_FILE_DICT
    for component_id in file_dict.keys():

        path = data_dir + '/' + file_dict[component_id] + '.feather'
        global_path = data_dir + '/' + file_dict[component_id] + '_global'+ '.feather'

        # sdg
        print('outputting ' + file_dict[component_id])
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(path)

        # global
        print('outputting ' + file_dict[component_id] + ' global')
        df_id_global = df_global[df_global.indicator_component_id == component_id]
        df_id_global.reset_index(drop=True, inplace=True)
        df_id_global.to_feather(global_path)



def process_dismod_draws(past_future, version=dw.DISMOD_VERS):

    if past_future == 'past':
        index_cols = dw.DISMOD_GROUP_COLS
        data_dir = dw.INPUT_DATA_DIR + 'dismod' + '/' + str(version)
        db_pops = qry.get_pops()
    elif past_future == 'future':
        index_cols = ['indicator_component_id'] + INDEX_COLS_FUTURE
        data_dir = dw.FORECAST_DATA_DIR + 'dismod' + '/' + str(version)
        db_pops = load_population()
    else:
        raise ValueError('The past_future arg must be set to "past" or "future".')

    component_ids = [14, 17, 242, 245] # no child sex abuse (pulled later)
    dfs = []
    
    for component_id in component_ids:
        print("pulling {c}".format(c=component_id))
        df = pd.read_feather(data_dir + '/' + str(component_id) + '.feather')
        df.loc[:,'indicator_component_id'] = component_id
        dfs.append(df)

    print("concatenating")
    df = pd.concat(dfs, ignore_index=True)

    df = df[index_cols + dw.DRAW_COLS]

    # COLLAPSE SEX/AGES
    df = df.merge(db_pops, how='left')
    df_sex_split = df[df.indicator_component_id.isin([14, 17])]
    df.loc[df['indicator_component_id'].isin([14, 17]),'sex_id'] = 3 # physical and sexual violence sex aggregation
    df = agg.age_sex_aggregate(df, group_cols = index_cols)
    df = pd.concat([df, df_sex_split], ignore_index=True)

    # AGE STANDARDIZE
    df_age_stand_all_age = df.loc[df['indicator_component_id'].isin([14, 17])]
    df_age_stand_15_plus = df.loc[df['indicator_component_id'].isin([242, 245])] # int partner and non-int partner violence 

    # global
    df_aa_global = agg.aggregate_locations_to_global(df_age_stand_all_age, index_cols,
                                                     age_standardized=True,
                                                     age_group_years_start=0,
                                                     age_group_years_end=125,
                                                     age_group_id=27)

    df_15_plus_global = agg.aggregate_locations_to_global(df_age_stand_15_plus, index_cols,
                                                          age_standardized=True,
                                                          age_group_years_start=15,
                                                          age_group_years_end=125,
                                                          age_group_id=29)

    # national/subnational
    df_age_stand_all_age = agg.age_standardize(df_age_stand_all_age, index_cols, 0, 125, 27)
    df_age_stand_15_plus = agg.age_standardize(df_age_stand_15_plus, index_cols, 15, 125, 29)

    # concat
    print("concatenating")
    df = pd.concat([df_age_stand_all_age, df_age_stand_15_plus], ignore_index=True)
    df_global = pd.concat([df_aa_global, df_15_plus_global], ignore_index=True)

    # output
    df = df[index_cols + dw.DRAW_COLS]

    file_dict = dict(zip(component_ids, ['1094', '1095', '1047', '1098']))

    for component_id in file_dict.keys():

        path = data_dir + '/' + file_dict[component_id] + '.feather'
        global_path = data_dir + '/' + file_dict[component_id] + '_global'+ '.feather'

        # sdg
        print('outputting ' + file_dict[component_id])
        df_id = df[df.indicator_component_id == component_id]
        df_id.reset_index(drop=True, inplace=True)
        df_id.to_feather(path)

        # global
        print('outputting ' + file_dict[component_id] + ' global')
        df_id_global = df_global[df_global.indicator_component_id == component_id]
        df_id_global.reset_index(drop=True, inplace=True)
        df_id_global.to_feather(global_path)

    #############################################
    # child sex abuse
    index_cols = index_cols.remove('indicator_component_id')

    df_csa = pd.read_feather(data_dir + '/' + '1064_age_disagg.feather')
    df_csa = df_csa.merge(db_pops, how='left')

    # aggregation and output
    df_csa.loc[:,'age_group_id'] = 202
    df_csa = agg.age_sex_aggregate(df_csa, group_cols = index_cols)

    df_csa_global = df_csa.copy(deep=True)

    df_csa = df_csa[index_cols + dw.DRAW_COLS]
    df_csa.reset_index(drop=True, inplace=True)
    print('outputting 1064')
    df_csa.to_feather(data_dir + '/' + '1064.feather')

    df_csa_global = agg.aggregate_locations_to_global(df_csa_global, index_cols)
    df_csa_global = df_csa_global[index_cols + dw.DRAW_COLS]
    df_csa_global.reset_index(drop=True, inplace=True)
    print('outputting 1064 global')
    df_csa_global.to_feather(data_dir + '/' + '1064_global.feather')
    
    return df