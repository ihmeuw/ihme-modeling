from elmo import run
import pandas as pd
import numpy as np
from db_queries import get_envelope, get_model_results, get_demographics
from db_tools import ezfuncs

################################################################################################

def get_emr_pred(model_version_id):
    path = ("ADDRESS"
            "model_estimate_fit.csv")
    path = path.format(mv=model_version_id)
    emrpred = pd.read_csv(path)
    emrpred = emrpred.ix[emrpred['measure_id'] == 9]
    emrpred['pred_se'] = (emrpred['pred_upper'] - emrpred['pred_lower']) / (2*1.96)
    return emrpred

#can also use model_version_id instead of gbd_id
def combined_get_model_results(gbd_id=None, 
                                location_id='all', 
                                prev_filepath=None, 
                                inc_filepath=None, 
                                model_version_id=263738):
    age_ids = [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235]
    year_ids = [1990,1995,2000,2005,2010,2017]
    sex_ids = [1,2]

    #get incidence and prevalence data
    if (prev_filepath):
        print("Using file for prev")
        prev = pd.read_excel(prev_filepath)
        # get excel
    else:
        print("querying get_model_results for prev...")
        prev = get_model_results('epi', 
                                gbd_id=gbd_id, 
                                measure_id=5, 
                                location_id='all', 
                                year_id=year_ids, 
                                age_group_id=age_ids, 
                                sex_id=sex_ids, 
                                status='best', 
                                gbd_round_id=4)
    
    if (inc_filepath):
        print("Using file for inc")
        inc = pd.read_excel(inc_filepath)
        # get excel
    else:
        print("querying get_model_results for inc...")
        inc = get_model_results('epi', 
                                gbd_id=gbd_id, 
                                measure_id=6, 
                                location_id='all', 
                                year_id=year_ids, 
                                age_group_id=age_ids, 
                                sex_id=sex_ids, 
                                status='best', 
                                gbd_round_id=4)
        
    #prev['prev_se'] = (prev["upper"] - prev["lower"]) / (2*1.96)
    #inc['inc_se'] = (inc["upper"] - inc["lower"]) / (2*1.96)
    prev = prev.rename(columns={'mean':'prev_mean', 
                                'lower':'prev_lower', 
                                'upper':'prev_upper',
                                'standard_error':'prev_se'})
    inc = inc.rename(columns={'mean':'inc_mean', 
                            'lower':'inc_lower', 
                            'upper':'inc_upper',
                            'standard_error':'inc_se'})
    #prev = adj_data_template(df=prev)
    #inc = adj_data_template(df=inc)

    #load custom (HIV-neg + HIV-pos) csmr
    print("loading custom csmr data...")
    csmr = pd.read_csv("FILEPATH")
    #csmr['csmr_se'] = (csmr["upper"] - csmr["lower"]) / (2*1.96)
    csmr = csmr.rename(columns={'mean':'csmr_mean', 
                                'lower':'csmr_lower', 
                                'upper':'csmr_upper',
                                'standard_error':'csmr_se'})
    csmr = csmr[['age_group_id', 
                'location_id', 
                'year_id', 
                'sex_id', 
                'csmr_mean', 
                'csmr_se', 
                'csmr_lower', 
                'csmr_upper']].copy()
    
    #get acmr data
    print("querying get_envelope for acmr...")
    acmr = get_envelope(age_group_id=age_ids, 
                        location_id='all', 
                        year_id=year_ids, 
                        sex_id=sex_ids, 
                        gbd_round_id=5, 
                        with_shock=1, 
                        with_hiv=1, 
                        rates=1)
    acmr['acmr_se'] = (acmr["upper"] - acmr["lower"]) / (2*1.96)
    acmr = acmr.rename(columns={'mean':'acmr_mean', 
                                'lower':'acmr_lower', 
                                'upper':'acmr_upper'})
    
    #get remission data
    #remission should equal 2. upper and lower bounds 1.8-2.2

    #get emr-predicted data
    emrpred = get_emr_pred(model_version_id)

    merge_inc = pd.merge(left=inc, 
                        right=csmr, 
                        on=['age_group_id', 'sex_id', 'year_id', 'location_id'], 
                        how='left')

    merge_inc = pd.merge(left=merge_inc, 
                        right=acmr, 
                        on=['age_group_id', 'sex_id', 'year_id', 'location_id'], 
                        how='left')

    merge_inc = pd.merge(left=merge_inc, 
                        right=emrpred, 
                        on=['age_group_id', 'sex_id', 'year_id'], 
                        how='left')

    merge_inc['rem_mean'] = 2
    merge_inc['rem_se'] = .1020408
    merge_inc = merge_inc.rename(columns={'location_id_x':'location_id'})
    #merge data required for incidence-based emr calculation
    merge_prev = pd.merge(left=prev, 
                        right=csmr, 
                        on=['age_group_id', 'sex_id', 'year_id', 'location_id'], 
                        how='left')
    #merge data required for prevalence-based emr calculation
    return (merge_prev, merge_inc)

##Final calculations
def compute_emr_from_inc(merge_inc):
    #drop rows where csmr data does not exist, as calculation is impossible without it
    merge_inc = merge_inc.dropna(subset=['csmr_mean'])
    merge_inc['mean'] = (merge_inc['csmr_mean'] *
                (merge_inc['rem_mean'] + ( merge_inc['acmr_mean'] - merge_inc['csmr_mean'] ) + merge_inc['pred_mean']) /
                merge_inc['inc_mean'])

    merge_inc['standard error'] = (merge_inc['mean'] *
                np.sqrt(
                    (merge_inc['inc_se'] / merge_inc['inc_mean'])**2 +
                    (merge_inc['csmr_se'] / merge_inc['csmr_mean'])**2 +
                    (merge_inc['acmr_se'] / merge_inc['acmr_mean'])**2 +
                    (merge_inc['pred_se'] / merge_inc['pred_mean'])**2 +
                    (merge_inc['rem_se'] / merge_inc['rem_mean'])**2 
                    ))
    merge_inc = merge_inc.drop(labels=['inc_mean','inc_se','inc_lower','inc_upper',
                                        'csmr_mean','csmr_se','csmr_lower','csmr_upper',
                                        'acmr_mean','acmr_se','acmr_lower','acmr_upper',
                                        'pred_mean','pred_se','pred_lower','pred_upper',
                                        'rem_mean','rem_se',
                                        'location_id_y','run_id_x','run_id_y'],axis=1)
    merge_inc['emr_calc'] = 'incidence'
    merge_inc['measure_id'] = '9'
    merge_inc['measure'] = 'mtexcess'

    return merge_inc

def compute_emr_from_prev(merge_prev):
    merge_prev = merge_prev.dropna(subset=['csmr_mean'])
    merge_prev['mean'] = merge_prev['csmr_mean'] / merge_prev['prev_mean']
    merge_prev['standard error'] = (merge_prev['mean'] *
                                    np.sqrt(
                                        (merge_prev['prev_se']/merge_prev['prev_mean'])**2 + 
                                        (merge_prev['csmr_se'] / merge_prev['csmr_mean'])**2))
    
    merge_prev = merge_prev.drop(labels=['prev_mean','prev_se','prev_lower','prev_upper',
                                        'csmr_mean','csmr_se','csmr_lower','csmr_upper',
                                        'run_id'],axis=1)
    merge_prev['emr_calc'] = 'prevalence'
    merge_prev['measure_id'] = '9'
    merge_prev['measure'] = 'mtexcess'



    return merge_prev

    ###########################################################

def get_emr_results(gbd_id=None, 
                    location_id='all', 
                    prev_fp=None, 
                    inc_fp=None, 
                    model_version_id=263738):

    write_to_excel = True

    (merge_prev, merge_inc) = combined_get_model_results(prev_filepath=prev_fp, 
                                                        inc_filepath=inc_fp, 
                                                        model_version_id=model_version_id)
    emr_prev = compute_emr_from_prev(merge_prev)
    emr_inc = compute_emr_from_inc(merge_inc)
    emr = pd.concat([emr_prev,emr_inc])
    
    # check 4-5 star countries, and drop any data where CSMR is not available.

    print('concatenating and exporting excel file...')
    if write_to_excel:
        emr.to_excel("FILEPATH", 
                    index=False, 
                    sheet_name='extraction', 
                    encoding='utf-8')
