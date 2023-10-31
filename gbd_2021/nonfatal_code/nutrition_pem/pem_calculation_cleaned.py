# %load pem_calculation.py
# %load pem_calculation.py
from nch_utils.ModelableEntity import ModelableEntity
from gbd.constants import measures
from get_draws.api import get_draws
import pandas as pd
import config 
import argparse
from pathlib import Path
import os


models = dict()
def read_inputs(location_id:int):
    from config import input_me_id as inputs
    modelable_entities = dict()
    modelable_entities['prev_pem'] = ModelableEntity(
        me_id = inputs['PEM'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PREVALENCE,
        age_group_id = config.all_ages,
        location_id = location_id).df
    
    modelable_entities['inc_pem'] = ModelableEntity(
        me_id = inputs['PEM'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.INCIDENCE,
        age_group_id = config.all_ages,
        location_id = location_id).df
    
    modelable_entities['prop_edema_in_total_wasting'] = ModelableEntity(
        me_id = inputs['prop_edema_in_total_wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PROPORTION, 
        age_group_id = config.all_ages,
        location_id = location_id).df
    
    modelable_entities['prop_edema_in_sev_wasting'] = ModelableEntity(
        me_id = inputs['prop_edema_in_sev_wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PROPORTION, 
        age_group_id = config.all_ages,
        location_id = location_id).df
    
    modelable_entities['prev_sev_wasting'] = ModelableEntity(
        me_id = inputs['prev_severe_wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PREVALENCE, 
        age_group_id = config.ages_under_5,
        location_id = location_id).df
    
    modelable_entities['prev_mod_wasting'] = ModelableEntity(
        me_id = inputs['prev_moderate_wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PREVALENCE, 
        age_group_id = config.ages_under_5,
        location_id = location_id).df
    
    modelable_entities['prev_total_wasting'] = ModelableEntity(
        me_id = inputs['prev_total_wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PREVALENCE, 
        age_group_id = config.ages_under_5,
        location_id = location_id).df
    
    return modelable_entities

def ignore_measure_id(dataframe):
    if 'measure_id' in dataframe.index.names or 'measure_id' in dataframe.columns:
        return dataframe.reset_index('measure_id').drop(columns='measure_id')
    else:
        return dataframe

def restrict_to_over_5(dataframe):
    ages_above_5 = config.ages_above_5
    return dataframe.query("age_group_id in @ages_above_5")

def pem_pipeline(location_id:int):
    global models
    models = read_inputs(location_id)
    models['ratio_inc_prev'] = ratio_inc_prev(location_id)
    models['sev_wasting_with_edema'] = calc_severe_wasting_with_edema(location_id)
    models['sev_wasting_without_edema'] = calc_severe_wasting_without_edema(location_id)
    models['mod_wasting_with_edema'] = calc_mod_wasting_with_edema(location_id)
    models['mod_wasting_without_edema'] = calc_mod_wasting_without_edema(location_id)

    out_dir = config.results_folder
    for output_description, me_id in config.output_me_id.items():
        out_path = Path(out_dir).joinpath(f'{me_id}')
        out_path.mkdir(parents=True, exist_ok=True)
        file_path = str(out_path.joinpath(f'{location_id}.csv'))
        #models[output_description].to_hdf(file_path, key="draws")
        models[output_description].to_csv(file_path)

def restrict_to_under_5(dataframe):
    under5 = config.ages_under_5
    if 'age_group_id' in dataframe.columns:
        return dataframe.query('age_group_id in @under5')
    else:
        return dataframe.reset_index('age_group_id').query('age_group_id in @under5').set_index('age_group_id', append=True)
    
def ratio_inc_prev(location_id:int):
    pem_prev = models['prev_pem']
    pem_inc = models['inc_pem']
    ratio = ignore_measure_id(pem_inc) / ignore_measure_id(pem_prev)
    return ratio

def calc_severe_wasting_with_edema(location_id:int):
    prop_edema_in_severe_wasting = \
        restrict_to_under_5(ignore_measure_id(models['prop_edema_in_sev_wasting']))

    severe_wasting_prev = \
        ignore_measure_id(models['prev_sev_wasting'])

    severe_with_edema_prev = severe_wasting_prev * prop_edema_in_severe_wasting
    severe_with_edema_prev['measure_id'] = measures.PREVALENCE

    inc_prev_ratio = restrict_to_under_5(ratio_inc_prev(location_id))
    severe_with_edema_inc = severe_with_edema_prev * inc_prev_ratio
    severe_with_edema_inc['measure_id'] = measures.INCIDENCE
    severe_with_edema_prev.set_index('measure_id', append=True, inplace=True)
    severe_with_edema_inc.set_index('measure_id', append=True, inplace=True) 
    severe_with_edema_both = pd.concat([severe_with_edema_prev, 
        severe_with_edema_inc])
    return severe_with_edema_both

def calc_severe_wasting_without_edema(location_id:int):
    #under 5
    prev_sev_wasting = models['prev_sev_wasting']
    prev_sev_wasting_w_edema = models['sev_wasting_with_edema'].loc[:,:,:,:,measures.PREVALENCE]
    u5_sev_prev_wo_edema = prev_sev_wasting - prev_sev_wasting_w_edema

    #proportion at 5
    prop_severe_to_total_1to4 = \
        models['prev_sev_wasting'].query('age_group_id==34') / \
        models['prev_total_wasting'].query('age_group_id == 34')
    prop_severe_to_total_1to4 = prop_severe_to_total_1to4.clip(upper=1)
    prop_severe_to_total_1to4 = prop_severe_to_total_1to4.fillna(1)

    prop_severe_to_total_1to4.reset_index('age_group_id', inplace=True)
    prop_severe_to_total_1to4.drop(columns=['age_group_id'], inplace=True)

    #over 5
    over5 = config.ages_above_5
    dismod_over_5 = models['prev_pem'].query("age_group_id in @over5")
    a5_sev_prev_wo_edema = dismod_over_5 * prop_severe_to_total_1to4

    #join
    all_ages_prev_sev_wo_edema = pd.concat([u5_sev_prev_wo_edema.reset_index(),
        a5_sev_prev_wo_edema.reset_index()]).set_index(['year_id','age_group_id',
                                                        'sex_id','location_id','measure_id'])

    #incidence, all ages 
    inc_prev_ratio = ratio_inc_prev(location_id)
    all_ages_inc_sev_wo_edema = all_ages_prev_sev_wo_edema * inc_prev_ratio
    all_ages_inc_sev_wo_edema = all_ages_inc_sev_wo_edema.reset_index()
    all_ages_inc_sev_wo_edema['measure_id'] = measures.INCIDENCE
    all_ages_inc_sev_wo_edema = all_ages_inc_sev_wo_edema.set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])

    all_ages_both_mod_wo_edema = pd.concat([all_ages_prev_sev_wo_edema.reset_index(), 
                      all_ages_inc_sev_wo_edema.reset_index()]).set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])
    return all_ages_both_mod_wo_edema

def calc_prev_total_wasting_with_edema(location_id:int):
    prop_edema_in_total_wasting = restrict_to_under_5(
        ignore_measure_id(models['prop_edema_in_total_wasting']))
    #models['prop_edema_in_total_wasting'].df
    prev_total_wasting = restrict_to_under_5(
        models['prev_total_wasting'].loc[:,:,:,:,measures.PREVALENCE])

    prev_total_wasting_w_edema = prop_edema_in_total_wasting * prev_total_wasting
    prev_total_wasting_w_edema = prev_total_wasting_w_edema.reset_index()
    prev_total_wasting_w_edema['measure_id'] = measures.PREVALENCE
    prev_total_wasting_w_edema = prev_total_wasting_w_edema.set_index(['year_id',
            'age_group_id','sex_id','location_id','measure_id'])
    return prev_total_wasting_w_edema

def calc_mod_wasting_with_edema(location_id: int):
    prev_total_wasting_w_edema = calc_prev_total_wasting_with_edema(location_id)
    prev_severe_wasting_w_edema = models['sev_wasting_with_edema'].loc[:,:,:,:,measures.PREVALENCE]
    prev_mod_wasting_w_edema = prev_total_wasting_w_edema - \
        prev_severe_wasting_w_edema
    prev_mod_wasting_w_edema = prev_mod_wasting_w_edema.clip(lower=0)
    prev_mod_wasting_w_edema = prev_mod_wasting_w_edema.reset_index()
    prev_mod_wasting_w_edema['measure_id'] = measures.PREVALENCE
    prev_mod_wasting_w_edema = prev_mod_wasting_w_edema.set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])

    ### NEED TO ENSURE THAT I'M NOT MIXING UP PREVALENCE AND INCIDENCE
    inc_prev_ratio = restrict_to_under_5(ratio_inc_prev(location_id))
    inc_mod_wasting_w_edema = inc_prev_ratio * prev_mod_wasting_w_edema.loc[:,:,:,:,measures.PREVALENCE] 
    inc_mod_wasting_w_edema['measure_id'] = measures.INCIDENCE
    #inc_mod_wasting_w_edema.set_index('measure_id', append=True, inplace=True)
    mod_wasting_w_edema = pd.concat([prev_mod_wasting_w_edema.reset_index(),
        inc_mod_wasting_w_edema.reset_index()]).set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])
    return mod_wasting_w_edema


def calc_mod_wasting_without_edema(location_id:int):
    #under 5
    prev_mod_wasting = restrict_to_under_5(models['prev_mod_wasting'])
    prev_mod_wasting_w_edema = restrict_to_under_5(
        calc_mod_wasting_with_edema(location_id).loc[:,:,:,:,measures.PREVALENCE])
    u5_mod_prev_wo_edema = prev_mod_wasting - prev_mod_wasting_w_edema
    u5_mod_prev_wo_edema = u5_mod_prev_wo_edema.clip(lower=0)
    #proportion of moderate in all wasting for age group 34
    prop_moderate_to_total_1to4 = \
        prev_mod_wasting.query('age_group_id==34') / \
        models['prev_total_wasting'].query('age_group_id == 34')
    prop_moderate_to_total_1to4.reset_index('age_group_id', inplace=True)
    prop_moderate_to_total_1to4.drop(columns=['age_group_id'], inplace=True)
    prop_moderate_to_total_1to4 = prop_moderate_to_total_1to4.clip(upper=1)
    prop_moderate_to_total_1to4 = prop_moderate_to_total_1to4.fillna(1)
    #over 5
    over5 = config.ages_above_5
    dismod_over_5 = models['prev_pem'].query("age_group_id in @over5")
    o5_mod_prev_wo_edema = dismod_over_5 * prop_moderate_to_total_1to4
    #join
    all_ages_prev_mod_wo_edema = pd.concat([u5_mod_prev_wo_edema.reset_index(),
        o5_mod_prev_wo_edema.reset_index()]).set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])

    #incidence, all ages
    inc_prev_ratio = ratio_inc_prev(location_id)
    all_ages_inc_mod_wo_edema = all_ages_prev_mod_wo_edema * inc_prev_ratio
    all_ages_inc_mod_wo_edema = all_ages_inc_mod_wo_edema.reset_index()
    all_ages_inc_mod_wo_edema['measure_id'] = measures.INCIDENCE
    all_ages_inc_mod_wo_edema = all_ages_inc_mod_wo_edema.set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])

    all_ages_both_mod_wo_edema = pd.concat([all_ages_prev_mod_wo_edema.reset_index(), 
                      all_ages_inc_mod_wo_edema.reset_index()]).set_index(['year_id',
                            'age_group_id','sex_id','location_id','measure_id'])
    return all_ages_both_mod_wo_edema

def calc_severe_wasting_with_edema_old(location_id:int):
    prop_edema_in_severe_wasting = ignore_measure_id(ModelableEntity(
        me_id = inputs['Proportion of edema among severe wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PROPORTION, 
        age_group_id = config.ages_under_5,
        location_id = location_id))
    
    severe_wasting_prev = ignore_measure_id(ModelableEntity(
        me_id = inputs['Severe wasting'], 
        gbd_round_id = config.gbd_round_id,
        decomp_step = config.decomp_step,
        measure_id= measures.PREVALENCE, 
        age_group_id = config.ages_under_5,
        location_id = location_id))
    
    severe_with_edema_prev = severe_wasting_prev * prop_edema_in_severe_wasting
    severe_with_edema_prev['measure_id'] = measures.PREVALENCE

    inc_prev_ratio = ratio_inc_prev(location_id)
    severe_with_edema_inc = severe_with_edema_prev * inc_prev_ratio
    severe_with_edema_inc['measure_id'] = measures.INCIDENCE
    severe_with_edema_prev.set_index('measure_id', append=True, inplace=True)
    severe_with_edema_inc.set_index('measure_id', append=True) 
    severe_with_edema_both = pandas.concat([severe_with_edema_prev, severe_with_edema_inc])
    return severe_with_edema_both

if  __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("--location_id",
        help="the location_id that will be used in the calculations", type=int)
    args = parser.parse_args()
    
    pem_pipeline(args.location_id)