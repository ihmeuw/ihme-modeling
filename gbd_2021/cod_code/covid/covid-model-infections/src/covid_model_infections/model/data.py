from typing import List, Tuple, Dict
from pathlib import Path
from loguru import logger
import dill as pickle

import pandas as pd


def parent_inheritance(location_id: int,
                       hierarchy: pd.DataFrame,
                       data: pd.DataFrame, data_name: str,):
    # model location hierarchy information
    is_location = hierarchy['location_id'] == location_id
    path_to_top_parent = hierarchy.loc[is_location, 'path_to_top_parent'].item()
    location_name = hierarchy.loc[is_location, 'location_name'].item()
    
    # locs in data
    baseline_locations = data.reset_index()['location_id'].unique().tolist()

    # inherit parent
    if location_id not in baseline_locations:
        for parent_id in reversed(path_to_top_parent.split(',')[:-1]):
            if int(parent_id) in baseline_locations:
                is_parent = hierarchy['location_id'] == int(parent_id)
                parent_name = hierarchy.loc[is_parent, 'location_name'].item()
                logger.info(f'Using {parent_name} estimates of {data_name} for {location_name}.')
                data = data.append(
                    pd.concat({location_id: data.loc[int(parent_id)]}, names=['location_id'])
                )
                break
            else:
                pass
    
    return data


def compile_input_data_object(location_id: int, hierarchy: pd.DataFrame,
                              em_scalar_data: pd.Series,
                              durations: List[Dict[str, int]],
                              daily_deaths: pd.Series, cumul_deaths: pd.Series, ifr: pd.Series, ifr_rr: pd.Series,
                              daily_hospital: pd.Series, cumul_hospital: pd.Series, ihr: pd.Series,
                              daily_cases: pd.Series, cumul_cases: pd.Series, idr: pd.Series,
                              **kwargs,):
    location_model_data = {}
    modeled_location = False
    
    # parent inheritance for rate
    ifr = parent_inheritance(
        location_id, hierarchy, ifr, 'IFR',
    )
    ifr_rr = parent_inheritance(
        location_id, hierarchy, ifr_rr, 'high/low risk IFR relative ratios',
    )
    ihr = parent_inheritance(
        location_id, hierarchy, ihr, 'IHR',
    )
    idr = parent_inheritance(
        location_id, hierarchy, idr, 'IDR',
    )
    
    # deaths
    if location_id in daily_deaths.reset_index()['location_id'].unique().tolist():
        modeled_location = True
        location_model_data.update({'deaths':{'daily': daily_deaths.loc[location_id],
                                              'cumul': cumul_deaths.loc[location_id],
                                              'ratio': ifr.loc[location_id],
                                              'scalar': em_scalar_data,
                                              'lags': [d['exposure_to_death'] for d in durations],},})
    # hospital admissions
    if location_id in daily_hospital.reset_index()['location_id'].unique().tolist():
        modeled_location = True
        location_model_data.update({'hospitalizations':{'daily': daily_hospital.loc[location_id],
                                                        'cumul': cumul_hospital.loc[location_id],
                                                        'ratio': ihr.loc[location_id],
                                                        'scalar': pd.Series([1] * len(em_scalar_data),
                                                                            index=em_scalar_data.index,
                                                                            name=em_scalar_data.name),
                                                        'lags': [d['exposure_to_admission'] for d in durations],},})
    # cases
    if location_id in daily_cases.reset_index()['location_id'].unique().tolist():
        modeled_location = True
        location_model_data.update({'cases':{'daily': daily_cases.loc[location_id],
                                             'cumul': cumul_cases.loc[location_id],
                                             'ratio': idr.loc[location_id],
                                             'scalar': pd.Series([1] * len(em_scalar_data),
                                                                 index=em_scalar_data.index,
                                                                 name=em_scalar_data.name),
                                             'lags': [d['exposure_to_case'] for d in durations],},})
        
    if modeled_location:
        location_pred_rates = {
            'ifr': ifr.loc[location_id],
            'ifr_rr': ifr_rr.loc[location_id],
            'ihr': ihr.loc[location_id],
            'idr': idr.loc[location_id],
        }
    else:
        location_pred_rates = {}
        
    return location_model_data, location_pred_rates, modeled_location


def load_model_inputs(location_id: int, model_in_dir: Path, verbose: bool = True) -> Tuple[Dict, float]:
    hierarchy_path = model_in_dir / 'hierarchy.parquet'
    hierarchy = pd.read_parquet(hierarchy_path)
    location_name = hierarchy.loc[hierarchy['location_id'] == location_id, 'location_name'].item()
    path_to_top_parent = hierarchy.loc[hierarchy['location_id'] == location_id, 'path_to_top_parent'].item()
    is_us = '102' in path_to_top_parent.split(',')
    if verbose:
        logger.info(f'Model location: {location_name}')
        
    pop_path = model_in_dir / 'pop_data.parquet'
    all_populations = pd.read_parquet(pop_path)
    population = all_populations.loc[location_id].item()
    
    model_data_path = model_in_dir / 'model_data.pkl'
    with model_data_path.open('rb') as file:
        model_data = pickle.load(file)
    
    no_deaths = model_data['no_deaths']
    
    em_scalar_path = model_in_dir / 'em_scalar_data.parquet'
    em_scalar_data = pd.read_parquet(em_scalar_path)
    em_scalar_data = parent_inheritance(
        location_id, hierarchy, em_scalar_data, 'total covid scalar',
    )
    em_scalar_data = em_scalar_data.loc[location_id, 'em_scalar']
    
    model_data, pred_rates, modeled_location = compile_input_data_object(
        location_id=location_id, hierarchy=hierarchy, em_scalar_data=em_scalar_data,
        **model_data
    )
    
    vaccine_path = model_in_dir / 'vaccine_data.parquet'
    vaccine_data = pd.read_parquet(vaccine_path)
    vaccine_data = vaccine_data.join(all_populations)
    vaccine_data['cumulative_all_effective'] /= vaccine_data['population']
    del vaccine_data['population']
    vaccine_data = parent_inheritance(
        location_id, hierarchy, vaccine_data, 'vaccination rates',
    )
    vaccine_data = vaccine_data.loc[location_id] * population
    
    cross_variant_immunity_path = model_in_dir / 'cross_variant_immunity.pkl'
    with cross_variant_immunity_path.open('rb') as file:
        cross_variant_immunity = pickle.load(file)
    
    escape_variant_prevalence_path = model_in_dir / 'escape_variant_prevalence.parquet'
    escape_variant_prevalence = pd.read_parquet(escape_variant_prevalence_path)
    escape_variant_prevalence = parent_inheritance(
        location_id, hierarchy, escape_variant_prevalence, 'escape variant prevalence',
    )
    if location_id in escape_variant_prevalence.reset_index()['location_id'].to_list():
        escape_variant_prevalence = escape_variant_prevalence.loc[location_id, 'escape_variant_prevalence']
    else:
        escape_variant_prevalence = pd.Series()
    
    return model_data, pred_rates, vaccine_data, cross_variant_immunity, \
           escape_variant_prevalence, modeled_location, population, location_name, \
           is_us, no_deaths


def load_extra_plot_inputs(location_id: int, model_in_dir: Path):
    sero_path = model_in_dir / 'sero_data.parquet'
    sero_data = pd.read_parquet(sero_path)
    sero_data = sero_data.reset_index()
    sero_data = (sero_data
                 .loc[sero_data['location_id'] == location_id]
                 .drop('location_id', axis=1)
                 .set_index('date'))
        
    ifr_model_data_path = model_in_dir / 'ifr_model_data.parquet'
    ifr_model_data = pd.read_parquet(ifr_model_data_path)
    ifr_model_data = ifr_model_data.reset_index()
    ifr_model_data = (ifr_model_data
                      .loc[ifr_model_data['location_id'] == location_id]
                      .drop('location_id', axis=1)
                      .set_index('date'))
    
    ihr_model_data_path = model_in_dir / 'ihr_model_data.parquet'
    ihr_model_data = pd.read_parquet(ihr_model_data_path)
    ihr_model_data = ihr_model_data.reset_index()
    ihr_model_data = (ihr_model_data
                      .loc[ihr_model_data['location_id'] == location_id]
                      .drop('location_id', axis=1)
                      .set_index('date'))
    
    idr_model_data_path = model_in_dir / 'idr_model_data.parquet'
    idr_model_data = pd.read_parquet(idr_model_data_path)
    idr_model_data = idr_model_data.reset_index()
    idr_model_data = (idr_model_data
                      .loc[idr_model_data['location_id'] == location_id]
                      .drop('location_id', axis=1)
                      .set_index('date'))
    ratio_model_inputs = {
        'deaths': ifr_model_data,
        'hospitalizations': ihr_model_data,
        'cases': idr_model_data,
    }
    
    return sero_data, ratio_model_inputs
