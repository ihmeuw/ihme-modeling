import sys
from collections import defaultdict
from typing import Dict, List, Tuple
from loguru import logger
from copy import deepcopy
import functools
from pathos import multiprocessing
from tqdm import tqdm

import pandas as pd
import numpy as np

from mrtool import MRData
from mrtool.core.other_sampling import extract_simple_lme_specs, extract_simple_lme_hessian
from covid_historical_model.mrbrt import mrbrt
from covid_historical_model.cluster import OMP_NUM_THREADS
from covid_historical_model.utils.misc import get_random_seed


def run_cascade(model_name: str,
                model_data: pd.DataFrame,
                hierarchy: pd.DataFrame,
                var_args: Dict,
                global_prior_dict: Dict,
                location_prior_dict: Dict,
                level_lambdas: Dict,
                child_cutoff_level: int = 3,
                verbose: bool = True,):
    '''
    NOTE: `level_lambdas` apply to the stdev of the level to which they are keyed, and thus
        as priors for the next level. If no new data is added, it is multiplicatively applied.
        
    NOTE: If country level data is present, only using that in country model, and is thus the 
        estimate on which predictions for subnational locations without data are based - this
        is controlled by `child_cutoff_level`.
    '''
    model_locs = hierarchy['location_id'].isin(model_data['location_id'].to_list())
    locs_in_model_path = hierarchy.loc[model_locs, 'path_to_top_parent'].to_list()
    locs_in_model_path = list(set([int(l) for p in locs_in_model_path for l in p.split(',')]))
    is_cascade_location = hierarchy['location_id'].isin(locs_in_model_path)
    cascade_hierarchy = (hierarchy
                         .loc[is_cascade_location, ['location_id', 'level']])
    cascade_hierarchy = [(level, cascade_hierarchy.loc[cascade_hierarchy['level'] == level, 'location_id'].to_list())
                         for level in sorted(cascade_hierarchy['level'].unique())]
    
    mr_model_dict = {}
    prior_dict = {fe_var: {} for fe_var in var_args['fe_vars'] if fe_var not in global_prior_dict.keys()}
    prior_dict.update(global_prior_dict)
    prior_dicts = {l: prior_dict for l in cascade_hierarchy[0][1]}
    
    global_mr_data = mrbrt.create_mr_data(model_data,
                                          var_args['dep_var'], var_args['dep_var_se'],
                                          var_args['fe_vars'], var_args['group_var'])
    for level, location_ids in cascade_hierarchy:
        logger.debug(f'Modeling hierarchy level {level} ({len(location_ids)} location-models).')
        level_mr_model_dict, level_prior_dict = run_level(
            model_name=model_name,
            level_lambda=level_lambdas[level],
            level=level,
            location_ids=location_ids,
            model_data=model_data,
            hierarchy=hierarchy,
            prior_dicts=prior_dicts,
            var_args=var_args,
            location_prior_dict=location_prior_dict,
            child_cutoff_level=child_cutoff_level,
            global_mr_data=global_mr_data,
            verbose=verbose,
        )
        if level == 0:
            prior_dicts = {}
        mr_model_dict.update(level_mr_model_dict)
        prior_dicts.update(level_prior_dict)
        
    return mr_model_dict, prior_dicts


def run_level(model_name: str,
              level_lambda: Dict,
              level: int,
              location_ids: List[int],
              model_data: pd.DataFrame,
              hierarchy: pd.DataFrame,
              prior_dicts: Dict,
              var_args: Dict,
              location_prior_dict: Dict,
              child_cutoff_level: int,
              global_mr_data: MRData,
              verbose: bool,):
    level_mr_model_dict = {}
    level_prior_dicts = {}
    
    if verbose:
        logger.info(f'Modeling hierarchy level {level}.')
    _rl = functools.partial(
        run_location,
        model_name=model_name,
        level_lambda=level_lambda,
        level=level,
        model_data=model_data,
        hierarchy=hierarchy,
        prior_dicts=prior_dicts,
        var_args=var_args,
        location_prior_dict=location_prior_dict,
        child_cutoff_level=child_cutoff_level,
        global_mr_data=global_mr_data,
    )
    with multiprocessing.ProcessPool(int(OMP_NUM_THREADS)) as p:
        results = list(tqdm(p.imap(_rl, location_ids), total=len(location_ids), file=sys.stdout))
    level_mr_model_dict = {location_id: result[0] for location_id, result in zip(location_ids, results)}
    level_prior_dicts = {location_id: result[1] for location_id, result in zip(location_ids, results)}

    return level_mr_model_dict, level_prior_dicts


def run_location(location_id: int,
                 model_name: str,
                 level_lambda: Dict,
                 level: int,
                 model_data: pd.DataFrame,
                 hierarchy: pd.DataFrame,
                 prior_dicts: Dict,
                 var_args: Dict,
                 location_prior_dict: Dict,
                 child_cutoff_level: int,
                 global_mr_data: MRData):
    parent_id = hierarchy.loc[hierarchy['location_id'] == location_id, 'parent_id'].item()
    parent_prior_dict = prior_dicts[parent_id]
    location_in_path_hierarchy = hierarchy['path_to_top_parent'].apply(lambda x: str(location_id) in x.split(','))
    if level <= child_cutoff_level and location_id in model_data['location_id'].to_list():
        child_locations = [location_id]
    else:
        child_locations = hierarchy.loc[location_in_path_hierarchy, 'location_id'].to_list()
    location_in_path_model = model_data['location_id'].isin(child_locations)
    location_model_data = model_data.loc[location_in_path_model].copy()
    location_mr_model, _location_prior_dict = model_location(
        model_name=model_name,
        location_id=location_id,
        model_data=location_model_data,
        prior_dict=parent_prior_dict,
        location_prior_dict=location_prior_dict.get(location_id, {}),
        level_lambda=level_lambda,
        global_mr_data=global_mr_data,
        var_args=var_args,
    )

    return location_mr_model, _location_prior_dict


def model_location(model_name: str,
                   location_id: int,
                   model_data: pd.DataFrame,
                   prior_dict: Dict,
                   location_prior_dict: Dict,
                   level_lambda: Dict,
                   global_mr_data: MRData,
                   var_args: Dict,):
    location_var_args = deepcopy(var_args)
    combined_prior_dict = {}
    for data_var in list(set(location_var_args['fe_vars'] + location_var_args['re_vars'])):
        data_var_dict = prior_dict.get(data_var)
        data_var_dict.update(location_var_args['prior_dict'].get(data_var, {}))
        data_var_dict.update(location_prior_dict.get(data_var, {}))
        combined_prior_dict.update({data_var: {**data_var_dict}})
    location_var_args['prior_dict'] = combined_prior_dict
    key = get_random_seed(f'{model_name}_{location_id}')
    np.random.seed(key)
    mr_model = mrbrt.run_mr_model(
        model_data=model_data,
        global_mr_data=global_mr_data,
        **location_var_args
    )
    model_specs = extract_simple_lme_specs(mr_model)
    beta_mean = model_specs.beta_soln
    beta_std = np.sqrt(np.diag(np.linalg.inv(extract_simple_lme_hessian(model_specs))))
    for iv, iv_idx in zip(location_var_args['fe_vars'], mr_model.x_vars_indices):
        beta_std[iv_idx] *= level_lambda[iv]
    beta_solution = np.vstack([beta_mean, beta_std])
    prior_dict = {iv: {'prior_beta_gaussian': beta_solution[:, iv_idx]}
                  for iv, iv_idx in zip(var_args['fe_vars'], mr_model.x_vars_indices)}

    return mr_model, prior_dict


def find_nearest_modeled_parent(path_to_top_parent_str: str,
                                modeled_locations: List[int],):
    path_to_top_parent = list(reversed([int(l) for l in path_to_top_parent_str.split(',')]))
    for location_id in path_to_top_parent:
        if location_id in modeled_locations:
            return location_id
    raise ValueError(f'No modeled location present in hierarchy for {path_to_top_parent[0]}.')


def predict_cascade(pred_data: pd.DataFrame,
                    hierarchy: pd.DataFrame,
                    mr_model_dict: Dict,
                    pred_replace_dict: Dict,
                    pred_exclude_vars: List,
                    var_args: Dict,
                    verbose: bool = True,):
    if verbose:
        logger.info('Compiling predictions.')
    random_effects = pd.DataFrame(index=pd.Index([], name='location_id'))
    modeled_locations = list(mr_model_dict.keys())
    model_location_map = {l: find_nearest_modeled_parent(p, modeled_locations) for l, p in \
                          zip(hierarchy['location_id'].to_list(), hierarchy['path_to_top_parent'].to_list())}
    
    # should not need this, should figure out level0 loc(s) on its own
    global_model_location_id = 1
    pred = []
    pred_fe = []
    location_ids = hierarchy['location_id'].to_list()
    location_ids = [location_id for location_id in location_ids if location_id in pred_data['location_id'].to_list()]
    pred_location_map = defaultdict(list)
    for location_id in location_ids:
        location_pred_fe, _ = mrbrt.predict(
            pred_data=pred_data.loc[pred_data['location_id'] == location_id].reset_index(drop=True).copy(),
            hierarchy=hierarchy,
            mr_model=mr_model_dict[global_model_location_id],
            pred_replace_dict=pred_replace_dict,
            pred_exclude_vars=pred_exclude_vars,
            **var_args
        )
        pred_fe += [location_pred_fe.set_index(['location_id', 'date']).loc[:, var_args['dep_var']].rename(f"pred_fe_{var_args['dep_var']}")]
        
        model_location_id = model_location_map[location_id]
        if location_id != model_location_id:
            pred_location_map[model_location_id].append(location_id)

        location_pred, _ = mrbrt.predict(
            pred_data=pred_data.loc[pred_data['location_id'] == location_id].reset_index(drop=True).copy(),
            hierarchy=hierarchy,
            mr_model=mr_model_dict[model_location_id],
            pred_replace_dict=pred_replace_dict,
            pred_exclude_vars=pred_exclude_vars,
            **var_args
        )
        pred += [location_pred.set_index(['location_id', 'date']).loc[:, var_args['dep_var']].rename(f"pred_{var_args['dep_var']}")]
    pred = pd.concat(pred)
    pred_fe = pd.concat(pred_fe)

    if pred_location_map:
        location_map_msg = (
            '\n'
            'Substituted Location Map\n'
            '========================='
        )
        for model_loc, sub_locs in pred_location_map.items():
            location_map_msg += f'\n{model_loc:<4}: {sub_locs}'

    if verbose:
        logger.info(location_map_msg)
    
    return pred, pred_fe, model_location_map
