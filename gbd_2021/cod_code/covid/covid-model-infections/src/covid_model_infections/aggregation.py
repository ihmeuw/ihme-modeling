import sys
from typing import List, Dict, Tuple
from pathlib import Path
from tqdm import tqdm
from functools import partial
from multiprocessing import Pool

import pandas as pd
import numpy as np

from covid_model_infections import model


def aggregate_md_data_dict(md_data: Dict, hierarchy: pd.DataFrame, measures: List[str], mp_threads: int) -> Dict:
    parent_ids = hierarchy.loc[hierarchy['most_detailed'] != 1, 'location_id'].to_list()

    if mp_threads > 1:
        _cpd = partial(
            create_parent_dict,
            md_data=md_data, hierarchy=hierarchy, measures=measures,
        )
        with Pool(mp_threads - 1) as p:
            agg_data = list(tqdm(p.imap(_cpd, parent_ids), total=len(parent_ids), file=sys.stdout))
        
    else:
        agg_data = []
        for parent_id in tqdm(parent_ids, total=len(parent_ids), file=sys.stdout):
            agg_data.append(create_parent_dict(parent_id, md_data, hierarchy, measures))

    agg_data = {parent_id: ad for parent_id, ad in zip(parent_ids, agg_data)}
    
    return agg_data


def get_child_ids(parent_id: int, hierarchy: pd.DataFrame) -> List:
    is_child = (hierarchy['path_to_top_parent'].apply(lambda x: str(parent_id) in x.split(',')))
    is_most_detailed = hierarchy['most_detailed'] == 1
    child_ids = hierarchy.loc[is_child & is_most_detailed, 'location_id'].to_list()
    
    return child_ids

    
def create_parent_dict(parent_id: int, md_data: Dict, hierarchy: pd.DataFrame, measures: str) -> Dict:
    child_ids = get_child_ids(parent_id, hierarchy)
    children_data = [md_data.get(child_id, None) for child_id in child_ids]
    children_data = [cd for cd in children_data  if cd is not None]
    
    parent_data = sum_data_from_child_dicts(children_data, measures)
    
    return parent_data

    
def sum_data_from_child_dicts(children_data: Dict, measures: List[str]) -> Dict:
    child_dict = {}
    for measure in measures:
        measure_dict = {}
        metrics = [list(child_data.get(measure, {}).keys()) for child_data in children_data]
        if not all([bool(m) for m in metrics]) or not metrics:
            metrics = []
        else:
            metrics = metrics[0]
        for metric in metrics:
            metric_data = []
            for child_data in children_data:
                if metric == 'ratio':
                    ratio_data = (child_data[measure]['daily'] / child_data[measure][metric]['ratio']).rename('ratio')
                    ratio_fe_data = (child_data[measure]['daily'] / child_data[measure][metric]['ratio_fe']).rename('ratio_fe')
                    metric_data.append(pd.concat([ratio_data, ratio_fe_data], axis=1).dropna())
                elif metric == 'scalar':
                    scalar_data = child_data[measure]['cumul'].max()  * child_data[measure]['scalar']
                    metric_data.append(scalar_data)
                elif isinstance(child_data[measure][metric], list) and metric != 'lags':
                    metric_data.append(pd.concat(child_data[measure][metric]).groupby(level=0).mean())
                else:
                    metric_data.append(child_data[measure][metric])
            if isinstance(metric_data[0], int) or isinstance(metric_data[0], list):
                metric_data = metric_data[0]
            else:
                metric_data = pd.concat(metric_data)
                if metric == 'scalar':
                    metric_data = metric_data.groupby(level=0).sum()
                elif metric_data.index.names[0] == 'draw':
                    if metric_data.index.names[1] != 'date':
                        raise ValueError('If draw in index, date should be second level.')
                    metric_data_count = metric_data.groupby(level=1).count()
                    metric_data_count /= metric_data.reset_index()['draw'].max() + 1
                    keep_idx = metric_data_count[metric_data_count == len(children_data)].index
                    metric_data = metric_data.groupby(level=[1, 0]).sum()
                    metric_data = metric_data.loc[keep_idx]
                    metric_data = metric_data.groupby(level=[1, 0]).sum()
                else:
                    if metric_data.index.names != ['date']:
                        raise ValueError('Cannot aggregate multi-index.')
                    metric_data_count = metric_data.groupby(level=0).count()
                    keep_idx = metric_data_count[metric_data_count == len(children_data)].index
                    metric_data = metric_data.groupby(level=0).sum()
                    metric_data = metric_data.loc[keep_idx]
            measure_dict.update({metric: metric_data})
        
        if 'ratio' in metrics:
            if measure_dict['daily'].empty:
                 measure_dict['ratio'] = (measure_dict['ratio'] * np.nan).dropna()
            else:
                ratio_data = (measure_dict['daily'] / measure_dict['ratio']['ratio']).rename('ratio')
                ratio_fe_data = (measure_dict['daily'] / measure_dict['ratio']['ratio_fe']).rename('ratio_fe')
                measure_dict['ratio'] = pd.concat([ratio_data, ratio_fe_data], axis=1).dropna()
        
        if 'scalar' in metrics:
            if measure_dict['cumul'].empty:
                 measure_dict['scalar'] = (measure_dict['scalar'] * np.nan).dropna()
            else:
                measure_dict['scalar'] /= measure_dict['cumul'].max()
            
        if metrics:
            child_dict.update({measure: measure_dict})
    
    return child_dict


def subset_to_parent_md_draws(md_draws: pd.DataFrame, parent_id: int, hierarchy: pd.DataFrame) -> pd.DataFrame:
    child_ids = get_child_ids(parent_id, hierarchy)
    
    idx_names = md_draws.index.names
    parent_draws = md_draws.reset_index()
    parent_draws['parent_id'] = parent_id
    parent_draws = parent_draws.loc[parent_draws['location_id'].isin(child_ids)]
    
    return parent_draws.set_index(idx_names).sort_index()
    
    
def aggregate_md_draws(md_draws: pd.DataFrame, hierarchy: pd.DataFrame, mp_threads: int) -> pd.DataFrame:
    parent_ids = hierarchy.loc[hierarchy['most_detailed'] != 1, 'location_id'].to_list()
    parent_draws_list = [subset_to_parent_md_draws(md_draws, parent_id, hierarchy) for parent_id in parent_ids]
    
    empty_parent_draws = [parent_draws.empty for parent_draws in parent_draws_list]
    parent_ids = [parent_id for parent_id, is_empty in zip(parent_ids, empty_parent_draws) if not is_empty]
    parent_draws_list = [parent_draws for parent_draws, is_empty in zip(parent_draws_list, empty_parent_draws) if not is_empty]
    
    if mp_threads > 1:
        with Pool(mp_threads - 1) as p:
            agg_draws = list(tqdm(p.imap(create_parent_draws, parent_draws_list), total=len(parent_ids), file=sys.stdout))
    else:
        agg_draws = []
        for parent_draws in tqdm(parent_draws_list, total=len(parent_ids), file=sys.stdout):
            agg_draws.append(create_parent_draws(parent_draws))
    agg_draws = pd.concat(agg_draws)
    
    return agg_draws
    

def create_parent_draws(parent_draws: pd.DataFrame) -> pd.DataFrame:
    n_child_locations = parent_draws.reset_index()['location_id'].unique().size
    parent_id = parent_draws['parent_id'].unique().item()
    del parent_draws['parent_id']
    if parent_draws.index.names != ['location_id', 'date']:
        raise ValueError("Multi-index differs from expected (['location_id', 'date']).")
    parent_draws_count = parent_draws.groupby(level=1).count().iloc[:,0]
    keep_idx = parent_draws_count[parent_draws_count == n_child_locations].index
    nulls = parent_draws.isnull().groupby(level=1).sum() > 0
    parent_draws = parent_draws.groupby(level=1).sum()
    parent_draws = parent_draws.cumsum()
    parent_draws[nulls] = np.nan
    parent_draws = parent_draws.loc[keep_idx]
    parent_draws = parent_draws.diff().fillna(parent_draws)
    parent_draws['location_id'] = parent_id
    parent_draws = (parent_draws
                    .reset_index()
                    .set_index(['location_id', 'date'])
                    .sort_index())
    
    return parent_draws


def fill_w_region(sub_location: int, infections_draws: pd.DataFrame,
                  hierarchy: pd.DataFrame, pop_data: pd.DataFrame) -> pd.DataFrame:
    region_id = int(hierarchy.loc[hierarchy['location_id'] == sub_location, 'path_to_top_parent'].str.split(',').item()[2])
    region_countries = hierarchy.loc[(hierarchy['region_id'] == region_id) & (hierarchy['level'] == 3), 'location_id'].to_list()
    
    infections_draws = infections_draws.reset_index()
    infections_draws = infections_draws.loc[infections_draws['location_id'].isin(region_countries)]
    infections_draws = infections_draws.set_index(['location_id', 'date'])
    rc_populations = infections_draws.join(pop_data)[['population']].values
    infections_draws /= rc_populations
    
    n_locations = infections_draws.reset_index()['location_id'].unique().size
    if infections_draws.index.names != ['location_id', 'date']:
        raise ValueError("Multi-index differs from expected (['location_id', 'date']).")
    loc_date_count = infections_draws.groupby(level=1).count().iloc[:,0]
    keep_idx = loc_date_count[loc_date_count == n_locations].index
    #nulls = infections_draws.isnull().groupby(level=1).sum() > 0
    infections_draws = infections_draws.groupby(level=1).mean()
    #infections_draws[nulls] = np.nan
    infections_draws = infections_draws.loc[keep_idx]
    infections_draws *= pop_data.loc[sub_location].item()
    infections_draws['location_id'] = sub_location
    infections_draws = (infections_draws
                        .reset_index()
                        .set_index(['location_id', 'date'])
                        .sort_index())
    
    return infections_draws


def get_sub_loc_deaths(sub_location: int, n_draws: int,
                       sub_infections_draws: pd.DataFrame, ifr: pd.DataFrame,
                       durations: List[Dict], reported_deaths: pd.Series,) -> Tuple[pd.DataFrame, pd.Series]:
    if sub_location in reported_deaths.reset_index()['location_id'].to_list():
        reported_deaths = reported_deaths.loc[sub_location]
    else:
        reported_deaths = 1
        
    loc_deaths = []
    loc_scalar = []
    for draw in range(n_draws):
        _ifr = ifr.loc[sub_location, draw]['ratio']
        _deaths = sub_infections_draws.loc[sub_location, f'draw_{draw}'].reset_index()
        _deaths['date'] += pd.Timedelta(days=durations[draw]['exposure_to_death'])
        _deaths = _deaths.set_index('date').loc[:, f'draw_{draw}']
        _deaths = (_deaths * _ifr).dropna().rename(f'draw_{draw}')
        trim_days = durations[draw]['exposure_to_death'] - durations[draw]['exposure_to_case']
        _deaths = _deaths[:-trim_days]
        loc_scalar.append(_deaths.sum() / reported_deaths)
        loc_deaths.append(_deaths)
    loc_deaths = pd.concat(loc_deaths, axis=1).dropna()
    loc_deaths['location_id'] = sub_location
    loc_deaths = (loc_deaths
                  .reset_index()
                  .set_index(['location_id', 'date'])
                  .sort_index())
    loc_scalar = pd.DataFrame({'draw': list(range(n_draws)), 'location_id': sub_location, 'em_scalar': loc_scalar,})
    loc_scalar = (loc_scalar
                  .set_index(['draw', 'location_id'])
                  .sort_index()
                  .loc[:, 'em_scalar'])
    
    return loc_deaths, loc_scalar


def plot_aggregate(location_id: int,
                   inputs: Dict, outputs: Dict, infections_draws: pd.DataFrame,
                   hierarchy: pd.DataFrame,
                   pop_data: pd.DataFrame,
                   sero_data: pd.DataFrame,
                   cross_variant_immunity: List,
                   escape_variant_prevalence: pd.DataFrame,
                   ifr_model_data: pd.DataFrame,
                   ihr_model_data: pd.DataFrame,
                   idr_model_data: pd.DataFrame,
                   plot_dir: Path):
    sero_data = sero_data.reset_index()
    sero_data = (sero_data
                 .loc[sero_data['location_id'] == location_id]
                 .drop('location_id', axis=1)
                 .set_index('date'))
    
#     if location_id in escape_variant_prevalence.reset_index()['location_id'].to_list():
#         escape_variant_prevalence = escape_variant_prevalence.loc[location_id]
#     else:
    escape_variant_prevalence = pd.DataFrame()
    
    population = pop_data.loc[location_id].item()
    
    location_name = hierarchy.loc[hierarchy['location_id'] == location_id, 'location_name'].item()
    
    infections_mean = infections_draws.mean(axis=1).rename('infections')
    
    ratio_model_inputs = {}
    for measure, ratio_model_data in [('deaths', ifr_model_data),
                                      ('hospitalizations', ihr_model_data),
                                      ('cases', idr_model_data)]:
        ratio_model_data = ratio_model_data.copy()
        ratio_model_data = ratio_model_data.reset_index()
        ratio_model_data = (ratio_model_data
                            .loc[ratio_model_data['location_id'] == location_id]
                            .drop('location_id', axis=1)
                            .set_index('date'))
        ratio_model_inputs.update({measure: ratio_model_data})
        
    for measure in outputs.keys():
        outputs[measure]['infections_daily'] = [outputs[measure]['infections_daily']]
        outputs[measure]['infections_cumul'] = [outputs[measure]['infections_cumul']]

    model.plotter.plotter(
        plot_dir, location_id, location_name,
        inputs, sero_data, ratio_model_inputs, cross_variant_immunity, escape_variant_prevalence,
        outputs, infections_mean, infections_draws, population
    )
    