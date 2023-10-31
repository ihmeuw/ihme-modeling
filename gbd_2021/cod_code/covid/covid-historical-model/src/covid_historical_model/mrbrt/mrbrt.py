from typing import List, Dict, Tuple
from loguru import logger

import pandas as pd
import numpy as np

from mrtool import MRData, LinearCovModel, MRBRT

from covid_historical_model.utils.misc import suppress_stdout

# TODO: pass in trans function for prediction (rather than inferring from name)


def create_mr_data(model_data: pd.DataFrame, dep_var: str, dep_var_se: str, fe_vars: List[str],
                   group_var: str, pred: bool = False, **kwargs):
    if pred:
        mr_data = MRData(
            covs={fe_var: model_data[fe_var].values for fe_var in fe_vars},
            study_id=model_data[group_var].values
        )
    else:
        mr_data = MRData(
            obs=model_data[dep_var].values,
            obs_se=model_data[dep_var_se].values,
            covs={fe_var: model_data[fe_var].values for fe_var in fe_vars},
            study_id=model_data[group_var].values
        )
    
    return mr_data


def run_mr_model(model_data: pd.DataFrame,
                 dep_var: str,
                 dep_var_se: str,
                 fe_vars: List[str],
                 re_vars: List[str],
                 group_var: str,
                 inlier_pct: float = 1.,
                 inner_max_iter: int = 1000,
                 outer_max_iter: int = 1000,
                 prior_dict: Dict = None,
                 global_mr_data: MRData = None,
                 **kwargs) -> MRBRT:
    mr_data = create_mr_data(model_data, dep_var, dep_var_se, fe_vars, group_var)
    
    if len(set(re_vars) - set(fe_vars)) > 0:
        raise ValueError('RE vars must also be FE vars.')
    if prior_dict is None:
        prior_dict = {fe_var: {} for fe_var in fe_vars}
    cov_models = [LinearCovModel(fe_var, use_re=fe_var in re_vars, **prior_dict[fe_var]) for fe_var in fe_vars]

    with suppress_stdout():
        mr_model = MRBRT(mr_data, cov_models, inlier_pct=inlier_pct)
        mr_model.attach_data(global_mr_data)
        mr_model.fit_model(inner_max_iter=inner_max_iter, outer_max_iter=outer_max_iter)

    return mr_model


def apply_parent_random_effects(pred_data: pd.DataFrame, hierarchy: pd.DataFrame, mr_model: MRBRT, verbose: bool):
    # # duplicate REs for child locations
    # parent_ids = [location_id for location_id in random_effects.index if location_id in hierarchy.loc[hierarchy['most_detailed'] == 0, 'location_id'].to_list()]
    # child_ids_lists = [hierarchy.loc[hierarchy['path_to_top_parent'].str.contains(f',{parent_id},'), 'location_id'].to_list() for parent_id in parent_ids]
    # child_ids_lists = [list(set(child_ids) - set(random_effects.index)) for child_ids in child_ids_lists]
    # parent_children_pairs = list(zip(parent_ids, child_ids_lists))
    # parent_children_pairs = [(parent_id, child_ids) for parent_id, child_ids in parent_children_pairs if len(child_ids) > 0]

    # parent_random_effects = []
    # for parent_id, child_ids in parent_children_pairs:
    #     parent_name = hierarchy.loc[hierarchy['location_id'] == parent_id, 'location_name'].item()
    #     child_names = hierarchy.loc[hierarchy['location_id'].isin(child_ids), 'location_name'].to_list()
    #     child_names = ', '.join(child_names)
    #     if verbose:
    #         logger.info(f'Using parent {parent_name} RE for {child_names}.')
    #     parent_random_effects += [random_effects.loc[parent_id].rename(child_id) for child_id in child_ids]
    # parent_random_effects = pd.DataFrame(parent_random_effects)
    # parent_random_effects.index.names = ['location_id']
    # random_effects = random_effects.append(parent_random_effects).sort_index()
    # if not random_effects.index.is_unique:
    #     raise ValueError('Duplicated random effect in process of applying parents.')
    # pass
    pass


def predict(pred_data: pd.DataFrame,
            hierarchy: pd.DataFrame,
            mr_model: MRBRT,
            pred_replace_dict: Dict[str, str],
            pred_exclude_vars: List[str],
            dep_var: str,
            dep_var_se: str,
            fe_vars: List[str],
            re_vars: List[str],
            group_var: str,
            **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    keep_vars = list(pred_replace_dict.keys()) + fe_vars
    if len(set(keep_vars)) != len(keep_vars):
        raise ValueError('Duplicate in replace_var + fe_vars.')
    for replace_var in list(pred_replace_dict.values()):
        if replace_var in pred_data.columns:
            del pred_data[replace_var]
    pred_data = pred_data.rename(columns=pred_replace_dict)
    pred_data = (pred_data
                 .loc[:, ['location_id', 'date'] + fe_vars]
                 .dropna()
                 .drop_duplicates())
    for pred_exclude_var in pred_exclude_vars:
        pred_data[pred_exclude_var] = 0
    
    if re_vars:
        raise ValueError('Not propagating random effects (finish `apply_parent_random_effects` method).')
    
    pred_mr_data = create_mr_data(pred_data, dep_var, dep_var_se, fe_vars, group_var, pred=True)
    pred = mr_model.predict(pred_mr_data)
    pred_fe = mr_model.predict(pred_mr_data, predict_for_study=False)
    
    pred = pd.concat([pred_data.loc[:, ['location_id', 'date']],
                      pd.DataFrame({dep_var:pred})], axis=1)
    pred_fe = pd.concat([pred_data.loc[:, ['location_id', 'date']],
                         pd.DataFrame({dep_var:pred_fe})], axis=1)
        
    return pred, pred_fe


def r2_score(observed, predicted):
    ss_res = np.sum((observed - predicted) ** 2)
    ss_tot = np.sum((observed - np.mean(observed)) ** 2)
    r2 = 1. - (ss_res / ss_tot)
    
    return r2
