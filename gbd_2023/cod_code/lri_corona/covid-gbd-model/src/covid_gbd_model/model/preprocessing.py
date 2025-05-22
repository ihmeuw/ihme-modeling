from pathlib import Path
from typing import List
from loguru import logger
import yaml
import shutil

import pandas as pd
import numpy as np


def copy_config(
    config_dir: Path,
    model_root: Path,
    obs_measure: str,
    pipeline_stage: str,
    location_effects: str,
    t_threshold: float,
    n_draws: int,
    drop_covs: List[str],
    combine_alpha_beta_gamma_delta: bool,
    combine_omicron_ba5: bool,
):
    with open(config_dir / 'settings.yml', 'r') as file:
        settings = yaml.full_load(file)
    settings['input_path'] = str(model_root / 'inputs.parquet')

    settings['rover_covsel']['t_threshold'] = t_threshold

    if pipeline_stage == 'age_pattern':
        drop_covs += ['idr']
    elif pipeline_stage == 'idr_adj':
        pass
    else:
        raise ValueError(f'Invalid pipeline stage: {pipeline_stage}')

    if obs_measure == 'ifr':
        drop_covs += ['inf_ancestral']

    if combine_alpha_beta_gamma_delta:
        drop_covs += ['inf_alpha', 'inf_beta', 'inf_gamma', 'inf_delta']
    else:
        drop_covs += ['inf_alpha_beta_gamma_delta']
    if combine_omicron_ba5:
        drop_covs += ['inf_omicron', 'inf_ba5']
    else:
        drop_covs += ['inf_omicron_ba5']
    for sub_dict in ['cov_exploring', 'cov_fixed']:
        settings['rover_covsel']['rover'][sub_dict] = [
            cf for cf in settings['rover_covsel']['rover'][sub_dict]
            if cf not in drop_covs
        ]

    settings['rover_covsel']['rover_fit']['coef_bounds'] = {
        k: v for k, v in settings['rover_covsel']['rover_fit']['coef_bounds'].items()
        if k not in drop_covs
    }

    settings['rover_covsel']['rover_fit']['coef_bounds'] = {
        k:[-10, 10] if k.startswith('inf_') and k != 'inf_ancestral'
        else v
        for k, v 
        in settings['rover_covsel']['rover_fit']['coef_bounds'].items()
    }

    if location_effects == 'g_l':
        settings['spxmod']['xmodel']['spaces'] = [
            space for space
            in settings['spxmod']['xmodel']['spaces']
            if 'region_id' not in space['name']
        ]
        settings['spxmod']['xmodel']['var_builders'] = [
            var_builder for var_builder
            in settings['spxmod']['xmodel']['var_builders']
            if 'region_id' not in var_builder['space']
        ]
    elif location_effects == 'g_sr_l':
        settings['spxmod']['xmodel']['spaces'] = [
            space for space
            in settings['spxmod']['xmodel']['spaces']
            if 'region_id' not in space['name'] or 'super' in space['name']
        ]
        settings['spxmod']['xmodel']['var_builders'] = [
            var_builder for var_builder
            in settings['spxmod']['xmodel']['var_builders']
            if 'region_id' not in var_builder['space'] or 'super' in var_builder['space']
        ]
    elif location_effects != 'g_sr_r_l':
        raise ValueError('Invalid location_effects specification')

    settings['kreg']['kreg_uncertainty']['num_samples'] = n_draws

    with open(model_root / 'config' / 'settings.yml', 'w') as file:
        yaml.dump(settings, file)

    _ = shutil.copy(config_dir / 'resources.yml', str(model_root / 'config' / 'resources.yml'))
    (model_root / 'config' / 'resources.yml').chmod(0o755)


def prepare_covariates(
    inputs_root: Path,
    combine_alpha_beta_gamma_delta: bool,
    combine_omicron_ba5: bool,
):
    covariates = pd.read_parquet(inputs_root / 'covariates.parquet')

    if combine_alpha_beta_gamma_delta:
        covariates['inf_alpha_beta_gamma_delta'] = covariates.loc[:, ['inf_alpha', 'inf_beta', 'inf_gamma', 'inf_delta']].sum(axis=1)
        covariates = covariates.drop(['inf_alpha', 'inf_beta', 'inf_gamma', 'inf_delta'], axis=1)

    if combine_omicron_ba5:
        covariates['inf_omicron_ba5'] = covariates.loc[:, ['inf_omicron', 'inf_ba5']].sum(axis=1)
        covariates = covariates.drop(['inf_omicron', 'inf_ba5'], axis=1)

    return covariates


def subset_to_covid_locations(data: pd.DataFrame, hierarchy: pd.DataFrame):
    # hierarchy = pd.read_csv(covid_seir_outputs_root / 'output_miscellaneous' / 'hierarchy.csv')
    location_ids = (
        hierarchy
        .loc[hierarchy['location_id'].isin(data.index.get_level_values('location_id').unique())]
        .loc[:, 'location_id']
    )

    data = data.loc[:, location_ids, :, :, :]

    return data


def prepare_data(
        inputs_root: Path,
        pipeline_stage: str,
        population: pd.Series,
        model_locations: List[int],
        location_metadata_covid: pd.DataFrame,
        prov_pop_scalar: float = 1.,
        surv_pop_scalar: float = 1.,
        # idr_adj_pop_scalar: float = 0.25,
    ):
    data = pd.read_parquet(inputs_root / 'cod_data.parquet')
    index_columns = data.index.names
    if pipeline_stage == 'age_pattern':
        data = (
            data.loc[:, 'corr_obs']
            .rename('obs')
            .to_frame()
            .join(population.rename('weights'))
            .reorder_levels(index_columns)
        )
        data = subset_to_covid_locations(data, location_metadata_covid)
    elif pipeline_stage == 'idr_adj':
        data = (
            data.loc[:, 'corr_obs']
            .rename('obs')
            .to_frame()
            .join(population.rename('weights'))
        )
        data['is_corr_vr'] = True
        supp_data = pd.concat(
            [
                pd.read_parquet(inputs_root / 'age_sex_split_provisional_data.parquet').join((population * prov_pop_scalar).rename('weights')),
                pd.read_parquet(inputs_root / 'age_sex_split_surveillance_data.parquet').join((population * surv_pop_scalar).rename('weights'))
            ]
        )
        supp_data['is_corr_vr'] = False
        data = pd.concat([data.reorder_levels(index_columns), supp_data.reorder_levels(index_columns)])
    else:
        raise ValueError(f'Invalid pipeline stage specified: {pipeline_stage}')

    data_locations = data.index.get_level_values('location_id').unique().tolist()
    data_model_locations = [model_location for model_location in model_locations if model_location in data_locations]
    data = data.loc[:, data_model_locations, :, :, :]
    duplicate_locs = data.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id'])['obs'].count().sort_values()
    duplicate_locs = duplicate_locs.loc[duplicate_locs > 1].index.get_level_values('location_id').unique()
    if duplicate_locs.size > 0:
        raise ValueError(f'Duplicate records found for the following locations: {[str(dl) for dl in duplicate_locs]}')

    data['test'] = False  # not preserving any data for end-stange testing
    data['missing'] = 1
    data_ids = data.index.get_level_values('data_id').to_list()
    for i in range(1, 11):
        np.random.seed(int(f'{i}{i}{i}{i}'))
        holdout_data_ids = np.random.choice(data_ids, size=int(len(data_ids) * 0.3), replace=False)
        data[f'holdout{i}'] = 0
        data.loc[holdout_data_ids, [f'holdout{i}']] = 1

    return data


def load_metadata(inputs_root: Path):
    location_metadata = pd.read_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata_covid = pd.read_parquet(inputs_root / 'location_metadata_covid.parquet')
    age_metadata = pd.read_parquet(inputs_root / 'age_metadata.parquet')
    population = pd.read_parquet(inputs_root / 'population.parquet').loc[:, 'population']

    return location_metadata, location_metadata_covid, age_metadata, population


def preprocessing(
    inputs_root: Path,
    model_root: Path,
    obs_measure: str,
    pipeline_stage: str,
    location_effects: str,
    t_threshold: float,
    n_draws: int,
    description: str,
    config_dir: Path,
    drop_covs: List[str],
    combine_alpha_beta_gamma_delta: bool,
    combine_omicron_ba5: bool,
):
    logger.info(model_root)

    metadata = {
        'inputs_root': str(inputs_root),
        'model_root': str(model_root),
        'obs_measure': obs_measure,
        'pipeline_stage': pipeline_stage,
        'location_effects': location_effects,
        't_threshold': t_threshold,
        'n_draws': n_draws,
        'description': description,
    }
    with open(model_root / 'metadata.yml', 'w') as file:
        yaml.dump(metadata, file)

    (model_root / 'config').mkdir()
    copy_config(
        config_dir,
        model_root,
        obs_measure,
        pipeline_stage,
        location_effects,
        t_threshold,
        n_draws,
        drop_covs,
        combine_alpha_beta_gamma_delta, combine_omicron_ba5
    )

    (
        location_metadata, location_metadata_covid, 
        age_metadata, population
    ) = load_metadata(inputs_root)

    data = prepare_data(
        inputs_root,
        pipeline_stage,
        population,
        location_metadata.loc[location_metadata['model_location'] == 1, 'location_id'].to_list(),
        location_metadata_covid,
    )
    covariates = prepare_covariates(
        inputs_root,
        combine_alpha_beta_gamma_delta,
        combine_omicron_ba5,
    )
    data = data.join(covariates, how='right')
    data['test'] = data['test'].fillna(True)

    if pipeline_stage == 'idr_adj':
        data['is_corr_vr'] = data['is_corr_vr'].fillna(False)
        data['raw_idr'] = data['idr']
        data.loc[data['is_corr_vr'], ['idr']] = 1

    data['idr'] = np.log(data['idr'])
    if pipeline_stage == 'idr_adj':
        data['raw_idr'] = np.log(data['raw_idr'])
    
    if combine_alpha_beta_gamma_delta:
        non_ancestral_inf_covs = ['inf_alpha_beta_gamma_delta']
    else:
        non_ancestral_inf_covs = ['inf_alpha', 'inf_beta', 'inf_gamma', 'inf_delta']
    if combine_omicron_ba5:
        non_ancestral_inf_covs += ['inf_omicron_ba5']
    else:
        non_ancestral_inf_covs += ['inf_omicron', 'inf_ba5']

    # turn ancestral into "total infections" and other variants into variant proportions
    data['inf_ancestral'] += data.loc[:, non_ancestral_inf_covs].sum(axis=1)
    data.loc[:, non_ancestral_inf_covs] = data.loc[:, non_ancestral_inf_covs].divide(data['inf_ancestral'], axis=0).fillna(0)

    if obs_measure == 'ifr':
        # set observation to be infections (will drop inf_ancestral from covariates in config)
        data = data.join(population, how='left')
        data['obs'] *= data['population']
        # data['weights'] = (data['inf_ancestral'] * data['population'] + 1)
        data['obs'] = (data['obs'] / ((data['inf_ancestral'] * data['population'] + 1))).clip(0, 1)
        data = data.drop('population', axis=1)
    elif obs_measure != 'mx':
        raise ValueError(f'Invalid obs_measure: {obs_measure}')

    data = data.join(location_metadata.set_index('location_id').loc[:, ['location_name', 'region_id', 'region_name', 'super_region_id', 'super_region_name', 'sort_order']])
    data = data.join(age_metadata.set_index('age_group_id').loc[:, ['age_group_name', 'age_mid']])
    data = (
        data
        .reset_index()
        .sort_values(
            ['sort_order', 'year_id', 'age_mid', 'sex_id']
        )
        .drop('sort_order', axis=1)
    )

    data.drop('data_id', axis=1).to_parquet(model_root / 'inputs.parquet')
