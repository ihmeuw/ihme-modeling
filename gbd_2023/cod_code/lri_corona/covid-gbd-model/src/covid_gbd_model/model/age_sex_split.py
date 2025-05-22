import sys
from pathlib import Path
import yaml
from typing import List, Dict
from loguru import logger

import numpy as np
import pandas as pd

DATA_TYPES = ['provisional', 'surveillance', 'overlap_surveillance']


def make_aggregate(agg_col: str, agg_id: int, child_ids: List[int], numerator: pd.Series, denominator: pd.Series = None):
    if agg_id in numerator.index.get_level_values(agg_col).unique().tolist():
        # logger.warning(f'Value `{agg_id}` already present in index `{agg_col}`.')
        agg = None
    else:
        # logger.info(f'Aggregating {agg_id}')
        id_cols = [id_col for id_col in numerator.index.names if id_col != agg_col]

        agg = numerator.to_frame().query(f'{agg_col} in {child_ids}').loc[:, numerator.name]
        if denominator is not None:
            agg *= denominator.loc[agg.index]
        agg = agg.groupby(id_cols).sum()
        agg = pd.concat([agg], keys=[agg_id], names=[agg_col]).reorder_levels(numerator.index.names)
        if denominator is not None:
            agg /= denominator.loc[agg.index]

    return agg


def load_inputs(data_type: str, inputs_root: Path, model_root: Path):
    with open(inputs_root / 'age_map.yaml', 'r') as file:
        age_map = yaml.full_load(file)

    population = pd.read_parquet(inputs_root / 'population.parquet').loc[:, 'population']
    population = pd.concat(
        [population]
        + [
            make_aggregate(
                'age_group_id',
                agg_id,
                agg_id_data['child_age_group_ids'],
                population,
            ) for agg_id, agg_id_data in age_map.items()
        ]
    )

    data = pd.read_parquet(inputs_root / f'{data_type}_data.parquet')['deaths']
    data /= population.loc[data.index]
    age_bounds = pd.DataFrame(age_map).T.loc[:, ['age_group_years_start', 'age_group_years_end']].astype(float)
    age_bounds.index.name = 'age_group_id'
    data = data.rename('obs').to_frame().join(age_bounds, how='left')
    if data['age_group_years_start'].isnull().any():
        raise ValueError('Incompatible age metadata merge on data.')

    model = pd.read_parquet(model_root / 'results' / 'kreg' / 'predictions.parquet')
    model = model.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id']).sort_index().loc[:, 'pred']
    model = pd.concat(
        [model]
        + [
            make_aggregate(
                'sex_id',
                3,
                [1, 2],
                model,
                population,
            )
        ]
    )
    model = pd.concat(
        [model]
        + [
            make_aggregate(
                'age_group_id',
                agg_id,
                agg_id_data['child_age_group_ids'],
                model,
                population,
            ) for agg_id, agg_id_data in age_map.items()
        ]
    )

    age_metadata = pd.read_parquet(inputs_root / 'age_metadata.parquet')
    child_age_group_id_map = {
        agg_id: agg_id_data['child_age_group_ids'] for agg_id, agg_id_data in age_map.items()
    }
    for det_id in age_metadata['age_group_id']:
        child_age_group_id_map.update({det_id: [det_id]})

    age_metadata = age_metadata.set_index('age_group_id').loc[:, ['age_group_years_start', 'age_group_years_end']]
    supp_age_metadata = pd.DataFrame(age_map).T.drop('child_age_group_ids', axis=1)
    supp_age_metadata.index.name = 'age_group_id'
    supp_age_metadata = supp_age_metadata.drop(age_metadata.index, errors='ignore')
    age_metadata = pd.concat([age_metadata, supp_age_metadata]).sort_index()

    model = model.to_frame().join(age_metadata, how='left')
    if model['age_group_years_start'].isnull().any():
        raise ValueError('Incompatible age metadata merge on model.')

    data['obs_sd'] = np.sqrt((data['obs'] * (1 - data['obs'])) / population.loc[data.index])
    model['pred_sd'] = np.sqrt((model['pred'] * (1 - model['pred'])) / population.loc[model.index])

    return data, model, population, child_age_group_id_map


def crude_splitter(
    obs: pd.Series, pattern: pd.Series, population: pd.Series,
    child_age_group_id_map: Dict, child_sex_id_map: Dict = {1: [1], 2: [2], 3: [1, 2]},
):
    split_obs = []
    for agg_age_group_id, child_age_group_ids in child_age_group_id_map.items():
        for agg_sex_id, child_sex_ids in child_sex_id_map.items():
            try:
                agg_obs = obs.loc[:, :, [agg_age_group_id], [agg_sex_id]]
                det_pattern = pattern.loc[:, :, child_age_group_ids, child_sex_ids]
                agg_obs = (agg_obs * population.loc[agg_obs.index]).reset_index(['age_group_id', 'sex_id'], drop=True)
                det_pattern *= population.loc[det_pattern.index]
                det_pattern /= det_pattern.groupby(['location_id', 'year_id']).sum()

                det_obs = agg_obs.multiply(det_pattern, axis=0).dropna()
                det_obs /= population.loc[det_obs.index]

                split_obs.append(det_obs)
            except KeyError:
                pass
    split_obs = pd.concat(split_obs).sort_index().rename('obs')

    return split_obs


def age_sex_split(data_type: str, inputs_root: Path, model_root: Path):
    if data_type not in DATA_TYPES:
        raise ValueError(f'Invalid data type: {data_type}')
    logger.info(f'Data type: {data_type}')
    logger.info(f'Data path: {inputs_root}')
    logger.info(f'Pattern path: {model_root}')

    data, model, population, child_age_group_id_map = load_inputs(data_type, inputs_root, model_root)

    data_locations = data.index.get_level_values('location_id').unique()
    model_locations = model.index.get_level_values('location_id').unique()
    valid_locations = [location_id for location_id in data_locations if location_id in model_locations]
    invalid_locations = [location_id for location_id in data_locations if location_id not in model_locations]
    if len(invalid_locations) > 0:
        logger.warning(f"No model results for the following locations: {', '.join([str(location_id) for location_id in invalid_locations])}")
    data = data.loc[valid_locations]

    split_data = crude_splitter(
        data.loc[:, 'obs'], model.loc[:, 'pred'],
        population,
        child_age_group_id_map,
    )

    split_data = split_data.sort_index().to_frame()
    split_data['data_id'] = -(np.arange(len(split_data)) + len(split_data))
    split_data = (
        split_data
        .set_index('data_id', append=True)
        .reorder_levels(['data_id'] + split_data.index.names)
    )

    split_data.to_parquet(inputs_root / f'age_sex_split_{data_type}_data.parquet')


if __name__ == '__main__':
    for DATA_TYPE in DATA_TYPES:
        age_sex_split(
            data_type=DATA_TYPE,
            inputs_root=Path(sys.argv[1]),
            model_root=Path(sys.argv[2]),
        )
