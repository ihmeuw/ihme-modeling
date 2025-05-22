import sys
from pathlib import Path
from typing import List
import functools
import hashlib
import tqdm
import multiprocessing
import yaml

import pandas as pd
import numpy as np

from onemod.utils import get_handle

from covid_gbd_model.variables import MODEL_YEARS


def load_draws(model_root: Path):
    dataif, config = get_handle(str(model_root))
    
    draws = (
        dataif
        .load_kreg('predictions.parquet')
        .set_index(config.ids)
        .loc[:, [f'{config.pred}_draw_{i}' for i in range(config.kreg.kreg_uncertainty.num_samples)]]
        .rename(columns={f'{config.pred}_draw_{i}': f'draw_{i}' for i in range(config.kreg.kreg_uncertainty.num_samples)})
    )

    return draws


def get_random_seed(key: str):
    # 4294967295 == 2**32 - 1 which is the maximum allowable seed for a `numpy.random.RandomState`.
    seed = int(hashlib.sha1(key.encode('utf8')).hexdigest(), 16) % 4294967295
    return seed


def resample_tails(draws: pd.DataFrame, population: pd.Series, resample_q: int):
    n_two_sided = int(draws.shape[1] * (resample_q / 100) / 2)

    def resample_row(data: pd.Series, n_two_sided: int = n_two_sided):
        np.random.seed(get_random_seed('-'.join([str(n) for n in data.name])))

        # print(data.quantile([0, 1]))
        idx = data.index
        data = data.sort_values()
        resampled = np.random.choice(data[n_two_sided:-n_two_sided], n_two_sided * 2)
        data[:n_two_sided] = resampled[:n_two_sided]
        data[-n_two_sided:] = resampled[n_two_sided:]
        # print(data.quantile([0, 1]))

        return data.loc[idx]

    draws = draws.apply(resample_row, axis=1)

    return draws


def identify_locations_not_to_rake(inputs_root: Path, model_root: Path, draws: pd.DataFrame, location_metadata: pd.DataFrame):
    location_metadata_covid = pd.read_parquet(inputs_root / 'location_metadata_covid.parquet')
    input_data = pd.read_parquet(model_root / 'data' / 'data.parquet').groupby(['location_id', 'year_id'])['obs'].sum(min_count=1).dropna()
    data_locations = input_data.index.get_level_values('location_id').unique()
    location_metadata_covid = (
        location_metadata_covid
        .loc[location_metadata_covid['location_id'].isin(data_locations)]
        .set_index('location_id')
    )

    # do not rake subnat-years if...
    # (a) they have data but are not in the covid hierarchy (i.e., no real IDR), however there is no parent aggregate
    non_covid_subnat_data = (
        draws
        .groupby(['location_id', 'year_id'])['draw_0'].sum()
        .loc[input_data.index]
        .loc[location_metadata.loc[(location_metadata['location_id'].isin(data_locations)) & (location_metadata['level'] > 3), 'location_id']]
        .sort_index()
        .drop(location_metadata_covid.sort_index().index, errors='ignore')
    )
    non_covid_subnat_location_years = non_covid_subnat_data.index
    for location_id, year_id in non_covid_subnat_location_years:
        country_id = int(location_metadata.set_index('location_id').loc[location_id, 'path_to_top_parent'].split(',')[3])
        if input_data.get((country_id, year_id), False):
            non_covid_subnat_data = non_covid_subnat_data.drop((location_id, year_id))

    # (b) they have data and are in the covid hierarchy
    input_data = input_data.loc[location_metadata_covid.loc[location_metadata_covid['level'] > 3].index]

    do_not_rake_idx = pd.concat([non_covid_subnat_data, input_data]).sort_index().index

    return do_not_rake_idx


def raker(
    data: pd.Series,
    location_metadata: pd.DataFrame,
    population: pd.Series,
    do_not_rake_idx: pd.MultiIndex
):
    modeled_location_ids = data.index.get_level_values('location_id').unique().tolist()
    is_modeled_location = location_metadata['location_id'].isin(modeled_location_ids)
    is_most_detailed = location_metadata['most_detailed'] == 1
    is_modeled_parent = is_modeled_location & ~is_most_detailed

    for level in location_metadata.loc[is_modeled_parent, 'level'].sort_values().unique():
        level_raking_factor = []
        is_level = location_metadata['level'] == level
        for parent_id in location_metadata.loc[is_modeled_parent & is_level, 'location_id'].to_list():
            child_level = level + 1
            searching = True
            while searching:
                is_child = location_metadata['path_to_top_parent'].str.contains(f',{parent_id},')
                if child_level == location_metadata.loc[is_child, 'level'].max():
                    is_level_or_md = location_metadata['most_detailed'] == 1
                else:
                    is_level_or_md = location_metadata['level'] == child_level
                child_location_ids = (
                    location_metadata
                    .loc[is_child & is_level_or_md]
                    .loc[:, 'location_id']
                    .to_list()
                )
                if all([i in modeled_location_ids for i in child_location_ids]):
                    searching = False
                elif child_level > location_metadata['level'].max():
                    raise ValueError(f'Cannot find modeled child locations for {parent_id}.')
                else:
                    child_level += 1

            child_agg_data = (
                (data.loc[child_location_ids] * population.loc[child_location_ids]).groupby(['year_id', 'age_group_id', 'sex_id']).sum()
                / population.loc[child_location_ids].groupby(['year_id', 'age_group_id', 'sex_id']).sum()
            )
            parent_child_ratio = data.loc[parent_id] / child_agg_data
            level_raking_factor.append(
                pd.concat([parent_child_ratio] * len(child_location_ids), keys=child_location_ids, names=['location_id'])
            )
        level_raking_factor = pd.concat(level_raking_factor).sort_index()
        level_raking_factor = level_raking_factor.drop(do_not_rake_idx, errors='ignore')
        level_raking_factor = level_raking_factor.reindex(data.index).fillna(1)

        data *= level_raking_factor

    return data


def format_most_detailed(
    draws: pd.Series, location_metadata: pd.DataFrame, age_metadata: pd.DataFrame, model_years: List[int] = MODEL_YEARS,
) -> pd.DataFrame:
    location_ids = location_metadata.loc[location_metadata['most_detailed'] == 1].sort_values('sort_order').loc[:, 'location_id']
    age_group_ids = age_metadata.sort_values('age_group_years_start').loc[:, 'age_group_id']
    sex_ids = [1, 2]

    return draws.loc[location_ids, model_years, age_group_ids, sex_ids]


def aggregate_parent(parent_id: int, draws: pd.DataFrame, parent_child_map: pd.Series) -> pd.DataFrame:
    child_ids = parent_child_map.loc[parent_id]
    draws = (
        draws
        .loc[child_ids]
        .groupby([idx for idx in draws.index.names if idx != 'location_id']).sum()
    )
    draws = pd.concat([draws], keys=[parent_id], names=['location_id'])

    return draws


def aggregate_locations(
    draws: pd.DataFrame, location_metadata: pd.DataFrame, population: pd.Series
) -> pd.DataFrame:
    draws = (
        draws
        .multiply(population.loc[draws.index], axis=0)
        .loc[location_metadata.loc[location_metadata['most_detailed'] == 1, 'location_id']]
    )

    level_parents = (
        location_metadata
        .loc[location_metadata['most_detailed'] != 1]
        .groupby('level')['location_id'].apply(lambda x: x.to_list())
        .sort_index(ascending=False)
    )
    parent_child_map = (
        location_metadata
        .loc[location_metadata['parent_id'] != location_metadata['location_id']]
        .groupby('parent_id')['location_id'].apply(lambda x: x.to_list())
        .sort_index(ascending=False)
    )
    for level, parent_ids in level_parents.items():
        # logger.info(f'Aggregating level {level}')
        level_draws = pd.concat(
            [
                aggregate_parent(parent_id, draws, parent_child_map)
                for parent_id in parent_ids  # tqdm.tqdm(parent_ids, total=len((parent_ids)))
            ]
        )
        draws = pd.concat([level_draws, draws])

    draws = draws.divide(population.loc[draws.index], axis=0)

    return draws


def aggregate_ages_sexes(draws: pd.DataFrame, age_metadata: pd.DataFrame, population: pd.Series) -> pd.DataFrame:
    draws = draws.multiply(population.loc[draws.index], axis=0)

    for agg_id, agg_var, by_var in [(22, 'age_group_id', 'sex_id'), (3, 'sex_id', 'age_group_id')]:
        draws = pd.concat(
            [
                draws,
                pd.concat(
                    [draws.groupby(['location_id', 'year_id', by_var]).sum()],
                    keys=[agg_id], names=[agg_var]
                ).reorder_levels(draws.index.names)
            ]
        )

    draws = draws.divide(population.loc[draws.index], axis=0)

    age_weights = age_metadata.set_index('age_group_id').loc[:, 'age_group_weight_value']
    draws = pd.concat(
        [
            draws,
            pd.concat(
                [
                    draws.loc[:, :, age_weights.index, :]
                    .multiply(age_weights, axis=0)
                    .groupby(['location_id', 'year_id', 'sex_id']).sum()
                ],
                keys=[27], names=['age_group_id']
            ).reorder_levels(draws.index.names)
        ]
    )

    return draws.sort_index()


def process_draws(
    inputs_root: Path,
    model_root: Path,
    obs_measure: str,
    rake: bool,
    resample_q: int = 10,
    ceiling: float = 0.9,
    verbose: bool = False
):
    draws = load_draws(model_root)

    location_metadata = pd.read_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata_covariate = pd.read_parquet(inputs_root / 'location_metadata_covariate.parquet')
    age_metadata = pd.read_parquet(inputs_root / 'age_metadata.parquet')
    population = pd.read_parquet(inputs_root / 'population.parquet').loc[:, 'population']

    if resample_q > 0:
        draws = resample_tails(draws, population, resample_q)

    if obs_measure == 'ifr':
        infections = pd.read_parquet(model_root / 'data' / 'data.parquet')
        infections = infections.set_index(draws.index.names).loc[draws.index, 'inf_ancestral']
        draws = draws.multiply(infections, axis=0)

    if rake:
        do_not_rake_idx = identify_locations_not_to_rake(inputs_root, model_root, draws, location_metadata)
        _raker = functools.partial(
            raker,
            location_metadata=location_metadata,
            population=population.loc[draws.index],
            do_not_rake_idx=do_not_rake_idx,
        )
        with multiprocessing.Pool(20) as pool:
            draws = list(tqdm.tqdm(pool.imap(_raker, [draws[d] for d in draws.columns]), total=draws.shape[1], disable=not verbose))
        draws = pd.concat(draws, axis=1)

    draws = draws.clip(0, ceiling)

    draws = format_most_detailed(draws, location_metadata, age_metadata)

    draws = aggregate_locations(draws, location_metadata_covariate, population)

    draws = aggregate_ages_sexes(draws, age_metadata, population)

    return draws


def summarize(draws: pd.DataFrame) -> pd.DataFrame:
    summaries = pd.concat(
        [
            draws.mean(axis=1).rename('mean_value'),
            draws.quantile(0.025, axis=1).rename('lower_value'),
            draws.quantile(0.975, axis=1).rename('upper_value'),
        ], axis=1
    )

    return summaries


def postprocessing(inputs_root: Path, model_root: Path):
    with open(model_root / 'metadata.yml', 'r') as file:
        metadata = yaml.full_load(file)
    processed_dir = model_root / 'processed'
    processed_dir.mkdir()

    unraked_draws = process_draws(inputs_root, model_root, metadata['obs_measure'], rake=False)
    unraked_draws.to_parquet(processed_dir / 'unraked_draws.parquet')
    summarize(unraked_draws).to_parquet(processed_dir / 'unraked_summaries.parquet')

    draws = process_draws(inputs_root, model_root, metadata['obs_measure'], rake=True)
    draws.to_parquet(processed_dir / 'raked_draws.parquet')
    summarize(draws).to_parquet(processed_dir / 'raked_summaries.parquet')


if __name__ == '__main__':
    postprocessing(
        inputs_root=Path(sys.argv[1]),
        model_root=Path(sys.argv[2]),
    )
