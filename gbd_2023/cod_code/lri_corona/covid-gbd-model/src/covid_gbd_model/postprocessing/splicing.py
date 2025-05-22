import sys
from pathlib import Path
from typing import Dict
import shutil
import yaml
import pickle

import pandas as pd

from covid_gbd_model.postprocessing import postprocessing
from covid_gbd_model.diagnostics.diagnostics import make_plots


def get_md_splicing_schema(version_root: Path, splicing_schema: Dict) -> Dict:
    location_metadata = pd.read_parquet(version_root / 'inputs' / 'location_metadata.parquet')
    is_model_location = location_metadata['model_location'] == 1

    covered_location_ids = []
    for location_effects, location_ids in splicing_schema.items():
        version_location_ids = []
        for location_id in location_ids:
            in_path = location_metadata['path_to_top_parent'].apply(lambda x: str(location_id) in x.split(','))
            version_location_ids += location_metadata.loc[in_path & is_model_location, 'location_id'].to_list()
        splicing_schema[location_effects] = version_location_ids
        covered_location_ids += version_location_ids

    all_mod_location_ids = location_metadata.loc[is_model_location, 'location_id'].to_list()

    if any([location_id not in covered_location_ids for location_id in all_mod_location_ids]):
        raise ValueError('Not all model locations covered in splicing schema.')

    if any([location_id not in all_mod_location_ids for location_id in covered_location_ids]):
        raise ValueError('Overlapping model locations present in splicing schema.')

    return splicing_schema


def project_2023_to_2024(predictions: pd.DataFrame, version_root: Path, china_2024_scalar: float):
    location_metadata = pd.read_parquet(version_root / 'inputs' / 'location_metadata.parquet')

    predictions = predictions.loc[predictions['year_id'] <= 2023]
    predictions_2024 = predictions.loc[predictions['year_id'] == 2023]
    predictions_2024.loc[:, ['year_id']] = 2024

    chn_location_metadata = location_metadata.loc[location_metadata['path_to_top_parent'].apply(lambda x: '6' in x.split(','))]
    chn_location_metadata = chn_location_metadata.loc[chn_location_metadata['model_location'] == 1]
    chn_location_ids = chn_location_metadata['location_id'].to_list()

    pred_cols = [col for col in predictions_2024.columns if 'pred' in col]
    predictions_2024.loc[predictions_2024['location_id'].isin(chn_location_ids), pred_cols] *= china_2024_scalar

    predictions = pd.concat([predictions, predictions_2024]).sort_values(['location_id', 'year_id', 'age_group_id', 'sex_id']).reset_index(drop=True)

    return predictions


def compile(splicing_schema: Dict, version_root: Path, project_2023: bool, china_2024_scalar: float):
    spliced = {
        'data': [],
        'spxmod': [],
        'kreg': [],
    }

    for location_effects, location_ids in splicing_schema.items():
        data = pd.read_parquet(version_root / f'{location_effects}_model' / 'data' / 'data.parquet')
        spliced['data'].append(data.loc[data['location_id'].isin(location_ids)])

        spxmod = pd.read_parquet(version_root / f'{location_effects}_model' / 'results' / 'spxmod' / 'predictions.parquet')
        spliced['spxmod'].append(spxmod.loc[spxmod['location_id'].isin(location_ids)])

        kreg = pd.read_parquet(version_root / f'{location_effects}_model' / 'results' / 'kreg' / 'predictions.parquet')
        spliced['kreg'].append(kreg.loc[kreg['location_id'].isin(location_ids)])

    spliced['data'] = pd.concat(spliced['data']).reset_index(drop=True)
    if project_2023:
        spliced['spxmod'] = project_2023_to_2024(
            pd.concat(spliced['spxmod']).reset_index(drop=True),
            version_root,
            china_2024_scalar,
        )
        spliced['kreg'] = project_2023_to_2024(
            pd.concat(spliced['kreg']).reset_index(drop=True),
            version_root,
            china_2024_scalar,
        )
    else:
        spliced['spxmod'] = pd.concat(spliced['spxmod']).reset_index(drop=True)
        spliced['kreg'] = pd.concat(spliced['kreg']).reset_index(drop=True)

    return spliced


def splicer(version_root: Path, splicing_schema: Dict, project_2023: bool, china_2024_scalar: float):
    md_splicing_schema = get_md_splicing_schema(version_root, splicing_schema)
    spliced = compile(md_splicing_schema, version_root, project_2023, china_2024_scalar)

    # this is dumb, already defined in runner
    spliced_model_root = version_root / 'spliced_model'

    reference_location_effect = list(splicing_schema.keys())[0]

    with open(version_root / f'{reference_location_effect}_model' / 'metadata.yml', 'r') as file:
        metadata = yaml.full_load(file)
    metadata['description'] = {'splicing_schema': {}}
    for location_effect, splice_location_ids in splicing_schema.items():
        metadata['description']['splicing_schema'][location_effect] = splice_location_ids
    with open(spliced_model_root / 'metadata.yml', 'w') as file:
        yaml.dump(metadata, file)

    (spliced_model_root / 'config').mkdir()
    with open(version_root / f'{reference_location_effect}_model' / 'config' / 'settings.yml', 'r') as file:
        settings = yaml.full_load(file)
    settings['input_path'] = settings['input_path'].replace(reference_location_effect, 'spliced')
    with open(spliced_model_root / 'config' / 'settings.yml', 'w') as file:
        yaml.dump(settings, file)
    _ = shutil.copy(str(version_root / f'{reference_location_effect}_model' / 'config' / 'resources.yml'), str(spliced_model_root / 'config' / 'resources.yml'))
    (spliced_model_root / 'config' / 'resources.yml').chmod(0o755)

    (spliced_model_root / 'data').mkdir()
    spliced['data'].to_parquet(spliced_model_root / 'data' / 'data.parquet')

    (spliced_model_root / 'results').mkdir()
    (spliced_model_root / 'results' / 'spxmod').mkdir()
    spliced['spxmod'].to_parquet(spliced_model_root / 'results' / 'spxmod' / 'predictions.parquet')

    (spliced_model_root / 'results' / 'kreg').mkdir()
    spliced['kreg'].to_parquet(spliced_model_root / 'results' / 'kreg' / 'predictions.parquet')

    postprocessing(
        version_root / 'inputs',
        spliced_model_root,
    )
    make_plots(
        version_root / 'inputs',
        spliced_model_root,
    )


if __name__ == '__main__':
    kwargs_path = sys.argv[1]
    with open(kwargs_path, 'rb') as file:
        kwargs = pickle.load(file)
    splicer(**kwargs)
