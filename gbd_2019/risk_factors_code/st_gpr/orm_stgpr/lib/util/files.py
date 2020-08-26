"""
Code for saving things to files. This will all go away soon, so
it isn't well written and it doesn't have comments.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from orm_stgpr.db import lookup_tables
from orm_stgpr.lib.constants import parameters
from orm_stgpr.lib.util import helpers, offset, old, query, transform
from orm_stgpr.lib.validation import data_validation


def get_model_output_directory(stgpr_version_id: int):
    return 'FILEPATH'


def save_to_files(
        stgpr_version_id: int,
        prepped_df: pd.DataFrame,
        custom_stage_1_df: pd.DataFrame,
        square_df: pd.DataFrame,
        data_df: pd.DataFrame,
        location_hierarchy_df: pd.DataFrame,
        params: Dict[str, Any],
        year_ids: List[int],
        age_group_ids: List[int],
        sex_ids: List[int],
        path_to_config: str,
        output_path: Optional[str]
) -> None:
    run_root = output_path or get_model_output_directory(stgpr_version_id)

    population_df = query.get_population(
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        params[parameters.LOCATION_SET_ID],
        year_ids,
        params[parameters.YEAR_END],
        age_group_ids,
        sex_ids,
        square_df
    )

    # offset, transform, keep original data and variance columns
    prepped_df = prepped_df.copy()
    prepped_df['original_data'] = prepped_df['data']
    prepped_df['original_variance'] = prepped_df['variance']

    prepped_df = offset.offset_and_transform_data(
        prepped_df,
        params[parameters.GBD_ROUND_ID],
        params[parameters.DECOMP_STEP],
        params[parameters.TRANSFORM_OFFSET],
        params[parameters.DATA_TRANSFORM]
    )

    logging.info('Saving to files')
    os.makedirs(run_root, exist_ok=True)

    _save_demographics_to_files(
        square_df, location_hierarchy_df, population_df, run_root
    )
    _save_data_to_file(data_df, prepped_df, run_root)
    run_type = helpers.determine_run_type(params)
    _save_parameters_to_file(
        params,
        custom_stage_1_df,
        year_ids,
        run_type,
        path_to_config,
        run_root
    )


def _save_parameters_to_file(
        params: Dict[str, Any],
        custom_stage_1_df: pd.DataFrame,
        year_ids: List[int],
        run_type: lookup_tables.RunType,
        path_to_config: str,
        run_root
) -> None:
    n_params = old.determine_n_parameter_sets(params)
    config = params.copy()
    config['custom_stage1'] = int(custom_stage_1_df is not None)
    config['prediction_year_ids'] = ','.join([str(i) for i in year_ids])
    config['run_type'] = str(run_type.name)
    config['n_params'] = n_params
    config['holdouts'] = params[parameters.HOLDOUTS]
    config['draws'] = params[parameters.GPR_DRAWS]
    config['path_to_config'] = path_to_config
    config['my_model_id'] = params[parameters.MODEL_INDEX_ID]
    config['decomp_step'] = params[parameters.DECOMP_STEP]
    config['rake_logit'] = int(params['rake_logit'])
    h5_path = f'{run_root}/parameters.h5'
    csv_path = f'{run_root}/parameters.csv'
    config_to_save = config.copy()
    for k, v in config_to_save.items():
        if isinstance(v, list):
            config_to_save[k] = ','.join([str(x) for x in v])
    config_df = pd.DataFrame(config_to_save, index=[0]).assign(
        bundle_id=lambda df: df.bundle_id.astype(float),
        crosswalk_version_id=lambda df: df.crosswalk_version_id.astype(float),
        covariate_id=lambda df: df.covariate_id.astype(float),
        gpr_amp_cutoff=lambda df: df.gpr_amp_cutoff.astype(float),
        holdouts=lambda df: df.holdouts.astype(float),
        model_index_id=lambda df: df.model_index_id.astype(float),
        modelable_entity_id=lambda df: df.modelable_entity_id.astype(float)
    )
    # Remove 0 from density cutoffs. It makes sense to include 0 in density
    # cutoffs during registration, but the model expects density cutoffs not
    # to start at 0.
    # NOTE: it's safe to use [1:] even if there is only one item in the list.
    cutoffs = config_df['density_cutoffs'].iat[0]
    cutoffs = ','.join(cutoffs.split(',')[1:])
    config_df['density_cutoffs'] = cutoffs
    config_df.to_hdf(h5_path, 'parameters')
    config_df.to_csv(csv_path, index=False)

    grid = old.create_hyperparameter_grid(
        params,
        params[parameters.DENSITY_CUTOFFS],
        run_type
    )
    old.set_up_hyperparam_system(run_root, grid, run_type)

    if custom_stage_1_df is not None:
        custom_stage_1_df = custom_stage_1_df.copy()
        data_validation.validate_data_bounds_for_transformation(
            custom_stage_1_df, 'val', params[parameters.DATA_TRANSFORM]
        )
        custom_stage_1_df['cv_custom_stage_1'] = transform.transform_data(
            custom_stage_1_df['val'],
            params[parameters.DATA_TRANSFORM]
        )
        custom_stage_1_df.to_csv(
            f'{run_root}/custom_stage1_df.csv',
            index=False
        )


def _save_demographics_to_files(
        square_df: pd.DataFrame,
        location_hierarchy_df: pd.DataFrame,
        population_df,
        run_root
) -> None:
    path = f'{run_root}/square.h5'
    if os.path.isfile(path):
        os.remove(path)

    store = pd.HDFStore(path, 'a')
    store.put('square', square_df, format='fixed', data_columns=True)
    store.put(
        'populations',
        population_df,
        format='fixed',
        data_colmns=True,
        index=False
    )
    location_hierarchy_df['path_to_top_parent'] = location_hierarchy_df.path_to_top_parent.astype(
        str)
    for col in location_hierarchy_df.columns:
        if 'level' in col:
            location_hierarchy_df[col] = location_hierarchy_df[col].astype(
                object)
    store.put(
        'location_hierarchy',
        location_hierarchy_df,
        format='fixed',
        data_colmns=True
    )
    store.close()
    os.chmod(path, 0o775)


def _save_data_to_file(
        data_df: pd.DataFrame,
        prepped_df: pd.DataFrame,
        run_root
) -> None:
    for col in prepped_df.columns:
        if 'level' in col:
            prepped_df[col] = prepped_df[col].astype(object)
    h5_path = f'{run_root}/data.h5'
    prepped_df['location_id'] = prepped_df.location_id.astype(int)
    prepped_df.to_hdf(h5_path, 'prepped')
    data_df.to_hdf(h5_path, 'data')

    csv_path = f'{run_root}/prepped.csv'
    prepped_df.to_csv(csv_path, index=False)
