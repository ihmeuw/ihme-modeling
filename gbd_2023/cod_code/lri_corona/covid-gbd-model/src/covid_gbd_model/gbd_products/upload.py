from pathlib import Path
from loguru import logger
import time

import pandas as pd

from save_results import save_results_cod, save_results_covariate

from covid_gbd_model.variables import RELEASE_ID


def upload(db_target: str, model_root: Path, mark_best: str, description: str, release_id: int = RELEASE_ID) -> pd.DataFrame:
    if mark_best not in ['True', 'False']:
        raise ValueError('Invalid option for `mark_best`.')

    if not model_root.exists():
        raise ValueError(f'Invalid version root: {model_root}')

    logger.info(f'UPLOAD TYPE: {db_target}')
    logger.info(f'UPLOAD MODEL ROOT: {model_root}')
    logger.info(f'MARKING BEST: {mark_best}')
    logger.info(f'UPLOAD DESCRIPRION: {description}')

    time.sleep(10)

    if db_target == 'cod':
        draws_dir = model_root / 'gbd' / 'cod'
        model_version_df = save_results_cod(
            input_dir=str(draws_dir),
            input_file_pattern='{year_id}.csv',
            cause_id=1048,
            description=description,
            metric_id=3,
            sex_id=[1, 2],
            release_id=release_id,
            mark_best=mark_best=='True',
        )
    elif db_target == 'covariate':
        covariate_dir = model_root / 'gbd' / 'covariate'
        model_version_df = save_results_covariate(
            input_dir=str(covariate_dir),
            input_file_pattern='covid_asdr.csv',
            covariate_id=2598,
            description=description,
            age_std=True,
            release_id=release_id,
            # mark_best=mark_best=='True',
        )
    return model_version_df
