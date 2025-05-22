import sys
from pathlib import Path
from typing import List
from loguru import logger
import pickle

from onemod.main import run_pipeline
from onemod.utils import get_handle

from covid_gbd_model.model.preprocessing import preprocessing
from covid_gbd_model.model.idr_adjustment import idr_adjustment

VALID_STAGES = ['fit_spxmod', 'fit_kreg', 'draws']


def model_pipeline(
    inputs_root: Path, model_root: Path, onemod_stages: List[str], adjust_idr: bool,
):
    if any([stage not in VALID_STAGES for stage in onemod_stages]):
        raise ValueError('Invalid pipeline stage.')

    if 'fit_spxmod' in onemod_stages:
        logger.info('Running rover and spxmod')
        if not (model_root / 'results').exists():
            run_pipeline(
                directory=str(model_root),
                stages=['rover_covsel','spxmod'],
            )
        _, config = get_handle(str(model_root))
        if not config.ids == ['location_id', 'year_id', 'age_group_id', 'sex_id']:
            raise ValueError(f'Invalid sequence for `config.ids`.')

    if 'fit_kreg' in onemod_stages:
        if adjust_idr:
            logger.info('Adjusting data and spxmod fit to remove IDR effect.')
            idr_adjustment(inputs_root, model_root)
        secondary_stages = ['kreg']
        log_message = 'Running kreg'
        if 'draws' in onemod_stages:
            secondary_stages += ['uncertainty']
            log_message += ' (with uncertainty).'
        else:
            log_message += '.'
    elif 'draws' in onemod_stages:
        secondary_stages = ['uncertainty']
        log_message = 'Running kreg uncertainty only.'
    else:
        secondary_stages = []

    if secondary_stages:
        logger.info(log_message)
        run_pipeline(
            directory=str(model_root),
            stages=secondary_stages,
        )


if __name__ == '__main__':
    kwargs_path = sys.argv[1]
    with open(kwargs_path, 'rb') as file:
        kwargs = pickle.load(file)
    preprocessing(
        **kwargs['preprocessing']
    )
    model_pipeline(
        **kwargs['model']
    )
