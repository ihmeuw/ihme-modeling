import sys
from pathlib import Path
from loguru import logger

from covid_gbd_model.paths import OUTPUT_ROOT
from covid_gbd_model.gbd_products import (
    store_gbd,
    upload,
)


def gbd(inputs_root: Path, model_root: Path, mark_best: str, description: str):
    logger.info(f'Model root: {model_root}')
    logger.info('Preparing GBD inputs')
    store_gbd(inputs_root, model_root)

    logger.info('Uploading to database(s)')
    for _db_target in ['covariate', 'cod']:
        upload(
            db_target=_db_target,
            model_root=model_root,
            mark_best=mark_best,
            description=description,
        )


if __name__ == '__main__':
    gbd(
        inputs_root=OUTPUT_ROOT / 'modeling' / sys.argv[1] / 'inputs',
        model_root=OUTPUT_ROOT / 'modeling' / sys.argv[1] / 'spliced_model',
        mark_best=sys.argv[2],
        description=sys.argv[3],
    )
