import sys
from pathlib import Path
import yaml
from loguru import logger

import warnings
warnings.simplefilter('ignore')

from covid_gbd_model.diagnostics import (
    grid_plots,
    scatters,
    mapper,
)

def make_plots(inputs_root: Path, model_root: Path):
    with open(model_root / 'metadata.yml', 'r') as file:
        metadata = yaml.full_load(file)

    plot_dir = model_root / 'plots'
    plot_dir.mkdir(exist_ok=True)
    logger.info('Scatters')
    scatters(inputs_root, model_root)
    logger.info('Grid plots')
    grid_plots(str(inputs_root), str(model_root), metadata['obs_measure'])
    logger.info('Maps')
    mapper(inputs_root, model_root)


if __name__ == '__main__':
    make_plots(
        inputs_root=Path(sys.argv[1]),
        model_root=Path(sys.argv[2]),
    )
