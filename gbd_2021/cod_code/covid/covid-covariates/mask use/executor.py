from pathlib import Path
from typing import Union

from covid_beta_inputs.utilities import MASK_USE_PATH, run_command


MASK_USE_SCRIPT_LOCATION = MASK_USE_PATH / 'mask_use.R'


def run_covid_mask_use(app_metadata, lsvid: int, model_inputs_root: Union[str, Path],
                       outputs_directory: Union[str, Path], r_singularity_image: Union[str, Path]):
    mask_use_args = (f'--lsvid {lsvid} '
                     f'--outputs-directory {outputs_directory} '
                     f'--inputs-directory {model_inputs_root} ')

    run_command(r_singularity_image, MASK_USE_SCRIPT_LOCATION, outputs_directory, mask_use_args)
