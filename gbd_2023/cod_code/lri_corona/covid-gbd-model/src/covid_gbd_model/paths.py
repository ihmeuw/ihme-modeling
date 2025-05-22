from pathlib import Path
import datetime

COVID_MODEL_INPUTS_ROOT = Path('')
COVID_SEIR_FIT_ROOT = Path('')
COVID_SEIR_OUTPUTS_ROOT = Path('')

OUTPUT_ROOT = Path('')

TOTAL_COVID_ASDR_PATH = Path('')

DEMOG_AGE_METADATA_PATH = Path('')

SHAPEFILE_PATH = Path('')

WHO_SUPPLEMENT_PATH = Path('')


def make_version_path(output_root: Path, version: str = None) -> str:
    if version is None:
        date_str = str(datetime.datetime.now().date()).replace('-', '_')
        flag = True
        for i in range(1, 100):
            date_run = str(i).zfill(2)
            version = f'{date_str}.{date_run}'
            if not (output_root / version).exists():
                flag = False
                break
        if flag:
            raise ValueError(f'Versions in output root {str(output_root)} for date {date_str} exceed limit.')
        (output_root / version).mkdir()
    else:
        if not (output_root / version).exists():
            raise ValueError(f'Version manually specified but version path does not exist: {output_root / version}')

    return output_root / version
