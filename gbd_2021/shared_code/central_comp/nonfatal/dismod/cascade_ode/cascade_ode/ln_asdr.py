import logging
from pathlib import Path
from typing import List

from cascade_ode import csmr
from gbd.gbd_round import gbd_round_from_gbd_round_id
import pandas as pd
import numpy as np


ASDR_DIR = "FILEPATH/lnasdr/gbd_{GBD_ROUND}/complete"
FPATH_TEMPLATE = "{cause_id}_{codcorrect_version_id}.h5"
LNASDR_COLS = [
    'location_id', 'sex_id', 'year_id',
    'model_version_id', 'asdr_cause',
    'csmr_cod_output_version_id', 'raw_c_lnasdr_{cause_id}.0'
]


def read_lnasdr(cause_id, codcorrect_version_id, gbd_round_id):
    """
    Given a cause_id, gbd_round_id, and codcorrect version id, return a
    dataframe of imputed Ln-ASDR fit for use in the given round.

    This function applies to steps 4+iterative for GBD 2019 and steps
    1+iterative for GBD 2020. It can retrieve cause specific Ln-ASDR for all
    combinations of cause and codcorrect version ever used in a GBD 2017 and
    GBD 2019 best model.

    Args:
        cause_id (int)
        codcorrect_version_id (int)
        gbd_round_id (int)

    Returns:
        pd.DataFrame

    Raises:
        RuntimeError if Ln-ASDR could not be found.
    """
    fname = FPATH_TEMPLATE.format(
        cause_id=cause_id, codcorrect_version_id=codcorrect_version_id
    )
    fpath = Path(ASDR_DIR.format(
        GBD_ROUND=gbd_round_from_gbd_round_id(gbd_round_id))) / Path(fname)

    log = logging.getLogger(__name__)
    log.info(f"reading ASDR from {fpath}")

    try:
        df = pd.read_hdf(fpath)
    except FileNotFoundError:
        log.error(
            f"Failed to find Ln-ASDR for cause_id {cause_id} and codcorrect "
            "version {codcorrect_version_id}"
        )
        raise RuntimeError
    return df


def get_lnasdr_from_db(
    cause_id,
    codcorrect_version_id,
    gbd_round_id,
    model_version_id
):
    """Get log-normalized age-standardized death rate from get_outputs.

    To be used for GBD2020 iterative modeling, assumes given
    codcorrect_version_id is bested.

    Args:
        cause_id (int): cause to pull
        codcorrect_version_id (int): best GBD2020 step2 CC versiond
        gbd_round_id (int): gbd_round_id to pull from
    """
    log = logging.getLogger(__name__)
    log.info(
        f"pulling LNASDR from outputs database. "
        f"CC verison: {codcorrect_version_id}")
    df = csmr.read_csmr_from_db(
        codcorrect_run_id=codcorrect_version_id,
        cause_id=cause_id,
        model_version_id=model_version_id,
        age_standardized=True)

    output_cols = _get_lnasdr_cols(cause_id)
    value_col = output_cols[-1]
    df['csmr_cod_output_version_id'] = codcorrect_version_id
    df['asdr_cause'] = cause_id
    df['model_version_id'] = np.NaN
    df = df.rename(columns={
        'x_sex': 'sex_id',
        'time_lower': 'year_id',
        'meas_value': value_col
    })
    df[value_col] = np.log(df[value_col])
    return df[output_cols]


def _get_lnasdr_cols(cause_id: int) -> List[str]:
    """Gets expected output columns in expected order for lnasdr."""
    cols = LNASDR_COLS.copy()
    cols[-1] = cols[-1].format(cause_id=str(cause_id))
    return cols
