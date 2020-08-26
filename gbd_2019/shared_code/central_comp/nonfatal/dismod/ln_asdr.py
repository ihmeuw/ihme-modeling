import logging
from pathlib import Path

from gbd.gbd_round import gbd_round_from_gbd_round_id
import pandas as pd

ASDR_DIR = "PATH"
FPATH_TEMPLATE = "{cause_id}_{codcorrect_version_id}.h5"


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
