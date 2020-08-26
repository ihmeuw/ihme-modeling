import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional

from db_tools.ezfuncs import query
from gbd.constants import decomp_step as ds
from gbd.gbd_round import (gbd_round_from_gbd_round_id,
                           gbd_round_id_from_gbd_round)
import pandas as pd

from cascade_ode import db
from cascade_ode.constants import Methods, Queries
from cascade_ode import prior_round

FAUXCORRECT_VERSION = 17

CODCORRECT_CSMR_DIR = "PATH"
FAUXCORRECT_CSMR_DIR = (
    "PATH")
FPATH_TEMPLATE = "{cause_id}_{version_id}.h5"

CSMR_INFO_FNAME = 'csmr_info.json'
ME_with_missing_csmr = 1450


def find_csmr_for_model_gbd2019(
    model_version_meta: pd.DataFrame,
    root_dir: str,
    decomp_method: Dict[str, bool]
) -> pd.DataFrame:
    """
    This function uses model version information to decide which
    version of imputed csmr to read, and returns a dataframe of csmr.

    It records information about the csmr version used in the specified root
    directory

    It is appropriate for steps4 and iterative of gbd 2019
    """
    csmr_cause_id = (
        model_version_meta.add_csmr_cause.squeeze())
    cc_vid = model_version_meta.csmr_cod_output_version_id
    cc_vid = cc_vid.squeeze()
    cc_vid = maybe_update_codcorrect_version(
        model_version_meta,
        csmr_cause_id,
        cc_vid)
    gbd_round_id = model_version_meta.gbd_round_id.squeeze()

    csmr_source, csmr_vid = determine_source(
        csmr_cause_id, cc_vid, decomp_method, gbd_round_id)
    df = read_csmr(csmr_cause_id, csmr_vid, csmr_source, gbd_round_id)

    record_csmr_version(csmr_source, csmr_vid, Path(root_dir))
    return df


def find_csmr_for_model_gbd2020(
    model_version_meta: pd.DataFrame,
    this_model_root_dir: str
) -> pd.DataFrame:
    """
    This function uses model version information to decide which
    version of imputed csmr to read, and returns a dataframe of csmr.

    It records information about the csmr version used in the specified root
    directory

    It is appropriate for steps1 and iterative of gbd 2020
    """
    decomp_step = model_version_meta['decomp_step'].get(0)
    model_version_id = model_version_meta.model_version_id.get(0)
    gbd_round_id = model_version_meta.gbd_round_id.get(0)
    csmr_cause_id = (
        model_version_meta.add_csmr_cause.squeeze())
    root_dir = os.path.join(this_model_root_dir, '..')

    if decomp_step not in [ds.TWO, ds.ITERATIVE]:
        raise NotImplementedError

    # since we know we're in iterative or step1, we always want last round's
    # best model
    last_round_best_model = prior_round.get_last_round_best_model(
        model_version_id)
    csmr_info = read_csmr_info(last_round_best_model, root_dir)
    if not csmr_info:
        csmr_df = db.execute_select(
            Queries.CSMR_INFO_OF_MODEL,
            params={'model_version_id': last_round_best_model})
        version_id = csmr_df['csmr_cod_output_version_id'].get(0)
        source = 'codcorrect'  # predates fauxcorrect
        csmr_info = CSMRData(source=source, version_id=version_id)

    record_csmr_version(
        csmr_info.source, csmr_info.version_id, Path(this_model_root_dir))

    df = read_csmr(csmr_cause_id, csmr_info.version_id, csmr_info.source,
                   gbd_round_id)
    return df


def read_csmr(cause_id, cod_faux_version_id, source, gbd_round_id):
    """
    Given a cause_id, gbd_round_id,  and codcorrect or fauxcorrect version id,
    return a dataframe of imputed CSMR.

    This function applies to steps2+iterative for gbd 2020, and
    steps4+iterative for gbd 2019. It can retrieve CSMR for all combinations of
    cause and codcorrect version ever used in a GBD 2017 or GBD 2019 best
    model.

    Args:
        cause_id (int)
        cod_faux_version_id (int)
        source (str): 'codcorrect' or 'fauxcorrect'
        gbd_round_id (int)

    Returns:
        pd.DataFrame

    Raises:
        RuntimeError if CSMR could not be found.
    """
    is_codcorrect = source == 'codcorrect'

    fpath_func = get_fpath if is_codcorrect else get_fauxcorrect_fpath
    fpath = fpath_func(cause_id, cod_faux_version_id, gbd_round_id)
    log = logging.getLogger(__name__)
    log.info(f"reading CSMR from {fpath}, csmr source = {source}")

    try:
        df = pd.read_hdf(fpath)
    except FileNotFoundError:
        err = (f"Failed to find CSMR for cause_id {cause_id} and {source} "
               f"version {cod_faux_version_id}")
        log.error(err)
        raise RuntimeError(err)
    return df


def get_fpath(cause_id, codcorrect_version_id, gbd_round_id):
    fname = FPATH_TEMPLATE.format(
        cause_id=cause_id, version_id=codcorrect_version_id,
    )
    fpath = Path(CODCORRECT_CSMR_DIR.format(
        GBD_ROUND=gbd_round_from_gbd_round_id(gbd_round_id))) / Path(fname)
    return fpath


def get_fauxcorrect_fpath(cause_id, version_id, gbd_round_id):
    fname = FPATH_TEMPLATE.format(
        cause_id=cause_id, version_id=version_id,
    )
    fpath = Path(FAUXCORRECT_CSMR_DIR.format(
        FAUXCORRECT_VERSION=version_id,
        GBD_ROUND=gbd_round_from_gbd_round_id(gbd_round_id))) / Path(fname)
    return fpath


def current_best_codcorrect(gbd_round_id=None):
    """
    Returns the most recent codcorrect marked best. Generally the viz assigns
    this during model creation, but in the case where a model is trying
    to use missing csmr, we must fall back to current best codcorrect

    Arguments:
        gbd_round_id [int]: optionally filter to a specific round
    """
    qry = """
        SELECT
      max(co.output_version_id) AS version
    FROM cod.output_version co
    join shared.decomp_step using (decomp_step_id)
    WHERE co.is_best = 1
      AND co.best_end IS NULL
        """
    if gbd_round_id:
        qry = qry + f"and gbd_round_id = {gbd_round_id}"

    df = db.execute_select(
        qry,
        conn_def='cod'
    )
    return df.version.squeeze()


def missing_csmr(cause_id, codcorrect_version):
    return cause_id == 346 and codcorrect_version == 84


def update_if_missing_from_2017(
    model_version_meta,
    csmr_cause_id,
    codcorrect_version_id
):
    """
    """
    modelable_entity_id = model_version_meta.modelable_entity_id.squeeze()
    gbd_round_id = model_version_meta.gbd_round_id.squeeze()
    # CCOMP-1578
    is_certain_me = (
        missing_csmr(csmr_cause_id, codcorrect_version_id)
        and
        modelable_entity_id == ME_with_missing_csmr
        and
        gbd_round_id == gbd_round_id_from_gbd_round(2019)
    )

    # CCOMP-1867
    file_is_missing = not get_fpath(
        csmr_cause_id, codcorrect_version_id, gbd_round_id).is_file()

    if is_certain_me or file_is_missing:
        return current_best_codcorrect()
    else:
        return codcorrect_version_id


def decomp_exempt_mes_list():
    """
    List of MEs belonging to decomp exempt causes.

    decomp exempt causes are defined as causes new to gbd 2019 plus the
    decomp exempt ntds
    """
    decomp_exempt_ntds = [346, 347, 348, 349, 350, 352, 353, 354, 355, 357,
                          358, 359, 362, 363, 364, 843, 936, 365]

    new_to_gbd_2019 = [1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012,
                       1013, 1014, 1015, 1016, 1017]

    decomp_exempt_causes = decomp_exempt_ntds + new_to_gbd_2019
    decomp_exempt_me_df = query((
       "select modelable_entity_id from epi.modelable_entity_cause where "
       "cause_id in :decomp_exempt_causes"),
       parameters={"decomp_exempt_causes": decomp_exempt_causes},
       conn_def="epi")

    decomp_exempt_me_list = decomp_exempt_me_df.modelable_entity_id.tolist()

    return decomp_exempt_me_list


def update_if_decomp_exempt(
    model_version_meta,
    csmr_cause_id,
    codcorrect_version_id
):
    """
    if a model is decomp exempt, we want to use current best codcorrect

    CCMHD-10287
    """
    modelable_entity_id = model_version_meta.modelable_entity_id.squeeze()
    is_decomp_exempt = modelable_entity_id in decomp_exempt_mes_list()
    if is_decomp_exempt:
        return current_best_codcorrect()
    else:
        return codcorrect_version_id


def maybe_update_codcorrect_version(
    model_version_meta,
    csmr_cause_id,
    codcorrect_version_id
):
    """
    Sometimes we don't want to use the codcorrect version that the viz assigns
    """
    codcorrect_version_id = update_if_missing_from_2017(
        model_version_meta, csmr_cause_id, codcorrect_version_id)

    codcorrect_version_id = update_if_decomp_exempt(
        model_version_meta, csmr_cause_id, codcorrect_version_id)

    return codcorrect_version_id


def determine_source(
    cause_id,
    codcorrect_version_id,
    decomp_method,
    gbd_round_id
):
    """
    We use fauxcorrect csmr if we're in step4 or iterative, and the csmr file
    for that cause exists (not all do), and there isn't a  codcorrect version
    marked best for current round
    """
    file_exists = get_fauxcorrect_fpath(
        cause_id, FAUXCORRECT_VERSION, gbd_round_id).exists()
    codcorrect_best_for_current_round = current_best_codcorrect(
        gbd_round_id=gbd_round_id)

    if (
        decomp_method[Methods.ENABLE_CSMR_FAUXCORRECT] and file_exists
        and not codcorrect_best_for_current_round
    ):
        return 'fauxcorrect', FAUXCORRECT_VERSION
    else:
        return 'codcorrect', codcorrect_version_id


def record_csmr_version(source, version_id, directory):
    csmr_info = {
        'source': source,
        'version': int(version_id)
    }
    with open(directory / Path(CSMR_INFO_FNAME), 'w') as f:
        json.dump(csmr_info, f)


class CSMRData:

    def __init__(self, source: str, version_id: int) -> None:
        self.source = source
        self.version_id = version_id

    @staticmethod
    def from_json(json_path: str):
        with open(json_path, 'r') as f:
            data = json.load(f)
        return CSMRData(
            source=data['source'],
            version_id=data['version'])


def read_csmr_info(model_version_id: int, root_dir: str) -> Optional[CSMRData]:
    json_path = os.path.join(root_dir, str(model_version_id), CSMR_INFO_FNAME)
    file_exists = os.path.isfile(json_path)
    if file_exists:
        return CSMRData.from_json(json_path)
