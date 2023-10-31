import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from db_tools.ezfuncs import query
from gbd.constants import decomp_step as ds
from gbd.constants import age, metrics
from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd.gbd_round import (gbd_round_from_gbd_round_id,
                           gbd_round_id_from_gbd_round)

from cascade_ode import db, demographics, shared_functions
from cascade_ode.constants import Methods, Queries

CODCORRECT_CSMR_DIR = "FILEPATH/csmr/gbd_{GBD_ROUND}/complete"
FAUXCORRECT_CSMR_DIR = (
    "FILEPATH/fauxcorrect_csmr/gbd_{GBD_ROUND}/"
    "{FAUXCORRECT_VERSION}/formatted")
FPATH_TEMPLATE = "{cause_id}_{version_id}.h5"

CSMR_INFO_FNAME = 'csmr_info.json'


def find_csmr_for_model_gbd2020(
    model_version_meta: pd.DataFrame,
    this_model_root_dir: str
) -> pd.DataFrame:
    """
    This function returns csmr for the cause specified in the given model's
    parameters

    It records information about the csmr version used in the specified root
    directory
    """
    VALID_GBD_2020_DISMOD_DECOMP_STEPS = [ds.ITERATIVE, ds.RELEASE_TWO]
    decomp_step = model_version_meta['decomp_step'].get(0)
    gbd_round_id = model_version_meta.gbd_round_id.get(0)
    csmr_cause_id = (model_version_meta.add_csmr_cause.squeeze())
    log = logging.getLogger(__name__)

    if decomp_step not in VALID_GBD_2020_DISMOD_DECOMP_STEPS:
        raise NotImplementedError(
            f'gbd2020 only allows dismod models for the following decomp_steps: '
            f'{VALID_GBD_2020_DISMOD_DECOMP_STEPS}')

    step2_best_codcorrect = current_best_codcorrect(
        gbd_round_id=gbd_round_id, decomp_step='step2')
    log.info(f"Best step2 codcorrect: {step2_best_codcorrect}")

    if step2_best_codcorrect:
        record_csmr_version(
            'codcorrect', step2_best_codcorrect, Path(this_model_root_dir))
        return read_csmr_from_db(
            step2_best_codcorrect,
            csmr_cause_id,
            model_version_meta.model_version_id.squeeze())

    csmr_info = CSMRData(
        source='codcorrect', version_id=BEST_2019_CODCORRECT_VERSION_ID)

    record_csmr_version(
        csmr_info.source, csmr_info.version_id, Path(this_model_root_dir))

    df = read_csmr(
        csmr_cause_id, csmr_info.version_id, csmr_info.source, gbd_round_id)
    return df


def read_csmr(cause_id, cod_faux_version_id, source, gbd_round_id):
    """
    Given a cause_id, gbd_round_id,  and codcorrect or fauxcorrect version id,
    return a dataframe of imputed CSMR.

    This function applies to iterative for gbd 2020, and
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


def current_best_codcorrect(
    gbd_round_id=None,
    decomp_step=None
) -> Optional[int]:
    """
    Returns the most recent codcorrect marked best, optionally
    filtered to the given gbd_round_id. Generally the viz assigns
    this during model creation, but in the case where a model is trying
    to use missing csmr, we must fall back to current best codcorrect

    Arguments:
        gbd_round_id [int]: optionally filter to a specific round
        decomp_step [str]: optionally filter to a specific decomp_step
    """
    qry = """
        SELECT MAX(co.output_version_id) AS version
        FROM cod.output_version co
        JOIN shared.decomp_step ds USING (decomp_step_id)
        WHERE co.is_best = 1
        AND co.best_end IS NULL
        """
    if gbd_round_id:
        qry = qry + f" AND ds.gbd_round_id = {gbd_round_id}"
    if decomp_step:
        if not gbd_round_id:
            raise RuntimeError(
                'if decomp_step specified, must also provide gbd_round_id')
        decomp_step_id = decomp_step_id_from_decomp_step(
            decomp_step, gbd_round_id)
        qry = qry + f" AND ds.decomp_step_id = {decomp_step_id}"

    df = db.execute_select(
        qry,
        conn_def='cod'
    )
    return df.version.squeeze()


def missing_csmr(cause_id, codcorrect_version):
    return cause_id == 346 and codcorrect_version == 84


def update_if_decomp_exempt(
    model_version_meta,
    csmr_cause_id,
    codcorrect_version_id
):
    """
    if a model is decomp exempt, we want to use current best codcorrect
    """
    modelable_entity_id = model_version_meta.modelable_entity_id.squeeze()
    is_decomp_exempt = modelable_entity_id in decomp_exempt_mes_list()
    if is_decomp_exempt:
        return current_best_codcorrect()
    else:
        return codcorrect_version_id


def determine_source(
    cause_id,
    codcorrect_version_id,
    decomp_method,
    gbd_round_id
) -> Tuple[str, int]:
    """
    determine_source returns a tuple of:
        str: 'codcorrect' or 'fauxcorrect'
        int: associated version_id

    based on the provided arguments:
        cause_id (int): used to locate a fauxcorrect flat file
        codcorrect_version_id (int): what codcorrect version we intend
                to source csmr from
        decomp_method [Dict[str, bool]]: used to determine
                whether to read fauxcorrect or codcorrect csmr
        gbd_round_id (int): used to see if given gbd_round has a codcorrect
                version marked best

    We use fauxcorrect csmr if we're in step4 or iterative, and the csmr file
    for that cause exists (not all do), and there isn't a codcorrect version
    marked best for current round
    """
    fauxcorrect_file_exists = get_fauxcorrect_fpath(
        cause_id, FAUXCORRECT_VERSION, gbd_round_id).exists()
    codcorrect_best_for_current_round = current_best_codcorrect(
        gbd_round_id=gbd_round_id)

    if (
        decomp_method[Methods.ENABLE_CSMR_FAUXCORRECT] and
        fauxcorrect_file_exists and
        not codcorrect_best_for_current_round
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


def read_csmr_from_db(
    codcorrect_run_id: int,
    cause_id: int,
    model_version_id: int,
    age_standardized: bool = False
) -> pd.DataFrame:
    """
    We read imputed CSMR from flat files, but for codcorrect CSMR that doesn't
    need imputation, we can just read from the database.

    This function reads death counts for a given cause and codcorrect and then
    uses the current best population envelope for the given step to convert
    to rates.

    Args:
        codcorrect_run_id: the version of codcorrect to retrieve results for
        cause_id: the cause to get CSMR for
        model_version_id: used to determine demographics
        age_standardized: if True, return results for the age-standardized
            age_group_id. Used to calculate LNASDR.

    Returns:
        a dataframe with the following columns:
            ['location_id', 'time_lower', 'age_group_id', 'x_sex',
            'age_lower', 'age_upper', 'meas_value', 'meas_lower',
            'meas_upper']
    """
    process_version_df = db.execute_select(
        Queries.PROCESS_VERSION_OF_CODCORRECT, 'gbd',
        params={'codcorrect_version': codcorrect_run_id})
    try:
        process_version_id = int(
            process_version_df.gbd_process_version_id.iat[0])
    except IndexError:
        err = (
            f"Codcorrect run_id {codcorrect_run_id} was not found in gbd "
            "database"
        )
        raise RuntimeError(err)

    logging.info(f"CodCorrect process version id is {process_version_id}")

    demo = demographics.Demographics(model_version_id)
    if age_standardized:
        age_group_id = age.AGE_STANDARDIZED
    else:
        age_group_id = demo.mortality_age_grid
    df = shared_functions.get_outputs(
        "cause",
        process_version_id=process_version_id,
        cause_id=cause_id,
        cause_set_id=2,  # computation set, avoids cause-metadata call
        location_id="all",
        location_set_id=demo.DEFAULT_LOCATION_SET_ID,
        age_group_id=age_group_id,
        year_id=demo.mortality_years,
        sex_id=demo.sex_ids,
        metric_id=metrics.RATE
    )
    nulls = df.val.isna()
    percent_null = nulls.sum()/len(df)
    logging.info(
        f"Found {percent_null} fraction of CSMR null. Dropping {nulls.sum()} "
        "rows")
    df = df[~nulls]

    df = df.rename(columns={
        'sex_id': 'x_sex',
        'year_id': 'time_lower',
        'val': 'meas_value',
        'upper': 'meas_upper',
        'lower': 'meas_lower'
    })
    age_spans = shared_functions.get_age_spans().rename(
        columns={'age_group_years_start': 'age_lower',
                 'age_group_years_end': 'age_upper'})
    df = df.merge(age_spans, how='left')

    df = df[['location_id', 'time_lower', 'age_group_id', 'x_sex',
             'age_lower', 'age_upper', 'meas_value', 'meas_lower',
             'meas_upper']]
    if len(df) == 0:
        raise ValueError(
            f"There was a problem with the selected  CodCorrect version "
            f"({codcorrect_run_id}) + csmr_cause_id {cause_id}"
        )
    return df
