import concurrent.futures
import glob
import logging
import os

import pandas as pd

from gbd import constants

from hale.common import path_utils, query_utils, session_management
from hale.common.constants import columns


def upload_hale(hale_version: int, conn_def: str) -> None:
    """Infiles both single- and muti-year HALE summaries to the GBD database"""
    with concurrent.futures.ProcessPoolExecutor(2) as executor:
        executor.submit(_infile_summaries, hale_version, conn_def, True)
        executor.submit(_infile_summaries, hale_version, conn_def, False)


def _infile_summaries(
        hale_version: int,
        conn_def: str,
        single_year: bool
) -> None:
    suffix = 'summary'
    ordered_cols = columns.ORDERED_SINGLE
    if not single_year:
        suffix = 'percent_change'
        ordered_cols = columns.ORDERED_MULTI

    # Collect summary files.
    summaries_root = path_utils.get_summaries_root(hale_version)
    files = glob.glob(os.path.join(summaries_root, f'*{suffix}.csv'))
    summaries = pd.concat([pd.read_csv(f) for f in files])
    summaries = summaries[summaries.location_id.isin(
        [60356])]

    # Set constant measure and metric columns.
    summaries[columns.MEASURE_ID] = constants.measures.HALE
    summaries[columns.METRIC_ID] = constants.metrics.YEARS

    # Set permissions and save temporary CSV for infiling.
    # Sort in primary key order to speed up infiling.
    os.umask(0o0002)
    infile_path = path_utils.get_infile_path(hale_version, single=single_year)
    logging.info(f'Writing temporary infile CSV: {infile_path}')
    summaries\
        .rename(columns={
            columns.HALE_MEAN: columns.VAL,
            columns.HALE_LOWER: columns.LOWER,
            columns.HALE_UPPER: columns.UPPER})\
        .sort_values(by=ordered_cols)\
        .loc[:, ordered_cols]\
        .to_csv(infile_path, index=False)

    # Execute the infile.
    logging.info(f'Infiling from {infile_path}')
    with session_management.session_scope(conn_def) as session:
        query_utils.infile(session, hale_version, single=single_year)
