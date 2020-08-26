import glob
import os
import shutil

import numpy as np
import pandas as pd

import gbd
import gbd_outputs_versions
from db_tools import ezfuncs

import probability_of_death
from probability_of_death.lib.constants import columns, paths, queries

# Number of rows in one infile batch.
_CHUNK_SIZE = 1000000


def upload(gbd_round_id: int, decomp_step: str) -> None:
    """Creates a GBD process version and uploads a probability of death run to it.

    We take a few steps to improve infiling performance:
        - Sort in primary key order.
        - Turn off autocommit.
        - Turn off uniqueness checks. Check for uniqueness in-memory before infiling.
        - Turn off foreign key checks. This is probably okay since all of the data used in
            probability of death is pulled from the database, so it has already gone through
            foreign key checks.

    For more information, see:
    https://mariadb.com/kb/en/how-to-quickly-insert-data-into-mariadb/

    Args:
        gbd_round_id: ID of the GBD round to use to create a GBD process version.
        decomp_step: step to use to create a GBD process version.
    """
    upload_df = _get_upload_dataframe()
    num_chunks = len(upload_df) // _CHUNK_SIZE + 1
    _chunk_upload_dataframe(upload_df, num_chunks)

    version = gbd_outputs_versions.GBDProcessVersion.add_new_version(
        gbd_process_id=gbd.constants.gbd_process.PROBABILITY_OF_DEATH,
        gbd_process_version_note="Probability of Death",
        code_version=probability_of_death.__version__,
        metadata={},
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        env=gbd_outputs_versions.db.DBEnvironment.PROD,
    ).gbd_process_version_id

    _infile(num_chunks, table=f"gbd.output_pod_v{version}")
    shutil.rmtree(paths.POD_TMP)


def _get_upload_dataframe() -> pd.DataFrame:
    """Concatenates intermediate files to get total upload DataFrame.

    Also adds constant columns: upper, lower, measure_id, metric_id.
    Sorts in primary key order for infiling, and checks for duplicate primary keys since
    infiling operates with uniqueness checks disabled.

    Returns:
        DataFrame to upload.

    Raises:
        RuntimeError: if data contains duplicates by primary key columns.
    """
    files = glob.glob(os.path.join(paths.POD_TMP, "*.csv"))
    upload_df = (
        pd.concat([pd.read_csv(f) for f in files])
        .assign(
            **{
                columns.UPPER: r"\N",  # Assign nulls for upload
                columns.LOWER: r"\N",
                columns.MEASURE_ID: gbd.constants.measures.POD,
                columns.METRIC_ID: gbd.constants.metrics.PROBABILITY_OF_DEATH,
            }
        )
        .sort_values(by=columns.PRIMARY_KEY)
        .loc[:, columns.OUTPUT]
    )

    duplicates = upload_df.duplicated(subset=columns.PRIMARY_KEY)
    if duplicates.any():
        duplicate_rows = (
            upload_df.loc[duplicates, columns.PRIMARY_KEY].head().to_dict("records")
        )
        raise RuntimeError(f"Upload data contains duplicates: {duplicate_rows}")

    return upload_df


def _chunk_upload_dataframe(upload_df: pd.DataFrame, num_chunks: int) -> pd.DataFrame:
    """Splits upload DataFrame into chunks and saves chunks to CSVs for infiling."""
    os.umask(0o0002)
    for chunk_number, chunk in enumerate(np.array_split(upload_df, num_chunks)):
        chunk_path = paths.INFILE_FORMAT.format(chunk_number=chunk_number)
        chunk.to_csv(chunk_path, index=False)


def _infile(num_chunks: int, table: str) -> None:
    """Infiles data to a table in chunks."""
    session = ezfuncs.get_session("gbd")
    try:
        session.execute(queries.PREP_INFILE)
        session.commit()
        for chunk_number in range(num_chunks):
            infile_path = paths.INFILE_FORMAT.format(chunk_number=chunk_number)
            query = queries.INFILE.format(path=infile_path, table=table)
            session.execute(query)
            session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
