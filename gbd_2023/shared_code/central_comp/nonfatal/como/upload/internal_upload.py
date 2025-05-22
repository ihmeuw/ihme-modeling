"""Uploads to internal gbd host."""

import glob
import logging
from typing import List

from loguru import logger
from sqlalchemy.exc import IntegrityError, OperationalError, ResourceClosedError

import db_tools_core
from db_tools.loaders import Infiles
from gbd import conn_defs

from como.lib import constants as como_constants
from como.lib.upload import upload_utils
from como.lib.version import ComoVersion

logging.basicConfig(level=logging.INFO)

year_type_dict = {
    como_constants.YearType.SINGLE.value: {
        como_constants.Component.CAUSE.value: "output_epi_single_year_v{}",
        como_constants.Component.IMPAIRMENT.value: "output_impairment_single_year_v{}",
        como_constants.Component.INJURY.value: "output_injury_single_year_v{}",
        como_constants.Component.SEQUELA.value: "output_sequela_single_year_v{}",
    },
    como_constants.YearType.MULTI.value: {
        como_constants.Component.CAUSE.value: "output_epi_multi_year_v{}",
        como_constants.Component.IMPAIRMENT.value: "output_impairment_multi_year_v{}",
        como_constants.Component.INJURY.value: "output_injury_multi_year_v{}",
        como_constants.Component.SEQUELA.value: "output_sequela_multi_year_v{}",
    },
}


def configure_upload(como_version: ComoVersion, all_locs: List[int]) -> None:
    """Configure internal gbd host upload."""
    logger.info("configuring internal host gbd tables.")

    _make_partitions(
        como_version=como_version,
        year_type=como_constants.YearType.SINGLE.value,
        location_ids=all_locs,
    )
    if como_version.change_years:
        _make_partitions(
            como_version=como_version,
            year_type=como_constants.YearType.MULTI.value,
            location_ids=all_locs,
        )


def run_upload(upload_task: upload_utils.UploadTask) -> None:
    """Run internal gbd upload."""
    with db_tools_core.session_scope(conn_def=conn_defs.GBD) as scoped_session:
        table_tmp = year_type_dict[upload_task.year_type][upload_task.component]
        table = table_tmp.format(upload_task.process_version_id)
        infiler = Infiles(table, "gbd", scoped_session)

        indir_glob_root = ("FILEPATH")
        if upload_task.year_type == como_constants.YearType.SINGLE.value:
            indir_glob = indir_glob_root + "/*.csv"
            infiler.indir(
                path=indir_glob,
                commit=True,
                partial_commit=True,
                with_replace=False,
                rename_cols={"mean": "val"},
                no_raise=(IntegrityError, OperationalError, ResourceClosedError),
            )
        else:
            indir_glob = indir_glob_root + ".csv"
            file_path = glob.glob(indir_glob)
            if len(file_path) != 1:
                raise RuntimeError(
                    f"Expected 1 file to match {indir_glob} found {len(file_path)}"
                )
            infiler.infile(
                path=file_path[0],
                commit=True,
                with_replace=False,
                rename_cols={"mean": "val"},
            )


def _make_partitions(
    como_version: ComoVersion, year_type: str, location_ids: List[int]
) -> None:
    """Make db partitions on internal gbd host.

    Note: this uses cursor.callproc() for SPROCs as recommended by SQLAlchemy.
          It's engine.dispose() call takes a few seconds for the db thread to
          actually close, so you might see a thread sleep for a few seconds on
          the db's PROCESSLIST
    """
    schema = "gbd"
    engine = db_tools_core.get_engine(conn_def=conn_defs.GBD)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    for component in como_version.components:
        table_tmp = year_type_dict[year_type][component]
        table = table_tmp.format(como_version.gbd_process_version_id)
        location_ids.sort()
        for location_id in location_ids:
            try:
                cursor.callproc("gbd.add_partition", [schema, table, location_id])
                connection.commit()
            except Exception as e:
                logger.error(
                    f"make_partitions(CALL gbd.add_partition({schema} table: "
                    f"{table}, location_id: {location_id}). Exception: {e}"
                )
                pass
    connection.close()
    engine.dispose()
