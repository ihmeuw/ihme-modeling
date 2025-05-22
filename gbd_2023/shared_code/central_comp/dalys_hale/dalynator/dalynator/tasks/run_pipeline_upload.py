import traceback
import logging
import time
import os
import json
import sys
from pathlib import Path

import pandas as pd
import glob
from sqlalchemy import exc
from typing import Iterable, List

from db_tools.loaders import Infiles
import db_tools_core
from dual_upload.lib.utils import constants as upload_constants
import gbd.conn_defs as gbd_conn_defs
import gbd.constants as gbd
from gbd_outputs_versions import GBDProcessVersion, DBEnvironment as DBEnv

import dalynator.app_common as ac

from dalynator import constants, get_input_args, logging_utils

VALID_COMPONENT_TYPES = ['risk', 'etiology', 'summary']

SUCCESS_LOG_MESSAGE = "DONE write File"

logger = logging_utils.module_logger(__name__)


def make_file_pattern(
    out_dir: str,
    location_id: int,
    measure_id: int,
    table_type: str,
    prefix: str
    ) -> str:
    """ Create a file pattern to pass to glob.glob

    Arguments:
        out_dir (str): absolute path up to nator version ID directory
        location_id (int): location ID to upload summaries for
        measure_id (int): measure ID to upload summaries for
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'

    Returns:
        str
    """
    file_pattern = "{o}/draws/{l}/upload/{m}/{tt}/{p}*.csv".format(
        o=out_dir, l=location_id, m=measure_id, tt=table_type, p=prefix)
    return file_pattern


def get_pct_change_years(out_dir: str) -> Iterable:
    """Get percent change years from cached version file."""
    versions_file = out_dir + "/gbd_processes.json"
    with open(versions_file, 'r') as fp:
        versions = json.load(fp)
    start_years = versions["metadata"][str(gbd.gbd_metadata_type.YEAR_START_IDS)]
    end_years = versions["metadata"][str(gbd.gbd_metadata_type.YEAR_END_IDS)]

    return zip(start_years, end_years)


def make_file_list(
    out_dir: str,
    measure_id: int,
    table_type: str,
    prefix: str,
    location_id: int,
    year_ids: List[int],
    ) -> List[Path]:
    """Search for csvs to upload and check for missing files

    Arguments:
        out_dir (str): absolute path up to nator id directory
        measure_id (int): either 1, 2, 3, or 4
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'
        location_id (int): location to upload
        year_ids (List(int)): year ids of the run

    Returns:
        List of Path objects
    """
    file_pattern = make_file_pattern(out_dir, location_id, measure_id, table_type, prefix)

    logger.info("Search for files in pattern {}".format(file_pattern))
    if table_type == constants.SINGLE_YEAR_TABLE_TYPE:
        files = [
            Path(file_pattern.replace("*", f"_{location_id}_{year_id}")) 
            for year_id in year_ids
        ]
    elif table_type == constants.MULTI_YEAR_TABLE_TYPE:
        pct_change_years = get_pct_change_years(out_dir)
        files = [
            Path(file_pattern.replace("*", f"_{location_id}_{year_start}_{year_end}")) 
            for year_start, year_end in pct_change_years
        ]
    else:
        raise ValueError("Impossible table type '{}'".format(table_type))

    missing_list = [file for file in files if not file.exists()]

    if len(missing_list) != 0:
        logger.error(f"There were {len(missing_list)} missing input files to the upload")
        raise ValueError(f"FAILING UPLOAD due to missing input files {missing_list}")

    if table_type == constants.SINGLE_YEAR_TABLE_TYPE:
        files = consolidate_year_summaries(files, file_pattern, location_id)

    return files


def consolidate_year_summaries(files: List[Path], file_pattern: str, location_id: int) -> List[Path]:
    """Consolidate multiple single-year files into a single file.
    """
    output_file = Path(file_pattern.replace("*", f"_{location_id}"))
    with open(output_file, "w") as out_fp:
        for file in files:
            with open(file, "r") as in_fp:
                if file != files[0]:
                    # advance past the header, except for the first one
                    in_fp.readline()
                out_fp.write(in_fp.read())

    logger.info(f"Consolidated single year files to {output_file}.")
    return [output_file]


def count_rows_from_file_pattern(file_pattern: str, row_count: int) -> int:
    logger.info("Search for files in pattern {}".format(file_pattern))
    for f in glob.glob(file_pattern):
        logger.info("Appending {}".format(f))
        row_count += len(pd.read_csv(f))
    return row_count


def output_null_inf_count(
        read_dir: str,
        write_dir: str,
        table_type: str,
        prefix: str,
        location_id: int,
        measure_id: int
    ) -> None:
    """
    Separate the reading directory from the writing directory so that
    tests can read from read-only data directories.

    Args:
        read_dir (str): Where the nulls are
        write_dir (str):  Where the resulting summary file will be written
        table_type (str): The table_type in the filepath. Either 'single_year' or 'multi_year'
        prefix (str): The prefix in the filepath. Either 'upload_risk' or 'upload_eti' 
        location_id (int): The location ID in the file path
        measure_id (int): The measure ID in the file path
    """
    null_row_count = 0
    inf_row_count = 0
    null_file_pattern = make_file_pattern(read_dir,
                                          location_id,
                                          measure_id,
                                          table_type,
                                          'NONE_{}'.format(prefix))
    
    inf_file_pattern = make_file_pattern(read_dir,
                                          location_id,
                                          measure_id,
                                         table_type,
                                         'INF_{}'.format(prefix))
    null_row_count = count_rows_from_file_pattern(null_file_pattern,
                                                  null_row_count)
    inf_row_count = count_rows_from_file_pattern(inf_file_pattern,
                                                 inf_row_count)
    fn = os.path.join(write_dir, "null_inf_json", "null_inf_{}_{}_{}_{}.json".format(
        table_type, prefix, location_id, measure_id))
    to_json = {'null': null_row_count, 'inf': inf_row_count}
    with open(fn, 'w') as f:
        json.dump(to_json, f)


class NatorUpload:

    _schema = "gbd"

    def __init__(self,
                 upload_root: str,
                 gbd_process_version_id: int,
                 gbd_component: str,
                 table_type: str,
                 measure_id: int,
                 table_info: pd.DataFrame,
                 upload_to_test: bool):
        self.upload_root = upload_root
        self.gbd_process_version_id = gbd_process_version_id
        self.gbd_component = self.validate_component(gbd_component)
        self.table_type = self.validate_table_type(table_type)
        self.measure_id = self.validate_measure(measure_id)
        self.upload_to_test = upload_to_test
        self.table_info = table_info
        self.table_name = self.get_table_name()
        self.conn_def = self.get_conn_def()
        self._engine = None

    @staticmethod
    def validate_table_type(table_type: str) -> str:
        if table_type not in constants.NATOR_TABLE_TYPES:
            raise ValueError(f"Only {constants.NATOR_TABLE_TYPES} are valid "
                             f"table_types. Passed: {table_type}")
        return table_type

    @staticmethod
    def validate_component(gbd_component: str) -> str:
        if gbd_component not in VALID_COMPONENT_TYPES:
            raise ValueError(f"Only {VALID_COMPONENT_TYPES} are valid "
                             f"gbd_components. Passed: {gbd_component}")
        return gbd_component
    
    @staticmethod
    def validate_measure(measure_id: int) -> int:
        if measure_id not in constants.MEASURE_TYPES:
            raise ValueError(f"Only {constants.MEASURE_TYPES.keys()} are valid "
                             f"measure ids. Passed: {measure_id}")
        return measure_id

    def get_table_name(self) -> str:
        """Get the table name from GBD process version table info."""
        table_name = self.table_info[
            (self.table_info["measure_id"] == self.measure_id)
            & (self.table_info["output_table_name"].str.contains(self.table_type))
        ].output_table_name.iloc[0]
        return table_name
    
    def get_conn_def(self) -> str:
        """Identify conn def via table name."""
        if self.upload_to_test:
            conn_def = gbd_conn_defs.GBD_TEST
        else:
            host = self.table_info[
                self.table_info.output_table_name == self.table_name
            ].output_data_host_name.iloc[0]
            conn_def = gbd_conn_defs.get_conn_def_from_host_name_and_user_name()
        return conn_def

    def get_file_pattern(self, location_id: int) -> str:
        if self.gbd_component == 'etiology':
            file_prefix = 'eti'
        else:
            file_prefix = self.gbd_component
        return (f"upload_{file_prefix}*.csv")

    def run_internal_upload_task(self, file_list: List[Path]) -> None:
        with db_tools_core.session_scope(conn_def=self.conn_def) as scoped_session:
            scoped_session.execute("SET wait_timeout=3600")
            infiler = Infiles(self.table_name, self._schema, scoped_session)
            for file in file_list:
                try:
                    start_time = time.time()
                    infiler.infile(str(file), with_replace=False, commit=False,
                                rename_cols={'mean': 'val'})
                    end_time = time.time()
                    elapsed = end_time - start_time
                except exc.IntegrityError:
                    logger.info("Recieved IntegrityError for file {}, "
                                "skipping infile at time {}".format(
                                    file, time.time()))
                except Exception as e:
                    logger.info(e)
                    raise


def run_upload(
    out_dir: str,
    gbd_process_version_id: int,
    location_id: int,
    table_type: str,
    measure_id: int,
    year_ids: List[int],
    upload_to_test: bool
) -> None:
    """
    Upload summary files to GBD Outputs risk tables

    Args:
        out_dir (str): the root directory for this run
        gbd_process_version_id (int): GBD Process Version to upload to
        location_id (int): location id to upload for
        table_type (str): type of DB table to upload into
            (single or multi year)
        measure_id (int): measure id to upload for
        year_ids (List[int]): years for the run
        upload_to_test (bool): determines whether to upload to the test db
    """
    start_time = time.time()
    logger.info("START pipeline upload at {}".format(start_time))
    # Get process version
    if upload_to_test:
        upload_env = DBEnv.DEV
    else:
        upload_env = DBEnv.PROD

    pv = GBDProcessVersion(gbd_process_version_id, env=upload_env)
    
    # Get the prefix for file names
    prefix = f"upload_{constants.PROCESS_NAMES_SHORT[pv.gbd_process_id]}"
    gbd_component = upload_constants.PROCESS_NAMES[pv.gbd_process_id]

    output_null_inf_count(out_dir, out_dir, table_type, prefix, location_id, measure_id)

    logger.info(
        "Uploading {} data, {}, measure id {}, location id {}, time = {}".format(
            gbd_component, table_type, measure_id, location_id, time.time()
        )
    )

    file_list = make_file_list(out_dir, measure_id, table_type, prefix,
                            location_id, year_ids)

    uploader = NatorUpload(
        out_dir,
        gbd_process_version_id,
        gbd_component,
        table_type,
        measure_id,
        pv.table_info,
        upload_to_test,
    )

    uploader.run_internal_upload_task(file_list)
    logger.info("End uploading data, time = {}".format(time.time()))

    # Delete temporary file made from consolidating single year files
    if table_type == constants.SINGLE_YEAR_TABLE_TYPE:
        file_list[0].unlink()

    # End log
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE upload pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    """ Upload Burdenator or Dalynator summaries to the internal database.

        Args:
        gbd_process_version_id (int): Process version to upload. Used to
            determine the table name to upload to
        table_type (str): single_year or multi_year
        measure_id (int): measure id to upload
        year_id (List[int]): years for the run
        out_dir (str): The output directory of this 'nator run
        location_id (int): The location ID to upload summaries for
        upload_to_test (optional flag): If included, sorts the summaries in
            preparation to upload to the test database. Production otherwise
"""

    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_upload()
    args = get_input_args.construct_args_upload(parser)

    run_upload(
        out_dir=args.out_dir,
        gbd_process_version_id=args.gbd_process_version_id,
        location_id=args.location_id,
        table_type=args.table_type,
        measure_id=args.measure_id,
        year_ids=args.year_ids,
        upload_to_test=args.upload_to_test
    )


if __name__ == "__main__":
    main()
