import traceback
import logging
import time
import os
import json
import sys

import pandas as pd
import glob
from numpy import atleast_1d
from sqlalchemy import exc
from multiprocessing import Queue, Process
from typing import List, Dict

from gbd_outputs_versions import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv
from gbd_outputs_versions.db import DBStorageEngine, getDSN
from db_tools.loaders import Infiles
from db_tools.ezfuncs import get_engine

from dalynator import get_input_args
from dalynator.write_csv import sub_pub_for_cc

# Define GBD Process IDs for risk and etiology
ETI_GBD_PROCESS_ID = 5
RISK_GBD_PROCESS_ID = 4
SUMM_GBD_PROCESS_ID = 6

# LOOKUP TO GET MEASURE LABELS FROM MEASURE IDS
MEASURE_LABELS = {
    1: "death",
    2: "daly",
    3: "yld",
    4: "yll",
}

VALID_COMPONENT_TYPES = ['risk', 'etiology', 'summary']
VALID_TABLE_TYPES = ['single_year', 'multi_year']

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

logger = logging.getLogger('dalynator.tasks.run_pipeline_upload')


def find_file_list_from_file_pattern(file_pattern):
    logger.info("Search for files in pattern {}".format(file_pattern))
    return glob.glob(file_pattern)


def format_file_names(file_list, base_dir, table_type):
    """Given a list of absolute paths to csvs, and the base path up to
    base folder "draws", extract location_id and year_id and return
    a dataframe containing files in sorted order
    """
    raw_df = pd.DataFrame(file_list, columns=["raw"])
    len_of_base = len(base_dir.split("/"))
    dir_splits = raw_df.raw.str.split("/")
    fname_splits = dir_splits.map(lambda lst: lst[-1]).str.split("_")

    raw_df['location_id'] = dir_splits.map(
        lambda lst: int(lst[len_of_base + 1]))
    raw_df['measure_id'] = dir_splits.map(
        lambda lst: int(lst[len_of_base + 3]))
    if table_type == 'single_year':
        raw_df['year_id'] = fname_splits.map(
            lambda lst: int(lst[3].replace(".csv", "")))
    elif table_type == 'multi_year':
        raw_df['year_start_id'] = fname_splits.map(
            lambda lst: int(lst[3].replace(".csv", "")))
        raw_df['year_end_id'] = fname_splits.map(
            lambda lst: int(lst[4].replace(".csv", "")))

    return raw_df


def make_file_pattern(out_dir, table_type, prefix):
    """ Create a file pattern to pass to glob.glob

    Arguments:
        out_dir (str): absolute path up to nator id directory
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'

    Returns:
        str
    """

    file_pattern = "FILEPATH".format(
        o=out_dir, tt=table_type, p=prefix)
    return file_pattern


def make_file_df_cs(out_dir, table_type, prefix,
                    location_ids=None):
    """ search for csvs to upload and return a sorted table of file paths

    Arguments:
        out_dir (str): absolute path up to nator id directory
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'
        location_ids (List[int], optional): if specified, subset files
            based on this list of ids

    Returns:
        pd.DataFrame
    """
    raise NotImplementedError


def make_file_df(out_dir, table_type, prefix, location_ids=None):
    """ search for csvs to upload and return a sorted table of file paths

    Arguments:
        out_dir (str): absolute path up to nator id directory
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'
        location_ids (List[int], optional): if specified, subset files
            based on this list of ids

    Returns:
        pd.DataFrame
    """
    file_pattern = make_file_pattern(out_dir, table_type, prefix)
    file_names = find_file_list_from_file_pattern(file_pattern)
    file_df = format_file_names(file_names, out_dir, table_type)
    # Check that we have all the files we expect to have.
    # Should be a full cross-product

    if table_type == 'single_year':
        cols = ["measure_id", "year_id", "location_id"]
        file_pattern_no_stars = ("FILEPATH")
    elif table_type == 'multi_year':
        cols = ["measure_id", "year_start_id", "year_end_id", "location_id"]
        file_pattern_no_stars = ("FILEPATH")
    else:
        raise ValueError("Impossible table type '{}'".format(table_type))

    file_df = file_df.sort_values(cols)
    raise_if_missing_input_files(file_df, table_type, out_dir, prefix, cols,
                                 file_pattern_no_stars)
    if location_ids:
        file_df = file_df[file_df.location_id.isin(location_ids)]
    return file_df


def raise_if_missing_input_files(file_df, table_type, out_dir, prefix, cols,
                                 validate_path_pattern):
    """
    Logs missing files as defined by find_missing_input_files.
    Raises ValueError if any files are missing.
    """
    missing_df = find_missing_input_files(file_df, table_type, out_dir, prefix,
                                          cols, validate_path_pattern)
    r, c = missing_df.shape
    if r != 0:
        logger.error("There were {} missing input files to the upload, "
                     "see following lines:".format(r))
        for f in missing_df['raw']:
            logger.error("  Missing {}".format(f))
        raise ValueError("FAILING UPLOAD due to {} missing input files, see "
                         "individual upload log file for full list".format(r))


def find_missing_input_files(file_df, table_type, out_dir, prefix, cols,
                             validate_path_pattern):
    """Returns a dataframe with all the missing files, assuming that it it
    should be square in hyperspace.
    Filenames are absolute, i.e. from / """
    num_rows, num_cols = file_df.shape

    # Quick check on the cross-product size of the hypercube
    indexes = {}
    size_cross_product = 1
    if table_type == "multi_year":
        cols.remove("year_start_id")
        cols.remove("year_end_id")
        tup_cols = ['year_start_id', 'year_end_id']
        uniq = file_df[tup_cols].drop_duplicates()
        indexes['year_start_end_tup'] = list(zip(uniq['year_start_id'],
                                                 uniq['year_end_id']))
    for col in cols:
        indexes[col] = file_df[col].unique()
        size_cross_product *= len(indexes[col])

    cross_product_df = pd.MultiIndex.from_product(
        iterables=indexes.values(), names=indexes.keys()).to_frame(index=False)

    if table_type == 'single_year':
        cross_product_df['raw'] = cross_product_df.apply(
            lambda row: validate_path_pattern.format(
                o=out_dir, p=prefix, m=row['measure_id'], y=row['year_id'],
                loc=row['location_id']), axis=1)
    else:
        cross_product_df['year_start_id'] = (
            cross_product_df['year_start_end_tup'].apply(lambda x: x[0]))
        cross_product_df['year_end_id'] = (
            cross_product_df['year_start_end_tup'].apply(lambda x: x[1]))
        cross_product_df['raw'] = cross_product_df.apply(
            lambda row: validate_path_pattern.format(
                o=out_dir, p=prefix, m=row['measure_id'],
                y1=row['year_start_id'], y2=row['year_end_id'],
                loc=row['location_id']), axis=1)

    # Do a merge with the indicator column set
    cross_product_df = cross_product_df.merge(file_df, on='raw', how='outer',
                                              indicator=True)
    # Drop all that were matched
    cross_product_df = cross_product_df[
        cross_product_df['_merge'] == 'left_only']
    # Reduce columns to just the original set of indexes, plus 'raw'
    del cross_product_df['_merge']
    for col in cols:
        del cross_product_df["{}_y".format(col)]
        cross_product_df.rename(index=str, columns={"{}_x".format(col): col},
                                inplace=True)
    return cross_product_df


def count_rows_from_file_pattern(file_pattern, row_count):
    logger.info("Search for files in pattern {}".format(file_pattern))
    for f in glob.glob(file_pattern):
        logger.info("Appending {}".format(f))
        row_count += len(pd.read_csv(f))
    return row_count


def output_null_inf_count(read_dir, write_dir, table_type, prefix):
    """
    Separate the reading directory from the writing directory so that
    tests can read from read-only data directories.

    :param read_dir:  Where the nulls are
    :param write_dir:  Where the resulting summary file will be written
    """
    null_row_count = 0
    inf_row_count = 0
    null_file_pattern = make_file_pattern(read_dir,
                                          table_type,
                                          'FILEPATH'.format(prefix))
    inf_file_pattern = make_file_pattern(read_dir,
                                         table_type,
                                         'FILEPATH'.format(prefix))
    null_row_count = count_rows_from_file_pattern(null_file_pattern,
                                                  null_row_count)
    inf_row_count = count_rows_from_file_pattern(inf_file_pattern,
                                                 inf_row_count)
    fn = os.path.join(write_dir, "FILEPATH".format(
        table_type, prefix))
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
                 location_ids: List[int],
                 upload_to_test: bool,
                 storage_engine: str='INNODB'):
        self.upload_root = upload_root
        self.gbd_process_version_id = gbd_process_version_id
        self.gbd_component = self.validate_component(gbd_component)
        self.table_type = self.validate_table_type(table_type)
        self.location_ids = sorted(list(atleast_1d(location_ids)))
        self.upload_to_test = upload_to_test
        self.storage_engine = storage_engine
        self.upload_env = DBEnv.DEV if self.upload_to_test else DBEnv.PROD
        self.table_name = self.get_table_name()
        self.conn_def = getDSN(
            self.upload_env, DBStorageEngine[self.storage_engine])
        self._engine = None

    @staticmethod
    def validate_table_type(table_type):
        if table_type not in VALID_TABLE_TYPES:
            raise ValueError(f"Only {VALID_TABLE_TYPES} are valid "
                             f"table_types. Passed: {table_type}")
        return table_type

    @staticmethod
    def validate_component(gbd_component):
        if gbd_component not in VALID_COMPONENT_TYPES:
            raise ValueError(f"Only {VALID_COMPONENT_TYPES} are valid "
                             f"gbd_components. Passed: {gbd_component}")
        return gbd_component

    @property
    def engine(self):
        if not self._engine:
            self._engine = get_engine(conn_def=self.conn_def,
                                      connectable=False)
            self._engine.pool_recycle = 40.0
        return self._engine

    def get_table_name(self):
        return (f"output_{self.gbd_component}_{self.table_type}_"
                f"v{self.gbd_process_version_id}")

    def get_file_pattern(self, location_id):
        if self.gbd_component == 'etiology':
            file_prefix = 'eti'
        else:
            file_prefix = self.gbd_component
        return (f"FILEPATH")

    def make_partitions(self):
        session = self.engine.create_session().session
        for loc in self.location_ids:
            try:
                session.execute(
                    "CALL gbd.add_partition(:schema, :table, :location_id)",
                    params={
                        'schema': self._schema,
                        'table': self.table_name,
                        'location_id': loc
                        })
                session.commit()
            except exc.OperationalError as e:
                if 'Duplicate partition' in str(e):
                    pass
                else:
                    raise
        session.close()

    def load_location(self, file_list):
        self.engine.engine.dispose()
        session = self.engine.create_session(scoped=True).session
        infiler = Infiles(self.table_name, self._schema, session)
        for file in file_list:
            try:
                start_time = time.time()
                infiler.infile(file, with_replace=False, commit=True,
                               rename_cols={'mean': 'val'})
                end_time = time.time()
                elapsed = end_time - start_time
                logger.info("Infiling file {} at time {}".format(
                    file, elapsed))
            except exc.IntegrityError:
                logger.info("Recieved IntegrityError for file {}, "
                            "skipping infile at time {}".format(
                                file, time.time()))
            except Exception as e:
                logger.info(e)
                raise
        session.close()

    def _q_upload_location(self, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                self.load_location(*params)
                out_q.put((False, params))
            except Exception as e:
                tb = traceback.format_exc()
                out_q.put((ExceptionWrapper(e), tb, params))

    def run_all_uploads_mp(self,
                           file_dict: Dict,
                           n_processes: Dict={'risk': {'single_year': 6,
                                                       'multi_year': 4},
                                              'etiology': {'single_year': 4,
                                                           'multi_year': 2},
                                              'summary': {'single_year': 10,
                                                          'multi_year': 10}
                                             }):

        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # spin up the processes
        procs = []
        for i in range(n_processes[self.gbd_component][self.table_type]):
            # run upload in parallel
            p = Process(target=self._q_upload_location,
                        args=(inq, outq))
            procs.append(p)
            p.start()

        # upload summaries
        for location_id in self.location_ids:
            inq.put((file_dict[location_id],))

        # make the workers die after
        for _ in procs:
            inq.put(None)
        # get results
        results = []
        for _ in self.location_ids:
            proc_result = outq.get()
            results.append(proc_result)

        # check for errors
        for result in results:
            if result[0]:
                print(result[2:])
                logger.error(f"Failed upload job with exception {result[1]}. "
                             f"Ceased at time: {time.time()}")
                raise result[0].re_raise()


class ExceptionWrapper:

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        raise self.ee.with_traceback(self.tb)


def run_upload(out_dir, gbd_process_version_id, location_ids,
               table_type, upload_to_test, storage_engine='INNODB'):
    """
    Upload summary files to GBD Outputs risk tables

    Args:
        out_dir (str): the root directory for this run
        gbd_process_version_id (int): GBD Process Version to upload to
        location_ids (List[int]): location ids used in this run
        table_type (str): type of DB table to upload into
            (single or multi year)
        upload_to_test (bool): determines whether to upload to the test db
        storage_engine (str, default='INNODB'): either 'COLUMNSTORE' or
            'INNODB.' Determines which database to upload to.
        skip_raise_on (Exception, tuple): will not raise on errors specified.
            All other exceptions are raised and result in a failed job.
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
    if pv.gbd_process_id == RISK_GBD_PROCESS_ID:
        prefix = 'upload_risk'
        gbd_component = 'risk'
    elif pv.gbd_process_id == ETI_GBD_PROCESS_ID:
        prefix = 'upload_eti'
        gbd_component = 'etiology'
    elif pv.gbd_process_id == SUMM_GBD_PROCESS_ID:
        prefix = 'upload_summary'
        gbd_component = 'summary'

    output_null_inf_count(out_dir, out_dir, table_type, prefix)

    # call load data infile on summary files in primary key order
    if storage_engine == "INNODB":
        file_df = make_file_df(out_dir, table_type, prefix,
                               location_ids)
    elif storage_engine == "COLUMNSTORE":
        file_df = make_file_df_cs(out_dir, table_type, prefix,
                                  location_ids)
    else:
        logger.info("Unxepected storage engine type when creating file_df {}".format(storage_engine))
        file_df = make_file_df(out_dir, table_type, prefix,
                               location_ids)

    logger.info("Uploading {} data, {}, time = {}".format(gbd_component,
                                                          table_type,
                                                          time.time()))
    uploader = NatorUpload(out_dir, gbd_process_version_id, gbd_component,
                           table_type, location_ids, upload_to_test,
                           storage_engine=storage_engine)
    uploader.make_partitions()
    # make a dictionary where keys are location_id and values
    # are lists of file paths for that location
    file_dict = {
        location_id: file_df.loc[
            file_df['location_id'] == location_id, 'raw'].tolist()
        for location_id in uploader.location_ids}
    uploader.run_all_uploads_mp(file_dict)
    logger.info("End uploading data, time = {}".format(time.time()))

    # End log
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE upload pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_upload()
    args = get_input_args.construct_args_upload(parser)

    if args.storage_engine == "COLUMNSTORE":
        upload_root = sub_pub_for_cc(args.out_dir)
    else:
        upload_root = args.out_dir

    run_upload(upload_root, args.gbd_process_version_id, args.location_ids,
               args.table_type, args.upload_to_test,
               args.storage_engine)


if __name__ == "__main__":
    main()
