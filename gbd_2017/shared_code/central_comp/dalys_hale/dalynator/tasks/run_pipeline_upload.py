import traceback
import logging
import time
import os
import json

import pandas as pd
import glob
from gbd_outputs_versions import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv
from gbd_outputs_versions.db import DBStorageEngine, getDSN
from db_tools.loaders import Infiles
from db_tools.ezfuncs import get_session

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

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

logger = logging.getLogger('dalynator.tasks.run_pipeline_upload')


def find_file_list_from_file_pattern(file_pattern):
    logger.info("Search for files in pattern {}".format(file_pattern))
    return glob.glob(file_pattern)


def format_file_names(file_list, base_dir, table_type):
    """Given a list of absolute paths to csvs, and the base path up to
    base folder "draws", extract location_id and year_id and return
    a dataframe containing files in sorted order (by year, then location)
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


def make_file_pattern(out_dir, measure_id, table_type, prefix):
    """ Create a file pattern to pass to glob.glob

    Arguments:
        out_dir (str): absolute path up to nator id directory
        measure_id (int): measure_id to upload
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'

    Returns:
        str
    """

    # Notice that an underscore is not needed after {p}
    file_pattern = "{o}/draws/*/upload/{m}/{tt}/{p}*.csv".format(
        o=out_dir, m=measure_id, tt=table_type, p=prefix)
    return file_pattern


def make_file_df(out_dir, measure_id, table_type, prefix, location_ids=None):
    """ search for csvs to upload and return a sorted table of file paths

    Arguments:
        out_dir (str): absolute path up to nator id directory
        measure_id (int): measure_id to upload
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'
        location_ids (List[int], optional): if specified, subset files
            based on this list of ids

    Returns:
        pd.DataFrame
    """
    file_pattern = make_file_pattern(out_dir, measure_id, table_type, prefix)
    file_names = find_file_list_from_file_pattern(file_pattern)
    file_df = format_file_names(file_names, out_dir, table_type)
    # Check that we have all the files we expect to have.
    # Should be a full cross-product

    if table_type == 'single_year':
        cols = ["measure_id", "year_id", "location_id"]
        file_pattern_no_stars = ("{o}/draws/{loc}/upload/{m}/single_year/"
                                 "{p}_{loc}_{y}.csv")
    elif table_type == 'multi_year':
        cols = ["measure_id", "year_start_id", "year_end_id", "location_id"]
        file_pattern_no_stars = ("{o}/draws/{loc}/upload/{m}/multi_year/"
                                 "{p}_{loc}_{y1}_{y2}.csv")
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
    should be square in hyperspace."""
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


def upload_to_table(file_list, table_name, dsn, raise_on_failure):
    sesh = get_session(conn_def=dsn, connectable=True)
    infiler = Infiles(table=table_name, schema="gbd", session=sesh)

    logger.info("Beginning infile, time = {}".format(time.time()))
    for f in file_list:
        try:
            logger.info("infiling {}, time = {}".format(f, time.time()))
            # summaries are saved with col mean, need to upload to table as
            # val
            infiler.infile(f, with_replace=False, commit=True,
                           rename_cols={'mean': 'val'})
        except Exception:
            if raise_on_failure:
                raise
            tb = traceback.format_exc()
            logger.error("failed to infile {} with exception {};"
                         "Skipping to next file".format(f, tb))
    logger.info("Done infile, time = {}".format(time.time()))


def count_rows_from_file_pattern(file_pattern, row_count):
    logger.info("Search for files in pattern {}".format(file_pattern))
    for f in glob.glob(file_pattern):
        logger.info("Appending {}".format(f))
        row_count += len(pd.read_csv(f))
    return row_count


def output_null_inf_count(out_dir, measure_id, table_type, prefix):
    null_row_count = 0
    inf_row_count = 0
    null_file_pattern = make_file_pattern(out_dir, measure_id,
                                          table_type,
                                          'NONE_{}'.format(prefix))
    inf_file_pattern = make_file_pattern(out_dir, measure_id,
                                         table_type,
                                         'INF_{}'.format(prefix))
    null_row_count = count_rows_from_file_pattern(null_file_pattern,
                                                  null_row_count)
    inf_row_count = count_rows_from_file_pattern(inf_file_pattern,
                                                 inf_row_count)
    fn = os.path.join(out_dir, "null_inf_{}_{}_{}.json".format(
        measure_id, table_type, prefix))
    to_json = {'null': null_row_count, 'inf': inf_row_count}
    with open(fn, 'w') as f:
        json.dump(to_json, f)


def run_upload(out_dir, gbd_process_version_id, location_ids, measure_id,
               table_type, upload_to_test, storage_engine='INNODB',
               raise_on_error=False):
    """
    Upload summary files to GBD Outputs risk tables

    Args:
        out_dir (str): the root directory for this run
        gbd_process_version_id (int): GBD Process Version to upload to
        location_id (List[int]): location ids used in this run
        measure_id (int): measure_id of the upload
        table_type (str): type of DB table to upload into
            (single or multi year)
        upload_to_test (bool): determines whether to upload to the test db
        storage_engine (str, default='INNODB'): either 'COLUMNSTORE' or
            'INNODB.' Determines which database to upload to.
        raise_on_error (bool, False): While infiling, if an exception is
            caught, raise. If False, will just log and continue to next file
    """
    start_time = time.time()
    logger.info("START pipeline upload at {}".format(start_time))
    # Get process version
    if upload_to_test:
        upload_env = DBEnv.DEV
    else:
        upload_env = DBEnv.PROD
    storage_engine = DBStorageEngine[storage_engine]
    odbc_dsn = getDSN(upload_env, storage_engine)

    pv = GBDProcessVersion(gbd_process_version_id, env=upload_env)

    # Get the prefix for file names
    if pv.gbd_process_id == RISK_GBD_PROCESS_ID:
        prefix = 'upload_risk'
    elif pv.gbd_process_id == ETI_GBD_PROCESS_ID:
        prefix = 'upload_eti'
    elif pv.gbd_process_id == SUMM_GBD_PROCESS_ID:
        prefix = 'upload_summary'

    output_null_inf_count(out_dir, measure_id, table_type, prefix)

    # call load data infile on summary files in primary key order
    # (year/location)
    file_df = make_file_df(out_dir, measure_id, table_type, prefix,
                           location_ids)

    # Upload data
    if pv.gbd_process_id == RISK_GBD_PROCESS_ID:
        table_name = "output_risk_{}_v{}".format(
            table_type, pv.gbd_process_version_id)
    elif pv.gbd_process_id == ETI_GBD_PROCESS_ID:
        table_name = "output_etiology_{}_v{}".format(
            table_type, pv.gbd_process_version_id)
    elif pv.gbd_process_id == SUMM_GBD_PROCESS_ID:
        table_name = "output_summary_{}_v{}".format(
            table_type, pv.gbd_process_version_id)

    # Columnstore has different table structure, sharded by measure
    if storage_engine == DBStorageEngine.COLUMNSTORE:
        table_name = "{tn}_{ml}".format(
            tn=table_name, ml=MEASURE_LABELS[measure_id])
    logger.info("Uploading data to table {}, time = {}".format(table_name,
                                                               time.time()))
    upload_to_table(file_df.raw.tolist(), table_name, odbc_dsn,
                    raise_on_error)
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
               args.measure_id, args.table_type, args.upload_to_test,
               args.storage_engine)


if __name__ == "__main__":
    main()
