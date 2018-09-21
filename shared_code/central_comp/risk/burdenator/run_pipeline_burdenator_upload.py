import os
import traceback
import logging
import time

import pandas as pd
import glob
from gbd_outputs_versions import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv
from gbd_outputs_versions.db import _ConnDefs
from db_tools.loaders import Infiles
from db_tools.ezfuncs import get_session

import dalynator.get_input_args as get_input_args

# Define GBD Process IDs for risk and etiology
ETI_GBD_PROCESS_ID = 5
RISK_GBD_PROCESS_ID = 4

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

logger = logging.getLogger(__name__)


def find_file_list_from_file_pattern(file_pattern):
    logger.info("Search for files in pattern {}".format(file_pattern))
    return glob.glob(file_pattern)


def format_file_names(file_list, base_dir, table_type):
    """Given a list of absolute paths to csvs, and the base path up to
    burdenator folder "draws", extract location_id and year_id and return
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
    ''' Create a file pattern to pass to glob.glob

    Arguments:
        out_dir (str): absolute path up to burdenator id directory
        measure_id (int): measure_id to upload
        table_type (str): either 'single_year' or 'multi_year'
        prefix (str): either 'upload_risk' or 'upload_eti'

    Returns:
        str
    '''
    file_pattern = "{o}/draws/*/upload/{m}/{tt}/{p}*.csv".format(
        o=out_dir, m=measure_id, tt=table_type, p=prefix)
    return file_pattern


def make_file_df(out_dir, measure_id, table_type, prefix, location_ids=None):
    ''' search for csvs to upload and return a sorted table of file paths

    Arguments:
        out_dir (str): absolute path up to burdenator id directory
        measure_id (int): measure_id to upload
        table_type (str): either 'single' or 'multi'
        prefix (str): either 'upload_risk' or 'upload_eti'
        location_ids (List[int], optional): if specified, subset files
            based on this list of ids

    Returns:
        pd.DataFrame
    '''
    file_pattern = make_file_pattern(out_dir, measure_id, table_type, prefix)
    file_names = find_file_list_from_file_pattern(file_pattern)
    file_df = format_file_names(file_names, out_dir, table_type)
    # Check that we have all the files we expect to have.
    # Should be a full cross-product

    if table_type == 'single_year':
        cols = ["measure_id", "year_id", "location_id"]
        file_pattern_no_stars = "{o}/draws/{l}/upload/{m}/single_year/{p}_{m}_{y}.csv"
    elif table_type == 'multi_year':
        cols = ["measure_id", "year_start_id", "year_end_id", "location_id"]
        file_pattern_no_stars = "{o}/draws/{l}/upload/{m}/multi_year/{p}_{m}_{y1}_{y2}.csv"
    else:
        raise ValueError("Impossible table type {}".format(table_type))

    file_df = file_df.sort_values(cols)
    warn_if_missing_input_files(file_df, table_type, out_dir, prefix, cols, file_pattern_no_stars)
    if location_ids:
        file_df = file_df[file_df.location_id.isin(location_ids)]
    return file_df


def warn_if_missing_input_files(file_df, table_type, out_dir, prefix, cols, validate_path_pattern):
    """Logs missing files as defined by find_missing_input_files"""
    missing_df = find_missing_input_files(file_df, table_type, out_dir, prefix, cols, validate_path_pattern)
    r, c = missing_df.shape
    if r != 0:
        logger.error("There were {} missing input files to the upload, see following lines:".format(r))
        for f in missing_df['raw']:
            logger.error("  Missing {}".format(f))


def find_missing_input_files(file_df, table_type, out_dir, prefix, cols, validate_path_pattern):
    """Returns a dataframe with all the missing files, assuming that it it should be square in hyperspace.
    The 'expected' range for each index is the set of unique values in that column in file_df.
    So if an entire index value is missing then it won't know.
    It only finds missing 'holes', not nmissing 'slices.'
    Expects the input DF to have a column named 'raw' with filenames.
    Filenames are absolute, i.e. from / """
    num_rows, num_cols = file_df.shape

    # Quick check on the cross-product size of the hypercube
    indexes = {}
    size_cross_product = 1
    for col in cols:
        indexes[col] = file_df[col].unique()
        size_cross_product *= len(indexes[col])
    if num_rows == size_cross_product:
        return pd.DataFrame({'raw': []})
    else:
        # Work out which ones are missing by creating a full cross-product DF and subtracting the dataframes
        cross_product_df = pd.DataFrame()
        for col in cols:
            identical = [1 for _ in indexes[col]]
            df = pd.DataFrame({'identical': identical, col: indexes[col]})
            if cross_product_df.empty:
                cross_product_df = df
            else:
                cross_product_df = pd.merge(cross_product_df, df, on='identical')
        if table_type == 'single_year':
            cross_product_df['raw'] = cross_product_df.apply(
                lambda row: validate_path_pattern.format(
                    o=out_dir, p=prefix, m=row['measure_id'], y=row['year_id'], l=row['location_id']), axis=1)
        else:
            cross_product_df['raw'] = cross_product_df.apply(
                lambda row: validate_path_pattern.format(
                    o=out_dir, p=prefix, m=row['measure_id'], y1=row['year_start_id'], y2=row['year_end_id'],
                    l=row['location_id']), axis=1)

        # Do a merge with the indicator column set
        del cross_product_df['identical']
        cross_product_df = cross_product_df.merge(file_df, on='raw', how='outer', indicator=True)
        # Drop all that were matched
        cross_product_df = cross_product_df[cross_product_df['_merge'] == 'left_only']
        # Reduce columns to just the original set of indexes, plus 'raw'
        del cross_product_df['_merge']
        for col in cols:
            del cross_product_df["{}_y".format(col)]
            cross_product_df = cross_product_df.rename(index=str, columns={"{}_x".format(col): col})
        return cross_product_df


def upload_to_table(file_list, table_name, env, raise_on_failure):
    if env == DBEnv.PROD:
        conn_def = _ConnDefs.PROD.value
    elif env == DBEnv.DEV:
        conn_def = _ConnDefs.DEV.value
    sesh = get_session(conn_def=conn_def, connectable=True)
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


def run_burdenator_upload(out_dir, gbd_process_version_id, location_ids,
                          measure_id, table_type, upload_to_test,
                          raise_on_error=False):
    """
    Upload summary files to GBD Outputs risk tables

    Args:
        out_dir (str): the root directory for this burdenator run
        gbd_process_version_id (int): GBD Process Version to upload to
        location_id (List[int]): location ids used in this burdenator run
        measure_id (int): measure_id of the upload
        table_type (str): type of DB table to upload into
            (single or multi year)
        upload_to_test (bool): determines whether to upload to the test db
        raise_on_error (bool, False): While infiling, if an exception is
            caught, raise. If False, will just log and continue to next file
    """
    start_time = time.time()
    logger.info("START pipeline burdenator upload at {}".format(start_time))
    # Get process version
    if upload_to_test:
        upload_env = DBEnv.DEV
    else:
        upload_env = DBEnv.PROD

    pv = GBDProcessVersion(gbd_process_version_id, env=upload_env)

    # Get the prefix for file names
    if pv.gbd_process_id == RISK_GBD_PROCESS_ID:
        prefix = 'upload_risk_'
    elif pv.gbd_process_id == ETI_GBD_PROCESS_ID:
        prefix = 'upload_eti_'

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
    logger.info("Uploading data to table {}, time = {}".format(table_name, time.time()))
    upload_to_table(file_df.raw.tolist(), table_name, upload_env,
                    raise_on_error)
    logger.info("End uploading data, time = {}".format(time.time()))

    # End log
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE upload pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    parser = get_input_args.construct_parser_burdenator_upload()
    args = get_input_args.construct_args_burdenator_upload(parser)

    run_burdenator_upload(args.out_dir, args.gbd_process_version_id,
                          args.location_ids, args.measure_id,
                          args.table_type, args.upload_to_test)


if __name__ == "__main__":
    main()
