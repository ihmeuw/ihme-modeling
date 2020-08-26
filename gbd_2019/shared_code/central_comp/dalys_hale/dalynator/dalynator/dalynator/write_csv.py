import logging
import os
import numpy as np
import pandas as pd

from dalynator.compute_summaries import ComputeSummaries
from dalynator.constants import UMASK_PERMISSIONS
from dalynator.data_container import remove_unwanted_star_id_column
from dalynator.makedirs_safely import makedirs_safely

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)

PK_SINGLE_YEAR_RISK = ['measure_id', 'year_id', 'location_id', 'sex_id',
                       'age_group_id', 'cause_id', 'rei_id', 'metric_id']
PK_MULTI_YEAR_RISK = ['measure_id', 'year_start_id', 'year_end_id',
                      'location_id', 'sex_id', 'age_group_id', 'cause_id',
                      'rei_id', 'metric_id']
PK_SINGLE_YEAR_NORISK = ['measure_id', 'year_id', 'location_id', 'sex_id',
                         'age_group_id', 'cause_id', 'metric_id']
PK_MULTI_YEAR_NORISK = ['measure_id', 'year_start_id', 'year_end_id',
                        'location_id', 'sex_id', 'age_group_id', 'cause_id',
                        'metric_id']


def detect_pk(df):
    """Detect and return the probably db-matching PK for the given DataFrame"""

    # Using this set method, since NORISK is a subset of RISK, must check the
    # RISK column lists first
    for pk in [PK_SINGLE_YEAR_RISK, PK_MULTI_YEAR_RISK,
               PK_SINGLE_YEAR_NORISK, PK_MULTI_YEAR_NORISK]:
        if len(set(pk) & set(df.columns)) == len(pk):
            return pk
    return None


def sort_for_db(df):
    """Returns a copy of the DataFrame that has been sorted according
    to the GBD database's PK"""
    pk = detect_pk(df)
    if pk:
        return df.sort_values(pk)
    else:
        raise ValueError("Columns of df do not match any known PK's, could "
                         "not sort for database")


def write_csv(df, filename, write_columns_order=None,
              write_out_star_ids=False, dual_upload=False):
    """Assumes we are writing a CSV for the purposes of eventually uploading
    to a database, sorts df accordingly and writes to filename"""

    df = separate_rejected_data_to_csv(df, filename)

    # Prioritize sorting for the database. If that's not possible, fallback
    # to write_columns_order or unsorted
    try:
        write_df = sort_for_db(df)
    except ValueError:
        if write_columns_order:
            write_df = df.sort_values(write_columns_order)
        else:
            write_df = df

    # Write to file, make sure to ignore star_id if stars aren't wanted
    if write_columns_order:
        cols = remove_unwanted_star_id_column(write_columns_order,
                                              write_out_star_ids)
    else:
        cols = remove_unwanted_star_id_column(df.columns.tolist(),
                                              write_out_star_ids)

    write_df.to_csv(filename, columns=cols, index=False)

    if dual_upload:
        pub_up_filename = sub_pub_for_cc(filename)
        pub_up_dir = os.path.dirname(pub_up_filename)
        makedirs_safely(pub_up_dir)
        write_df.to_csv(pub_up_filename, columns=cols, index=False)


def df_to_csv(this_df, index_cols, this_out_dir, out_file_basename,
              write_columns_order, dual_upload=False):
    summaries = ComputeSummaries(
        this_df,
        write_columns_order,
        index_cols
    )

    new_df = summaries.get_data_frame()
    makedirs_safely(this_out_dir)

    filename = os.path.join(this_out_dir, out_file_basename)

    logger.info("Summary file output path {}".format(filename))

    write_csv(new_df, filename, write_columns_order=write_columns_order,
              dual_upload=dual_upload)


def separate_rejected_data_to_csv(df, filename):
    a = filename.split("/")
    outpath = filename[0:filename.find(a[-1])]
    logger.debug("in write_csv {} before catch null/inf df shape {}"
                 .format(filename, df.shape))

    nan_mask = df.isnull().any(axis=1)
    inf_mask = np.isinf(df).any(axis=1)

    nan_rows = df.loc[nan_mask]
    inf_rows = df.loc[inf_mask]

    to_keep = (~nan_mask & ~inf_mask)
    df = df.loc[to_keep]

    if not nan_rows.empty:
        logger.debug("find NaN value in df when write summaries")
        nan_rows.to_csv("{}NONE_{}".format(outpath, a[-1]), index=False)

    if not inf_rows.empty:
        logger.debug("find inf value in df when write summaries")
        inf_rows.to_csv("{}INF_{}".format(outpath, a[-1]), index=False)

    logger.debug("in write_csv after catch null/inf df shape {}"
                 .format(df.shape))
    return df


def sub_path_substring(path, old_substr, new_substr):
    return path.replace(old_substr, new_substr)


def sub_pub_for_cc(path):
    """To support writing to a separate directory for public uploads,
    subtitutes 'pub_uploads' for 'centralcomp' in a path string"""
    return sub_path_substring(path, 'centralcomp', 'pub_uploads')
