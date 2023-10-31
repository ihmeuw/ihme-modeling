"""
Created on 9/19/2017
Updated 12/4/2019 to handle our new mapping database and some other issues

Hospital data has been stored wide during the formatting process
in the following directory- FILEPATH
This script will combine and collapse these files to produce the dataframe
structure is required for producing the EN matrix.

"""
import pandas as pd
import numpy as np
import ntpath
import glob
import platform
import getpass
import sys
import time
import os.path
import subprocess
import time

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# load our functions
from clinical_info.Functions import hosp_prep

USER = getpass.getuser()

delete_op = sys.argv[1]
run_id = sys.argv[2]
MAP_VERSION = sys.argv[3]

if delete_op == "False":
    delete_op = False
elif delete_op == "True":
    delete_op = True
else:
    raise ValueError(f"Delete op should be convertable to a bool, {delete_op} isnt'")

print(sys.argv[1])
print(delete_op, run_id, MAP_VERSION)


def get_file_names(file_dir, file_type, samp_size=0):
    """
    get a list of file names given a directory and file extension
    optional: set a sample_size to use for testing
    """
    all_files = glob.glob(file_dir + "/*." + file_type)
    if samp_size > 0:
        samp_files = all_files.sample(samp_size)
        return samp_files.tolist()
    phl = sorted([f for f in all_files if "PHL" in f])
    sorted_af = sorted([p for p in all_files if "PHL" not in p])
    assert len(all_files) == len(phl) + len(sorted_af)
    all_files = phl + sorted_af
    return all_files


def delete_tmp_files():
    """
    removes all the existing files in the temp output folder for the qsub jobs in main
    """
    files = glob.glob(f"FILEPATH")
    for tmp_file in files:
        os.remove(tmp_file)
    return


def en_keeper(df):
    """
    we only need baby sequelae that begin with e-code and n-code
    FLAG -- verify there aren't odd e or n baby sequelae
    """
    print("Starting with {} rows".format(df.shape[0]))
    dx_cols = df.filter(regex="^dx_|ecode_").columns.tolist()
    # print(dx_cols)
    ecode = " | ".join(
        ["(df['{}'].str.contains('e-code'))".format(col) for col in dx_cols]
    )
    ncode = " | ".join(
        ["(df['{}'].str.contains('n-code'))".format(col) for col in dx_cols]
    )
    test = ecode + " | " + ncode
    df = df[eval(test)]
    print("Ending with {} rows".format(df.shape[0]))
    return df


def read_hdf_chunks(fname, chunksize=1000, chunks=1):
    """
    Pass this function a filepath for a large dta file and it will read the
    first 5000 rows. You can choose to read more or less with the other arguments
    """
    itr = pd.read_hdf(fname, chunksize=chunksize, iterator=True)
    count = 0
    x = pd.DataFrame()
    for chunk in itr:
        count += 1
        x = x.append(chunk)
        if count == chunks:
            break
    return x


def tmp_reader(files, sleep=True, fun_size=False):
    df_list = []
    locations = []
    tmp_dir = f"FILEPATH"

    for fpath in files:

        file_name = ntpath.basename(fpath)[:-4]
        tmp_file = tmp_dir + file_name + ".H5"

        print("waiting for {}".format(tmp_file))
        while not os.path.exists(tmp_file):
            time.sleep(60)

        if os.path.isfile(tmp_file):
            if sleep:
                time.sleep(1)
            # read in hdf fill
            if fun_size:
                # doesn't work df = read_hdf_chunks(tmp_file)
                df = pd.read_hdf(tmp_file)
                df = df.sample(1000)

            else:
                df = pd.read_hdf(tmp_file)

            # df = fill_missing(df)
            df = make_bs_none(df)
            df = null_col_dropper(df)

            # get list of dx and ecode columns
            str_cols = df.columns[df.columns.str.contains("dx_|ecode_")]
            for col in str_cols:  # cast to categorical
                df[col] = df[col].astype("category")

            # df = en_keeper(df)

            # add to unique locations
            locations = locations + df.location_id.unique().tolist()
            df_list.append(df)
            # os.remove(tmp_file)  # delete the tempfile for next run
            print("finished it")
        else:
            raise ValueError("%s isn't a file!" % tmp_file)
    print("all done, returning either a list or concatted df")
    df = pd.concat(df_list, ignore_index=True)

    # get list of dx and ecode columns
    str_cols = df.columns[df.columns.str.contains("dx_|ecode_")]
    for col in str_cols.tolist() + ["source", "facility_id"]:  # cast to categorical
        df[col] = df[col].astype("category")

    locations = list(set(locations))  # just unique locs
    return df, locations


def fill_missing(df):
    # dx cols to fill NAs with strings
    dx_cols = df.filter(regex="^dx_").columns.tolist()
    ecode_cols = df.filter(regex="^ecode_").columns.tolist()

    # fill missing values with _none
    df[dx_cols + ecode_cols] = df[dx_cols + ecode_cols].fillna("_none")
    return df


def test_data_structure(df):
    assert df.live_discharges.isnull().sum() == 0, "Null discharges"
    assert (df.isnull().sum() == 0).all(), "There are null values in {}".format(
        df.columns[df.isnull().sum() > 0]
    )


def collapser(df, verbose=False):
    if verbose:
        print("Starting rows are {}".format(df.shape[0]))

    groups = df.columns.drop(["live_discharges", "source"]).tolist()
    # do the collapse
    df = df.groupby(groups).agg({"live_discharges": "sum"}).reset_index()
    if verbose:
        print("Ending rows are {}".format(df.shape[0]))
    return df


def make_bs_none(df):
    """
    if a baby seq isn't an e or ncode assign it to "_none"
    """
    dxcols = df.filter(regex="^dx_|^ecode").columns.tolist()
    to_drop = []
    counter = 0
    for col in dxcols:
        # print(col, df[col].unique().size)
        # assign anything that's not an e or n baby sequelae to none
        df.loc[~df[col].str.contains("e-code|n-code"), col] = "_none"
    return df


def null_col_dropper(df):
    """
    This function assigns any baby sequelae that doesn't contain the phrase
    e-code or n-code to "_none" then drops columns which consist of nothing
    but "_none"
    """
    startcols = df.shape[1]
    if "ncode_1" in df.columns:
        dx_cols = df.filter(regex="^ncode_|^ecode").columns.tolist()
    else:
        dxcols = df.filter(regex="^dx_|^ecode").columns.tolist()
    to_drop = []
    counter = 0
    for col in dxcols:
        if (df[col] == "_none").all():
            to_drop.append(col)
        counter += 1
        # print("{} percent done".format(float(counter)/len(dxcols) * 100))
    df.drop(to_drop, axis=1, inplace=True)
    endcols = df.shape[1]
    print("{} columns were dropped".format(startcols - endcols))
    return df


def null_row_dropper(df):
    """
    drop rows which have no discernable ecodes or ncodes, just filled with "none"
    """
    start_rows = df.shape[0]

    # drop rows where every ncode col is filled with "none"
    if "ncode_1" in df.columns:
        cols = df.filter(regex="^ncode_").columns
        df = df[df[cols].apply(pd.Series.nunique, axis=1) > 1]
        # drop rows where every ecode col is filled with "none"
        cols = df.filter(regex="^ecode_").columns
        df = df[df[cols].apply(pd.Series.nunique, axis=1) > 1]

    if "dx_1_nfc" in df.columns:
        cols = df.filter(regex="^dx_").columns
        df = df[df[cols].apply(pd.Series.nunique, axis=1) > 1]
    end_rows = df.shape[0]
    print("{} rows were dropped".format(start_rows - end_rows))

    return df


def swap_out_ecodes(df):
    """
    this function is meant to swap ecodes into the ecode cols as part of a
    multi processing loop but it's quite resource intensive
    """
    # create list of dx cols, which will be ncode cols eventually
    col_list = list(df.filter(regex="^(dx_)"))
    # sort them from low to high
    hosp_prep.natural_sort(col_list)
    col_list.reverse()

    # create list of ecode cols
    ecol_list = list(df.filter(regex="^(ecode_)"))
    # sort them from low to high
    hosp_prep.natural_sort(ecol_list)
    ecol_list.reverse()

    # this does the actual e-n code col swapping
    for ecol in ecol_list:
        for col in col_list:
            # find rows in the dx_n_nfc cols which start with e-code
            ncode_cond = "(df['{}'].str.startswith('e-code'))".format(col)
            # find rows in ecode_n_nfc that start with "_none"
            ecode_cond = "(df['{}'].str.startswith('_none'))".format(ecol)

            # swap e and n codes
            df.loc[eval(ncode_cond) & eval(ecode_cond), [col, ecol]] = df.loc[
                eval(ncode_cond) & eval(ecode_cond), [ecol, col]
            ].values
    return df


def clean_en_cols(df, cores):
    """
    clean up the messy E-N matrix data
    """
    # create list of dx cols, which will be ncode cols eventually
    col_list = list(df.filter(regex="^(dx_)"))
    # sort them from low to high
    hosp_prep.natural_sort(col_list)
    col_list.reverse()

    # create list of ecode cols
    ecol_list = list(df.filter(regex="^(ecode_)"))
    # sort them from low to high
    hosp_prep.natural_sort(ecol_list)
    ecol_list.reverse()

    # make sure there are no ncodes in the ecode cols
    evals = []  # list for unique baby sequelae names
    for i in np.arange(1, len(ecol_list) + 1, 1):  # loop over each ecode col
        # store all the unique baby sequelae from each col
        evals = evals + df["ecode_{}_nfc".format(i)].unique().tolist()

    # cast list to series so we can use a pandas function
    evals = pd.Series(evals)
    # the assert that will break if an n-code is present in any e-code col
    assert (
        evals[evals.str.contains("n-code")].size == 0
    ), "there are ncodes the ecode col"

    counter = 0
    for ecol in ecol_list:
        counter += 1
        for col in col_list:
            # find rows in the dx_n_nfc cols which start with e-code
            ncode_cond = "(df['{}'].str.startswith('e-code'))".format(col)
            # find rows in ecode_n_nfc that start with "_none"
            ecode_cond = "(df['{}'].str.startswith('_none'))".format(ecol)

            # swap e and n codes
            df.loc[eval(ncode_cond) & eval(ecode_cond), [col, ecol]] = df.loc[
                eval(ncode_cond) & eval(ecode_cond), [ecol, col]
            ].values
        print(
            "{} percent done cleaning e-n cols".format(
                round((float(counter) / len(ecol_list)) * 100, 1)
            )
        )

    # make sure there are no ecodes in the ncode cols, same basic process as above
    evals = []
    for col in col_list:
        evals = evals + df[col].unique().tolist()

    evals = pd.Series(evals)

    # if there are ecode evals write them to my drive for testing
    if evals[evals.str.contains("e-code")].size != 0:
        print("there are ecodes in the ncode (dx) cols")
        if user == "USER":
            df.to_csv("FILEPATH", index=False)

    assert (
        evals[evals.str.contains("e-code")].size == 0
    ), "there are ecodes in the ncode col"

    # rename the dx_nfc cols to ncode
    new_cols = []
    for i in np.arange(1, len(col_list) + 1, 1):
        new_cols.append("ncode_" + str(i))
    # new_cols.reverse()
    new_names = dict(list(zip(col_list, new_cols)))
    df.rename(columns=new_names, inplace=True)
    # rename ecode_nfc cols to just ecode
    new_cols = []
    for i in np.arange(1, len(ecol_list) + 1, 1):
        new_cols.append("ecode_" + str(i))
    new_names = dict(list(zip(ecol_list, new_cols)))
    df.rename(columns=new_names, inplace=True)
    df.rename(columns={"live_discharges": "cases"}, inplace=True)

    # sort columns
    pre_cols = df.columns
    ncodes = list(df.filter(regex="^ncode_").columns)
    hosp_prep.natural_sort(ncodes)
    ncodes.reverse()
    ecodes = list(df.filter(regex="^ecode_").columns)
    hosp_prep.natural_sort(ecodes)
    ecodes.reverse()

    order_col = [
        "location_id",
        "sex_id",
        "age_start",
        "age_end",
        "facility_id",
        "cases",
    ]
    order_col = order_col + ncodes + ecodes
    df = df[order_col]

    assert (
        pre_cols.sort_values() == df.columns.sort_values()
    ).all(), "some cols were dropped"
    return df


def organize_ncodes(df, check_order=False):
    """
    the raw data can have an n-code anywhere which means there are a lot of
    columns which could probably be removed.
    So this function moves up the ncodes to the first dx positions when
    when possible since order doesn't matter for the e-n matrix
    works but is slow as heck
    """
    col_list1 = list(df.filter(regex="^(dx_)"))
    hosp_prep.natural_sort(col_list1)
    col_list1.reverse()
    col_list2 = list(df.filter(regex="^(dx_)"))
    hosp_prep.natural_sort(col_list2)

    counter = 0
    for col1 in col_list1:  # lowest to highest
        if check_order:
            print("col1 var is {}".format(col1))
        counter += 1
        col_list2.remove(col1)
        for col2 in col_list2:  # highest to lowest
            if check_order:
                print("col2 var is {}".format(col2))
            # find rows in the dx_n_nfc cols which start with e-code
            low_cond = "(df['{}'].str.startswith('n-code') == False)".format(col1)
            # find rows in ecode_n_nfc that start with "_none"
            high_cond = "(df['{}'].str.startswith('n-code'))".format(col2)

            # swap e and n codes
            df.loc[eval(low_cond) & eval(high_cond), [col1, col2]] = df.loc[
                eval(low_cond) & eval(high_cond), [col2, col1]
            ].values
    return df


def main(files, run_id=None, delete=True, by_location=False, write_hdf=True):
    # delete the existing temp files which will be created with the qsub below
    if delete:
        delete_tmp_files()

        # send out the qsubs to create new files if the old ones were deleted
        for fpath in files:
            source = ntpath.basename(fpath)[-19:-7]
            if "AUT_HDD" in source:  # AUT has no ecodes
                continue
            if source == "USA_HCUP_SID":
                mem = 160
            else:
                mem = 90

            qsub = "QSUB".format(
                name=ntpath.basename(fpath)[:-4],
                threads=2,
                memory=mem,
                fpath=fpath,
                run_id=run_id,
                user=USER,
                mapv=MAP_VERSION,
            )
            # print(qsub)
            subprocess.call(qsub, shell=True)

    df, locs = tmp_reader(files, sleep=delete)

    print("All the data was read in")

    start = time.time()
    if by_location:
        # collapsed_list = []
        for loc in df.location_id.unique():
            loc = int(loc)
            print("starting")
            start_loc = time.time()
            loc_df = df[df.location_id == loc].copy()
            # collapse data again
            print("Fill missing values for {}".format(loc))
            # print(loc_df.shape)
            loc_df = fill_missing(loc_df)
            # print(loc_df.info(memory_usage='deep'))
            test_data_structure(loc_df)
            print("Test passed, collapsing...")

            # print(loc_df.shape)
            loc_df = collapser(loc_df)
            loc_df = null_col_dropper(loc_df)
            # print(loc_df.shape)
            # write a master output
            print(
                "Collapse finished in {} min. Begin writing data.".format(
                    (time.time() - start_loc) / 60
                )
            )
            # collapsed_list.append(loc_df)

            try:
                loc_df.to_hdf(
                    "FILEPATH".format(loc), key="df", mode="w",
                )
            except:
                exit
    else:
        print("starting")
        start = time.time()
        # drop zero discharges
        if getpass.getuser() == "USER":
            df[df.live_discharges == 0].to_csv("FILEPATH", index=False)
        print("start rows with zeros are {}".format(df.shape[0]))
        df = df[df.live_discharges > 0]
        print("end rows are {}".format(df.shape[0]))

        print("Fill missing values")
        print("start shape", df.shape)
        case_sum = df.live_discharges.sum()
        df = fill_missing(df)
        assert case_sum == df.live_discharges.sum()

        # df = make_bs_none(df)
        df = null_col_dropper(df)
        df = null_row_dropper(df)
        case_sum = df.live_discharges.sum()
        assert case_sum == df.live_discharges.sum()
        print("null col dropper shape", df.shape)
        # print(df.info(memory_usage='deep'))
        test_data_structure(df)
        assert case_sum == df.live_discharges.sum()

        print("Tests passed, collapsing...")
        df = collapser(df)
        assert (case_sum - df.live_discharges.sum()) < 10

        print("collapsed shape", df.shape)

        # cast back to str type
        str_cols = df.columns[df.columns.str.contains("dx_|ecode_")]
        for col in str_cols.tolist() + ["facility_id"]:  # cast to categorical
            df[col] = df[col].astype(str)

        print("start organizing ncodes")
        df = organize_ncodes(df, check_order=False)
        df = null_col_dropper(df)

        print(
            "Collapse finished in {} min. Begin cleaning data.".format(
                (time.time() - start) / 60
            )
        )
        df = clean_en_cols(df, cores=4)
        assert (case_sum - df.cases.sum()) < 10

        df = null_row_dropper(df)
        print(
            "Cleaning finished in {} min. Begin writing data.".format(
                (time.time() - start) / 60
            )
        )
        # write a master output
        write_path = "FILEPATH".format(run_id)
        # writing a csv regardless
        df.to_csv(
            "FILEPATH".format(run_id), index=False,
        )

        if write_hdf:
            try:
                hosp_prep.write_hosp_file(df, write_path, backup=True)
            except:
                print("Writing an hdf and backup file didn't work")

    return df


if __name__ == "__main__":
    files = get_file_names("FILEPATH", "dta")
    skip_sources = [
        "AUT_HDD_1989",
        "AUT_HDD_2001",
        "AUT_HDD_2011",
        "AUT_HDD_2012",
        "AUT_HDD_2013",
        "AUT_HDD_2014",  # AUT doesn't have E-codes
    ]
    files = [f for f in files if os.path.basename(f)[:-4] not in skip_sources]
    main(files, run_id, delete=delete_op, by_location=False)
