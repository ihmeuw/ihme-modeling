"""
This script identifies which files are duplicates and creates a list of which
files should be processed and saves them by year in a location where
format_BRA_SIH.py can find them.
"""

import filecmp
import glob
import os
import re
import pickle
import pandas as pd

from simpledbf import Dbf5


def read_one_dbf(f):
    """
    Reads a DBF file and keeps the desired columns.

    Parameters
    ----------
    f: string
        filepath
    """
    df = Dbf5(f, codec="latin")
    df = df.to_dataframe()

    keep = [
        "MUNIC_RES",
        "DT_INTER",
        "DIAG_PRINC",
        "DIAG_SECUN",
        "IDADE",
        "COD_IDADE",
        "SEXO",
        "MORTE",
        "DT_INTER",
        "DT_SAIDA",
    ]
    existing_cols = df.columns.tolist()
    # safe way to drop only columns that surely exist
    drop_cols = list(set(existing_cols) - set(keep))
    df = df.drop(drop_cols, axis=1)

    return df


def deep_compare(pair):
    """
    compares two files, identified by the list pair, to see if they are
    identical

    Parameters
    ----------
    pair: list
        list of two files that may or may not be duplicates
    """

    assert len(pair) == 2

    dfa = read_one_dbf(pair[0])
    dfb = read_one_dbf(pair[1])

    dfa["val"] = 1
    dfb["val"] = 1

    dfa = dfa.replace(to_replace=pd.np.nan, value="NULL")
    dfb = dfb.replace(to_replace=pd.np.nan, value="NULL")

    dfa = dfa.groupby(
        [col for col in dfa.columns if col != "val"], as_index=False
    ).val.sum()
    dfb = dfb.groupby(
        [col for col in dfb.columns if col != "val"], as_index=False
    ).val.sum()

    are_equal = dfa.equals(dfb)

    return are_equal


def pickler(files_list, year):
    """
    Saves a pickled list for worker scripts to use. The list contains
    filepaths for one year of data

    Parameters
    ----------
    files_list: list
        list of files to be saved
    year: int
        year the files are for
    """

    # file extension is arbatrary
    filepath = "FILEPATH"
    with open(filepath, "wb") as fp:
        pickle.dump(files_list, fp)


# prep empty list to append to
master_list = []


for rd in ["RD_1992_1999", "RD_2000_2007", "RD_2008_2014"]:

    # glob for all possible state dirs inside any this one rd dir
    states = glob.glob(f"FILEPATH")
    # make sure all results are directories (not files)
    states = [s for s in states if os.path.isdir(s)]
    # create unique sorted list of all state directories
    states = sorted(set([os.path.basename(s) for s in states]))
    # make sure all the directories have letters in them
    states = [s for s in states if bool(re.search(pattern="[A-Za-z]", string=s))]

    for state in states:
        # list of directories inside each state dir, which should be years
        years = glob.glob(f"FILEPATH")
        # make sure all results are directories (not files)
        years = [y for y in years if os.path.isdir(y)]
        # create unique sorted list of all year directories
        years = sorted(set([os.path.basename(y) for y in years]))
        # make sure all of these directories are 4 digit numbers, i.e. years.
        years = [y for y in years if bool(re.fullmatch(pattern="\d{4}", string=y))]

        for year in years:
            # list of all DBF files for a year and state
            all_files = glob.glob("FILEPATH")

            # get the filename from the end of the full filepaths for this dir
            filenames = [os.path.basename(f) for f in all_files]

            # make unique
            unique_filenames = list(set(filenames))

            for f in unique_filenames:
                # get list of files that share the same filename for a given
                # state and year, but in different RD_* directories
                # NOTE: same state, same year, same filename, but different RD
                # need to exclude 2004
                # so basically this is looking for similarly named files in
                # other RD folders, but not in RD_2004_2013
                data_files = sorted(glob.glob(("FILEPATH")))

                # excluded RD_2004_2013
                data_files = [f for f in data_files if "RD_2004_2013" not in f]

                # master_list is list of files in the data directories.
                # each entry in master list is itself a list.
                # each entry is either of length 1 or 2. 1 if it is unique,
                # 2 if it shares a name with another file (i.e. is a potential
                # duplicate)
                # It would be 0 if the only thing in data_files was a file from
                # RD_2004_2013
                if len(data_files) != 0:
                    master_list.append(data_files)
            # end unique filenames loop
        # end year loop
    # end state loop
# end rd loop

# get lenths of the lists inside master_list to find potential duplicates
lengths = [len(i) for i in master_list]
assert set(lengths) == {1, 2}, (
    "Assume that there are either unique files or "
    "pairs of duplicates (no triplets or beyond)"
)

# potential_duplicates is a list of lists. each list has two entries, both
# of which are full filepaths to data that needs to be compared
potential_duplicates = [i for i in master_list if len(i) == 2]

# figure out which of the potential duplicates are actually not duplicates
not_duplicates = []
counter = 0
# loop over all potential duplicate pairs
for pair in potential_duplicates:
    # if files are not the same, save them for later
    # This checks if files are identical.
    # filecmp uses the stat command (linux) and bytes of the file to compare
    # filecmp.cmp will return True early if shallow=True and the stat calls
    # are identical. So, if shallow=True, and two files were modified at
    # different times, so that the stat call will be different, filecmp.cmp
    # will continue running and do more tests, such as reading the bytes of
    # the files and comparing them.
    if not filecmp.cmp(pair[0], pair[1], shallow=True):

        # if not filecmp, then the bytes are different, but,
        # the data could be identical in the columns we actually want to use.

        # read in both files keeping only the columns needed, collapse, compare
        are_equal = deep_compare(pair)
        if not are_equal:
            not_duplicates.append(pair)
    counter += 1

not_duplicates = [sorted(l) for l in not_duplicates]
not_duplicates = sorted(not_duplicates)


# make list of duplicated pairs
duplicated_pairs = [l for l in potential_duplicates if l not in not_duplicates]

# take just one of the duplicated files. doesn't matter which because
# they are identical files.
deduplicated_files_to_drop = [sorted(l)[1] for l in duplicated_pairs]

# flatten master_list, so that it's a list of strings
master_list = [item for sublist in master_list for item in sublist]

# remove duplicates from master_list
files_to_read = list(set(master_list) - set(deduplicated_files_to_drop))

files_to_read = sorted(files_to_read)

# save a list of files to read for each year
for year in list(range(1992, 2014 + 1)):
    pickler([f for f in files_to_read if f"/{year}/" in f], year)
