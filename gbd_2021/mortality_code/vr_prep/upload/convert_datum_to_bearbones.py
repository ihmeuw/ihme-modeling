"""
This script converts empirical deaths data from the datum (wide) age format
into the age_group_id (long) aka bearbones format. It tests that age and sex
are properly converted.

"""

import sys
import getpass
import re
import pandas as pd
import time

sys.path.append("".format(getpass.getuser()))
sys.path.append("".format(getpass.getuser()))

# Globals
# Passed in arguments
NEW_RUN_ID = sys.argv[1]
INPUT_FOLDER = "".format(NEW_RUN_ID)

# Global variable for final columns
FINAL_COLS = ['sex_id', 'source', 'source_type_id',
              'nid', 'underlying_nid', 'outlier', 'deaths', 'age_group_id',
              'location_id', 'year_id']


def timeit(func):
    """A decorator for timing runtime of functions"""
    def timed(*args, **kwargs):
        ts = time.time()    # start
        func_result = func(*args, **kwargs)
        te = time.time()    # end

        return func_result

    return timed


@timeit
def convert_datum_to_bearbones(df):
    """
    converts 'DATUMXtoY' vars to bear bones age groups and renames columns
    """

    # Rename datum variables to age_group_id, in long format
    datum_vars = [d for d in list(df) if ('datum' in d.lower())]
    id_vars = [d for d in list(df) if ('datum' not in d.lower())]

    empty_datum_cols = [col for col in datum_vars if df[col].isnull().all()]

    # drop empty datum columns from the data
    df = df.drop(empty_datum_cols, axis=1)
    # remove the empty dataum columns from the list of datum columns
    datum_vars = list(set(datum_vars) - set(empty_datum_cols))

    valid_vars = id_vars + datum_vars
    df = df[valid_vars]

    print("Reshaping to long format")
    df = melt_stub(df, 'DATUM', j='DATUM_names')

    # make dictionary that maps datum to age_group_name
    datum_dict = _make_datum_to_years_dictionary(datum_vars)

    # use dictionary to map datum to age_group_name
    print("Adding age_group_ids")
    df['age_group_name'] = df['DATUM_names'].str.lower().map(datum_dict)
    df = df.drop("DATUM_names", axis=1)

    # use age_group_name to merge on age_group_id
    ages = pd.read_csv("{}/age_map.csv".format(INPUT_FOLDER))
    # left merge is necessary to detect missing age_group_id
    pre = df.shape[0]  # store shape before merge
    df = df.merge(ages, on='age_group_name', how='left')
    df = df.dropna(subset = ['age_group_id'])
    assert df.age_group_id.notnull().all(), ("age_group_id is missing in some "
                                             "cases")
    del df['age_group_name']

    # Rename other variables for consistency
    print("Reformatting variables")
    df = _format_vars(df)

    # this acts as a check and a filter
    df = df[FINAL_COLS]

    return df


@timeit
def melt_stub(df, stub, j):
    """
    A wrapper around pandas.melt()

    Arguments:
        df (Pandas DataFrame) in wide format
        stub (str): stubname to get wide columns
        j (str): name of column created

    Returns:
        Pandas DataFrame in long format
    """

    value_cols = df.filter(regex=stub).columns.tolist()

    i = [x for x in list(df) if x not in value_cols]  # Use all cols as ids
    df = pd.melt(df, id_vars=i, value_vars=value_cols, value_name=stub,
                 var_name=j)

    df.dropna(subset=[stub], inplace=True)

    return df


@timeit
def _format_vars(df):
    """
    Renames needed columns. Drops unneeded columns. Converts needed ids.
    For example, column 'sex' is renamed to 'sex_id'. The values in 'sex' are
    converted from values such as 'male' into ids such as 1.
    """

    # Re-name variables
    df.rename(columns={'sex': 'sex_id',
                       'country': 'location_name',
                       'year': 'year_id',
                       'age': 'age_group_id',
                       'deaths_nid': 'nid',
                       'deaths_underlying_nid': 'underlying_nid',
                       'DATUM': 'deaths',
                       'deaths_source': 'source'}, inplace=True)

    # Replace 'country' and 'ihme_loc_id' with 'location_id'
    del df['location_name']

    mort_comp_loc = pd.read_csv("".format(INPUT_FOLDER))
    locs = mort_comp_loc[['location_id', 'location_name', 'ihme_loc_id']]

    df.loc[df.ihme_loc_id == "MAC", 'ihme_loc_id'] = "CHN_361"

    df = df.merge(locs, on='ihme_loc_id', how='left')

    if df.location_id.isnull().any():
        bad_map = df.loc[df.location_id.isnull(), 'location_id'].tolist()
        raise KeyError("No location_id for country names: {}"
                       .format(bad_map))

    del df['location_name']
    del df['ihme_loc_id']
    del df['deaths_footnote']

    # replace sex with ids
    sex_dict = {'male': 1, 'female': 2, 'both': 3}
    df.loc[:, 'sex_id'] = df['sex_id'].map(sex_dict)

    return df


def _make_datum_to_years_dictionary(datum_vars):
    """
    Creates a dictionary with the key of DATUMXtoY and the value as
    the years in a string that matches to the age table from get_ids().
    Works for one or more DATUMXtoY variables.
    """

    # Ensure correct types
    if type(datum_vars) is str:
        datum_vars = [datum_vars]
    else:
        list(datum_vars)

    # Ensure case is lower
    datum_vars = [d.lower() for d in datum_vars]

    # Parse the 'DATUM' variables to extract info matching age table
    datum_dict = {"datum15to49": "15-49 years",
                  "datum70plus": "70+ years",
                  "datum0to1": "0",
                  "datum0to0": "<1 year",
                  "datumunk": "Unknown",
                  "datumtot": "All Ages",
                  "datum15plus": "15+ years",
                  "datum50to69": "50-69 years",
                  "datum5to14": "5-14 years",
                  "datum0to20": "<20 years",
                  "datum0to4": "Under 5",
                  "datumenntoenn" : "Early Neonatal",
                  "datumlnntolnn" : "Late Neonatal",
                  "datumneonatal" : "Neonatal",
                  "datumpostneonatal" : "Post Neonatal",
                  "datumpnatopna" : "1-5 months",
                  "datumpnbtopnb" : "6-11 months",
                  "datum12to23months" : "12 to 23 months",
                  "datum2to4" : "2 to 4",
                  "datum0to19" : "< 19 years",
                  "datum_2years" : "2"}
    for datum in datum_vars:
        # Check if it's in the atypical (default) datum_dict list
        if datum in datum_dict:
            continue
        else:
            m = re.match('datum(\d{1,2})to(\d{1,3})', datum,
                         flags=re.IGNORECASE)
            if m:
                year_start = m.group(1)
                year_end = m.group(2)
                # Age ranges
                if year_start != year_end:
                    datum_dict.update(
                        {datum: '{} to {}'.format(year_start, year_end)})
                # Single ages
                elif year_start == year_end:    # e.g. DATUM7to7
                    datum_dict.update({datum: str(year_start)})
            # Handle atypical cases: e.g. 'DATUMUNK, DATUMXXXplus'
            else:
                matchplus = re.match('datum(\d{1,3})plus', datum,
                                     flags=re.IGNORECASE)
                if matchplus:
                    year_start = matchplus.group(1)
                    datum_dict.update({datum: '{} plus'.format(year_start)})
                else:
                    raise ValueError("Could not parse column {}".format(datum))
    _test_datum_to_years_dictionary(datum_dict, datum_vars)
    return datum_dict


def _test_datum_to_years_dictionary(datum_dict, datum_vars):
    def test_names():
        """
        Checks that every mapped datum value in the data dictionary has a
        corresponding age_group_id

        Raises:
            AssertionError if there are any datums that don't have an
            age_group_id
        """
        age_names = pd.read_csv("".format(INPUT_FOLDER))['age_group_name']
        invalid_names = []
        for key in datum_dict:
            # print(datum_dict[key])
            if not age_names.isin([datum_dict[key]]).any():
                invalid_names.append(key)
                print(key)
        assert not invalid_names, ("These Datum values do not have a valid "
                                   "age_group_id associated with them:\n{}"
                                   .format(invalid_names))

    def test_mapping():
        """
        Checks that every datum column has an entry in the datum dictionary. If
        a Datum column has an entry it means that
        _make_datum_to_years_dictionary was able to convert the datum column
        name into a potentially valid age_group_name

        Raises:
            AssertionError if there are datum columns without entries in the
            dictionary
        """
        missing_vars = []
        for datum in datum_vars:
            if datum not in datum_dict:
                missing_vars.append(datum)
        assert not missing_vars, ("These Datum values do not have an entry in "
                                  "the datum dictionary:\n{}"
                                  .format(missing_vars))

    # call all testing functions
    test_names()
    test_mapping()
