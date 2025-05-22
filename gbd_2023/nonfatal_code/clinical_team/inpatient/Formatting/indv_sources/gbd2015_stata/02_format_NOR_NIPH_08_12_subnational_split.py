import datetime
import sys

import db_queries
import pandas as pd
from pandas.compat import u  # for printing nice location names

from crosscutting_functions import *


def get_location_info():
    """
    Function to get norways subnational info
    """

    locs = db_queries.get_location_metadata(
        location_set_id=9, gbd_round_id=7, decomp_step="iterative"
    )

    nor_subnationals = locs[locs.path_to_top_parent.str.contains(",90,")][
        ["location_id", "location_name"]
    ]

    nor_subnationals.rename(
        columns={
            "location_id": "subnational_location_id",
            "location_name": "subnational_location_name",
        },
        inplace=True,
    )
    nor_subnationals["location_id"] = 90

    return nor_subnationals


def expand(df, location_info):
    """
    Function to expand norway and give it subnationals
    """

    shape_before = df.shape

    df = df.merge(location_info, how="left", on="location_id")

    assert (
        df.subnational_location_id.value_counts().values == shape_before[0]
    ).all(), "the value counts of each subnational location_id should all equal the shape of df before the merge"

    df.drop("location_id", axis=1, inplace=True)
    df.rename(columns={"subnational_location_id": "location_id"}, inplace=True)

    return df


def fix_ages(df):
    """
    Function that attaches age_group_id to df by first fixing age end
    """

    # switch to age_group_id
    # switch to exclusive age end
    df.loc[df.age_end > 1, "age_end"] = df.loc[df.age_end > 1, "age_end"] + 1
    df.loc[
        df.age_end == 100, "age_end"
    ] = 125  # for the correct population, remember to switch back later
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=False)

    return df


def get_pop(df):
    """
    Function that aquires population estimates using information in df, and
    merges it onto df. nothing happens to df.
    """

    # gather information for get_population
    sexes = list(df.sex.unique())
    ages = list(df.age_group_id.unique())
    years = list(df.year.unique())
    locations = list(df.location_id.unique())

    pop = db_queries.get_population(
        location_id=locations,
        age_group_id=ages,
        year_id=years,
        sex_id=sexes,
        gbd_round_id=7,
        decomp_step="iterative",
    )

    pop.rename(columns={"year_id": "year", "sex_id": "sex"}, inplace=True)
    pop.drop("run_id", axis=1, inplace=True)

    pop = pop.sort_values(["age_group_id", "sex", "year", "location_id"])

    return pop


def make_weights(pop):
    """
    Function that makes population based weights
    """

    pop["denominator"] = pop.groupby(["age_group_id", "year", "sex"])[
        "population"
    ].transform("sum")
    pop["weights"] = pop.population / pop.denominator

    return pop


def apply_weights(df, pop):
    """
    Function that attaches and applies the weights
    """

    shape_before = df.shape[0]

    df = df.merge(pop, how="left", on=["age_group_id", "year", "sex", "location_id"])

    assert shape_before == df.shape[0]

    df["cases_split"] = df.cases * df.weights

    return df


def test_splitting(b, a, tol):
    """
    Function that tests that the totals before and after splitting are correct

    Returns:
        True / False

    Raises: AssertionError
    """

    # basic test: no respect to demographic or disease groups.
    assert_msg = """
    The grand totals before and after don't match
    before: {}
    after: {}
    before - after = {}
    """.format(
        b, a, b - a
    )
    assert abs(a - b) <= tol


def clean_up(df, before_columns):
    """
    Function that drops the extra columns and renames columns so that
    the format of the output is identical to how it was at the beginning. Also
    changes age_end back to what it was, inclusive, i.e. the values of age_end
    end in 4s and 9s.  also checks that things match before and after.
    """

    df = df.drop(
        [
            "age_group_id",
            "denominator",
            "population",
            "subnational_location_name",
            "weights",
            "cases",
        ],
        axis=1,
    )

    df = df.rename(columns={"cases_split": "cases"})

    df.loc[df.age_end > 1, "age_end"] = df.loc[df.age_end > 1, "age_end"] - 1
    df.loc[df.age_end == 124, "age_end"] = 99

    assert_msg = """
    Columns do not match what they should be.
    The set diff is {}
    """.format(
        set(df.columns).symmetric_difference(set(before_columns))
    )
    assert (
        set(df.columns).symmetric_difference(set(before_columns)) == set()
    ), assert_msg

    return df


def split_into_subnationals(df, save=True):
    """
    This is the main function
    """

    # fit norway into our all sex_id = 3 paradigm
    df.loc[df.sex == 9, "sex"] = 3

    # gather some information before beginning
    original = df.copy()
    before_total = df.cases.sum()
    before_columns = df.columns

    # get location info
    nor_subnats = get_location_info()

    # expand norway data so that is has all the subnational location ids
    df = expand(df.copy(), location_info=nor_subnats)

    # fix ages in df so that we can get the right populations
    df = fix_ages(df.copy())

    # get population
    pop = get_pop(df)

    # make weights
    pop = make_weights(pop)

    # attach and apply weights to df
    df = apply_weights(df, pop)

    # check our work
    test_splitting(b=before_total, a=df.cases_split.sum(), tol=0.1)

    # clean up
    df = clean_up(df, before_columns)

    # check that all the values besides case counts and location id are the same
    pd.testing.assert_frame_equal(
        left=original[
            [
                "diagnosis_id",
                "iso3",
                "subdiv",
                "national",
                "source",
                "NID",
                "year",
                "age_start",
                "sex",
                "platform",
                "icd_vers",
                "cause_code",
                "age_end",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True),
        right=df[
            [
                "diagnosis_id",
                "iso3",
                "subdiv",
                "national",
                "source",
                "NID",
                "year",
                "age_start",
                "sex",
                "platform",
                "icd_vers",
                "cause_code",
                "age_end",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True),
    )

    if save:
        # save at main location
        try:
            destination = "FILEPATH/formatted_NOR_NIPH_08_12.dta"
            df.to_stata(destination, write_index=False)
        except:
            print("WARNING: couldn't save at {}".format(destination))

        today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD

        # save backup
        try:
            destination = "FILEPATH/formatted_NOR_NIPH_08_12_{}.dta".format(
                today
            )
            df.to_stata(destination, write_index=False)
        except:
            print("WARNING: couldn't save at {}".format(destination))

    return df


if __name__ == "__main__":
    # read in the old national data
    df = pd.read_stata(
        "FILEPATH/NOR_NIPH_08_12/"
        "FILEPATH/formatted_NOR_NIPH_08_12_national.dta"
    )
    # split and write
    df = split_into_subnationals(df, save=True)
