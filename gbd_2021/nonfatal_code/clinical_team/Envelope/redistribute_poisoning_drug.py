"""
Split the injury poisoning-drug baby sequela into distinct other ones
"""

import platform
import pandas as pd
import numpy as np
from db_queries import get_location_metadata

# load our functions
from clinical_info.Functions import hosp_prep

if platform.system() == "Linux":
    root = r"FILEPATH"
else:
    root = "FILEPATH"


def clean_inj_drug_props():
    """
    We need to re-distribute one of the injury baby sequelae into 7 others
    using proportions that USER provided. The file below contains the props.
    This preps the code for application by region/sex/age/year
    """
    # read in the data that was manually prepped for reshaping
    df = pd.read_excel("FILEPATH")

    # reshape data long
    df = df.set_index(["region", "row_type"]).stack().reset_index()

    # split out sex, age, year start which was in the column names
    df["sex_id"], df["prop_age_start"], df["prop_year_start"] = (
        df["level_2"].str.split("_", 2).str
    )

    # drop the rows which aren't related to the proportions
    df = df[
        df.row_type.isin(
            [
                "Opioids",
                "Cannabis",
                "Cocaine",
                "Amphetamines",
                "Others drug ",
                "Alcohol",
                "inj_poisoning_other",
            ]
        )
    ]

    # give these categories actual baby sequelae when possible
    new_names = {
        "Opioids": "mental, opioids abuse",
        "Cannabis": "mental, cannabis abuse",
        "Cocaine": "mental, cocaine abuse",
        "Amphetamines": "mental, amphetamine abuse",
        # these don't have new names
        "Others drug ": "others drug",
        "Alcohol": "alcohol",
        "inj_poisoning_other": "poisoning_other",
    }
    df["row_type"] = df["row_type"].map(new_names)
    assert df["row_type"].isnull().sum() == 0, "nulls were created"

    # rename columns
    df.rename(
        columns={"row_type": "new_icg_name", "region": "prop_region", 0: "prop"},
        inplace=True,
    )

    # drop the col used to rehape
    df.drop("level_2", axis=1, inplace=True)

    # clean values
    # fit sex_id with our rubrik
    df.sex_id.replace(["m", "f"], [1, 2], inplace=True)
    # get exclusive age_end
    age_ends = df.prop_age_start.sort_values().unique().tolist()[1:] + [125]
    start_end_dict = dict(list(zip(df.prop_age_start.unique().tolist(), age_ends)))
    df["prop_age_end"] = df["prop_age_start"].map(start_end_dict)

    # fix the column data types
    for col in ["prop", "sex_id", "prop_age_start", "prop_age_end", "prop_year_start"]:
        df[col] = pd.to_numeric(df[col], errors="raise")
    return df


def subset_to_redis(df, to_subset):
    """
    Takes a dataframe of hospital data with the baby sequelae we're going to
    redistribute. Returns 2 DFs, one with that baby seq and one without it
    """
    assert type(to_subset) == str, "to_subset must be a string"

    # convert to lower case just to be sure
    df.icg_name = df.icg_name.str.lower()
    to_subset = to_subset.lower()

    # split out the data we will NOT redistro
    non_redis_df = df[df.icg_name != to_subset].copy()

    # subset on just the baby seq we're going to redistribute
    pre = df.shape[0]
    df = df[df.icg_name == to_subset].copy()
    print(
        (
            "Running redistribute_poison_drug.py: {} rows were will not be split. {} rows remain to split.".format(
                pre - df.shape[0], df.shape[0]
            )
        )
    )
    assert pre > df.shape[0], "The subset failed for some reason"
    return df, non_redis_df


def prep_to_merge(df, clinical_age_group_set_id):
    """
    the drug prop table doesn't fit perfectly with the stucture of our hosp
    inp data. We need to re-structure it to merge on perfectly.
    We need to make adjustments to- age_start, year_start, location(region)
    """
    # create drug prop age groups
    # go from group IDs to start and end
    df = hosp_prep.group_id_start_end_switcher(
        df, clinical_age_group_set_id=clinical_age_group_set_id, remove_cols=False
    )

    df["prop_age_start"] = np.nan  # create null prop age start column
    prop_age_ends = [10, 15, 30, 50, 70, 80, 125]  # list of all the ends
    # make a dictionary linking prop age end and prop age start
    prop_age_dict = {10: 0, 15: 10, 30: 15, 50: 30, 70: 50, 80: 70, 125: 80}

    # loop over age ends and fill prop age start with the age start from the
    # dictionary made above when the value isn't null
    for end in sorted(prop_age_ends):  # sort it just in case
        df.loc[
            (df.age_start < end) & (df.prop_age_start.isnull()), "prop_age_start"
        ] = prop_age_dict[end]

    # make the prop age end col
    prop_end_dict = dict((y, x) for x, y in list(prop_age_dict.items()))
    df["prop_age_end"] = df["prop_age_start"].map(prop_end_dict)

    # test it
    assert (df.age_start >= df.prop_age_start).all(), "prop ages didn't work"
    assert (df.prop_age_end >= df.age_end).all(), "prop age ends didn't work"
    # drop start and end
    df.drop(["age_start", "age_end"], axis=1, inplace=True)

    # create drug prop years
    df["prop_year_start"] = np.nan
    df.loc[df.year_start <= 2004, "prop_year_start"] = 1980
    df.loc[df.year_start > 2004, "prop_year_start"] = 2005

    # test years
    assert (df.year_start >= df.prop_year_start).all(), "prop year failed"

    # create drug prop regions
    americas_regions = [
        "Southern Latin America",
        "High-income North America",
        "Caribbean",
        "Andean Latin America",
        "Central Latin America",
        "Tropical Latin America",
    ]
    euro_regions = ["Western Europe", "Central Europe"]

    locs = get_location_metadata(location_set_id=9, gbd_round_id=6)
    locs["prop_region"] = "all_other_world"
    locs.loc[locs["region_name"].isin(americas_regions), "prop_region"] = "americas"
    locs.loc[locs["region_name"].isin(euro_regions), "prop_region"] = "aus_europe"
    # the prop region for Australia is mixed with europe
    locs.loc[locs["location_name"] == "Australia", "prop_region"] = "aus_europe"

    # make sure euro/americas regions aren't in the all other world prop region
    eu_am = americas_regions + euro_regions
    other_reg = (
        locs[locs.prop_region == "all_other_world"].region_name.unique().tolist()
    )
    diff = [reg for reg in eu_am if reg in other_reg]
    assert diff == [], (
        "There are locations in the all other world prop region that are " "incorrect"
    )
    # merge prop region onto inp data
    locs = locs[["location_id", "prop_region"]]  # just the cols we want
    pre = df.shape[0]
    df = df.merge(locs, how="left", on="location_id")
    assert pre == df.shape[0], "Number of rows changed, not good"
    assert df.prop_region.isnull().sum() == 0, "Missing prop regions"

    return df


def merge_on_props(df, to_split):
    """
    merge the proportions onto the inpatient data, this also duplicates data
    as the baby sequelae we're going to split are repeated on the right hand
    side of the merge
    """
    # get the proportions we'll use to split the data
    props = clean_inj_drug_props()
    props["icg_name"] = to_split.lower()
    # merge them onto our df
    to_merge = [
        "sex_id",
        "prop_age_start",
        "prop_age_end",
        "prop_year_start",
        "prop_region",
        "icg_name",
    ]
    pre = df.shape[0]
    df = df.merge(props, how="left", on=to_merge)
    assert pre * 7 == df.shape[0]
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
    assert abs(a - b) <= tol, assert_msg


def apply_props(df, break_it=False):
    """
    Function that applies the props to inp data and then checks the results
    """
    df["val_split"] = df["val"] * df["prop"]

    if break_it:  # break the assert to make sure it works
        df.loc[3, "val_split"] = 6.9
    # test our work
    test_splitting(b=df.val.sum() / 7, a=df.val_split.sum(), tol=0.1)

    return df


def write_poison_drug(df):
    """
    The target of our redistro include a few baby sequelae that already exist
    in the data so in order to review this if anything goes wrong we'll write
    the split data to FILEPATH
    """
    fpath = "FILEPATH"

    # make sure columns keep the right dtype
    for col in [
        "sex_id",
        "age_group_id",
        "location_id",
        "year_start",
        "nid",
        "representative_id",
    ]:
        df[col] = pd.to_numeric(df[col], errors="raise")
    hosp_prep.write_hosp_file(df=df, write_path=fpath, backup=True)
    return


def clean_data(df):
    """
    remove columns we won't be needing and rename the split val and new nfc
    columns to fit our standard data
    """
    # get a list of the cols we need to drop
    to_drop = df.filter(regex="^prop_").columns.tolist()
    to_drop = to_drop + ["val", "icg_name", "prop"]
    df.drop(to_drop, axis=1, inplace=True)  # drop em

    # rename our split cols to their standard name
    df.rename(columns={"val_split": "val", "new_icg_name": "icg_name"}, inplace=True)

    # apply icg_ids to new_icg_name
    df.loc[df.icg_name == "alcohol", "icg_id"] = 1272
    df.loc[df.icg_name == "mental, amphetamine abuse", "icg_id"] = 280
    df.loc[df.icg_name == "mental, cannabis abuse", "icg_id"] = 1055
    df.loc[df.icg_name == "mental, cocaine abuse", "icg_id"] = 276
    df.loc[df.icg_name == "mental, opioids abuse", "icg_id"] = 279
    df.loc[df.icg_name == "others drug", "icg_id"] = 1273
    df.loc[df.icg_name == "poisoning_other", "icg_id"] = 864

    return df


def redistribute_poison_drug(df, clinical_age_group_set_id):
    """
    run all the functions
    """
    if "year_id" in df.columns:
        df.rename(columns={"year_id": "year_start"}, inplace=True)
    pre_cols = df.columns
    # designate the baby sequelae we're to redistribute
    p_drug = "E-CODE, (_gc-poisoning_drug)"
    # split df up into redist and non redist data
    df, non_redis_df = subset_to_redis(df, to_subset=p_drug)
    # keep a copy of the redist data for checks
    og = df.copy()
    # match the data to the proportions table
    df = prep_to_merge(df, clinical_age_group_set_id)
    # merge on the proportions
    df = merge_on_props(df, to_split=p_drug)
    # apply the actual proportions
    df = apply_props(df)
    # remove and rename columns
    df = clean_data(df)
    # write to FILEPATH incase we need to review
    write_poison_drug(df)

    # run some tests
    assert (
        set(pre_cols).symmetric_difference(set(df.columns)) == set()
    ), "Some columns changed"

    # test unique combinations of every column value except for val and nfc
    cols_to_check = [
        "location_id",
        "age_group_unit",
        "sex_id",
        "nid",
        "facility_id",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "age_group_id",
        "year_start",
    ]
    df1 = og[cols_to_check].drop_duplicates().reset_index(drop=True)
    df2 = df[cols_to_check].drop_duplicates().reset_index(drop=True)
    assert df1.equals(df2)

    # combine redistro and non redistro data
    df = pd.concat([df, non_redis_df], ignore_index=True)

    pre_cases = df.val.sum()
    # groupby and collapse to combine the split nfc with the existing nfc
    group_cols = df.columns.drop("val").tolist()
    print("Beginning the groupby, groupby cols are {}".format(group_cols))
    df = df.groupby(group_cols).agg({"val": "sum"}).reset_index()

    assert round(pre_cases, 1) == round(df.val.sum(), 1), "{} cases were lost".format(
        pre_cases - df.val.sum()
    )

    if "year_id" not in df.columns:
        df.rename(columns={"year_start": "year_id"}, inplace=True)
    return df
