import warnings
from typing import Dict, Iterable, List, Union

import dask.dataframe as dd
import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.database import Database
from db_queries import get_location_metadata


def year_id_switcher(df: pd.DataFrame) -> pd.DataFrame:
    """
    Our process is moving from year start/end to year_id until we agg to 5 years
    this func should switch the cols, or if year ID is present do nothing
    Args:
        df: clinical data with a column identifying year
    """
    df_cols = df.columns
    good_years = ["year_start", "year_end", "year_id"]
    # get a list of year col names we use present in data
    year_cols = [y for y in df_cols if y in good_years]

    if "year_id" in year_cols:
        drop_cols = [y for y in year_cols if y != "year_id"]
        df.drop(drop_cols, axis=1, inplace=True)
        return df

    if not year_cols:
        print(
            "We can't recognize any potential year columns in the data, "
            f"possible types are {good_years}"
        )
        return df

    # make sure year start and year end are identical if they're present
    # if not identical return the df
    year_cols_present = set(year_cols).intersection({"year_start", "year_end"})
    if not set(year_cols).symmetric_difference({"year_start", "year_end"}):
        if (df["year_end"] != df["year_start"]).all():
            print(
                "Start and end values do not match. The data is aggregated in some way, "
                "switch failed.."
            )
            return df
        else:  # start end are identical so we're good
            df["year_id"] = df["year_start"]
    else:
        df["year_id"] = df[list(year_cols_present)]
    df.drop(year_cols, axis=1, inplace=True)

    return df


def retain_good_age_groups(df: pd.DataFrame, clinical_age_group_set_id: int) -> pd.DataFrame:
    """Retain only the age group ids that are "perfect" ie that we'll provide in
    our final data
    This Function will drop rows, not sources. So it can retain very uneven data
    i.e. it will drop entire states/years from HCUP but retain others
    """

    if "age_group_id" not in df.columns:
        raise ValueError("retain_good_age_groups won't work without age_group_id")

    ages = get_hospital_age_groups(clinical_age_group_set_id)
    pre = len(df)
    pre_ages = df["age_group_id"].unique().tolist()
    df = df[df["age_group_id"].isin(ages["age_group_id"])]
    post = len(df)
    dropped_ages = set(pre_ages) - set(df["age_group_id"].unique().tolist())
    print(f"We've dropped {pre - post} rows with 'bad' age group IDs: {dropped_ages}")

    return df


def retain_age_sex_split_age_groups(
    df: pd.DataFrame, run_id: int, round_id: int, clinical_age_group_set_id: int
) -> pd.DataFrame:
    """Sort of a wrapper for retain good ages, but for use only in age sex splitting
    2 changes: Print statement if to show a source was dropped and writes unique
    age-source combinations to the run"""

    parent_path = f"FILEPATH"

    pre_path = f"FILEPATH/pre_good_age_drop_r_{round_id}_ages.csv"
    store_source_ages(df=df, write_path=pre_path)

    pre_sources = df["source"].unique().tolist()
    df = retain_good_age_groups(df, clinical_age_group_set_id)

    post_sources = df["source"].unique().tolist()
    lost_src_count = len(pre_sources) - len(post_sources)
    print(f"We have completely removed {lost_src_count} sources.")
    if lost_src_count > 0:
        print(f"The sources dropped were {set(pre_sources) - set(post_sources)}")

    write_path = f"FILEPATH/round_{round_id}_final_ages.csv"
    store_source_ages(df=df, write_path=write_path)

    return df


def store_source_ages(df: pd.DataFrame, write_path: str) -> None:
    """We may use uneven age patterns in our 'good' sources for
    round 1 weights. In order to roughly track this we'll write
    all combos of source and age to file"""

    years = get_year_cols(df.columns.tolist())
    groups = ["source", "location_id", "age_group_id"] + years
    tmp = df.groupby(groups).val.sum().reset_index()
    tmp.to_csv(write_path, index=False)

    return


def get_year_cols(columns: Iterable[str]) -> List[str]:
    """Return a list of column names present in data for storing year values."""
    ycols = ["year_id", "year_start", "year_end"]
    years = [y for y in ycols if y in columns]

    return years


def sum_under1_data(
    df: pd.DataFrame,
    group_cols: List[str],
    sum_cols: List[str],
    clinical_age_group_set_id: int,
) -> pd.DataFrame:
    """Take in a dataframe with U1 detail and aggregates it into a single
    under 1 age group. Should leave Over 1 data untouched"""

    ages = get_hospital_age_groups(clinical_age_group_set_id)
    ages = ages[ages["age_start"] < 1]

    if 28 in df["age_group_id"].unique().tolist():
        print(
            (
                "age_group_id 28 (0-1 year olds) is already present in the "
                "data. It will be removed to avoid duplication. This is expected "
                "behavior for get_cached_pop. Ensure that removal is appropriate "
                "for your use case"
            )
        )
        df = df[df["age_group_id"] != 28]

    # make sure we don't use or gain sum col cases
    init_case_dict = {}
    for sum_col in sum_cols:
        init_case_dict[sum_col] = df[sum_col].sum().round(1)

    # split between under and over 1
    u1 = df[df["age_group_id"].isin(ages["age_group_id"])].copy()
    if len(u1) == 0:
        print(f"There is no under 1 data for clinical age set {clinical_age_group_set_id}")
    u1["age_group_id"] = 28  # hardcoding

    df = df[~df["age_group_id"].isin(ages["age_group_id"])]

    sum_dict = dict(zip(sum_cols, ["sum"] * len(sum_cols)))
    if u1.isnull().sum().sum() != 0:
        raise ValueError("There are nulls which will get lost")
    col_diff = set(u1.columns) - set(group_cols + sum_cols)
    if col_diff:
        print(
            (
                f"The columns {col_diff} will not be used in the groupby "
                "filled with nulls during concatination with over 1 data"
            )
        )
    u1 = u1.groupby(group_cols).agg(sum_dict).reset_index()

    df = pd.concat([df, u1], sort=False, ignore_index=True)

    for sum_col in sum_cols:
        pre = init_case_dict[sum_col]
        post = df[sum_col].sum().round(1)
        if pre != post:
            raise ValueError(
                (
                    f"Started with {pre} cases and finished with "
                    f"{post} cases which means something went wrong"
                )
            )
    return df


def confirm_age_group_set(
    age_col: List[int], clinical_age_group_set_id: int, full_match: bool
) -> None:
    """Validation. Raise a val error if a set of unique age_group_ids perfectly matches the
    clinical age group set or is a subset of it

    Args:
        age_col: Object castable to set containing age_group_ids
        clinical_age_group_set_id: The clinical sets we use to identify which
                                   age_groups we're processing
        full_match: Identifies if a perfect match is required, or if we only need to
                    confirm that age_col is present in the CAGSI
    """

    if len(age_col) != len(set(age_col)):
        raise ValueError("A unique vector or list is expected")
    ages = get_hospital_age_groups(clinical_age_group_set_id)

    if full_match:
        diff_groups = set(age_col).symmetric_difference(ages["age_group_id"])
        if diff_groups:
            raise ValueError(
                (
                    f"The data doesn't contain the exact age groups "
                    f"expected. Diff is {diff_groups}"
                )
            )
    else:
        diff_age = set(age_col) - set(ages["age_group_id"])
        if diff_age:
            raise ValueError(
                (
                    f"Age group ID {age_col} is not present in "
                    f"clinical age set {clinical_age_group_set_id}"
                )
            )
    return


def map_to_country(df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """
    much of our location data is subnational, but we sometimes want to tally things by country
    this function will map from any subnational location ID to its parent country
    """
    pre = df.shape[0]
    cols = df.shape[1]
    # get countries from locs
    locs = get_location_metadata(location_set_id=35, release_id=release_id)
    countries = locs.loc[
        locs.location_type == "admin0", ["location_id", "location_ascii_name"]
    ].copy()
    countries.columns = ["merge_loc", "country_name"]
    # get parent ids from locs
    df = df.merge(locs[["location_id", "path_to_top_parent"]], how="left", on="location_id")
    df = pd.concat([df, df.path_to_top_parent.str.split(",", expand=True)], axis=1)

    df["merge_loc"] = df[3].astype(int)
    df = df.merge(countries, how="left", on="merge_loc")

    # drop all the merging cols
    to_drop = ["path_to_top_parent", 0, 1, 2, 3, 4, 5, 6]
    # sometimes there are only a few expanded loc cols [0, 1, 2]
    to_drop = [d for d in to_drop if d in df.columns]
    df.drop(to_drop, axis=1, inplace=True)

    df.rename(columns={"merge_loc": "country_location_id"}, inplace=True)

    # run a few validations
    if df.shape[0] != pre:
        raise ValueError("The input data shape has changed")
    if df.shape[1] != cols + 2:
        raise ValueError("Unexpected column changes")
    if df.country_name.isnull().sum() != 0:
        raise ValueError("Something went wrong {}".format(df[df.country_name.isnull()]))
    return df


def confirm_contiguous_data(
    df: pd.DataFrame, col1: str, col2: str, break_if_not_contig: bool = False
) -> None:
    """
    Validates that binned ages don't have any gaps (eg 1-4, 5-9, 12-14, 15-20)
    Args:
        df: data that has been age_binned
        col1: First column with ages to use for age range
        col2: Second column with ages to use for age range
        break_if_not_contig: If True, a ValueError will be raised if there are any gaps in the
                            age pattern.
    """
    contig_df = df[[col1, col2]].drop_duplicates().sort_values(col1).reset_index(drop=True)
    # remove all age group
    contig_df = contig_df.query(f"({col1} != 0 | {col2} != 125)")

    # reset index to prevent errors in for loop
    contig_df = contig_df.reset_index(drop=True)

    bad_rows = []
    for idx in contig_df.index[:-1]:
        if contig_df.loc[idx, col2] != contig_df.loc[idx + 1, col1]:
            bad_rows += [idx, idx + 1]
    gap_df = contig_df.loc[bad_rows]
    msg = f"\nThere is a gap in this data. Please review \n{gap_df}"

    if bad_rows:
        if break_if_not_contig:
            raise ValueError(msg)
        else:
            warnings.warn(msg)
    return


def validate_age_bins(
    df: pd.DataFrame, under1_age_detail: bool, break_if_not_contig: bool = True
) -> None:
    """
    Validate the age binner at run-time to make sure that there are no issues.

    Validations include:
    Age is always >= age start and <= age end
    Age is never negative
    Age is never above 125
    Warn if age is between 110 and 125

    Args:
        df: data that has been age_binned
        under1_age_detail : If under1_age_detail is True, then we expect 4 Under-1 age groups
                            If age groups aren't contiguous, break
        break_if_not_contig: Gets passed on to confirm_contiguous_data(). If True, a ValueError
                             will be raised if there are any gaps in the age pattern.
    """
    if under1_age_detail:
        u1_age_starts = df.loc[df["age"] < 1, "age_start"].unique()
        if u1_age_starts.size < 4:
            raise ValueError(
                (
                    f"There are fewer than 4 unique age start values, "
                    f"this seems incorrect. These are {u1_age_starts} "
                    "This error maybe not be relevant if you're running "
                    "with a subset of Under 1 data. If that's the case "
                    "find another way to bin your data"
                )
            )

    confirm_contiguous_data(
        df, col1="age_start", col2="age_end", break_if_not_contig=break_if_not_contig
    )

    # init a list to store bad age subsets to
    output_list = []
    df_list = []
    if (df.age < 0).any():
        df_list.append(df[df.age < 0])
        output_list.append("There are some negative ages")

    if (df.age >= df.age_start).all() is False:
        df_list.append(df[df.age < df.age_start])
        output_list.append("Age is less than age start")

    if (df.age <= df.age_end).all() is False:
        df_list.append(df[df.age > df.age_end])
        output_list.append("Age is greater than age start")
    # check if impossibly large ages exist
    if (df.age > 125).any():
        df_list.append(df[df.age > 125])
        output_list.append("There are impossibly old ages present")

    if len(output_list) > 0:
        raise ValueError(f"One or more of the tests didn't pass {output_list}.")
    else:
        # warn if there are unusually old ages present
        if (df.age > 110).any():
            print(df[df.age > 110])
            warnings.warn(
                "There are {} rows with very, very old ages present".format(
                    df.age[df.age > 110].size
                )
            )


def validate_age_range(age_col: pd.Series, max_age: Union[int, float]) -> bool:
    """Checks if the age range could contain a valid terminal age.
    Prevents unwanted behavior such as bad binning neonatal bundles when using
    terminal_age_in_data=True. A dataset containing a valid terminal age
    is expected to not be an age specific subset.

    Args:
        age_col: Age column from a pandas.DataFrame
        max_age: Maximum age in column.

    Returns:
        bool: Flag to allow terminal_age_in_data behavior or not
    """
    dd_col = dd.from_pandas(age_col, npartitions=10)
    age_std = dd_col.std().compute()

    if max_age < 65:
        if age_std < 5:
            small_age_set_flag = False
        else:
            small_age_set_flag = True
    else:
        small_age_set_flag = True

    return small_age_set_flag


def age_binning(
    df: pd.DataFrame,
    clinical_age_group_set_id: int,
    drop_age: bool = False,
    terminal_age_in_data: bool = True,
    under1_age_detail: bool = False,
    break_if_not_contig: bool = True,
) -> pd.DataFrame:
    """
    function accepts a pandas DataFrame that contains age-detail and bins them
    into age ranges. Assumes that the DataFrame passed in contains a column
    named 'age'. terminal_age_in_data is used in formatting when the oldest
    age group present in the data is the terminal group. Null values in 'age'
    will be given age_start and age_end representing all ages. ALL ages over
    99 are converted to age 99 and binned accordingly

    This function can modify the column age, which may not be what you want
    if you are not dropping the column age

    Example: 32 would become 30, 35

    Example call: df = age_binning(df)

    Args:
        df: Any data with a column named 'age'
        clinical_age_group_set_id: which set of age groups to bin into. Values are stored in
            database table clinical.age_group_set.
        drop_age: Should the age col be retained after binning?
        terminal_age_in_data: Is the oldest age group available in the data? If True and
                              the oldest *age* present in the data is 66 then the terminal
                              age group will be 65-125. If False it will bin it into the
                              standard 65-70 age group.
                              THIS MODIFIES THE COLUMN 'age'
        under1_age_detail: Function assumes age is in years, but under 1 this becomes tricky.
                           If set to True then age 0 in the data MUST be neonatal age group.
                           Added a test to confirm. Used for testing and warnings.
        break_if_not_contig: Gets passed on to confirm_contiguous_data(). If True, a ValueError
                             will be raised if there are any gaps in the age pattern.

    """
    if df.empty:
        raise RuntimeError("No rows are present in the table.")

    df["age"] = pd.to_numeric(df["age"], errors="raise")

    if df.age.max() > 99:
        warnings.warn(
            "There are ages older than 99 in the data. Our terminal "
            "age group is 95-125. Any age older than 99 will be "
            "changed to age 99"
        )
        df.loc[df.age > 99, "age"] = 99

    if df.age.isnull().sum() > 0:
        warnings.warn(
            "There are {} null ages in the data. Be aware: these "
            "will be converted to age_start = 0 and age_end = 125 "
            "and age will be arbitrarily converted to 0".format(df.age.isnull().sum())
        )

    # get the max age in the data
    max_age = df.age.max()
    if max_age < 1 and not isinstance(max_age, int):
        max_age = round(max_age, 8)

    df.loc[df["age"] < 1, "age"] = df.loc[df["age"] < 1, "age"].round(8)

    if under1_age_detail:
        warnings.warn(
            (
                "ALL DATA WITH AGE_START 0 WILL BE CONVERTED "
                "TO THE 0-6 DAY NEONATAL AGE GROUP!"
            )
        )

    ages = get_hospital_age_groups(clinical_age_group_set_id=clinical_age_group_set_id)
    # labels for age columns are the lower and upper ages of bin
    age_start_list = ages.age_start.unique().tolist()
    age_end_list = ages.age_end.unique().tolist()

    # bins that age is sorted into
    age_bins = age_start_list + [101]

    # Create 2 new age columns
    df["age_start"] = pd.cut(df["age"], age_bins, labels=age_start_list, right=False)
    df["age_end"] = pd.cut(df["age"], age_bins, labels=age_end_list, right=False)

    # replace max_age with maximum age_start
    max_age = df.loc[df.age == max_age, "age_start"].unique()
    if len(max_age) != 1:
        raise ValueError("Multiple age_start values matching max age were identified.")
    max_age = max_age[0]

    if terminal_age_in_data:
        # Prevent young age specific subsets (neonatal bundles) from being binned at [0, 125]
        if validate_age_range(age_col=df["age"], max_age=max_age):
            # make the max age_start terminal
            df.loc[df["age_start"] == max_age, "age_end"] = 125
        else:
            msg = f"\nSmall age range with max age={max_age}, "
            "disabled 'terminal_age_in_data=True'."
            warnings.warn(msg)
    else:
        df.loc[df["age_start"] == 95, "age_end"] = 125

    # make age_start and age_end numeric, for some reason binning makes them
    # categorical dtype
    df["age_start"] = pd.to_numeric(df["age_start"], errors="raise")
    df["age_end"] = pd.to_numeric(df["age_end"], errors="raise")

    # assign null values to the any age group (0 - 125)
    df.loc[df["age"].isnull(), ["age_start", "age_end"]] = [0, 125]
    df["age"] = df["age"].fillna(0, inplace=False)

    if df["age_start"].isnull().any() or df["age_end"].isnull().any():
        raise RuntimeError("There are null age start or end values")

    # if age is between 115 and 125 assign it to group 110-115 and change age
    # to 114 as this is our maximum binable age
    if df.age.max() > 114:
        df.loc[(df.age > 114) & (df.age < 125), ["age", "age_start", "age_end"]] = [
            114,
            110,
            115,
        ]
    validate_age_bins(
        df, under1_age_detail=under1_age_detail, break_if_not_contig=break_if_not_contig
    )

    if drop_age:
        # Drop age variable
        df.drop("age", axis=1, inplace=True)

    # return dataframe with age_start,age_end features
    return df


def year_binner(df: pd.DataFrame) -> pd.DataFrame:
    """
    create 5 year bands for dismod, 88-92, 93-97, etc, bins that years are
    sorted into.  Does NOT collapse or aggregate, it simply replaces year_start
    and year_end.
    """
    if df["year_start"].min() <= 1987:
        raise ValueError(
            "There is data present before our earliest year bin (1988), "
            "please review these years"
            f":\n{df['year_start'].sort_values().unique()}"
        )

    bin_start = 1988
    bin_end = bin_start + 5**4

    year_bins = list(np.arange(bin_start, (bin_end + 1), 5))

    # Labels for year columns are the start and end years of bins.
    year_start_list = list(np.arange(bin_start, (bin_end - 1), 5))

    # Rename year columns.
    df.rename(columns={"year_start": "og_year_start", "year_end": "og_year_end"}, inplace=True)

    # Create two new year columns.
    df["year_start"] = pd.cut(
        df["og_year_start"], year_bins, labels=year_start_list, right=False
    )
    df["year_start"] = pd.to_numeric(df["year_start"])
    df["year_end"] = df["year_start"] + 4

    if not (df["og_year_start"] >= df["year_start"]).all():
        raise ValueError("Original year start is before than binned year start")
    if not (df["og_year_start"] <= df["year_end"]).all():
        raise ValueError("Original year start is after than binned year end")
    if not (df["og_year_end"] >= df["year_start"]).all():
        raise ValueError("Original year end is before than binned year start")
    if not (df["og_year_end"] <= df["year_end"]).all():
        raise ValueError("Original year end is after than binned year end")

    # drop original year columns
    df.drop(["og_year_start", "og_year_end"], axis=1, inplace=True)

    return df


def get_hospital_age_groups(clinical_age_group_set_id: int) -> pd.DataFrame:
    """
    Function that returns the age_group_ids that we use on the
    hosital prep team.  Use it to merge on age_group_id onto your data, or
    to merge age_start and age_end onto some covariate or model output

    clinical_age_group_set identifies which set of "good" ages to use
    see DATABASE for a
    description of each age set

    """
    db = Database()
    db.load_odbc("CONN")
    age_years = db.query(
        """QUERY"""
    )
    age_years = replace_rounded_under1_ages(age_years)

    ppath = "FILEPATH"
    age_sets = pd.read_csv(f"FILEPATH/clinical.age_group_set.csv")

    if clinical_age_group_set_id not in age_sets["clinical_age_group_set_id"].tolist():
        raise ValueError((f"Age set {clinical_age_group_set_id} is not recognized"))

    age_set_name = age_sets.loc[
        age_sets["clinical_age_group_set_id"] == clinical_age_group_set_id,
        "clinical_age_group_set_name",
    ].iloc[0]

    print(f"Reading in age group set {age_set_name}")

    age = pd.read_csv(f"FILEPATH/clinical.age_group_set_list.csv")
    age = age[age["clinical_age_group_set_id"] == clinical_age_group_set_id]
    age = age.merge(age_years, how="left", on="age_group_id", validate="1:1")

    age = age[["age_start", "age_end", "age_group_id"]]

    if clinical_age_group_set_id in [1, 2, 3]:
        chk_p = (
            "FILEPATH/"
            "original_hosp_prep_results.csv"
        )
        agechk = pd.read_csv(chk_p)
        agechk = replace_rounded_under1_ages(agechk)

        agechk = agechk[
            agechk["clinical_age_group_set_id"] == clinical_age_group_set_id
        ].reset_index(drop=True)
        agechk.drop("clinical_age_group_set_id", axis=1, inplace=True)
        if not age.equals(agechk):
            raise ValueError("The test against the former methods validation data failed")

    return age


def group_id_start_end_switcher(
    df: pd.DataFrame, clinical_age_group_set_id: int = 1, remove_cols: int = True
) -> pd.DataFrame:
    """
    Takes a dataframe with age start/end OR age group ID and switches from one
    to the other
    """
    # Determine if we're going from start/end to group ID or vise versa
    # if 'age_start' and 'age_end' and 'age_group_id' in df.columns:
    if sum([w in ["age_start", "age_end", "age_group_id"] for w in df.columns]) == 3:
        raise ValueError(
            "All age columns are present, unclear which output is desired. "
            "Simply drop the columns you don't want"
        )

    elif sum([w in ["age_start", "age_end"] for w in df.columns]) == 2:
        merge_on = ["age_start", "age_end"]
        switch_to = ["age_group_id"]
    elif "age_group_id" in df.columns:
        merge_on = ["age_group_id"]
        switch_to = ["age_start", "age_end"]
    else:
        raise ValueError("Age columns not present or named incorrectly")

    # pull in our hospital age groups
    ages = get_hospital_age_groups(clinical_age_group_set_id)

    # determine if the data contains only our hosp age groups or not
    for m in merge_on:
        # check that ages preset are within the set of good ages:
        if not set(df[m]).issubset(set(ages[m])):
            raise ValueError("Irregular ages found. Try running all_group_id switcher.")

    # merge on the age group we want
    pre = df.shape[0]
    df = df.merge(ages, how="left", on=merge_on)
    if pre != df.shape[0]:
        raise ValueError("rows were duplicated")

    # check the merge
    for s in switch_to:
        if df[s].isnull().sum() != 0:
            raise ValueError("{} contains missing values from the merge".format(s))

    if remove_cols:
        # drop the one we don't
        df.drop(merge_on, axis=1, inplace=True)
    return df


def all_group_id_start_end_switcher(
    df: pd.DataFrame,
    clinical_age_group_set_id: int,
    remove_cols: bool = True,
    ignore_nulls: bool = False,
) -> pd.DataFrame:
    """
    Takes a dataframe with age start/end OR age group ID and switches from one
    to the other

    Args:
        df: data to swich age labelling
        remove_cols: If True, will drop the column that was switched from
        ignore_nulls: If True, errors regarding missing ages will be ignored.
                      Not a good idea to use in production but is useful
                      for when you just need to quickly see what ages you have.
    """
    # Determine if we're going from start/end to group ID or vise versa
    if sum([w in ["age_start", "age_end", "age_group_id"] for w in df.columns]) == 3:
        raise ValueError(
            "All age columns are present, unclear which output is desired. "
            "Simply drop the columns you don't want"
        )

    elif sum([w in ["age_start", "age_end"] for w in df.columns]) == 2:
        merge_on = ["age_start", "age_end"]
        switch_to = ["age_group_id"]
    elif "age_group_id" in df.columns:
        merge_on = ["age_group_id"]
        switch_to = ["age_start", "age_end"]
    else:
        raise ValueError("Age columns not present or named incorrectly")

    # pull in our hospital age groups
    ages = get_hospital_age_groups(clinical_age_group_set_id=clinical_age_group_set_id)

    # determine if the data contains only our hosp age groups or not
    age_set = "hospital"
    for m in merge_on:
        ages_unique = ages[merge_on].drop_duplicates()
        df_unique = df[merge_on].drop_duplicates()
        # if their shapes aren't the same it's irregular ages
        if ages_unique.shape[0] != df_unique.shape[0]:
            age_set = "non_hospital"
        elif (
            ages_unique[m].sort_values().reset_index(drop=True)
            != df_unique[m].sort_values().reset_index(drop=True)
        ).all():
            age_set = "non_hospital"

    # Build a 3rd test here
    ages_unique.sort_values(merge_on).reset_index(drop=True)
    df_unique.sort_values(merge_on).reset_index(drop=True)
    if ages_unique.equals(df_unique):
        pass  # do nothing if they're equal
    else:
        age_set = "non_hospital"  # assign to non_hosp if not

    if age_set == "non_hospital":
        # if there are additional age groups we need to pull in the age groups from
        # a db and make some adjustments
        # get age info
        db = Database()
        db.load_odbc("CONN")
        ages = db.query(
            """QUERY"""
        )
        ages.rename(
            columns={"age_group_years_start": "age_start", "age_group_years_end": "age_end"},
            inplace=True,
        )

        if "age_end" in merge_on:
            # terminal age in hosp data is 99, switch to 125 so groups aren't duped
            df.loc[df["age_end"] == 100, "age_end"] = 125
        # drop duplicates age groups that cause rows added in the merge
        duped_ages = [294, 308, 27, 161, 38, 301, 49]
        ages = ages[~ages.age_group_id.isin(duped_ages)]

    dupes = ages[ages.duplicated(["age_start", "age_end"], keep=False)].sort_values(
        "age_start"
    )

    # merge on the age group we want
    pre = df.shape[0]
    df = df.merge(ages, how="left", on=merge_on)
    if pre != df.shape[0]:
        raise ValueError("Rows were duplicated, probably from these ages \n{}".format(dupes))

    # check the merge
    if not ignore_nulls:
        for s in switch_to:
            if df[s].isnull().sum() != 0:
                raise ValueError(
                    "{} contains missing values from "
                    "the merge. The values with Nulls are {}".format(
                        s,
                        df.loc[df[s].isnull(), merge_on]
                        .drop_duplicates()
                        .sort_values(by=merge_on),
                    )
                )

    if remove_cols:
        # drop the one we don't
        df.drop(merge_on, axis=1, inplace=True)
    return df


def replace_rounded_under1_ages(df: pd.DataFrame) -> pd.DataFrame:
    """The under 1 age groups have non-exact values depending on a few factors
    But the first 3 rounded digits are identifiable and let's uniformly assign these
    float values"""

    good_age_floats = [0.01917808, 0.07671233, 0.500]  # previous 6mo value 0.50136986

    # make sure the two age columns are float64
    df["age_start"] = df["age_start"].astype("float64")
    df["age_end"] = df["age_end"].astype("float64")

    # v important
    # for this column to be float64 or it still breaks the merge!
    for f in good_age_floats:
        if len(str(f)) > 3:
            n = 3
        else:
            n = 1
        df.loc[df.age_start.round(n) == round(f, n), "age_start"] = f
        df.loc[df.age_end.round(n) == round(f, n), "age_end"] = f

    return df


def get_child_locations(
    location_parent_id: int, release_id: int, location_set_id: int = 35, return_dict=True
) -> Union[Dict[str, int], List[int]]:
    """Given a GBD location ID this function will query the appropriate location metadata and
    return any direct child locations. NOTE: This will not move throughout multiple levels of
    the location hierarchy. It will only pull subnationals from national locs, for example.
    So lower level locations like UTLAs would not be included in the results for the UK.

    Args:
        location_parent_id: The direct parent ID location of interest.
        release_id: The location hierarchy changes from round to round, so to identify a
        specific version of it you must input a release.
        location_set_id: Centralized method for tracking location sets.
        See `shared.location_set`. Defaults to 35 - "Model Results".
        return_dict: If True the results will be returned as a dictionary with location
        name keys and location id values. Defaults to True.

    Returns:
        Either a dictionary with location_name: location_id or a list of location_ids for
        all direct child locations.
    """
    df = get_location_metadata(location_set_id=location_set_id, release_id=release_id)
    df = df.query(f"parent_id == {location_parent_id}")

    if return_dict:
        return df.set_index("location_ascii_name")["location_id"].to_dict()
    else:
        return df["location_id"].tolist()
