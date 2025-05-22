import argparse

import pandas as pd
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.formatting-functions.general import date_formatter
from crosscutting_functions import live_births

from clinical_info.Formatting.indv_sources.GEO_COL import constants_GEO_COL


def geo_reader(year_id: int) -> pd.DataFrame:
    """Reads in all years of GEO_COL data and pre-formats selected columns.

    Args:
        year_id (int): Year of geo data to read in.

    Raises:
        ValueError: No data available for year_id

    Returns:
        pd.DataFrame: Concatenated df from all years.
    """
    if year_id in constants_GEO_COL.PATH_DICT:
        col_dict = constants_GEO_COL.GROUP_YEAR[year_id]
        df = pd.read_excel(constants_GEO_COL.PATH_DICT[year_id])

        df["year_start"] = df["year_end"] = year_id

        df = date_formatter(df, columns=["Discharge Date"])
        df["age_unit"] = None

        df = df.rename(columns=col_dict)
        df = df[col_dict.values()]

    if df.empty:
        # Handle the case when df has not been defined because year_id was not in PATH_DICT
        raise ValueError(f"No data available for year_id {year_id}")

    return df


def format_age(df: pd.DataFrame) -> pd.DataFrame:
    """Formats age column based on age years months days and unit.

    Args:
        df (pd.DataFrame): Concatenated df from all years.

    Returns:
        pd.DataFrame: Concatenated df from all years with age col added.
    """
    values = {"age_years": 0, "age_months": 0, "age_days": 0, "age_unit": "Years"}
    df = df.fillna(value=values)

    # First, we need to drop two rows with Georgian characters meaning "Unknown"
    # in the age columns.
    df = df.loc[~df["age_months"].isin(constants_GEO_COL.GEO_CHARS)]
    df = df.loc[~df["age_years"].isin(constants_GEO_COL.GEO_CHARS)]
    df["age_years"] = pd.to_numeric(df["age_years"])

    # converting 2015,2018,2019,2020
    df.loc[df["age_unit"] == "Days", "age_years"] = df["age_years"].astype(float) / 365

    # converting months to days using 30.5 as the denom
    # this is in-line with CoD's process and was signed off by Steve
    df["age_months"] = df["age_months"].astype(float) * 30.5
    df.loc[df["age_years"] == 0, "age_years"] = (
        df["age_days"].astype(float) + df["age_months"].astype(float)
    ) / 365
    df = df.rename(columns={"age_years": "age"})

    return df


def age_bin(df: pd.DataFrame, clinical_age_group_set_id: int) -> pd.DataFrame:
    """Adds age start/end and age_group_id to the dataset.

    Args:
        df (pd.DataFrame): Concatenated df from all years.
        clinical_age_group_set_id (int): should be 2 for this source.

    Returns:
        pd.DataFrame: Concatenated df from all years with
                        age start/end age_group_id col added.
    """
    # age binning and get age_group_id
    df = demographic.age_binning(
        df, terminal_age_in_data=True, clinical_age_group_set_id=clinical_age_group_set_id
    )
    df = demographic.all_group_id_start_end_switcher(
        df=df,
        clinical_age_group_set_id=clinical_age_group_set_id,
        remove_cols=False,
    )

    return df[constants_GEO_COL.KEEP]


def fill_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fills dataset columns with constant values.

    Args:
        df (pd.DataFrame): Concatenated df from all years.

    Returns:
        pd.DataFrame: Concatenated df from all years with new cols added.
    """
    for column, value in constants_GEO_COL.FILL_COLS.items():
        df[column] = value

    df["nid"] = df["year_start"].map(constants_GEO_COL.NIDS)

    # drop null primary diag, 8 in this source
    df = df.reset_index(drop=True)
    df = df.loc[~df["dx_1"].isnull()]

    # First convert rows with Georgian characters for male/female
    df["sex_id"] = df["sex_id"].replace("მამრობითი", 1)
    df["sex_id"] = df["sex_id"].replace("მდედრობითი", 2)
    # Replace some typos in sex_id as well as missing data to equal 3
    df["sex_id"] = df["sex_id"].astype(float)
    df.loc[~df["sex_id"].isin([1, 2]), "sex_id"] = 3
    df["sex_id"] = df["sex_id"].astype(int)

    return df


def drop_day_cases(df: pd.DataFrame) -> pd.DataFrame:
    """Formats and drops day cases for GBD.

    Args:
        df (pd.DataFrame): Geo data of the given year.

    Returns:
        pd.DataFrame: Geo data of the given year without day cases.
    """
    # drop day cases (los < 1)
    # Georgia NCDC team confirmed that any values less than 1 under 
    # the 'bed days' should be treated as 'day cases'.
    df = df.loc[df["los"] >= 1]

    return df


def format_outcome(df: pd.DataFrame) -> pd.DataFrame:
    """Formats/maps outcome_id based on constant.
    Converts to int later before saving to disk.

    Args:
        df (pd.DataFrame): Concatenated df from all years.

    Returns:
        pd.DataFrame: Concatenated df from all years with outcome_id added.
    """
    # assign id to outcome
    df["outcome_id"] = df["outcome_id"].astype(str)
    df["outcome_id"] = df["outcome_id"].str.lower()
    df.loc[df["outcome_id"].isin([constants_GEO_COL.OUTCOME_VALS]), "outcome_id"] = "death"
    df.loc[df["outcome_id"] != "death", "outcome_id"] = "discharge"

    return df


def format_diags(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans diagnoses and converts them to long format with diagnosis_id.

    Args:
        df (pd.DataFrame): Concatenated df from all years.

    Returns:
        pd.DataFrame: Concatenated df from all years with diags in long format.
    """
    for column in df.columns:
        if column.startswith("dx_"):
            if df[column].isnull().all():
                df = df.drop(column, axis=1)
            else:
                df[column] = formatting.sanitize_diagnoses(df[column])

    # swap ecode with ncode, may switch to 11 in future
    df = formatting.swap_ecode_with_ncode(df, icd_vers=constants_GEO_COL.ICD_VERS)

    # swap live birth codes
    df = live_births.swap_live_births(df, drop_if_primary_still_live=False)

    # Reshape diagnoses from wide to long
    df = formatting.stack_merger(df)

    df["val"] = 1

    # Check number of events without primary dx.
    primary_dx_only = df.loc[(df["diagnosis_id"] == 1)]
    ev_missing_dx1 = set(df["patient_index"].unique()) - set(
        primary_dx_only["patient_index"].unique()
    )
    if len(ev_missing_dx1) > 0:
        raise RuntimeError(
            f"Not every event has a primary dx. Inspect patient_index(s) {ev_missing_dx1}"
        )

    # Check if all events have only 1 primary dx.
    tmp = primary_dx_only.groupby(by="patient_index")
    tmp_sz = pd.DataFrame(tmp.size(), columns=["size"]).reset_index()
    bad_dx1 = tmp_sz.loc[tmp_sz["size"] != 1]
    if len(bad_dx1) > 0:
        msg = "Some patient_index have more than 1 primary dx"
        sol = f"Inspect patient_index(s) {bad_dx1['patient_index']}"
        raise RuntimeError(f"{msg}. {sol}")

    return df


def groupby_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Drop date cols for gbd, check nulls, and group by aggregate val.

    Args:
        df (pd.DataFrame): Concatenated df from all years.

    Returns:
        pd.DataFrame: Concatenated df from all years with aggregated vals/admissions.
    """
    # drop dates before checking nulls
    if "discharge_date" in df.columns:
        df = df.drop("discharge_date", axis=1)

    print("Are there missing values in any row?\n")
    null_condition = df[constants_GEO_COL.GROUP_VARS + ["val"]].isnull().values.any()
    if null_condition:
        raise ValueError("There are rows with null values. Cannot group by agg.")

    agg_df = df.groupby(constants_GEO_COL.GROUP_VARS).agg({"val": "sum"}).reset_index()
    del df

    # Arrange columns in our standardized feature order
    agg_df = agg_df[constants_GEO_COL.HOSP_FRMAT_FEAT]

    return agg_df


def validate_cleaned_data(df: pd.DataFrame) -> None:
    """Validate data before saving to disk.
    Borrowed parts from NZL validation.
    TODO compile validation functions to a helper module.

    Args:
        df (pd.DataFrame): compiled dataset.

    Raises:
        ValueError: More than the expected sex_id found.
        ValueError: diagnosis_id should have 2 or less feature levels
        RuntimeError: Misalignment between year/NID.
        ValueError: for some reason there are negative case counts
    """
    # No more than male, female, other/unknown.
    if len(df["sex_id"].unique()) > 3:
        raise ValueError("Only 3 sex_id are expected.")

    if len(df["diagnosis_id"].unique()) > 2:
        raise ValueError("diagnosis_id should have 2 or less feature levels")

    # Same number of NID as years.
    if len(df["year_start"].unique()) != len(df["nid"].unique()):
        raise RuntimeError("Different number of years than NIDs")

    if (df["val"] < 0).any():
        raise ValueError("for some reason there are negative case counts")


def run_geo_format(year_id: int, outpath: str, clinical_age_group_set_id: int) -> None:
    """Main function of the format worker. Reads in and processes the
    GEO dataset all years. Currently must write to disk.

    Args:
        year_id (int): Year of ita data to read in.
        outpath (str): Output filepath.
        clinical_age_group_set_id (int): should be 2 for this source.
    """
    df = geo_reader(year_id=year_id)
    df = format_age(df=df)
    df = age_bin(df=df, clinical_age_group_set_id=clinical_age_group_set_id)
    df = format_outcome(df=df)
    df = fill_columns(df=df)
    df = format_diags(df=df)
    df = groupby_agg(df=df)
    df = general_purpose.to_int(df=df, integer_columns=constants_GEO_COL.INT_COLS)

    validate_cleaned_data(df=df)

    df.to_parquet(path=outpath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year_id", type=int, required=True, help="year of GEO_COL data to process"
    )
    parser.add_argument(
        "--clinical_age_group_set_id",
        type=int,
        required=True,
        help="clinical_age_group_set_id for this source.",
    )

    args = parser.parse_args()

    output_fn = f"geo_{args.year_id}.parquet"
    outpath = constants_GEO_COL.OUTPATH_DIR / "output_by_year" / output_fn

    run_geo_format(year_id=args.year_id, outpath=outpath, clinical_age_group_set_id=args.clinical_age_group_set_id)
