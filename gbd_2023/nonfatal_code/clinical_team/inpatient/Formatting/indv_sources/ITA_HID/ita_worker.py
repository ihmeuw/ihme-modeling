import argparse

import numpy as np
import pandas as pd
from crosscutting_functions import demographic
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions import live_births

from inpatient.Formatting.indv_sources.ITA_HID import constants_ITA_HID


def read_data(year_id: int) -> pd.DataFrame:
    """Wrapper function to read in the single year ita data.
    Different reader functions are called based on the year_id given.
    Read arguments specified for importing 2022 .txt data.

    Args:
        year_id (int): Year of ita data to read in.

    Returns:
        pd.DataFrame: ita data of the given year with columns in English.
    """
    if year_id in constants_ITA_HID.SPECIAL_PROCESS:
        fpath = constants_ITA_HID.SPECIAL_PROCESS[year_id]["fpath"]
        read_method = constants_ITA_HID.SPECIAL_PROCESS[year_id]["read_method"]

        if read_method == "read_stata":
            df = pd.read_stata(fpath)
        elif read_method == "read_sas":
            df = pd.read_sas(fpath)
        elif read_method == "read_csv":
            if year_id == 2022:
                df = pd.read_csv(
                    fpath, delimiter="\t", encoding="ISO-8859-1", low_memory=False
                )
            else:
                df = pd.read_csv(fpath)
        else:
            print("Read method not implemented. Check filetype and change accordingly.")

    else:
        fpath = f"{constants_ITA_HID.BASE_DIR}/gbd_{year_id}.dta"
        df = pd.read_stata(fpath)

    return df


def format_columns(df: pd.DataFrame, year_id: int) -> pd.DataFrame:
    """Format columns of the given df.

    Args:
        df (pd.DataFrame): ita data of the given year.
        year_id (int): Year of ita data to read in.

    Returns:
        pd.DataFrame: ita data of the given year with wanted columns.
    """
    to_keep = [n for n in constants_ITA_HID.IDEAL_KEEP if n in df.columns]
    df = df[to_keep]
    df["year_start"] = year_id

    df["gg_deg"] = pd.to_numeric(df["gg_deg"], errors="raise")

    # Rename features
    df = df.rename(columns=constants_ITA_HID.HOSP_WIDE_FEAT)

    # check that dates are always present
    if year_id not in constants_ITA_HID.PROCESS_DICT["no_admit_date"]:
        if df["dis_date"].isnull().sum() != 0:
            raise ValueError("There are null dis_date in df.")
        if df["adm_date"].isnull().sum() != 0:
            raise ValueError("There are null adm_date in df")

    # add columns that are not present yet
    new_col_df = pd.DataFrame(
        columns=list(set(constants_ITA_HID.HOSP_WIDE_FEAT.values()) - set(df.columns))
    )
    df = df.join(new_col_df)

    # If the data is 2007, many of the columns are bytes and not strings.
    if year_id == 2007:
        for col in constants_ITA_HID.COLS_2007:
            try:
                df[col] = df[col].str.decode("utf-8")
            # Catch error when column not found
            except KeyError:
                print(f"Year 2007 didn't have column {col}")
                pass

    return df


def fill_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fills dataset columns with constant values.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year with new cols added.
    """
    for column, value in constants_ITA_HID.FILL_COLS.items():
        df[column] = value

    df["year_end"] = df["year_start"]

    # fill NIDs
    df = formatting.fill_nid(df, constants_ITA_HID.NID_DICT)

    return df


def format_locs(df: pd.DataFrame, year_id: int) -> pd.DataFrame:
    """Format dataset location_id column

    Args:
        df (pd.DataFrame): ita data of the given year.
        year_id (int): Year of ita data to read in.

    Returns:
        pd.DataFrame: ita data of the given year with formatted sex_id col.
    """
    # map locations
    if year_id == 2007:
        df["cod_reg"] = df["cod_reg"].str.decode("utf-8")
    if year_id in constants_ITA_HID.PROCESS_DICT["no_admit_date"]:
        df["cod_reg"] = df["cod_reg"].astype(str)
        # make sure everything is 3 digits
        df["cod_reg"] = ("0" + df["cod_reg"]).str[-3:]

    df = df.merge(constants_ITA_HID.LOC_DF, how="left", on="cod_reg", validate="m:1")
    if df["location_id"].isnull().sum() != 0:
        raise ValueError("There are null locations after merge")

    df = df.drop(["cod_reg", "location_name"], axis=1)

    return df


def drop_day_cases(df: pd.DataFrame) -> pd.DataFrame:
    """Formats and drops day cases for GBD.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year without day cases.
    """
    # drop day cases (day_case == 2)
    df["day_case"] = pd.to_numeric(df["day_case"], errors="coerce")
    df = df.loc[~df["day_case"].isnull()]
    df["day_case"] = df["day_case"].astype(int)
    df = df.loc[df["day_case"] == 1]

    return df


def format_sex(df: pd.DataFrame) -> pd.DataFrame:
    """Format the sex_id column.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year with formatted sex_id col.
    """
    # replace sex_id with 3 (unknown) when it's not identified
    df["sex_id"] = pd.to_numeric(df["sex_id"], errors="coerce")
    df.loc[~df["sex_id"].isin([1, 2]), "sex_id"] = 3

    return df


def format_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    """Format the outcome column. Either death or discharge.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year with str outcome_id.
    """
    df["outcome_id"] = df["outcome_id"].astype(str)
    df["outcome_id"] = df["outcome_id"].replace(["1"], ["death"])
    df.loc[df["outcome_id"] != "death", "outcome_id"] = "discharge"

    return df


def format_age(df: pd.DataFrame, clinical_age_group_set_id: int) -> pd.DataFrame:
    """
    Args:
        df (pd.DataFrame): ita data of the given year.
        clinical_age_group_set_id (int): should be 2 for this source.

    Returns:
        pd.DataFrame: ita data of the given year with age formatted.
    """
    # make sure null vals are typed as np.nan bc of STATA and .txt import
    df["age_days"] = pd.to_numeric(df["age_days"], errors="coerce")

    # age stays the same unless age_days is present
    # we use age days to calc u1 ages
    df.loc[df["age"] == 0, "age"] = df.loc[df["age"] == 0, "age_days"]
    if df.loc[df["age"] == 0, "age_days"].isnull().sum() != 0:
        raise ValueError("age/age day logic went wrong")

    df.loc[df["age_days"].notnull(), "age"] = df[df["age_days"].notnull()].age_days / 365

    # now everything is in years
    df["age_group_unit"] = 1

    df = df.drop("age_days", axis=1)

    # age binning
    df = demographic.age_binning(
        df, under1_age_detail=True, clinical_age_group_set_id=clinical_age_group_set_id
    )

    return df


def format_diags(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans diagnoses and converts them to long format with diagnosis_id.
    Also swaps e and n codes, and live births prior to wide -> long.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year with diags in long format.
    """
    df = df[df["dx_1"].notnull()]
    df = df[df["dx_1"] != ""]

    # fill missing ecodes with null
    df["ecode_1"] = df["ecode_1"].replace(r"\s+", "", regex=True)
    df.loc[df["ecode_1"] == "", "ecode_1"] = np.nan

    # swap e and n codes
    if not df["ecode_1"].isnull().all():
        df["dx_7"] = np.nan
        # put n codes into dx_7 col
        df.loc[df["ecode_1"].notnull(), "dx_7"] = df.loc[df["ecode_1"].notnull(), "dx_1"]
        # overwrite n codes in dx_1
        df.loc[df["ecode_1"].notnull(), "dx_1"] = df.loc[df["ecode_1"].notnull(), "ecode_1"]
    # drop the ecode col
    df = df.drop("ecode_1", axis=1)

    # live birth swapping subroutine
    # remove dummy null values, swap live births requires actual nulls
    for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
        df.loc[df[col] == "", col] = np.nan

    # Find all columns with dx_ at the start
    diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
    # Remove non-alphanumeric characters from dx feats
    for feat in diagnosis_feats:
        df[feat] = formatting.sanitize_diagnoses(df[feat])

    # swap live birth codes
    df = live_births.swap_live_births(df, drop_if_primary_still_live=False)

    if len(diagnosis_feats) > 1:
        # Reshape diagnoses from wide to long
        df = formatting.stack_merger(df)
    elif len(diagnosis_feats) == 1:
        df = df.rename(columns={"dx_1": "cause_code"})
        df["diagnosis_id"] = 1
    else:
        print("Something went wrong, there are no ICD code features")

    # Ensure capitalized alphanumeric characters
    df["cause_code"] = df["cause_code"].str.upper()

    # individual record: add one case for every diagnosis
    df["val"] = 1

    return df


def groupby_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Drop date cols for gbd, check nulls, and group by aggregate val.

    Args:
        df (pd.DataFrame): ita data of the given year.

    Returns:
        pd.DataFrame: ita data of the given year with aggregated vals/admissions.
    """
    # drop dates before checking nulls
    if "dis_date" in df.columns:
        df = df.drop("dis_date", axis=1)
    if "adm_date" in df.columns:
        df = df.drop("adm_date", axis=1)

    print("Are there missing values in any row?\n")
    null_condition = df[constants_ITA_HID.GROUP_VARS + ["val"]].isnull().values.any()
    if null_condition:
        raise ValueError("There are rows with null values. Cannot group by agg.")

    agg_df = df.groupby(constants_ITA_HID.GROUP_VARS).agg({"val": "sum"}).reset_index()
    del df

    # Arrange columns in our standardized feature order
    agg_df = agg_df[constants_ITA_HID.HOSP_FRMAT_FEAT]

    return agg_df


def validate_cleaned_data(df: pd.DataFrame) -> None:
    """Validate data before saving to disk.

    Args:
        df (pd.DataFrame): compiled dataset.
    """
    # check data types
    for col in df.drop(
        ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
    ).columns:
        # assert that everything but the mentioned columns
        # are NOT object
        if df[col].dtype == object:
            raise ValueError(f"{col} should not be of type object")

    # check number of unique feature levels
    if len(df["year_start"].unique()) != len(df["nid"].unique()):
        raise ValueError("number of unique years and nids should match")

    if len(df["diagnosis_id"].unique()) > 2:
        raise ValueError("diagnosis_id should have 2 or less feature levels")

    if len(df["sex_id"].unique()) > 3 or len(df["sex_id"].unique()) <= 1:
        raise ValueError("sex_id should have 2-3 feature levels")

    if (df["val"] < 0).any():
        raise ValueError("for some reason there are negative case counts")

    # check for every row that there are no null location IDs in format_locs()
    if df["location_id"].isnull().any():
        raise ValueError("there are location ids that are missing")


def format_ita(year_id: int, outpath: str, clinical_age_group_set_id: int) -> None:
    """Main function of the format worker. Reads in and processes the
    ita dataset by year. Currently must write to disk.

    Args:
        year_id (int): Year of ita data to read in.
        outpath (str): Output filepath.
        clinical_age_group_set_id (int): specific age group set we use for this source.
    """
    df = read_data(year_id)
    df = format_columns(df, year_id)

    df = fill_columns(df)
    df = format_sex(df)
    df = format_outcomes(df)
    df = drop_day_cases(df)
    df = format_locs(df, year_id)
    df = format_age(df, clinical_age_group_set_id=clinical_age_group_set_id)
    df = format_diags(df)

    for col in constants_ITA_HID.INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
    for col in constants_ITA_HID.STR_COLS:
        df[col] = df[col].astype(str)

    df = groupby_agg(df)

    validate_cleaned_data(df)

    # write output
    df.to_parquet(outpath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year_id", type=int, required=True, help="year of ITA_HID data to process"
    )
    parser.add_argument(
        "--clinical_age_group_set_id",
        type=int,
        required=True,
        help="clinical_age_group_set_id for this source.",
    )

    args = parser.parse_args()

    output_fn = f"ita_{args.year_id}.parquet"
    outpath = constants_ITA_HID.OUTPATH_DIR / "output_by_year" / output_fn
    format_ita(args.year_id, outpath, args.clinical_age_group_set_id)
