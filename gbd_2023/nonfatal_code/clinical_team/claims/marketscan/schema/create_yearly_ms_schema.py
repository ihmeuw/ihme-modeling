"""
This contains much of the processing logic for creating the updated Marketscan schema.
It's called by the jobmon workflow to process in parallel by values of the partition columns.

Provide the create_schema function with a year and it will transform the parquet copies of
raw data into an updated schema for processing marketscan, optimized for pulling data at the
bundle (by way of ICD groups) level.
"""

import time
from typing import Dict, List

import click
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.live_births import live_births
from crosscutting_functions.clinical_map_tools.icd_groups.use_icd_group_ids import UseIcdGroups
from loguru import logger

from marketscan.schema import aggregate_denoms as ad
from marketscan.schema import config, egeoloc_location_id_map
from marketscan.schema import file_handlers as fh
from marketscan.schema import input_data_types


def death_eligibility(icd_mart_df: pd.DataFrame) -> pd.DataFrame:
    """Apply the final eligibility criteria which needs death status from the
    inpatient tables."""

    icd_mart_df.loc[icd_mart_df["dstatus"].isin(constants.DEATH_CODES), "in_std_sample"] = 1
    return icd_mart_df.drop(["dstatus"], axis=1)


def transfer_death_eligibility(
    icd_mart_df: pd.DataFrame, denominator_df: pd.DataFrame
) -> pd.DataFrame:
    """Death eligibility is derived from the inpatient data in the icd mart, but it must be
    transferred over to the denominator files as well."""

    denominator_df.loc[
        denominator_df["bene_id"].isin(
            icd_mart_df.loc[icd_mart_df["in_std_sample"] == 1, "bene_id"].unique()
        ),
        "in_std_sample",
    ] = 1
    return denominator_df


def attach_icd_group(icd_mart_df: pd.DataFrame) -> pd.DataFrame:
    """The 3 digit ICD code group is used to reduce run-time by allowing jobs to quickly query
    a subset of bundle-relevant codes while still containing all codes that could potentially
    be mapped to bundle via the Clinical team's truncation method.
    """

    icd_mart_df["icd_group_code"] = icd_mart_df["cause_code"].str[0:3]
    # map to icd group id
    icd_group_obj = UseIcdGroups()
    icd_group_obj.get_icd_group_lookup()

    icd_mart_df["cause_code"] = pd.Categorical(icd_mart_df["cause_code"])

    icd_mart_df = icd_mart_df.merge(
        icd_group_obj.icd_groups,
        how="left",
        on=["icd_group_code", "code_system_id"],
        validate="m:1",
    )
    # Use a placeholder for odd/unmapped group codes in case they're ever added to the map
    icd_mart_df["icd_group_id"] = icd_mart_df["icd_group_id"].fillna(0).astype(int)

    if icd_mart_df.icd_group_id.isnull().any():
        raise ValueError(
            "Missing some icd group IDs from the data. Might mean codes are "
            "present in data that arent in map? or codes are mislabeled"
        )

    icd_mart_df = icd_mart_df.drop(["code_system_id", "icd_group_code"], axis=1)
    return icd_mart_df


def swap_loc_ids_egeoloc(denominator_df: pd.DataFrame) -> pd.DataFrame:
    """
    Marketscan uses egeoloc in the raw data for enrollee location of residence.
    Merge on location id when needed or egeoloc when needed.
    """

    loc_map = egeoloc_location_id_map.EGEOLOC_LOCATION_DF

    if loc_map.shape[0] != loc_map.drop_duplicates().shape[0]:
        raise ValueError("Duplicate location mappings are present")

    merge_on = "egeoloc" if "egeoloc" in denominator_df.columns else "location_id"

    pre_shape = denominator_df.shape[0]
    denominator_df = denominator_df.merge(loc_map, how="left", on=merge_on, validate="m:1")

    if pre_shape != denominator_df.shape[0]:
        raise ValueError("Merging on location_id has added or removed rows")

    return denominator_df.drop(merge_on, axis=1)


def assign_cols(
    df: pd.DataFrame, year: int, data_type: input_data_types.dataclass
) -> pd.DataFrame:
    """Assign new columns based on year and data type.

    Args:
        df: Claims or Denominator table.
        year: Reporting year of data being processed.
        data_type: Data type class object.

    Returns:
        Claims data with additional columns.
    """
    df["year"] = year  # assign reporting year to data, not present on some tables

    # new column names and values are managed by the data type classes
    return df.assign(**data_type.assign_cols)


def get_claims_file(
    year: int, data_type: input_data_types.dataclass, source: str, filter_dict: Dict[str, int]
) -> pd.DataFrame:
    """
    This gets all the claims data from 1 parquet file, has the option to sample it,
    cleans and assigns some new columns.
    """

    cols = data_type.read_cols.copy()

    # MS schema changed slightly, included a new col to identify ICD system
    if year >= 2015:
        cols.append("dxver")

    file_path = fh.get_file_path(source=source, data_type=data_type, year=year)
    icd_mart_df = fh.read_ms_parquet(file_path, cols)

    icd_mart_df = clean_claims(icd_mart_df=icd_mart_df, data_type=data_type, year=year)
    icd_mart_df = assign_code_system(icd_mart_df)
    icd_mart_df = assign_cols(df=icd_mart_df, year=year, data_type=data_type)

    # flag duplicate rows
    icd_mart_df = identify_dupe_claims(icd_mart_df)

    # apply the filter values to each file
    icd_mart_df = filter_df(icd_mart_df, filter_dict)

    if data_type.swap_live_births:

        icd_mart_df = fh.dx_to_str(icd_mart_df)
        # swap live birth codes. Should only be applied to inpatient data
        if data_type.name != "inpatient":
            raise ValueError("Live birth swapping should only be applied to inpatient data")
        icd_mart_df = live_births.swap_live_births(
            icd_mart_df, drop_if_primary_still_live=False
        )

    return icd_mart_df


def get_claims_data(year: int, filter_dict: Dict[str, int]) -> pd.DataFrame:
    """Pull all claims data for a given year."""

    # initialize an empty list to append to before concatting all data together
    icd_mart_list = []

    # Instantiate data type class and then process only relevant data
    otp_val = filter_dict["is_otp"]
    if otp_val == 1:
        data_type = input_data_types.OutpatientData()
    elif otp_val == 0:
        data_type = input_data_types.InpatientData()
    else:
        raise ValueError(f"The is_otp value {otp_val} is not recognized")

    for source in constants.SOURCES:
        icd_mart_list.append(
            get_claims_file(
                year=year, data_type=data_type, source=source, filter_dict=filter_dict
            )
        )

    icd_mart_df = pd.concat(icd_mart_list, sort=False)

    # reshape dx from wide to long and attach the ICD group id
    icd_mart_df = reshape_dx(icd_mart_df, constants.IDX)
    logger.info("The reshape is complete")

    return icd_mart_df


def assign_code_system(icd_mart_df: pd.DataFrame) -> pd.DataFrame:
    """ICD code system in the US generally switched from ICD 9 to ICD 10 on Oct 1st 2015.

    Args:
        icd_mart_df: Claims table with dxver col.

    Returns:
        Claims table with an ICD code_system_id.
    """

    # Replace dxver ids with code system ids and then rename column
    icd_mart_df["dxver"] = icd_mart_df["dxver"].map(constants.DXVER_TO_CODE_SYS)
    icd_mart_df.rename(columns={"dxver": "code_system_id"}, inplace=True)

    icd_mart_df["code_system_id"] = pd.to_numeric(
        icd_mart_df["code_system_id"], downcast="integer"
    )


    pre = len(icd_mart_df)
    icd_mart_df = icd_mart_df[
        icd_mart_df["code_system_id"].isin(constants.CODE_SYSTEMS.values())
    ]
    post = len(icd_mart_df)
    logger.info(
        f"Removed {pre-post} rows with unusable code systems. "
        f"{round(((pre-post)/len(icd_mart_df)) * 100, 3)}% of the total rows"
    )

    return icd_mart_df


def reshape_dx(icd_mart_df: pd.DataFrame, idx: List[str]) -> pd.DataFrame:
    """Convert the ICD diagnosis codes from wide storage (eg dx_1, dx_2, dx_3, etc), to long
    storage (eg cause_code, diagnosis_id).

    Args:
        icd_mart_df: Dataframe with wide diagnoses in "dx_*" columns.
        idx: List of columns to use as the reshaped index.

    Returns:
        Dataframe with diagnoses stored long.
    """
    prereshape = len(icd_mart_df)

    icd_mart_df = icd_mart_df.set_index(idx).stack().reset_index()
    reshape1 = len(icd_mart_df)
    icd_mart_df = icd_mart_df[icd_mart_df[0] != ""]  # blank codes are unusable
    reshape2 = len(icd_mart_df)
    icd_mart_df = icd_mart_df.rename(
        columns={"level_{}".format(len(idx)): "diagnosis_id", 0: "cause_code"}
    )
    icd_mart_df["diagnosis_id"] = pd.to_numeric(
        icd_mart_df["diagnosis_id"].str.replace("dx_", ""), downcast="integer"
    )

    # fulfill req of cleaning ICD codes
    icd_mart_df["cause_code"] = formatting.sanitize_diagnoses(icd_mart_df["cause_code"])
    icd_mart_df["cause_code"] = icd_mart_df["cause_code"].str.upper()
    # The methods above cast the cause_code column from categorical to str
    icd_mart_df["cause_code"] = pd.Categorical(icd_mart_df["cause_code"])

    logger.info(
        f"Data row summary during reshape:\nInitial rows: {prereshape}\n"
        f"Reshaped rows:{reshape1}\n"
        f"Blank dx removed rows: {reshape2}\n"
        f"Final rows: {len(icd_mart_df)}"
    )
    return icd_mart_df


def assign_nulls(icd_mart_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace string placeholders with true nulls.


    Args:
        icd_mart_df: Claims table with diagnoses stored wide.

    Returns:
        Claims table with diagnoses stored wide and null placeholder strings converted
        to true null.
    """
    null_placeholders = ["NULL", "NONE", "NAN"]
    dxcols = [col for col in icd_mart_df.columns if "dx_" in col]
    for dx in dxcols:
        prenull = icd_mart_df[dx].isnull().sum()
        icd_mart_df.loc[
            icd_mart_df[dx].astype(str).str.upper().isin(null_placeholders), dx
        ] = None
        postnull = icd_mart_df[dx].isnull().sum()
        if prenull != postnull:
            logger.info(f"Null rows went from {prenull} to {postnull}")
        if prenull > postnull:
            raise ValueError("Somehow Nulls were removed")
    return icd_mart_df


def clean_claims(
    icd_mart_df: pd.DataFrame, data_type: input_data_types.dataclass, year: int
) -> pd.DataFrame:
    """Removes a db header value and casts some column dtypes and converts string placeholder
    nulls to true null.

    Args:
        icd_mart_df: Claims table with diagnosis codes stored wide.
        data_type: Data type class object.

    Returns:
        Claims table with adjusted dtypes and nulls.
    """
    icd_mart_df = icd_mart_df[icd_mart_df.dx_1 != "DX1"]  # headers present in marketscan db

    if year < 2015:
        icd_mart_df["dxver"] = constants.DX_VERS["ICD9"]

    if data_type.name == "inpatient":
        icd_mart_df["dstatus"] = pd.to_numeric(icd_mart_df["dstatus"], downcast="integer")
    else:
        pass

    icd_mart_df = assign_nulls(icd_mart_df)

    icd_mart_df["service_start"] = pd.to_datetime(icd_mart_df["service_start"])
    # remove null bene_ids, these rows are unusable
    icd_mart_df = icd_mart_df[icd_mart_df["bene_id"].notnull()]
    icd_mart_df["bene_id"] = icd_mart_df["bene_id"].astype(int)

    return icd_mart_df


def clean_denominator(denominator_df: pd.DataFrame) -> pd.DataFrame:
    """Remove nulls and prep some column data types.

    Args:
        denominator_df: Denominator table.

    Returns:
        Input denominator table with slightly different col dtypes.
    """
    # remove null bene_ids, these rows are unusable
    denominator_df = denominator_df[denominator_df["bene_id"].notnull()]
    denominator_df["bene_id"] = denominator_df["bene_id"].astype(int)

    date_cols = ["service_start"]
    for col in date_cols:
        denominator_df[col] = pd.to_datetime(denominator_df[col])

    return denominator_df


def get_denominator_file(
    year: int, data_type: input_data_types.dataclass, source: str
) -> pd.DataFrame:
    """Pull denominator data from a single file."""
    cols = data_type.read_cols.copy()
    if year > 2000:
        cols.append("mhsacovg")

    file_path = fh.get_file_path(source=source, data_type=data_type, year=year)
    denominator_df = fh.read_ms_parquet(file_path, cols)

    if year == 2000:

        denominator_df["mhsacovg"] = 1

    denominator_df = assign_cols(df=denominator_df, year=year, data_type=data_type)
    denominator_df = clean_denominator(denominator_df)

    return denominator_df


def get_denominator_data(year: int) -> pd.DataFrame:
    """Pull all denominator data for a given year. There are 2 denominator files per year."""

    # initialize an empty list to concat data from each file to
    denominator_list = []

    data_type = input_data_types.EnrollmentData()
    for source in constants.SOURCES:
        denominator_list.append(
            get_denominator_file(
                year=year, data_type=data_type, source=source
            ).drop_duplicates()
        )

    denominator_df = pd.concat(denominator_list, sort=False)

    # meet current reqs to assign min age per year and identify in-sample enrollees
    denominator_df = calc_min_age(denominator_df)
    denominator_df = year_or_u1_eligibility(denominator_df)
    denominator_df = adjust_mhsacovg(
        denominator_df
    )  # if any month of no mhsa cov, change to 0
    # eligibility reqs are done, remove monthly records from denominator table
    denominator_df = remove_monthly_record(denominator_df)

    # replace proprietary egeoloc codes with GBD location_id
    denominator_df = swap_loc_ids_egeoloc(denominator_df)
    return denominator_df


def calc_min_age(denominator_df: pd.DataFrame, age_diff_thresh=1000) -> pd.DataFrame:
    """MS processing uses the youngest age provided for an enrollee in each year.

    Args:
        denominator_df: Denominator table with unadjusted monthly age.
        age_diff_thresh: Amount of impossible age differences to allow for a
        year. Defaults to 1000.

    Returns:
        Denom table with age unified to the enrollees youngest age in a year
    """
    denominator_df["min_age"] = denominator_df.groupby(["bene_id"])["age"].transform("min")
    denominator_df["age_diff"] = denominator_df.age - denominator_df.min_age

    big_diffs = len(denominator_df.query("age_diff > 1"))
    if big_diffs > age_diff_thresh:
        raise RuntimeError(
            "The number of rows with age differences greater than 1 has "
            "surpassed the age diff threshold"
        )
    denominator_df = denominator_df[denominator_df["age_diff"] <= 1]

    denominator_df = denominator_df.drop(["age", "age_diff"], axis=1)
    denominator_df = denominator_df.rename(columns={"min_age": "age"})

    return denominator_df


def year_or_u1_eligibility(denominator_df: pd.DataFrame) -> pd.DataFrame:
    """Apply 2 of the 3 eligibility criteria to the data.

    Args:
        denominator_df: Denominator table with monthly eligibility info.

    Returns:
        Input table with a new col identifying if enrollee is in or out of sample.
    """
    # data validation: duplicates were found in db data
    if len(denominator_df) != len(
        denominator_df[["bene_id", "service_start"]].drop_duplicates()
    ):
        raise ValueError("Duplicate bene-months present")
    # assume enrollee is out of sample unless they meet criteria
    denominator_df["in_std_sample"] = 0
    # include if born in reporting year
    denominator_df.loc[denominator_df["age"] == 0, "in_std_sample"] = 1
    denominator_df["months"] = denominator_df.groupby("bene_id")["bene_id"].transform("size")
    # include if 12 months of enrollment. >= matches current processing

    denominator_df.loc[denominator_df["months"] >= 12, "in_std_sample"] = 1

    return denominator_df


def adjust_mhsacovg(denominator_df: pd.DataFrame) -> pd.DataFrame:
    """People have both values in the same year for mhsacov, this doesn't work with our
    enrollment criteria. Adjust values so if you have a single month of ineligbility then
    your full year is ineligible.
    """

    if denominator_df["year"].unique().size != 1:
        raise RuntimeError("Multiple years of data present, logic for this breaks")
    inelig_benes = (
        denominator_df.loc[denominator_df["mhsacovg"] == 0, "bene_id"].unique().tolist()
    )

    logger.info(
        f"There were {len(inelig_benes)} enrollees with at least 1 month of non coverage for "
        f"mhsa. {round((len(inelig_benes) / denominator_df.bene_id.unique().size) * 100, 3)}% "
        "of all enrollees"
    )

    # adjust the mhsacovg values for these benes
    denominator_df.loc[denominator_df["bene_id"].isin(inelig_benes), "mhsacovg"] = 0

    return denominator_df


def remove_monthly_record(denominator_df: pd.DataFrame) -> pd.DataFrame:
    """Convert enrollment records from monthly rows to a single row per year.

    Args:
        denominator_df: Denominator table with monthly eligibility info.

    Raises:
        RuntimeError if duplicate data is present in the denominators.

    Returns:
        Denominator table with 1 row per enrollee-year.
    """

    id_cols = ["bene_id", "year"]  # the PK for the denominator table
    xtra_cols = ["age", "sex_id", "egeoloc", "mhsacovg", "in_std_sample"]

    # this sorting will result in preferring locations for the earliest months in the year
    # usually January
    denominator_df = denominator_df.sort_values(["bene_id", "service_start"])

    # must have no more than 1 record per person per year
    denominator_df = denominator_df[id_cols + xtra_cols].drop_duplicates(subset=id_cols)

    if len(denominator_df) != len(denominator_df[id_cols].drop_duplicates()):
        raise RuntimeError("Duplicated data is present in the denominator df")

    return denominator_df


def identify_dupe_claims(icd_mart_df: pd.DataFrame) -> pd.DataFrame:
    """50% or so of Marketscan outpatient claims icd records are duplicated.
    Create a new column and tag these rows.
    """

    icd_mart_df["is_duplicate"] = icd_mart_df.duplicated(keep="first").map({False: 0, True: 1})

    # Inpatient admission data cannot be duplicated. The inpatient admits
    # file has already been prepped to remove duplicate services and we need to know if
    # someone is admitted multiple times, even if it's for the same cause.
    icd_mart_df.loc[icd_mart_df.is_otp == 0, "is_duplicate"] = 0

    return icd_mart_df


def clean_times(start: float, end: float) -> str:
    """Helper to log some run-times for processing and writing data."""
    time_min = round((end - start) / 60, 2)
    return f"{time_min} Minutes"


def filter_df(
    icd_mart_df: pd.DataFrame, filter_dict: Dict[str, int], strict: bool = False
) -> pd.DataFrame:
    """Given a dataframe and a dictionary of column: values to filter on return only rows
    with matching values. If strict is True then every column present in filter_dict must be
    in the ICD-Mart data.
    """
    pre = len(icd_mart_df)
    df_cols = icd_mart_df.columns.tolist()
    missing_cols = set(filter_dict.keys()) - set(df_cols)
    if strict and missing_cols:
        raise KeyError(
            "All columns in the filter dictionary must be present in data. "
            f"Missing: {missing_cols}"
        )

    for col, val in filter_dict.items():
        if col in df_cols:
            icd_mart_df = icd_mart_df[icd_mart_df[col] == val]

    if len(icd_mart_df) == 0:
        raise RuntimeError("All rows of data have been removed from the ICD Mart")
    post = len(icd_mart_df)
    logger.info(f"Rows were adjusted via filter from {pre} input rows to {post} output rows")
    return icd_mart_df


@click.command()
@click.option("--year", required=True, type=click.INT, help="Year of data to reshape")
@click.option("--is_otp", required=True, type=click.INT, help="Process only Outpatient data")
@click.option(
    "--in_std_sample", required=True, type=click.INT, help="Process only eligible data"
)
@click.option(
    "--is_duplicate", required=True, type=click.INT, help="Process only duplicated data"
)
@click.option(
    "--write_denominator",
    required=True,
    type=click.INT,
    help="Write the denominator and sample_size objects to disk",
)
def create_schema(
    year: int, is_otp: int, in_std_sample: int, is_duplicate: int, write_denominator: int
) -> None:
    """Main interface for creating the new Marketscan schema. Inpatient, Outpatient and
    Detail files must be converted to parquet in the FILEPATH dir
    before this can process a piece of data.

    Args:
        year: Year of data to process.
        is_otp: If 0 process only inpatient data, if 1 process only outpatient data
        in_std_sample: If 0 process only out of sample data, if 1 process only
        in sample data, eg data that meets eligibility criteria
        is_duplicate: If 0 process only duplicated claims data, if 1 process only
        non-duplicated data
        write_denominator: If 0 do not write the denominator and sample size objects
        to disk. If 1 write them to disk.
    """
    settings = config.get_settings()

    logger.add(
        sink=(
            "FILEPATH"
        )
    )
    logger.info(
        f"Parquet read dir is {settings.parquet_dir};" f"\n Write dir is {settings.write_dir};"
    )
    start = time.time()

    # helper dict to retain only rows of data which match the values from input args
    filter_dict = {
        "year": year,
        "is_otp": is_otp,
        "in_std_sample": in_std_sample,
        "is_duplicate": is_duplicate,
    }

    # prep denominator data
    denominator_df = get_denominator_data(year=year)
    logger.info("Denominator data has been created")

    # prep claims data
    icd_mart_df = get_claims_data(year=year, filter_dict=filter_dict)
    logger.info("ICD mart data has been created")

    # merge claims and denominator together
    icd_mart_df = icd_mart_df.merge(
        denominator_df,
        how="inner",
        on=["bene_id", "year"],
        suffixes=("_claims", "_denominator"),
        validate="m:1",
    )
    logger.info("ICD and denominator data have been merged and filtered")

    # Attempt to reduce peak memory use by deleting large denominator object
    if write_denominator == 0:
        del denominator_df

    # apply final elig criteria, death status derived from inpatient claims data
    icd_mart_df = death_eligibility(icd_mart_df)
    logger.info("Death eligibility adjustment applied to ICD Mart data")

    # Filter eligible sample data depending on arg value via filter_df/filter_dict
    icd_mart_df = filter_df(icd_mart_df, filter_dict, strict=True)

    icd_mart_df = attach_icd_group(icd_mart_df)
    logger.info("The icd id group values have been attached")

    # facility id not being stored as float for inpatient causing issues in reading a full year
    # of data. cast dtype once more
    icd_mart_df = fh.downcast_col_dtypes(icd_mart_df)

    if len(icd_mart_df) == 0:
        raise RuntimeError("All rows of data have been removed, this is unexpected")

    if set(constants.SORT_COLS).symmetric_difference(icd_mart_df.columns):
        raise ValueError("columns present in data are not included in sort order")

    icd_mart_df = icd_mart_df.sort_values(constants.SORT_COLS)

    prep_complete = time.time()
    logger.info(f"Data prepped in {clean_times(start, prep_complete)}")

    file_path = "FILEPATH"
    fh.check_icd_mart_path(file_path, filter_dict)
    fh.write_schema(
        df=icd_mart_df,
        file_path=file_path,
        part_cols=constants.PART_COLS,
        write_by_icd_group=True,
    )

    df_written = time.time()
    logger.info(f"ICD mart data written in {clean_times(prep_complete, df_written)}")

    if write_denominator:
        # eligibility due to death is derived from inpatient data, not enrollment detail data.
        # Transfer these eligible people back to the denominator data
        denominator_df = transfer_death_eligibility(icd_mart_df, denominator_df)
        logger.info("Death eligibility adjustment applied to denominator data")
        del icd_mart_df

        file_path = "FILEPATH"
        fh.check_path(file_path, year)
        fh.write_schema(
            df=denominator_df,
            file_path=file_path,
            part_cols=["year"],
            write_by_icd_group=False,
        )

        denominator_written = time.time()
        logger.info(
            f"Denominator data written in {clean_times(df_written, denominator_written)}"
        )

        # create aggregated sample sizes for use in pipeline
        samp = ad.MsSampleSize(denom=denominator_df)
        samp.create_sample_size()

        file_path = "FILEPATH"
        fh.check_path(file_path, year)
        fh.write_schema(
            df=samp.sample_size,
            file_path=file_path,
            part_cols=["year"],
            write_by_icd_group=False,
        )

    logger.info("ICD mart, sample sizes and denominator files should be on disk now.")


if __name__ == "__main__":
    create_schema()