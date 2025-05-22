import time

import click
import pandas as pd
from crosscutting_functions.pipeline_constants import poland as constants
from crosscutting_functions.mapping.mapping import CODE_SYSTEMS
from crosscutting_functions.clinical_metadata_utils.values import FACILITY_TYPE_ID
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.clinical_map_tools.icd_groups.use_icd_group_ids import UseIcdGroups
from loguru import logger

from pol_nhf.constants import CURRENT_RELEASE_ID
from pol_nhf.schema import admission_correction, file_handlers
from pol_nhf.utils.settings import SchemaRunSettings


def apply_ICD_group(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the 3 digit ICD group id for the ICD mart.

    Args:
        df (pd.DataFrame): Reshaped long table with ICD information.

    Returns:
        pd.DataFrame: Input table with icd_group_id attached.
    """
    null_counts = df.cause_code.isnull().sum()

    logger.info(f"\nThere are {null_counts} rows with empty cause codes")
    df = df[df.cause_code.notnull()]

    icd_g = UseIcdGroups()
    df = icd_g.attach_icd_group(df, remove_helper_cols=False)

    # validate dataset codes, bad codes would not get mapped to a group
    if not all(df.icd_group_id > 0):
        logger.info(f"\nCodes with ICD group id 0: \n{df[df.icd_group_id == 0]}\n")

    return df.drop("icd_group_code", axis=1)


def _mark_dups(df: pd.DataFrame) -> pd.DataFrame:
    """Create the 'is_dup' column.
    Identify dup admissions based on dx_n, bene_id, and admission_date.
    Intentionally ignores visit number and facility.

    Args:
        df (pd.DataFrame): Table with dx info stored wide.

    Returns:
        pd.DataFrame: Input table with duplicates marked.
    """
    dx_cols = [e for e in df.columns.tolist() if e.startswith("dx_")]
    cols = dx_cols + ["bene_id", "admission_date"]
    df["is_dup"] = df.duplicated(subset=cols).astype(int)
    logger.info(
        f"\nThere are: {df[df.is_dup==1].shape[0]} duplicate rows subsetted by "
        "bene_id, admission_date and ICD10"
    )
    return df


def reshape_dx_codes(df: pd.DataFrame, settings: SchemaRunSettings) -> pd.DataFrame:
    """Repshapes table, marks dups, and can correct admissions.

    Args:
        df (pd.DataFrame): Table with dx info stored wide.
        settings (SchemaRunSettings): Poland utils.settings object instance.

    Raises:
        RuntimeError: More than 0.01% of the data has null primary dx.
        RuntimeError: No columns found with the expected naming convention.
        RuntimeError: 1 row to each ICD code rule is broken.

    Returns:
        pd.DataFrame: Input table reshaped to store diagnosis information long.
    """

    df = _mark_dups(df=df)

    dx_cols = [e for e in df.columns if e.startswith("dx")]

    pre_shape = df.shape[0]
    df = df.loc[df[sorted(dx_cols)[0]].notnull()].reset_index(drop=True)
    diff = pre_shape - df.shape[0]

    if diff > (pre_shape * 0.0001):
        raise RuntimeError("More than 0.01% of the table has null primary dx.")

    logger.info(f"\nRemoved: {diff} claims with null primary dx.")

    total_code_count = df[dx_cols].notnull().sum().sum()

    if len(dx_cols) < 1:
        raise RuntimeError("No dx_* columns found in the table.")

    df = formatting.stack_merger(df=df, reduce_2_dx=False)
    df["cause_code"] = formatting.sanitize_diagnoses(col=df["cause_code"])

    if total_code_count != df.shape[0]:
        raise RuntimeError("Cause codes were lost in wide to long transformation")

    if settings.group_admissions:
        df = admission_correction.main(df)

    return df


def pull_and_clean_data(settings: SchemaRunSettings) -> pd.DataFrame:
    """Read raw parquet data from disk, assign static values, map ID values,
    and set constant column names. Casts, ID cols to integer and relies on to_int()
    col inference instead of passing an explicit list. Drops extra duplicated
    columns made during parquet conversion. Handles inpatient day-cases by including
    them in outpatient processing only.

    Args:
        settings (SchemaRunSettings): Poland utils.settings object instance.

    Returns:
        pd.DataFrame: Stage 1 table. Raw data with minor changes and
            diagnosis information stored wide.
    """
    start = time.perf_counter()
    df = pd.concat(file_handlers.read_partitioned_data(settings=settings), ignore_index=True)

    logger.info(f"\nRead data took: {round((time.perf_counter() - start)/ 60, 2)} minutes")

    # Inpatient day-cases to outpatient.
    df = day_case_filter(df, settings=settings)

    # Standardize column names
    df = df.rename(constants.COL_RENAME, axis=1)
    df["sex_id"] = df["sex_id"].replace(constants.SEX_ID)

    # Add static columns.
    df["code_system_id"] = CODE_SYSTEMS["ICD10"]
    df["is_otp"] = settings.is_otp
    df["file_source_id"] = settings.file_source_id

    # Replace GBD location names with GBD location IDs
    df["location_id"] = df["location_id"].replace(
        demographic.get_child_locations(
            location_parent_id=constants.POL_GBD_LOC_ID, release_id=CURRENT_RELEASE_ID
        )
    )
    df["location_id"] = df["location_id"].fillna(constants.POL_GBD_LOC_ID)

    # Cast ID cols to integers.
    df = general_purpose.to_int(df=df)

    return df


def mark_primary_dx_only_admissions(df: pd.DataFrame) -> pd.DataFrame:
    """Create primary_dx_only col
    Signals if a given bene / claim pair only has a primary dx.

    Args:
        df (pd.DataFrame): Table with dx info stored long.

    Returns:
        pd.DataFrame: Input table with new column marking multi-dx claims.
    """

    gb_cols = ["bene_id", "admission_date", "facility_id"]
    temp = df.groupby(gb_cols)["diagnosis_id"].nunique().reset_index(name="primary_dx_only")

    temp.loc[temp.primary_dx_only > 1, "primary_dx_only"] = 0
    return df.merge(temp, on=gb_cols, validate="m:1")


def day_case_filter(df: pd.DataFrame, settings: SchemaRunSettings) -> pd.DataFrame:
    """Filters inpatient day-case data based on the facility being processed.
    Inpatient processing will remove day cases, and outpatient processing will include
    inpatient day cases into outpatient.

    Args:
        df (pd.DataFrame): DataFrame with start_date and end_date columns.
        settings (SchemaRunSettings): Poland utils.settings object instance.

    Raises:
        KeyError: Invalid facility_type provided in settings.

    Returns:
        pd.DataFrame: Input DatFrame filtered by day-cases.
    """

    pre_filter = df.shape[0]

    # Intpatient gets only non-day cases.
    if settings.facility_type.lower() == "inpatient":
        # Removes all day cases during inpatient processing.
        df = df.loc[df["start_date"] != df["end_date"]].reset_index(drop=True)
        logger.info(
            f"\nFilter removed: {pre_filter - df.shape[0]}/{pre_filter} daycase "
            "claims from Inpatient."
        )

    # Outpatient gets only day cases from Intpatient claims.
    elif settings.facility_type.lower() == "outpatient":
        # Filters out any rows matching mask statement.
        # Removes any inpatient claim that does not have matching start and end date.
        mask = (df["place_of_visit"] == "INPATIENT") & (df["start_date"] != df["end_date"])
        df = df.loc[~mask].reset_index(drop=True)
        logger.info(
            f"\nFilter removed: {pre_filter - df.shape[0]} overnight "
            "Inpatient claims from Outpatient."
        )
    else:
        raise KeyError(f"facility_type {settings.facility_type} is invalid")

    return df


@click.command()
@click.option("--year_id", required=True, type=click.INT, help="Year of data to reshape")
@click.option(
    "--facility_type",
    required=True,
    type=click.STRING,
    help="Facility type matching raw partition.",
)
@click.option("--file_source_id", required=True, type=click.INT, help="ID for a raw file set.")
@click.option(
    "--uuid",
    required=True,
    type=click.STRING,
    help="User created UUID from Jobmon submission script",
)
@click.option(
    "--group_admissions",
    required=True,
    type=click.BOOL,
    help="Group same day inpatient admissions",
)
def create_schema(**kwargs):
    global_start = time.perf_counter()
    kwargs.update({"is_otp": FACILITY_TYPE_ID[kwargs["facility_type"].lower()]})
    kwargs.update({"retries": 1})
    settings = SchemaRunSettings(**kwargs)
    logger.remove()  # debug to remove all handles
    logger.add(sink=file_handlers.create_schema_log_file(settings))

    sf = file_handlers.SourceFiles()
    logger.info(
        f"\nStarting ICD Mart Conversion: \n\t{settings}"
        f"\n\tInputFile:\t{sf[settings.file_source_id]}"
    )

    df = pull_and_clean_data(settings=settings)

    logger.info(f"\nPre formatting shape: {df.shape}")

    start = time.perf_counter()
    df = reshape_dx_codes(df=df, settings=settings)
    logger.info(
        f"\nReshape completed in {round((time.perf_counter() - start) / 60, 2)} minutes"
    )
    logger.info(f"\nPost reshape process shape: {df.shape}")

    df = mark_primary_dx_only_admissions(df=df)
    df = apply_ICD_group(df=df)

    logger.info("\nWritting data")
    logger.info(
        f"\nTotal run time: {round((time.perf_counter() - global_start) / 60, 2)} minutes"
    )
    file_handlers.write_icd_mart(df=df)


if __name__ == "__main__":
    create_schema()
