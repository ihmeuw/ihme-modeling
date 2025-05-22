"""Contains functions to perform tasks related to mapping clinical data.

Specifically, this module allows a user to:
    Map clinical data from cause codes to ICG to bundle.
    NOTE: the phrase "GBD cause" is being used in a nonstandard way here. These are not
    the same as acause or cause_id. They are different levels of ICD code groupings used
    by the clinical informatics team

    Example of mapping from cause codes to ICG:
        map_to_gbd_cause(df, input_type='cause_code', output_type='icg',
                         write_unmapped=False, truncate_cause_codes=True,
                         extract_pri_dx=False, prod=True)

    Example of mapping from ICG to bundle:
        map_to_gbd_cause(df, input_type='icg', output_type='bundle', write_unmapped=False,
                         truncate_cause_codes=True, extract_pri_dx=False, prod=True)

    Example of adding a duration limit column to the data.
        df = clinical_mapping.apply_durations(df=df, cause_type='bundle', map_version=30)

    Example of removing rows outside of age-sex restrictions.
        df = clinical_mapping.apply_restrictions(df, age_set='indv',
                                                 cause_type='icg', prod=True)
"""

import datetime
import re
import warnings
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from crosscutting_functions.shared_constants.database import MappingTables
from crosscutting_functions.shared_constants.mapping import Measure
from crosscutting_functions import demographic
from crosscutting_functions.formatting-functions import formatting
from loguru import logger

from crosscutting_functions.mapping import (
    clinical_mapping_db,
    clinical_mapping_validations,
)


def clean_cause_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast the "cause_code" column of an input dataframe to str, remove any non-alphanumeric
    characters and convert to upper case.

    This aligns this important mapping column between our map and any raw or processed dataset.

    Args:
        df: Clinical data or a clinical mapping table.

    Returns:
        The input DataFrame with a modified cause code column.

    Raises:
        ValueError if the cause_code column is not present or a couple unique sources are
        mapped to the incorrect code system.
    """
    if "cause_code" not in df.columns:
        raise ValueError("The cause code column is not present.")

    # cast to string
    df["cause_code"] = df["cause_code"].astype(str)
    # remove non alphanumeric characters
    df["cause_code"] = formatting.sanitize_diagnoses(df["cause_code"])
    # convert to upper case
    df["cause_code"] = df["cause_code"].str.upper()

    # adding a little safety check here to make sure IDN and VTN aren't mapped to basic ICD 9
    # or 10 code systems like we were doing before
    if "nid" in df.columns:
        chk = df[(df["nid"].isin([206640, 299375])) & (df["code_system_id"].isin([1, 2]))]
        # the code system can move around, but ICD9/10 will still be 1/2 and these
        # sources can not use either
        if not chk.empty:
            raise ValueError(
                "The ICD 9 and/or 10 code systems appear to be assigned to Vietnam or "
                "Indonesia data. This is not correct."
            )

    return df


def merge_on_cause_code(df: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges fields in map_df (ICGs or condition) onto ICD codes using a mapping the
    Clinical team maintains.

    Args:
        df: Clinical data.
        map_df: A DataFrame of cause_code to another mapping entity.

    Returns:
        The input DataFrame with additional columns attached.

    Raises:
        ValueError if the row count of df changes after merge.
        ValueError if the set of unique cause codes within the df change after the merge.
    """
    # store variables for data check later
    before_values = list(df["cause_code"].unique())
    pre = df.shape[0]

    df = df.merge(map_df, how="left", on=["cause_code", "code_system_id"])

    if pre != df.shape[0]:
        dupes = map_df[
            map_df.duplicated(subset=["cause_code", "code_system_id"], keep=False)
        ].sort_values("cause_code")
        raise ValueError(
            "Number of rows have changed from {} to {}. "
            "Probably due to these duplicated codes {}".format(pre, df.shape[0], dupes)
        )

    after_values = list(df["cause_code"].unique())
    if set(before_values) != set(after_values):
        raise ValueError("Unique ICD codes are not the same.")
    return df


def extract_primary_dx(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the primary ICD 10 diagnosis when multiple diagnoses have been
    combined together in one columnn. The input df should contain only
    alphanumeric values in the cause_code column. Extract the first string of a letter
    followed by numbers and replace those cells in the data. For example, the
    cause code "A1234Z987" would become "A1234" in the return df.

    Args:
        df: Clinical data

    Returns:
        The input DataFrame with a single ICD 10 primary diagnosis code.
    """
    # split the icd codes on letter
    df["cause_code"] = df.cause_code.map(lambda x: re.split("([A-Z])", x))
    # keep primary icd code
    df["cause_code"] = df["cause_code"].str[0:3]
    df["cause_code"] = df["cause_code"].map(lambda x: "".join(map(str, x)))

    return df


def map_to_truncated(
    df: pd.DataFrame, map_df: pd.DataFrame, cause_type: str = "icg"
) -> pd.DataFrame:
    """
    Takes a dataframe of Clinical data with ICD codes which didn't map.
    Truncates these ICD codes from 7 to 2 digits and attempts to merge cause_type data onto
    the data at each truncation level. Returns only the data that has
    successfully mapped to a cause_type.

    Args:
        df: Clinical data with a cause_code column of ICD 9/10 codes.
        map_df: A DataFrame of cause code to cause_type mapping.
        cause_type: The target entity to map to. Valid options are 'icg' and 'condition'

    Returns:
        A subset of the input DataFrame where cause codes were successfully mapped
        to ICG or condition after truncation.
    """
    df_list = []
    # NOTE this overwrites the cause_code column on each loop with one fewer
    # digit until it either map_df or cause_code reduces to exactly 2 digits.
    for i in [7, 6, 5, 4, 3, 2]:
        df["cause_code"] = df["cause_code"].str[0:i]
        good = df.merge(map_df, how="left", on=["cause_code", "code_system_id"])
        # keep only rows that successfully merged
        good = good[good[f"{cause_type}_name"].notnull()]
        # drop the icd codes that matched from df
        df = df[~df.cause_code.isin(good.cause_code)].copy()
        df_list.append(good)

    dat = pd.concat(df_list, sort=False)
    logger.info(
        f"The map to truncated function received a DataFrame of shape {df.shape} "
        "and returned a DataFrame of truncated codes successfully mapped to "
        f"{cause_type}s of shape {dat.shape}."
    )
    return dat


def truncate_and_map(
    df: pd.DataFrame,
    map_df: pd.DataFrame,
    map_version: Union[str, int],
    extract_pri_dx: bool,
    write_unmapped: bool = True,
    cause_type: str = "icg",
) -> pd.DataFrame:
    """
    Performs the extraction and truncation of ICD codes when they fail to map to ICG.
    Writes a list of completely unmapped ICD codes to /test_and_sample_data along
    with the version of the map and source used.

    Args:
        df: a DataFrame of clinical data containing cause_codes which were
            not successfully mapped to ICG.
        map_df: A DataFrame of cause code to icg mapping.
        map_version: Identifies which version of the mapping set to use.
        extract_pri_dx: Should primary diagnoses be extracted from cells
                        containing multiple ICD diagnosis codes.
        write_unmapped: Should cause_codes which cannot be mapped be written to disk.
                        Defaults to true.
        cause_type: The target entity to map to. Valid options are 'icg' and 'condition'

    Returns:
        The input DataFrame with both successfull and unsuccessfull rows
        mapped to ICG after truncation. If the input data has only 3 digit ICD codes or
        is empty it is returned without attempting truncation.

    Raises:
        ValueError if non-ICD 9/10 maps are present.
        ValueError if data row counts by code system don't match after truncation.
        ValueError if case counts don't match after truncation.
    """
    # return the df if it's just 3 codes long
    if df["cause_code"].apply(len).max() <= 3:
        return df
    # or if it's empty
    if df.shape[0] == 0:
        return df
    # skip the special maps
    if df["code_system_id"].max() > 2:
        raise ValueError(
            "Only ICD 9 and 10 codes can be truncated. Special maps should not have "
            "unmapped cause codes."
        )

    pre_cols = df.columns
    pre = df.shape[0]
    if "val" in pre_cols:
        pre_cases = df.val.sum()
    else:
        pre_cases = pre
        df["val"] = 1

    # create a raw cause code column to remove icd codes that were
    # fixed and mapped successfully
    df["raw_cause_code"] = df["cause_code"]

    map_list = []
    for code_sys in df.code_system_id.unique():
        pre_code_sys = df.query(f"code_system_id == {code_sys}").shape
        # get just the codes from 1 code system that didn't map
        remap = df[df.code_system_id == code_sys].copy()

        # create remap_df to retry mapping for icd n
        remap = remap.drop([f"{cause_type}_name", f"{cause_type}_id", "map_version"], axis=1)

        if extract_pri_dx:
            # take the first icd code when they're jammed together
            remap = extract_primary_dx(remap)

        # now do the actual remapping, losing all rows that don't map
        remap = map_to_truncated(df=remap, map_df=map_df, cause_type=cause_type)

        # remove the rows where we were able to re-map to ICG
        goodmap = df[
            (~df.raw_cause_code.isin(remap.raw_cause_code)) & (df.code_system_id == code_sys)
        ].copy()

        # sanity check on the data that will get concatted back together, no
        # rows should be lost
        if pre_code_sys[0] != len(goodmap) + len(remap):
            raise ValueError("Not concatting back correctly. Rows should match perfectly")
        map_list += [goodmap, remap]

    df = pd.concat(map_list, sort=False, ignore_index=True)
    del map_list

    if "raw_cause_code" in df.columns:
        # drop cols used to truncate and split
        df = df.drop(["raw_cause_code"], axis=1)

    # we should not be losing ANY rows or cases
    if pre != df.shape[0]:
        ValueError("Rows have changed from {} to {}.".format(pre, df.shape[0]))
    try:
        case_test = pre_cases.round(3) == df.val.sum().round(3)
    except Exception as e:
        logger.info(f"A rounding method had failed, trying another {e}")
        case_test = round(pre_cases, 3) == round(df.val.sum(), 3)

    if not case_test:
        raise ValueError("Cases have changed from {} to {}.".format(pre_cases, df.val.sum()))

    # assert that we haven't lost  more than 5 cases
    case_diff = pre_cases - df.val.sum()
    if abs(case_diff) > 5:
        warnings.warn(
            "More than 5 cases were lost. To be exact, "
            f"the difference (before - after) is {case_diff}"
        )
    if "val" not in pre_cols:
        df = df.drop("val", axis=1)

    if write_unmapped:
        source = df.source.iloc[0]
        # write the data that didn't match for further inspection
        df[df[f"{cause_type}_name"].isnull()].to_csv(
            (
                "FILEPATH"
            ),
            index=False,
        )
    return df


def grouper(df: pd.DataFrame, cause_type: List[str]) -> pd.DataFrame:
    """
    Set every outcome that's not an explicit "death2" to "case".
    Groupby hardcoded columns and sum the admission counts. We have four outcome
    types leading up to this function depending on when the data was formatted.


    Args:
        df: Clinical data.
        cause_type: A list of columns to use in aggregation. Must be ['icg_name', 'icg_id']
                    or it looks like the groupby will fail.

    Returns:
        The input DataFrame after performing a groupby/sum on a set of hardcoded columns.
    """
    if "val" not in df.columns:
        logger.info("This functions sums only the val column. returning data w/o collapsing")
        return df

    logger.info("Performing a groupby and sum of admission counts.")
    # re form outcome_id column, make everything a 'case' unless it's
    # the type death2, which is a combo of deaths from all sources, including
    # the older sources we were dropping before
    df.loc[df["outcome_id"] != "death2", "outcome_id"] = "case"

    groups = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_group_id",
        "sex_id",
        "source",
        "nid",
        "facility_id",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "outcome_id",
    ] + cause_type

    # retain icg measure
    if cause_type == ["icg_name", "icg_id"]:
        groups = groups

    df = df.groupby(groups).agg({"val": "sum"}).reset_index()

    return df


def dtypecaster(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast the columns in the input df to either numeric or str data types. If a column defined
    in the int or str cols below is not present in the df object it will be skipped.

    Args:
        df: Clinical data.

    Returns:
        The input DataFrame with sets of hardcoded columns cast to certain data types.
    """
    columns_in_data = df.columns.tolist()
    # set the col types
    int_cols = [
        "location_id",
        "year_start",
        "year_end",
        "age_group_unit",
        "age_group_id",
        "sex_id",
        "nid",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "icg_id",
        "bundle_id",
        "val",
    ]  # , 'use_in_maternal_denom']
    int_cols = [integer_col for integer_col in int_cols if integer_col in columns_in_data]

    str_cols = ["source", "facility_id", "outcome_id", "icg_name", "bundle_measure"]
    str_cols = [string_col for string_col in str_cols if string_col in columns_in_data]

    # do the col casting
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="raise")

    for col in str_cols:
        df[col] = df[col].astype(str)

    return df


def expand_bundles(
    df: pd.DataFrame,
    map_version: Union[str, int],
    prod: bool = True,
    drop_null_bundles: bool = True,
    test_merge: bool = False,
) -> pd.DataFrame:
    """
    This function maps ICGs to Bundles.
    When our data is at the ICG level there are no duplicates, one cause_code
    goes to one icg_id. At the bundle level there are multiple bundles
    going to the same ICD code so we need to duplicate rows to process every
    bundle. This function maps bundle ID to ICG and duplicates rows,
    hence the 'expand' name.

    This function does not aggregate the df data to the bundle-level.

    Args:
        df: Clinical data which must have icg_name, icg_id columns.
        map_version: Identifies which version of the mapping set to use.
        drop_null_bundles: If true, will drop the rows of data with ICGs that do not
                           map to a bundle. Defaults to true.
        test_merge: If true, performs an expensive validatin using multiple versions of merged
                    data. Defaults to false.

    Returns:
        The input DataFrame with a bundle_id column attached. If drop null bundles is True
        this will also remove the rows of ICGs which do not map to bundle.

    Raises:
        ValueError if bundle_id is already present in df or icg id and name are missing.
        ValueError if test_merge is True and the data transformed with different
        merge methods do not exactly match.
    """

    clinical_mapping_db.check_map_version(map_version)

    if "bundle_id" in df.columns:
        raise ValueError("'bundle_id' has already been attached.")

    if "icg_name" and "icg_id" not in df.columns:
        raise ValueError("'icg_name' and 'icg_id' must be present in input df.")

    # get the icg to bundle map
    map_df = clinical_mapping_db.get_clinical_process_data(
        table=MappingTables.ICG_BUNDLE, map_version=map_version, prod=prod
    )
    map_df = map_df.drop("map_version", axis=1)


    if test_merge:
        test1 = df.merge(map_df.drop("icg_name", axis=1), how="left", on=["icg_id"])
        test2 = df.merge(map_df.drop("icg_id", axis=1), how="left", on=["icg_name"])

    # this merge duplicates data as we expect. b/c a single ICG goes
    # to multiple bundle IDs
    df = df.merge(map_df, how="left", on=["icg_name", "icg_id"])

    if test_merge:
        if not df.equals(test1):
            raise ValueError(
                "The merge is performing differently between icg_id alone "
                "and icg_id/name combined."
            )
        if not df.equals(test2):
            raise ValueError(
                "The merge is performing differently between icg_name alone "
                "and icg_id/name combined."
            )
        del test1, test2
        logger.info("The merge is identical between icg_name, icg_id and both.")

    if drop_null_bundles:
        # drop rows without bundle id
        df = df[df["bundle_id"].notnull()]

    return df


def log_unmapped_data(df: pd.DataFrame, write_log: bool) -> None:
    """
    Given a dataframe of mapped clinical data AND a 'source' column which should have just 1
    source this writes a file for all unmapped data, eg data which has a null icg_id value.
    If the df does not have a source column then nothing is logged.

    Args:
        df: Clinical data which has been mapped to ICG.
        write_log: If True will write a txt file at
            FILEPATH with information about the number of
            rows that didn't get mapped.
    """
    if "source" not in df.columns:
        logger.info("Source is not present in the data so we cannot store the unmapped stats.")
        return
    else:
        # count how many rows didn't map, ie have a null baby seq:
        no_match_count = float(df["icg_id"].isnull().sum())
        no_match_per = round(no_match_count / df.shape[0] * 100, 4)
        logger.info(
            (r"{} rows did not match an ICG in the map out of {}.").format(
                no_match_count, df.shape[0]
            )
        )
        logger.info(
            "{}% of total rows did not successfully map to an ICG.".format(no_match_per)
        )

        # arbitrary warning if over 15% of data unmapped
        if no_match_per > 15:
            warnings.warn(r"{}% or more of rows didn't match".format(no_match_per))

        if write_log:
            logger.info("Writing unmapped meta log file.")

            text = open(
                "FILEPATH",
                "w",
            )
            text.write(
                """
                       Data Source: {}
                       Number of unmatched rows: {}
                       Total rows of data: {}
                       Percent of unmatched:  {}
                       """.format(
                    df.source.unique(), no_match_count, df.shape[0], no_match_per
                )
            )
            text.close()

        return


def map_to_gbd_cause(
    df: pd.DataFrame,
    input_type: str,
    output_type: str,
    map_version: Union[str, int],
    retain_active_bundles: Optional[bool] = None,
    write_unmapped: bool = False,
    truncate_cause_codes: bool = True,
    extract_pri_dx: bool = True,
    prod: bool = True,
    write_log: bool = False,
    groupby_output: bool = False,
) -> pd.DataFrame:
    """
    This is the main interface for mapping clinical data with a cause_code and a code
    system id to the clinical team's ICG and bundle values.

    Args:
        df: Dataframe of clinical data, usually inpatient, outpatient or claims.
        input_type: Which level will you be mapping from? eg 'cause_code' or 'icg'.
        output_type: Which level will you be mapping to? eg 'icg', 'bundle', 'condition'.
        retain_active_bundles: Optional, applies only when the output type id 'bundle'.
                               Reduce the bundles present in the data to contain only
                               those present in the db table DATABASE.
        map_version: Identifies which version of the mapping set to use.
        write_unmapped: If true, write a file with unmapped data.
        truncate_cause_codes: Some sources use more detailed cause codes,
                              but the ICD system is a hierarchy so you can still
                              move up the hierarchy to find something to map to. If True
                              this will truncate these codes until they map to ICG.
        extract_pri_dx: Some sources have multiple ICD 10 diagnoses in the same
                        column (see PHL). This will extract the first alphanumeric code.
        prod: If true, any failed tests will break the code.
        write_log: Store stats for unmapped data. 
        groupby_output: If true, the data is grouped by the output type and collapsed,
                        summing the val column together and reducing the size of the df.
                        Please note that this argument is very old and relies on specific
                        properties in the data.

    Returns:
        The input DataFrame with additional columns to reflect the mapping to either ICG
        or bundle.

    Raises:
        ValueError if required columns are not present in df.
        ValueError if row counts change after merging.
        ValueError if df is empty after merging or case counts change.
    """

    clinical_mapping_db.check_map_version(map_version)

    # Validate the input and output type arguments.
    if input_type not in ["cause_code", "icg"]:
        raise ValueError("{} is not an acceptable input type.".format(input_type))
    if output_type not in ["icg", "bundle", "condition"]:
        raise ValueError("{} is not an acceptable output type.".format(output_type))
    if output_type == "condition" and input_type != "cause_code":
        raise ValueError("Custom mapping can only occur between cause codes and condition")

    # this doesn't apply when mapping to bundle level from icg
    if "code_system_id" in df.columns:
        clinical_mapping_validations.confirm_code_system_id(
            df["code_system_id"].unique().tolist(), map_version
        )

    # val basically signifies whether the data is otp/int and aggregatable or claims and not
    if "val" in df.columns:
        pre_cases = df.val.sum()
    else:
        pre_cases = df.shape[0]

    pre_rows = df.shape[0]

    if input_type == "cause_code":
        df = clean_cause_code(df)

    # map the data from cause codes to intermediate cause groups
    if (
        input_type == "cause_code"
        and output_type == "icg"
        or input_type == "cause_code"
        and output_type == "bundle"
    ):
        # prep the data and the map
        map_df = clean_cause_code(
            clinical_mapping_db.get_clinical_process_data(
                MappingTables.CAUSE_CODE_ICG, prod=prod, map_version=map_version
            )
        )

        map_vers_int = int(map_df["map_version"].unique())

        df = merge_on_cause_code(df, map_df)

        if truncate_cause_codes:
            # we want only rows where nfc is null to try to truncate and map
            no_map = df[df["icg_name"].isnull()].copy()
            # keep only rows in df where nfc is not null
            df = df[df["icg_name"].notnull()].copy()
            no_map = truncate_and_map(
                df=no_map,
                map_df=map_df,
                map_version=map_vers_int,
                extract_pri_dx=extract_pri_dx,
                write_unmapped=write_unmapped,
            )

            # bring them back together
            df = pd.concat([df, no_map], ignore_index=True, sort=False)

        log_unmapped_data(df, write_log=write_log)

        # map missing ICG to _none
        df.loc[df["icg_id"].isnull(), ["icg_name", "icg_id", "map_version"]] = [
            "_none",
            1,
            map_vers_int,
        ]

        if pre_rows != df.shape[0]:
            raise ValueError(
                "Row counts have changed after cleaning and merging. This is unexpected."
            )

    # map the data from intermediate cause groups to bundles
    if output_type == "bundle":
        # expand bundles
        df = expand_bundles(df, prod=prod, drop_null_bundles=True, map_version=map_version)
        pre_meas = df.shape[0]
        bun_meas = clinical_mapping_db.get_bundle_measure(map_version=map_version)
        df = df.merge(bun_meas, how="left", on=["bundle_id"])
        if pre_meas != df.shape[0]:
            raise ValueError("Row counts have changed.")

        # drop icg columns because data is now at the bundle level
        drops = [d for d in df.columns if "icg_" in d]
        df = df.drop(drops, axis=1)
        if retain_active_bundles:
            pre_active = len(df)
            # get a list of bundles present on active_bundle_metadata
            actives = clinical_mapping_db.get_active_bundles(map_version=map_version)
            actives = actives.bundle_id.unique().tolist()
            # retain only the bundles that are present on active_bundle_metadata
            df = df[df.bundle_id.isin(actives)].copy()
            if df.empty:
                raise ValueError(
                    f"No rows present in df. There were {pre_active} rows before filtering."
                )

    if output_type == "condition":
        if groupby_output:
            raise KeyError(
                "This function does not support a list of groupby columns. "
                "Please manually run a groupby after mapping."
            )
        map_df = clean_cause_code(
            clinical_mapping_db.get_clinical_process_data(
                MappingTables.CAUSE_CODE_CONDITION, prod=prod, map_version=map_version
            )
        )

        map_vers_int = int(map_df["map_version"].unique())

        df = merge_on_cause_code(df, map_df)

        if truncate_cause_codes:
            # we want only rows where mapping entity is null to try to truncate and map
            no_map = df[df["condition_id"].isnull()].copy()
            # keep only rows in df where mapping entity is not null
            df = df[df["condition_id"].notnull()].copy()
            no_map = truncate_and_map(
                df=no_map,
                map_df=map_df,
                map_version=map_vers_int,
                extract_pri_dx=extract_pri_dx,
                write_unmapped=write_unmapped,
                cause_type="condition",
            )

            # bring them back together
            df = pd.concat([df, no_map], ignore_index=True, sort=False)
        # NOTE: It will be on the user to handle cause codes which don't map to condition
        # entities.
        if pre_rows != df.shape[0]:
            raise ValueError(
                "Row counts have changed after cleaning and merging. This is unexpected."
            )
        if "val" in df.columns:
            if pre_cases != df["val"].sum():
                raise ValueError("Cases have been lost in the mapping process.")

    if groupby_output:
        df = _set_cols_and_groupby(df=df, output_type=output_type)

    if "val" in df.columns:
        # cast dtypes
        df = dtypecaster(df)

    _check_icg_cases(df=df, output_type=output_type, pre_cases=pre_cases)

    return df


def _set_cols_and_groupby(df: pd.DataFrame, output_type: str) -> pd.DataFrame:
    # set appropriate outcome ID columns
    # groupby and collapse
    if output_type == "icg":
        cause_type = ["icg_name", "icg_id"]
    elif output_type == "bundle":
        cause_type = ["bundle_id"]
    else:
        raise RuntimeError(f"Unable to group data for {output_type}")
    df = grouper(df, cause_type)
    return df


def _check_icg_cases(df: pd.DataFrame, output_type: str, pre_cases) -> None:
    """Run a few validations on ICG data."""
    if output_type == "icg":
        if "val" in df.columns:

            if set(df.nid.unique()) == set([205018, 264064]):
                if abs(pre_cases - df.val.sum()) >= 550:
                    raise ValueError("More than 550 cases have been lost in USA NAMCS data.")
            else:
                if round(pre_cases, 3) != round(df.val.sum(), 3):
                    raise ValueError(
                        "Some cases have been lost. Review the map_to_gbd_cause function."
                    )


def apply_restrictions(
    df: pd.DataFrame,
    age_set: str,
    cause_type: str,
    clinical_age_group_set_id: int,
    map_version: Union[str, int],
    prod: bool = True,
    break_if_not_contig: bool = True,
) -> pd.DataFrame:
    """
    Apply age and sex restrictions by ICG or bundle to a dataframe of clinical data. This
    function will remove rows of data that are outside of the restrictions.

    Args:
        df: Clinical data with either an ICG or bundle column.
        age_set: How is the year column in the data stored? Is the data in individual
                 year ages, binned age groups with start/end or age_group_ids?
                 Acceptable values are "indv", "binned", "age_group_id".
        cause_type: Determines which set of restrictions should be applied.
                    Acceptable values are "icg", "bundle", or "condition"
        clinical_age_group_set_id: The set of age group IDs present in the input df.
        map_version: Identifies which version of the mapping set to use.
        prod: If true, any failed tests will break the code.
        break_if_not_contig: Gets passed on to confirm_contiguous_data. If True, a ValueError
                             will be raised if there are any gaps in the age pattern.

    Returns:
        The input DataFrame with any rows outside of age and sex restrictions removed.

    Raises:
        ValueError if an invalid age_set or cause_type is passed.
        ValueError if row counts change after merging on restrictions.
        ValueError if columns present in input and output DataFrames are not identical.
    """
    if clinical_age_group_set_id == 1:
        logger.info(
            (
                "Using the GBD 2019 and earlier age set, This WILL NOT work "
                "with GBD2020 ICG restrictions for cancer."
            )
        )

    sex_diff = set(df.sex_id.unique()).symmetric_difference([1, 2])
    if sex_diff:
        warnings.warn(
            "There are sex_id values that won't have restrictions applied to them. "
            f"These are {sex_diff}."
        )

    if age_set not in ["indv", "binned", "age_group_id"]:
        raise ValueError("{} is not an acceptable age set.".format(age_set))

    clinical_mapping_db.check_map_version(map_version)

    start_cols = df.columns

    # prep the age column, we want binned start/end groups
    if age_set == "age_group_id":
        # switch from age group id to age start end
        df = demographic.all_group_id_start_end_switcher(df, clinical_age_group_set_id)
    elif age_set == "indv":
        df = demographic.age_binning(
            df,
            drop_age=False,
            terminal_age_in_data=False,
            break_if_not_contig=break_if_not_contig,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

    # set a column of interest column
    df["to_keep"] = 1

    # get the df of age/sex restrictions
    if cause_type == "icg":
        restrict = clinical_mapping_db.get_clinical_process_data(
            table=MappingTables.ICG_PROPERTIES, map_version=map_version, prod=prod
        )
    elif cause_type == "bundle":
        restrict = clinical_mapping_db.create_bundle_restrictions(map_version=map_version)
    elif cause_type == "condition":
        restrict = clinical_mapping_db.get_clinical_process_data(
            table=MappingTables.CONDITION_PROPERTIES, map_version=map_version, prod=prod
        )
    else:
        raise ValueError(f"The cause_type {cause_type} is not supported.")

    keep_cols = [cause_type + "_id", "male", "female", "yld_age_start", "yld_age_end"]

    # merge on restrictions
    pre = df.shape[0]
    df = df.merge(restrict[keep_cols], how="left", on=cause_type + "_id")
    if pre != df.shape[0]:
        raise ValueError("Row count of df has changed after merging on restrictions.")

    # set to_keep to zero where male in cause = 0
    df.loc[(df["male"] == 0) & (df["sex_id"] == 1), "to_keep"] = np.nan

    # set to_keep to zero where female in cause = 0
    df.loc[(df["female"] == 0) & (df["sex_id"] == 2), "to_keep"] = np.nan

    # set to_keep to zero where age end is smaller than yld age start
    df.loc[df["age_end"] <= df["yld_age_start"], "to_keep"] = np.nan

    # set to_keep to zero where age start is larger than yld age end
    df.loc[df["age_start"] > df["yld_age_end"], "to_keep"] = np.nan

    # drop the restricted values
    df = df[df["to_keep"].notnull()]

    df = df.drop(["male", "female", "yld_age_start", "yld_age_end", "to_keep"], axis=1)

    if age_set == "age_group_id":
        # switch from start-end to age group id
        df = demographic.all_group_id_start_end_switcher(df, clinical_age_group_set_id)
    elif age_set == "indv":
        df = df.drop(["age_start", "age_end"], axis=1)

    diff_cols = set(start_cols).symmetric_difference(set(df.columns))
    if diff_cols:
        raise ValueError("The diff columns are {}.".format(diff_cols))

    return df


def apply_durations(
    df: pd.DataFrame, cause_type: str, map_version: Union[str, int]
) -> pd.DataFrame:
    """
    Takes a df with an admission date and appends on the duration limit. This is for
    use with the data that has patient identifiers, like claims, HCUP SIDS, etc.
    This "applies" the durations in the sense that it adds a new column of duration limits
    to the data. It does not deduplicate data in any way.

    Args:
        df: Clinical data with an admission date column.
        cause_type: Determines which set of restrictions should be applied.
                    Acceptable values are "bundle" or "condition".
        map_version: Identifies which version of the mapping set to use.

    Returns:
        The input DataFrame with an additional admission limit column.

    Raises:
        ValueError if cause_type is not "bundle" or the "bundle_id" columns is not
        present in the data.
    """

    clinical_mapping_db.check_map_version(map_version)

    if "{}_id".format(cause_type) not in df.columns:
        raise ValueError(f"{cause_type}_id isn't present in the data and the merge will fail.")

    if cause_type == "bundle":
        duration_df = clinical_mapping_db.create_bundle_durations(map_version=map_version)
    elif cause_type == "condition":
        duration_df = clinical_mapping_db.get_clinical_process_data(
            table=MappingTables.CONDITION_PROPERTIES, map_version=map_version
        )
        duration_df = duration_df[[f"{cause_type}_id", "duration", "map_version"]].rename(
            columns={"duration": f"{cause_type}_duration"}
        )
    else:
        raise ValueError("{} is not a supported cause_type.".format(cause_type))
    # merge on durations
    df = df.merge(
        duration_df[[f"{cause_type}_id", f"{cause_type}_duration"]],
        how="left",
        on=f"{cause_type}_id",
    )

    # convert a col of ints to a days type time object
    temp_df = (
        df[f"{cause_type}_duration"].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit="D"))
    )

    # make sure adm date is a date time object
    df["adm_date"] = pd.to_datetime(df["adm_date"])
    # then add durations to adm_date to get the limit date
    df["adm_limit"] = df["adm_date"].add(temp_df)

    return df


def attach_measure_to_df(
    df: pd.DataFrame, map_version: Union[str, int], cause_type: str = "bundle"
) -> pd.DataFrame:
    """Attach the 'measure_id' column to a dataframe of clinical bundle or condition data
    using a specific map version.

    Args:
        df: Clinical data with a {cause_type}_id column.
        map_version: Identifies which version of the mapping set to use.
        cause_type: The mapping type to pull measure from. Valid options are
                    "bundle" and "condition"

    Returns:
        The input DataFrame with a measure_id column attached. If measure_id is already
        present the data is immediately returned.

    Raises:
        KeyError if a {cause_type}_id column is not present.
        ValueError if any measure_ids or the counts of string {cause_type}_id measures do not
                   match after merging on measure_id.
        ValueError if {cause_type}_ids in the data do not have a measure stored in the map.
        ValueError if the data shape changed beyond attaching a new column.
    """
    pre = df.shape
    if "measure_id" in df.columns:
        return df
    if f"{cause_type}_id" not in df.columns:
        raise KeyError(f"The input data must have a {cause_type}_id column.")

    if cause_type == "bundle":
        measure_df = clinical_mapping_db.get_bundle_measure(map_version).rename(
            columns={"bundle_measure": "measure"}
        )
    elif cause_type == "condition":
        measure_df = clinical_mapping_db.get_clinical_process_data(
            table=MappingTables.CONDITION_PROPERTIES, map_version=map_version
        )[["condition_id", "measure"]]

    measure_df["measure_id"] = measure_df["measure"].replace(
        [Measure.PREVALENCE_STR, Measure.INCIDENCE_STR], [5, 6]
    )

    if measure_df["measure_id"].isnull().any():
        raise ValueError(f"No null measure_ids are expected {measure_df}.")
    if (
        measure_df["measure"].value_counts().tolist()
        != measure_df["measure_id"].value_counts().tolist()
    ):
        raise ValueError(
            "The counts of measure before and after converting from str don't match."
        )

    entity_diff = set(df[f"{cause_type}_id"]) - set(measure_df[f"{cause_type}_id"])
    if entity_diff:
        raise ValueError(
            f"There are {cause_type}_ids present in data that are not present in the map"
            f" and will therefore not have any measures to attach. {entity_diff}"
        )

    # drop the str column and merge measure ID onto df
    measure_df = measure_df.drop("measure", axis=1)
    df = df.merge(measure_df, how="left", on=[f"{cause_type}_id"], validate="m:1")

    if pre != df.drop("measure_id", axis=1).shape:
        raise ValueError("Unexpected changes to either rows or columns has been made.")
    return df
