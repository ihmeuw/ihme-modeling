import warnings
from typing import List, Union

import pandas as pd
from clinical_constants.shared.database import MappingTables # TODO
from loguru import logger

from crosscutting_functions.mapping import clinical_mapping_db


def clinfo_process_validation(
    df: pd.DataFrame, table: str, prod: bool = True
) -> Union[List[str], str]:
    """
    This is the organizing function for validations. The validations we need to run
    depend on which table we're pulling data from.

    Args:
        df: Data from a given table in the clinical mapping database.
        table: The name of the table the data corresponds to in order to run
               the correct validating functions.
        prod: If true, any failed validations will break the code.

    Returns:
        A string or a list of strings containing additional information on the validation.

    Raises:
        ValueError if more than a single unique map version is present in input DataFrame.
    """
    if "map_version" in df.columns:
        if df.map_version.unique().size != 1:
            raise ValueError(f"There are too many map versions present in {table}")
        map_vers = int(df.map_version.unique())

    if table == MappingTables.CAUSE_CODE_ICG:
        result = validate_cc_icg_map(df, prod=prod)
    elif table == MappingTables.ICG_PROPERTIES:
        result = validate_icg_vals(df, map_version=map_vers, prod=prod)
    elif table == MappingTables.ICG_BUNDLE:
        result = validate_icg_bundle(df, prod=prod)
    elif table == MappingTables.CODE_SYSTEM:
        result = validate_code_sys(df, prod=prod)
    else:
        result = f"There is no validation for table {table}"

    return result


def validate_cc_icg_map(df: pd.DataFrame, prod: bool = True) -> Union[List[str], str]:
    """
    Runs a set of validations on the map in the cause_code_icg map. This is the workhorse map
    from a lot of different cause codes to our team's intermediate cause groups (ICGs).

    Args:
        df: A map of cause codes (ICD, etc) to intermediate cause groups.
        prod: Is this function running in a production-like environment? If true the
              script will break if any tests fail.

    Returns:
        A string or a list of strings containing additional information on the validation.

    Raises:
        ValueError if any validations of the cause_code to icg map fail and prod is true.
        These validations include:
            No duplicated rows in the mapping between cause code and ICG.
            No duplicated cause codes within a single code system.
            No more than a single map version present per ICG.
            No null values are present in the map.
            At least 63,000 cause codes are in the map.


    """
    failed_validations = []

    # many:1 relationship between ICD code and ICG. Cause codes can not be duplicated.
    cc_icg_groups = ["cause_code", "code_system_id", "icg_id", "icg_name"]
    dupes = df[df.duplicated(subset=cc_icg_groups, keep=False)].sort_values(cc_icg_groups)
    if not dupes.shape[0] == 0:
        msg = (
            "There are duplicated values of cause code and intermediate cause group, "
            "breaking the many to 1 relationship"
            "Please review this df  \n {}".format(dupes)
        )
        warnings.warn(msg)
        failed_validations.append(msg)

    # no duplicated icd codes within a code system
    cc_groups = ["code_system_id", "cause_code"]
    ccdupes = df[df.duplicated(subset=cc_groups, keep=False)].sort_values(cc_groups)
    if len(ccdupes) > 0:
        msg = f"There are duplicated ICD to ICG mappings {ccdupes}"
        failed_validations.append(msg)

    # the map should have only a single version present
    unique_df = df.groupby("icg_id").nunique()
    if not (unique_df["map_version"] == 1).all():
        msg = "Multiple map versions per ICG are present. " "These are {}".format(
            df.map_version.unique()
        )
        warnings.warn(msg)
        failed_validations.append(msg)

    # no nulls are present in any record
    if df.isnull().sum().sum():
        msg = (
            "Null values are present in the cause code to intermediate cause "
            "group table {}".format(df.isnull().sum())
        )
        warnings.warn(msg)
        failed_validations.append(msg)

    # the map should contain over 63,000 cause codes
    if df.shape[0] < 63_000:
        msg = "There are only {} rows, we expect atleast 63,000 in a complete map".format(
            df.shape[0]
        )
        warnings.warn(msg)
        failed_validations.append(msg)

    if prod:
        if failed_validations:
            raise ValueError(
                "{} tests have failed. Review the warning messages or {}".format(
                    len(failed_validations), failed_validations
                )
            )

    if failed_validations:
        result = failed_validations
    else:
        result = "tests passed"  # type: ignore

    return result


def validate_icg_vals(
    dat: pd.DataFrame, map_version: Union[str, int], prod: bool = True
) -> Union[List[str], str]:
    """
    Check the column values in the duration and restriction tables against the
    main cause code to intermediate cause group map. The second half of the tests will
    return a list of {column_name and  test_name} if any of the tests fail.

    Args:
        dat: DataFrame with either restrictions or durations to compare to df.
        map_version: Identifies which version of the mapping set to use.
        prod: If true, any failed tests will break the code.

    Returns:
        A string or a list of strings containing additional information on the validation.

    Raises:
        ValueError if icg_ids are present in data that are not in the map.
        ValueError if any validations of the restrictions against the ICG map fail and prod
        is True.
            These validations include:
                Age/sex restriction columns can not be below zero.
                Male and female columns can only contain the values 0 or 1.
                Age end should be smaller than 95.
    """

    clinical_mapping_db.check_map_version(map_version)

    # get the cc to icg map
    df = clinical_mapping_db.get_clinical_process_data(
        MappingTables.CAUSE_CODE_ICG, map_version=map_version, prod=prod
    )

    # check diffs between icg ids
    diffs = set(df.icg_id.unique()) - set(dat.icg_id.unique())
    if diffs:
        raise ValueError(
            "There are ICG IDs present which are not in the clean map. "
            "Please review this discrepancy in intermediate cause groups {}".format(diffs)
        )

    # get a list of value columns to run tests on
    val_cols = ["male", "female", "yld_age_start", "yld_age_end"]
    val_cols = [v for v in val_cols if v in dat.columns]

    # check for placeholder NA values
    failed_validations = []
    for col in val_cols:
        if dat[col].min() < 0:
            warnings.warn(
                "{} has values below zero. This is probably a placeholder for "
                "missing values. The data is not ready for production use".format(col)
            )
            failed_validations.append(col + "_placeholderNA")

        # male, female cols should only be 0 or 1.
        if col in ["male", "female"]:
            val_diff = set(dat[col].unique()).symmetric_difference(set([0, 1]))
            if val_diff:
                warnings.warn(
                    "{} has unexpected values, please review this set difference: {}".format(
                        col, val_diff
                    )
                )
                failed_validations.append(col + "_unexp_vals")

        # yld age end should be 95 at most
        if col in ["yld_age_end"]:
            if dat[col].max() > 95:
                max_df = dat[dat[col] > 95]
                warnings.warn(
                    "{} has values over 95 years. This may be acceptable but we haven't "
                    "seen it before. \n{}".format(col, max_df)
                )
                failed_validations.append(col + "_too_high")

    if prod:
        if failed_validations:
            raise ValueError(
                f"{len(failed_validations)} tests have failed. These are: "
                f"{failed_validations}. Review the warnings and "
                "source code for more information"
            )

    if failed_validations:
        result = failed_validations
    else:
        result = "Validations have passed"  # type: ignore

    return result


def validate_icg_bundle(df: pd.DataFrame, prod: bool = True) -> Union[List[str], str]:
    """
    Validate the intermediate cause group to bundle mapping table.

    Args:
        df: A map from ICGs to bundle IDs.
        prod: If true, any failed validations will break the code.

    Returns:
        A string or a list of strings containing additional information on the validation.

    Raises:
        ValueError if prod is True and there are duplicate bundles per ICG.
        ValueError if prod is True and there are duplicate ICGs per bundle.
    """

    failed_validations = []
    for icg in df.icg_id.unique():
        tmp = df[df.icg_id == icg]
        if tmp.shape[0] != tmp.bundle_id.unique().size:
            logger.info(
                f"ICG {icg} contains duplicate bundle mappings. "
                f"Please review the mapping:\n{tmp}"
            )
            failed_validations.append(icg)

    for bundle_id in df.bundle_id.unique():
        tmp = df[df.bundle_id == bundle_id]
        if tmp.shape[0] != tmp.icg_id.unique().size:
            logger.info(
                f"Bundle {bundle_id} contains duplicate ICG mappings. "
                f"Please review the mapping:\n{tmp}"
            )
            failed_validations.append(bundle_id)

    if prod:
        if failed_validations:
            raise ValueError(
                f"{len(failed_validations)} validations have failed. These are: "
                f" {failed_validations}. Review the warnings and source code for more "
                "information"
            )

    if failed_validations:
        result = failed_validations
    else:
        result = "ICG to bundle_id relationship validations have passed"  # type: ignore

    return result


def validate_code_sys(df: pd.DataFrame, prod: bool = True) -> str:
    """
    Check the code system table in the clinical mapping dB to ensure there aren't
    duplicated systems.

    Args:
        df: A DataFrame containing the code system lookup.
        prod: If true, any failed validations will break the code.

    Returns:
        A string containing additional information on the validation.

    Raises:
        ValueError if prod is True and there are duplicated code systems in the lookup table.
    """
    no_dupes = df[["code_system_id", "code_system_name"]].drop_duplicates()

    if prod:
        if df.shape[0] != no_dupes.shape[0]:
            raise ValueError("There are duplicated code systems")

    return "Code system validation has passed"


def confirm_code_system_id(
    code_system_ids: Union[List[int], int], map_version: Union[str, int]
) -> str:
    """
    Validate a code_system_id in the data actually exists in the database.

    Args:
        code_system_ids: One or more code systems to confirm are present in the map.
        map_version: Identifies which version of the mapping set to use.

    Returns:
        A string containing additional information on the validation.

    Raises:
        ValueError if the input code_system_ids are not present in the clinical map.
    """
    if isinstance(code_system_ids, int):
        code_system_ids = [code_system_ids]

    df = clinical_mapping_db.get_clinical_process_data(MappingTables.CODE_SYSTEM, map_version)

    missing_systems = set(code_system_ids) - set(df["code_system_id"])  # type: ignore
    if missing_systems:
        raise ValueError(f"The code system id(s) {missing_systems} are not recognized")
    else:
        result = "Code System ID confirmed"

    return result
