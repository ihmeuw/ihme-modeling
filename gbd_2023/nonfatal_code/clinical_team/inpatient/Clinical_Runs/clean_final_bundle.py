"""
Clean the final bundle level marketscan data to align with the database
structure

Also clean any other non-cms claims sources and concatenate onto the Marketscan
table before upload.

Main functions are: claims_main() and inp_main()
"""

import glob
import warnings
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from crosscutting_functions.pipeline import poland as pol_constants
from crosscutting_functions.clinical_constants.columns import FINAL_PIPELINE_COLS
from crosscutting_functions.clinical_metadata_utils.api.pipeline_wrappers import (
    ClaimsWrappers,
)
from crosscutting_functions import general_purpose
from crosscutting_functions import maternal as maternal_funcs
from crosscutting_functions.validations.decorators import clinical_typecheck
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db

from inpatient.Clinical_Runs.utils import constants, funcs
from inpatient.Clinical_Runs.utils.claims_run_tracker import BestClaimsRuns
from crosscutting_functions import bundle_check


@clinical_typecheck
def claims_main(
    clinical_data_universe_id: int,
    convert_sgp_data_to_otp_estimate: bool = True,
    sources_to_run: Union[List[str], str] = "all_claims",
    create_metadata: bool = False,
    write_out: bool = False,
    write_out_run_id: Optional[int] = None,
) -> Union[pd.DataFrame, None]:
    """First, formats MARKETSCAN for upload as a base table.
    Then appends on the formatted claims sources that aren't MS or in the
    exclusion list so that our entire set of claims data is prepped in a
    single file for upload.

    CMS estimate files do not have the same schema so they are excluded from
    this processing (June 2023). 

    Args:
        clinical_data_universe_id (int): ID to DB table.
            This universe ID must have all the NID in the compiled file.
        convert_sgp_data_to_otp_estimate (bool, optional): Change estimate_id
            for some bundles in SGP.
            Defaults to True. If True, any bested data receiving asfr will need
            to have a run_id >= 36.
        sources_to_run (Union[List[str], str]): Sources from the ClaimsRunTracker
            with bested runs to include in processing. Either a list of claim
            source abbreviations, or a string that is a claim source abbreviation
            to run a single source, or the keyword "all_claims".
            Defaults to "all_claims" which will include all claims sources with
            a bested run_id found in ClaimsRunTracker.
        create_metadata (bool, optional): True will add metadata to the run_metadata
            table in the DB.  Will also create run_id directories for newly created run.
            Defaults to False  which assumes that the run has been instantiated and the
            directories exist. Only used if write_out=True.
        write_out (bool, optional): If the final data should be written disk.
            Defaults to False.
        write_out_run_id (int, optional): If the final data should be written disk.
            Defaults to None which will infer the write out run_id.  This will be +1
            from the max run_id of the compiled sources.

    Raises:
        RuntimeError: If bested runs use more than 1 map version.
        RuntimeError: Uncertainty columns already present.
        RuntimeError: Column count changed when re-ordering.

    Returns:
        Union[pd.DataFrame, None]: Table ready for upload if write_out is False.
    """

    bcr = BestClaimsRuns()

    if isinstance(sources_to_run, str):
        if sources_to_run == "all_claims":
            sources_to_run = list(bcr.keys())
        elif sources_to_run in bcr.keys():
            sources_to_run = [sources_to_run]
        else:
            msg = "If not a list, sources_to_run needs to be keyword 'all_claims'"
            ext = "or a source abbreviation found in the bested run tracker."
            raise ValueError(f"{msg} {ext}")
    else:
        for source in sources_to_run:
            if source not in bcr.keys():
                raise ValueError(f"source '{source}' is has no bested run.")

    maps = []
    for claim_source, run_id in bcr.items():
        if claim_source in sources_to_run:
            meta = ClaimsWrappers(
                run_id=run_id, odbc_profile=constants.RunDBSettings.cw_profile
            ).pull_run_metadata()
            maps.append(int(meta["map_version"].unique()))

    # All bested runs must use the same map_version.
    if len(set(maps)) > 1:
        raise RuntimeError("Multiple bundle map versions used in bested runs.")

    map_version = maps[0]

    df = prep_marketscan(map_version=map_version)

    # Add the other claims sources
    df = aggregate_claims_sources(
        df=df,
        map_version=map_version,
        sources_to_run=sources_to_run,
    )

    # update columns
    if "lower" in df.columns or "upper" in df.columns:
        raise RuntimeError("Does not currently expect any claims uncertainty columns")
    df["upper"], df["lower"] = np.nan, np.nan

    sym_diff = set(df.columns).symmetric_difference(FINAL_PIPELINE_COLS)
    if len(sym_diff) > 0:
        raise RuntimeError(f"Losing some columns. The symmetric difference is \n{sym_diff}")

    df = df[FINAL_PIPELINE_COLS]

    if convert_sgp_data_to_otp_estimate:
        df = convert_sgp_bundles(df=df, map_version=map_version)

    df = final_test_claims(df=df, map_version=map_version, validation_schema="MS_AND_OTHER")

    if write_out:
        funcs.validate_universe(df=df, clinical_data_universe_id=clinical_data_universe_id)
        funcs.write_final_data(
            df=df,
            run_sources=sources_to_run,
            clinical_data_universe_id=clinical_data_universe_id,
            create_metadata=create_metadata,
            next_run_id=write_out_run_id,
        )

    else:
        return df


def inp_main(run_id: int, bin_years: bool) -> None:
    """
    Formats Inpatient data for upload
    """
    bin_dir = "single_years"
    if bin_years:
        bin_dir = "agg_5_years"
    inp_path = (FILEPATH
    )
    u1_path = (FILEPATH
    )
    u1 = pd.read_csv(u1_path)

    df = pd.read_hdf(inp_path)
    df = pd.concat([df, u1], sort=False, ignore_index=True)

    cols = df.columns

    if "diagnosis_id" not in cols:
        print("diagnosis_id wasn't in the columns, adding...")
        df["diagnosis_id"] = 1

    if "source_type_id" not in cols:
        # facility inpatient on the table
        df["source_type_id"] = 10

    for drop_col in [
        "estimate_type",
        "measure",
        "source",
        "haqi_cf",
        "correction_factor",
    ]:
        if drop_col in cols:
            df.drop(drop_col, axis=1, inplace=True)

    # update columns
    df["run_id"] = run_id

    pre = df.columns

    sym_diff = set(pre).symmetric_difference(FINAL_PIPELINE_COLS)
    msg = f"Losing some columns. The symmetric difference is \n{sym_diff}"
    if len(sym_diff) > 0:
        raise RuntimeError(msg)
    df = df[FINAL_PIPELINE_COLS]

    out_path = (FILEPATH
    )
    df.to_csv(out_path, index=False)
    print("Saved to {}.".format(out_path))


def format_poland(df: pd.DataFrame) -> pd.DataFrame:
    """Applies some formatting to poland.csv once read in to align
    the table with processing schema.

    Args:
        df (pd.DataFrame): Read in of poland.csv

    Returns:
        pd.DataFrame: Formatted input dataframe.
    """

    df = df[df.location_id != pol_constants.POL_GBD_LOC_ID]
    # Assign col values to match processing
    df["year_start"], df["year_end"] = df["year_id"], df["year_id"]
    df.drop("year_id", axis=1, inplace=True)
    df["representative_id"] = 1

    return df


def claims_read_helper(run_id: int, claim_source: str) -> pd.DataFrame:
    """The claims data is stored in different formats unfortunately.
    This reads them in by source

    Args:
        run_id (int): Clinical for the bested 'claim_source' output.
        claim_source (str): Source to process and concat to Marketscan.

    Raises:
        NotImplementedError: A claims source passed which is not supported.

    Returns:
        pd.DataFrame: Bundle estimate table for the claim_source.
    """

    claim_source = claim_source.lower()
    base = FILEPATH
    path = FILEPATH

    micro_claims_dict = {
        "sgp": f"{path}sgp_mediclaims.csv",
        "twn_nhi": f"{path}twn.csv",
        "rus_moh": f"{path}rus_moh.csv",
        "mng_h_info": f"{path}mng_claims.csv",
        "kor_hira": f"{path}kor_hira_claims.csv",
    }

    if claim_source in micro_claims_dict.keys():
        df = pd.read_csv(micro_claims_dict[claim_source])
    elif claim_source == "pol_nhf":
        if run_id <= 25:
            files = glob.glob(FILEPATH)
            if len(files) != 2:
                raise RuntimeError("We expect 2 and exactly 2 poland files")
            df = pd.concat([pd.read_csv(f) for f in files], sort=False, ignore_index=True)
        else:
            df = pd.read_csv(f"{path}poland.csv")
        df = format_poland(df=df)
    else:
        raise NotImplementedError(f"'{claim_source}' not supportted in concat.")

    if "run_id" not in df.columns:
        df["run_id"] = run_id

    df = funcs.map_diagnosis_id(df=df)
    df = funcs.assign_facility_source_type_from_est(df=df)

    return df


def aggregate_claims_sources(
    df: pd.DataFrame, map_version: int, sources_to_run: List[str]
) -> pd.DataFrame:
    """Immediately before writing to drive we should aggregate the other claims
    sources together with Marketscan.

    Args:
        df (pd.DataFrame): Table with only Marketscan data in it.
        map_version (int): Used in each run.
        sources_to_run (List[str]): Claims sources with bested runs to
            included in processing.

    Raises:
        RuntimeError: Any non-Marketscan schema columns are in dataframe.
        RuntimeError: Processing unexpectedly changed row count.
        RuntimeError: Processing unexpectedly changed column count.

    Returns:
        pd.DataFrame: Marketscan and other claims sources concatted to it
    """

    settings = constants.ClaimsRunSettings

    bcr = BestClaimsRuns()
    sources = [key for key in bcr.keys() if key in sources_to_run and key != "ms"]
    most_current = max(bcr.values())

    maternal_bundles = maternal_funcs.get_maternal_bundles(
        map_version=map_version, run_id=most_current
    )

    row_count = df.shape[0]
    col_count = df.shape[1]

    for claim_source in sources:
        run_id = bcr[claim_source]
        tmp = claims_read_helper(run_id=run_id, claim_source=claim_source)
        warnings.warn("The columns {} will be lost".format(set(tmp.columns) - set(df.columns)))

        # Drop any cols that aren't in marketscan.
        tmp = tmp[df.columns.tolist()]
        if len(set(tmp.columns).symmetric_difference(set(df.columns))) > 0:
            raise RuntimeError("DataFrames have differing columns.")

        for est in settings.flagged_estimates:
            if est in tmp["estimate_id"].unique():
                clinical_age_group_set_id = settings.flagged_age_group_set_id
            else:
                clinical_age_group_set_id = settings.clinical_age_group_set_id

        tmp = clinical_mapping.apply_restrictions(
            df=tmp,
            age_set="age_group_id",
            cause_type="bundle",
            map_version=map_version,
            prod=True,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

        if tmp[tmp["bundle_id"].isin(maternal_bundles)].shape[0] > 0:
            # pol and ms have asfr applied already
            if claim_source not in ["ms", "pol_nhf"]:
                msg = "we're applying the asfr adjustment for {claim_source}"
                ext = "Bundles are pulled from cause metadata"
                warnings.warn(f"{msg}. {ext}.")

                tmp = maternal_funcs.apply_asfr(
                    df=tmp,
                    maternal_bundles=maternal_bundles,
                    run_id=run_id,
                )

        row_count += len(tmp)

        df = pd.concat([df, tmp], sort=False, ignore_index=True)

    if df.shape[0] != row_count:
        raise RuntimeError(f"Pooling lost {row_count - df.shape[0]} expected rows.")

    if df.shape[1] != col_count:
        raise RuntimeError(f"Pooling lost {col_count - df.shape[1]} expected columns.")

    return df


def final_test_claims(
    df: pd.DataFrame, map_version: int, validation_schema: str
) -> pd.DataFrame:
    """Check for expected bundles and square by source and validates
    the total schema.

    Args:
        df (pd.DataFrame): Table of concatenated claims source estimates.
        map_version (int): Clinial map verison
        validation_schema (str): Schema name. Can be found within the module
            clinical_constants.pipeline.schema.clinical_run

    Raises:
        RuntimeError: If there are failures idenitfied in missing_bundle_dict.

    Returns:
        pd.DataFrame: Tested and validated claims estimate table.
    """

    failures = []
    # test the table
    # bundle_check.test_clinical_bundle_est()
    # check if the bundles we'll upload are present
    missing_bundle_dict = bundle_check.test_refresh_bundle_ests(
        df=df, pipeline="claims", prod=False, map_version=map_version
    )
    if missing_bundle_dict != "This df seems to have all estimates present":
        failures.append(missing_bundle_dict)

    # check if the data is square
    if len(failures) > 0:
        raise RuntimeError(f"These tests failed: {failures}")

    df = validate_schema(
        df=df, schema_set="clinical_run", schema_name=validation_schema, coerce=True
    )

    return df


def convert_sgp_bundles(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    """See first few lines of script but we're making a very specific
    bundle based adjust for a modeler.
    Also See ticket CCMHD-9835
    NOTE: this will not work with SGP subnational data"""
    bundles = [138, 139, 141, 142, 143, 3033]

    bundle_table = clinical_mapping_db.get_active_bundles(
        bundle_id=bundles, map_version=map_version, estimate_id=[17, 21]
    )
    assert (
        bundle_table["estimate_id"] == 21
    ).all(), "Some bundles estimates aren't 21, meaning they'll lose data!"
    warnings.warn("Do NOT pass this function subnational data from Singapore")
    assert (
        df.loc[(df["bundle_id"].isin(bundles)) & (df["location_id"] == 69), "estimate_id"]
        == 17
    ).all(), "Why were SGP estimates not 17?"
    df.loc[(df["bundle_id"].isin(bundles)) & (df["location_id"] == 69), "estimate_id"] = 21
    return df


def prep_marketscan(map_version: int) -> pd.DataFrame:
    """Reads in Marketscan estimates

    Args:
        map_version (int): Map version common accross bested claims runs.

    Returns:
        pd.DataFrame: Compiled Marketscan estimates prepared for upload.
    """

    settings = constants.ClaimsRunSettings

    bcr = BestClaimsRuns()
    base = FILEPATH
    mid = FILEPATH
    run_id = bcr["ms"]

    filepath = FILEPATH

    df = pd.read_csv(filepath)

    # assign col values to match process
    df["year_start"], df["year_end"] = df["year_id"], df["year_id"]
    df.drop("year_id", axis=1, inplace=True)
    df["representative_id"] = 4
    df["run_id"] = run_id

    df = general_purpose.to_int(df=df)

    df = clinical_mapping.apply_restrictions(
        df=df,
        age_set="age_group_id",
        cause_type="bundle",
        map_version=map_version,
        prod=True,
        clinical_age_group_set_id=settings.clinical_age_group_set_id,
    )

    df = funcs.map_diagnosis_id(df=df)
    df = funcs.assign_facility_source_type_from_est(df=df)

    return df
