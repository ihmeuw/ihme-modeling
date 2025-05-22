"""
Pull data / Read data
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq
from crosscutting_functions.clinical_constants.pipeline import poland as constants
from crosscutting_functions.clinical_constants.pipeline.schema import pol_pipeline
from crosscutting_functions.clinical_constants.shared.mapping import CODE_SYSTEMS
from crosscutting_functions import pipeline
from crosscutting_functions.clinical_functions.deduplication import estimates
from crosscutting_functions.clinical_io.io.io_manager import get_poland_best_year_file_source_id
from crosscutting_functions.clinical_map_tools.icd_groups.use_icd_group_ids import UseIcdGroups
from loguru import logger

from pol_nhf.pipeline.lib import mapping


def validate_and_cast_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Validate the ICD mart DataFrame columns and cast dtypes."""

    return pol_pipeline.icd_mart_schema.validate(df)


def get_icd_group_ids(bundle_id: int, map_version: int) -> Tuple[int]:
    """Pull ICD groups for given bundle then subset by coding system.

    Args:
        bundle_id (int): Bundle to pull data for from the ICD-Mart.
        map_version (int): Clinical bundle map version.

    Raises:
        ValueError: Passed a bundle_id outside of the map.
        ValueError: No icd_group_id's returned for the bundle_id.

    Returns:
        Tuple[int]: All of the icd groups IDs for a given bundle.
    """

    icd_groups = UseIcdGroups()
    group_ids = icd_groups.get_bundle_group_ids(bundle_id=bundle_id, map_version=map_version)
    if not any(group_ids):
        raise ValueError(f"Bundle id {bundle_id} is not present in map version {map_version}")
    icd_groups.get_icd_group_lookup()
    icd_group_df = icd_groups.icd_groups
    icd_group_df = icd_group_df[icd_group_df.code_system_id == CODE_SYSTEMS["ICD10"]]
    group_ids = tuple(
        e for e in group_ids if not icd_group_df[icd_group_df.icd_group_id == e].empty
    )
    if not any(group_ids):
        raise ValueError(
            (
                f"Bundle id {bundle_id} does not have any "
                f"code system id {CODE_SYSTEMS['ICD10']} codes"
            )
        )

    return group_ids


def read_bundle_data(
    bundle_id: int, run_id: int, estimate: estimates.ClaimsEstimate, year_ids: List[int]
) -> pd.DataFrame:
    """Read the partitioned icd groups for a given bundle.

    Args:
        bundle_id (int): Bundle to pull data for from the ICD-Mart.
        run_id (int): Clinical Run ID found in run_metadata.
        estimate (estimates.ClaimsEstimate): Estimate object from deduplication
            associated to a specific estiamte_id.
        year_ids (List[int]): Years to pull data for.

    Returns:
        pd.DataFrame: ICD-mart mapped to and filtered to the input bundle_id.
    """
    map_version = pipeline.get_map_version(run_id=run_id)

    icd_group_ids = get_icd_group_ids(bundle_id=bundle_id, map_version=map_version)

    paths = build_icd_mart_paths(year_ids=year_ids, groups=icd_group_ids, estimate=estimate)

    filters = [("location_id", "!=", constants.POL_GBD_LOC_ID)]

    # Build bundle level df from each associated icd_group.
    bundle_df_list = []
    for path in paths:
        tmp = read_from_parquet(file_path=path, filters=filters)  # type:ignore
        tmp["year_id"], tmp["is_otp"] = get_partitions_from_path(path=path)
        bundle_df_list.append(tmp)

    bundle_df = pd.concat(bundle_df_list, ignore_index=True)
    bundle_df = bundle_df[list(pol_pipeline.icd_mart_schema.columns.keys())]

    mapped_df = mapping.map_to_bundle(
        df=bundle_df, map_version=map_version, bundle_id=bundle_id
    )

    if len(mapped_df["measure_id"].unique()) > 1:
        ValueError("Clinical mapping module failed. There is more than one measure for bundle")

    return validate_and_cast_cols(mapped_df.reset_index(drop=True))


def read_from_parquet(
    file_path: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Tuple[str]]] = None,
) -> pd.DataFrame:
    """Access pyarrow read table directly to retain all datatypes,
    specifically datetime dtypes.

    Args:
        file_path (str): A path to a parquet file.
        columns (Optional[List[str]]): Columns to read in.
            Defaults to None which reads in all columns.
        filters (Optional[List[Tuple[str]]]): Pushdown filters. Defaults to None.

    Returns:
        pd.DataFrame: Parquet file read in as pandas table.
    """
    pq_table = pq.read_table(file_path, columns=columns, filters=filters)

    # retain datetime cols and reduce memory doubling via split and destruct args. see docs--
    # https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
    df = pq_table.to_pandas(date_as_object=False, split_blocks=True, self_destruct=True)

    return df


def get_partitions_from_path(path: str) -> Tuple[int, int]:
    """Get partition values from a Poland ICD-Mart path.

    Args:
        path (str): ICD-Mart path.

    Returns:
        Tuple[int, int]: year of data, boolean where 1 is outpatient.
    """
    year_id = YEAR_ID
    is_otp = IS_OTP

    return year_id, is_otp


def build_icd_mart_paths(
    year_ids: List[int], groups: Tuple[int], estimate: estimates.ClaimsEstimate
) -> List[str]:
    """Creates the paths to icd_group_id's for a given estimate.

    Args:
        year_ids (List[int]): Years of data being processed.
        groups (Tuple[int]): All of the icd groups IDs for a given bundle.
        estimate (estimates.ClaimsEstimate): Estimate object from deduplication
            associated to a specific estiamte_id.

    Returns:
        List[str]: Paths to the icd_groups in Poland ICD-mart.
    """
    base = (
        "FILEPATH"
    )

    file_sources_dict: Dict[int, List[int]] = get_poland_best_year_file_source_id(
        year_ids=year_ids
    )
    logger.info(f"File Source Dict: {file_sources_dict}")

    is_otp_ids: List[int] = pipeline.get_is_otp_ids(estimate_id=estimate.estimate_id)

    paths = []
    # Get only bested file_source-years.
    for fs_id, years in file_sources_dict.items():
        path = "FILEPATH"
        paths.extend([str("FILEPATH") for group in groups])
        for year_id in year_ids:
            for is_otp in is_otp_ids:
                path = "FILEPATH"
                # Paths must be strings for spark
                paths.extend([str("FILEPATH") for group in groups])

    icd_mart_paths = [fp for fp in paths if os.path.exists(fp)]

    return icd_mart_paths


def prep_for_upload(run_id: int) -> None:
    """Combine outputs produced from internal.main into a single csv.

    All bundles and estimates need to be brought back together in a single file. This
    file used in clean_final_bundles.py which preps claims files for the Uploader

    Args:
        run_id (int): Clinical Run ID found in run_metadata.
    """
    base = "FILEPATH"
    outpath = "FILEPATH"
    pipeline_results = "FILEPATH"
    paths = pipeline_results.glob("*.parquet")
    df = pd.concat([pd.read_parquet(path) for path in paths], sort=False)

    output_path = "FILEPATH"
    df.to_csv(output_path, index=False)
