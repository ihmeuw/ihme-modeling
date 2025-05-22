import os
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_constants.pipeline.cms import general_cols
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.clinical_info.Functions.clinical_functions import pipeline_functions
from crosscutting_functions.clinical_map_tools.icd_groups.use_icd_group_ids import UseIcdGroups
from crosscutting_functions.mapping import clinical_mapping
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import ClaimsWrappers
from pydantic import validate_arguments

from cms.utils.process_types import Deliverable


@validate_arguments()
def build_bundle_table(
    cms_system: str, run_id: int, estimate_id: int, bundle_id: int
) -> pd.DataFrame:
    """Main reader funtion to retrieve data from a cms-system specfic ICD-Mart into
    a bundle level table.

    Args:
        cms_system (str): CMS system abbreviation.
        run_id (int): Clinical and CMS run ID.
        estimate_id (int): Estimate ID to be processed
        bundle_id (int): Bundle ID to be to create an estiamte for.

    Returns:
        pd.DataFrame: Compisite bundle table built from ICD-Mart.
    """

    # Get the icd_group_id's for the given bundle.
    icd_groups = get_bundle_icd_groups(bundle_id=bundle_id, run_id=run_id)

    # Make paths to each icd_group_id.
    paths = build_icd_mart_paths(
        cms_system=cms_system, groups=icd_groups, estimate_id=estimate_id
    )

    # Build bundle level df from each associated icd_group.
    bundle_df_list = []
    for path in paths:
        tmp = read_from_parquet(file_path=path)
        tmp["year_id"], tmp["is_otp"] = get_partitions_from_path(
            path=path, cms_system=cms_system
        )
        bundle_df_list.append(tmp)

    bundle_df = pd.concat(bundle_df_list, ignore_index=True)

    map_version = pipeline_functions.get_map_version(run_id=run_id)

    mapped_df = clinical_mapping.map_to_gbd_cause(
        df=bundle_df,
        input_type="cause_code",
        output_type="bundle",
        map_version=map_version,
        retain_active_bundles=False,
        truncate_cause_codes=True,
        extract_pri_dx=False,
        prod=True,
        write_log=False,
        groupby_output=False,
    )

    mapped_df = mapped_df.loc[mapped_df["bundle_id"] == bundle_id]

    return mapped_df


def read_from_parquet(
    file_path: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Tuple[str]]] = None,
) -> pd.DataFrame:
    """Access pyarrow read table directly to retain all datatypes,
    specifically datetime dtypes

    Args:
        file_path (str): A path to a parquet file.
        columns (Optional[List[str]]): Columns to read in.
            Defaults to None which reads in all columns.
        filters (Optional[List[Tuple[str]]]): Pushdown filters. Defaults to None.

    Returns:
        pd.DataFrame: Parquet file read in as pandas table.
    """
    pq_table = pq.read_table(file_path, columns=columns, filters=filters)

    df = pq_table.to_pandas(date_as_object=False, split_blocks=True, self_destruct=True)
    del pq_table  # self-destruct arg makes this unusable

    return df


def get_bundle_icd_groups(bundle_id: int, run_id: int) -> List[int]:
    """Mapping to navigate the ICD mart and process in bundle space.
    bundle_id -> icg_id -> icd_group_id

    Returns:
        List[int]: ICD group IDs for a specified bundle.
    """

    meta = ClaimsWrappers(run_id=run_id, odbc_profile="SERVER").pull_run_metadata()
    map_version = int(meta.loc[meta["clinical_data_type_id"] == 3, "map_version"])

    groups = UseIcdGroups().get_bundle_group_ids(bundle_id=bundle_id, map_version=map_version)

    return groups


def build_icd_mart_paths(cms_system: str, groups: List[int], estimate_id: int) -> List[str]:
    """Gets valid paths to the icd_group_id groups for a given system-estimate.

    Args:
        cms_system (str): CMS system abbreviation.
        groups (List[int]): ICD group id's in a bundle.
        estimate_id (int): Estimate ID to be processed.

    Returns:
        List[str]: Paths to icd groups in a bundle.
    """

    base = Path(
        filepath_parser(ini="pipeline.cms", section="icd_mart", section_key="icd_mart_output")
    )
    path = "FILEPATH"
    years: List[int] = getattr(constants, f"{cms_system}_years")
    is_otp_ids: List[int] = pipeline_functions.get_is_otp_ids(estimate_id=estimate_id)

    paths = [
        "FILEPATH"
    ]

    icd_mart_paths = [fp for fp in paths if os.path.exists(fp)]

    return icd_mart_paths


def get_partitions_from_path(path: str, cms_system: str) -> Tuple[int, int]:
    """Get partition values from an ICD-Mart path.

    Args:
        path (str): ICD-Mart path.
        cms_system (str): CMS system abbreviation.

    Returns:
        Tuple[int, int]: year of data, boolean where 1 is outpatient.
    """

    base = filepath_parser(
        ini="pipeline.cms", section="icd_mart", section_key="icd_mart_output"
    )
    year_base = "FILEPATH"
    year_id = "FILEPATH" 

    fac_base = "FILEPATH"
    is_otp = "FILEPATH"

    return year_id, is_otp


def icd_mart_proc_cols(cms_system: str, deliverable: Deliverable) -> List[str]:
    """Creates the list of processing columns to use in the CMS pipeline based
    on cms system and the deliverable type.

    Room for improvement on this data structure

    Args:
        cms_system (str): CMS system abbreviation.
        deliverable (Deliverable): Custom Dataclass based on a
            specific processing deliverable.

    Raises:
        ValueError: An invalid cms_system was provided.

    Returns:
        List[str]: Columns to process.
    """
    cols = general_cols.copy()

    # cols specific to each system are added
    if cms_system == "mdcr":
        cols += ["five_percent_sample", "entitlement"]
    elif cms_system == "max":
        cols += ["restricted_benefit", "managed_care"]
    else:
        raise ValueError(f"Unknown cms system {cms_system}")

    # add columns by deliverable
    cols += deliverable.icd_mart_cols

    return cols
