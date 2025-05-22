"""File reading and writing logic, apart from parameters."""

import os
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

import ihme_dimensions
from draw_sources import draw_sources
from gbd.constants import measures, metrics
from ihme_cc_gbd_schema.common import ModelStorageMetadata

from ihme_cc_sev_calculator.lib import constants


def get_output_dir(version_id: int) -> Path:
    """Gets output directory for the SEV Calculator version."""
    return 


def get_manifest_path(version_id: int, output_dir: Optional[str] = None) -> Path:
    """Gets the manifest path, i.e. the path to the cache metadata used by ihme_cc_cache.

    Optionally allows passing in output_dir.
    """
    if output_dir is None:
        output_dir = get_output_dir(version_id)
    else:
        output_dir = Path(output_dir)

    return 


def create_run_dirs(
    output_dir: Union[str, Path], all_rei_ids: List[int], by_cause: bool
) -> None:
    """Create run directories at the beginning of a SEV Calculator run.

    If run is by_cause, adds extra directories for cause-specific SEVs

    Args:
        output_dir: root directory where outputs will be saved
        all_rei_ids: all REI IDs that we produce SEVs for
        by_cause: if SEVs by cause are also being produced
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    subdirectories = [
    ]

    # SEV draws are saved in risk-specific directories
    subdirectories += 

    if by_cause:
        subdirectories += 
        subdirectories += 

    for subdir in subdirectories:
        path = 
        path.mkdir(exist_ok=False, parents=True)


def cache_storage_metadata(output_dir: str, measure_ids: List[int]) -> None:
    """Cache storage metadata for draws.

    RRmax and SEV storage metadata are saved in the same directory (output_dir),
    differentiated by name.

    If SEVs are not being produced, we don't write SEV storage metadata. RRmax
    storage metadata is always written.

    Args:
        output_dir: output directory for the SEV run
        measure_ids: measure IDs for the SEV run. RRmax is always expected. If SEVs
            is included, also write storage metadata for SEVs
    """
    rr_max_storage_metadata = ModelStorageMetadata.from_dict(
        {"storage_pattern": }
    )
    rr_max_storage_metadata.to_file(
        directory=output_dir, basename=
    )

    if measures.SEV in measure_ids:
        sev_storage_metadata = ModelStorageMetadata.from_dict(
            {"storage_pattern": }
        )
        sev_storage_metadata.to_file(
            directory=output_dir, basename=
        )


def read_reis_in_paf_version(paf_version_id: int) -> pd.DataFrame:
    """Read REIs the given PAF version has PAFs for."""
    return pd.read_csv(constants.EXISTING_REIS_FILE.format(paf_version_id=paf_version_id))


def read_custom_rr_max_draws(
    rei_id: int, rei_metadata: pd.DataFrame, release_id: int, n_draws: int
) -> pd.DataFrame:
    """Read custom RRmax draws, downsampling as needed.

    Before run begins, we validate that this files exist but don't check the contents.
    These RRmax draws are validated later on before saving.
    """
    rei = rei_metadata.query(f"rei_id == {rei_id}")["rei"].iat[0]
    rr_max_df = pd.read_csv(
        constants.CUSTOM_RR_MAX_DRAW_FILE.format(release_id=release_id, rei=rei)
    )
    return ihme_dimensions.dfutils.resample(rr_max_df, n_draws=n_draws)


def save_rr_max_draws(
    rr_max_df: pd.DataFrame, rei_id: int, draw_cols: List[str], root_dir: str
) -> None:
    """Sort and save RRmax draws."""
    id_cols = ["rei_id", "cause_id", "age_group_id", "sex_id"]

    rr_max_df = rr_max_df.assign(rei_id=rei_id)
    rr_max_df[id_cols + draw_cols].drop_duplicates().sort_values(id_cols).to_csv(
        constants.RR_MAX_DRAW_FILE.format(root_dir=root_dir, rei_id=rei_id), index=False
    )


def read_rr_max_draws(rei_id: Union[int, List[int]], root_dir: str) -> pd.DataFrame:
    """Read RRmax draws for a given REI ID.

    Args:
        rei_id: REI ID or list of REI IDs to read in RRmax draws for
        root_dir: root directory of the SEV run
    """
    if isinstance(rei_id, int):
        rei_id = [rei_id]

    rr_max = [
        pd.read_csv(constants.RR_MAX_DRAW_FILE.format(root_dir=root_dir, rei_id=r_id))
        for r_id in rei_id
    ]
    return pd.concat(rr_max)


def save_rr_max_summaries(
    rr_max_df: pd.DataFrame, rei_id: int, root_dir: str, purpose: str
) -> None:
    """Save RRmax summaries.

    Args:
        rr_max_df: RRmax summaries for an REI
        rei_id: REI ID the RRmax summaries are for
        root_dir: root directory of the SEV run
        purpose: what the summaries are for, either 'upload' or 'fhs'. Determines where
            they are saved. Upload summaries are for uploading to the GBD database. FHS
            summaries are specific to FHS and include NAs for age group/sex restrictions.

    Raises:
        ValueError: if purpose is not either 'upload' or 'fhs'
    """
    if purpose == "upload":
        file = constants.RR_MAX_SUMMARY_UPLOAD_FILE.format(root_dir=root_dir, rei_id=rei_id)
    elif purpose == "fhs":
        file = constants.RR_MAX_SUMMARY_FHS_FILE.format(root_dir=root_dir, rei_id=rei_id)
    else:
        raise ValueError(f"Purpose '{purpose}' is not valid.")

    rr_max_df.to_csv(file, index=False)
    os.chmod(file, 0o775)


def cache_exposure_draws(
    exposure_df: pd.DataFrame, rei_id: int, location_id: int, root_dir: str
) -> None:
    """Cache exposure and exposure SD draws for use by edensity R code later on."""
    exposure_df.to_csv(
        constants.EXPOSURE_DRAW_FILE.format(
            root_dir=root_dir, rei_id=rei_id, location_id=location_id
        ),
        index=False,
    )


def read_risk_prevalence_draws(rei_id: int, location_id: int, root_dir: str) -> None:
    """Read risk prevalence draws written within R code."""
    if rei_id not in constants.EDENSITY_REI_IDS:
        raise ValueError(
            "Can only read risk prevalence draws for REI IDs "
            f"{constants.EDENSITY_REI_IDS}, not rei_id={rei_id}"
        )

    return pd.read_csv(
        constants.RISK_PREVALENCE_DRAW_FILE.format(
            root_dir=root_dir, rei_id=rei_id, location_id=location_id
        )
    )


def read_paf_draws(
    rei_id: int, location_id: int, year_ids: List[int], n_draws: int, paf_version_id: int
) -> pd.DataFrame:
    """Read PAF draws from given PAF Aggregator version for an REI.

    Filters PAF draws to given rei_id and YLL PAFs unless a cause outcome is YLD-only,
    in which case, filters to YLD draws for that cause. Downsamples to n_draws if needed.

    Can't use get_draws because get draws does not support pulling PAF draws post-PAF
    Aggregator. Reads files directly instead like the Burdenator (at time of writing this).

    Returns:
        Dataframe of PAF draws with the columns: location_id, year_id, age_group_id, sex_id,
        cause_id, draw_0, ..., draw_n
    """
    # Read in metadata with rei/cause/measure/sex
    existing_reis = read_reis_in_paf_version(paf_version_id)
    if rei_id not in existing_reis["rei_id"].unique().tolist():
        raise ValueError(f"rei_id {rei_id} not found in PAF Aggregator v{paf_version_id}")

    yld_only_cause_ids = (
        existing_reis.query(f"rei_id == {rei_id}")[["cause_id", "measure_id"]]
        .drop_duplicates()  # Drop sex dimension
        .groupby(["cause_id"])
        .max()  # Take max measure for each cause. Either 3 (YLD-only) or 4 (has YLLs)
        .reset_index()
        .query(f"measure_id == {measures.YLD}")["cause_id"]
        .tolist()
    )

    # Filter to PAFs for rei_id and YLL PAFs (unless any cause outcomes are YLD-only)
    query = (
        f"(rei_id == {rei_id}) & ((measure_id == {measures.YLD} & "
        f"cause_id.isin({yld_only_cause_ids})) | measure_id == {measures.YLL})"
    )

    all_pafs = []
    for year_id in year_ids:
        paf_df = pd.read_csv(
            constants.PAF_DRAW_FILE.format(
                paf_version_id=paf_version_id, location_id=location_id, year_id=year_id
            )
        ).query(query)
        all_pafs.append(paf_df)

    all_pafs = pd.concat(all_pafs).reset_index(drop=True)

    # Check for duplicated rows, which could happen if something went wrong when
    # filtering by measure
    index_cols = ["location_id", "year_id", "age_group_id", "sex_id", "cause_id"]
    if all_pafs[index_cols].duplicated().sum():
        raise RuntimeError("Duplicates found after reading PAF draws")

    # Downsample (if <1000 draws), prune columns, and return
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    return ihme_dimensions.dfutils.resample(all_pafs, n_draws=n_draws)[index_cols + draw_cols]


def save_sev_draws(
    sev_df: pd.DataFrame,
    rei_id: int,
    location_id: int,
    draw_cols: List[str],
    root_dir: str,
    by_cause: bool,
) -> None:
    """Save SEV draws.

    Draws are formatted, sorted, and written as CSVs. Can handle cause-specific SEVs
    as well as the standard SEVs averaged across causes.
    """
    sev_df = sev_df.assign(rei_id=rei_id, measure_id=measures.SEV, metric_id=metrics.RATE)

    index_cols = [
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "measure_id",
        "metric_id",
    ]
    if by_cause:
        index_cols = ["rei_id", "cause_id"] + index_cols
    else:
        index_cols = ["rei_id"] + index_cols

    # Check for duplicated rows
    duplicated = sev_df[sev_df[index_cols].duplicated()]
    if not duplicated.empty:
        raise RuntimeError(
            f"Duplicates found before saving SEVs for rei_id {rei_id}:\n{duplicated}"
        )

    sev_df = sev_df[index_cols + draw_cols].sort_values(by=index_cols)
    draw_dir = (
        constants.SEV_DRAW_DIR.format(root_dir=root_dir)
        if not by_cause
        else constants.SEV_DRAW_CAUSE_SPECIFIC_DIR.format(root_dir=root_dir)
    )
    sev_df.to_csv(
        constants.SEV_DRAW_FILE.format(
            draw_dir=draw_dir, rei_id=rei_id, location_id=location_id
        ),
        index=False,
    )


def read_temperature_sev_draws(
    location_id: int, year_ids: List[int], n_draws: int, release_id: int
) -> pd.DataFrame:
    """Read temperature SEV draws produced by research team.

    Returns SEV draws for all temperature REIs for the given location and years.
    """
    if release_id not in constants.TEMPERATURE_SEV_DRAW_DIR_MAP:
        raise ValueError(
            f"release_id {release_id} not in constants.TEMPERATURE_SEV_DRAW_DIR_MAP"
        )

    source_params = {
        "draw_dir": constants.TEMPERATURE_SEV_DRAW_DIR_MAP[release_id],
        "file_pattern": constants.TEMPERATURE_SEV_FILE_PATTERN,
    }
    source = draw_sources.DrawSource(source_params)
    sev_df = source.content(filters={"location_id": location_id, "year_id": year_ids})

    if sev_df.empty:
        raise RuntimeError(
            f"No temperature SEV draws found for location_id={location_id}, "
            f"year_id={year_ids}"
        )

    missing_year_ids = set(year_ids) - set(sev_df["year_id"])
    if missing_year_ids:
        raise RuntimeError(
            f"Missing temperature SEVs for the following years: {missing_year_ids}"
        )

    # Resample to desired number of draws and return
    return ihme_dimensions.dfutils.resample(sev_df, n_draws=n_draws)


def read_pre_conversion_paf_draws(
    rei_id: int, location_id: int, year_ids: List[int], n_draws: int
) -> pd.DataFrame:
    """Reads PAF draws saved prior to conversion to GBD causes, returning expected columns.
    Used for a small number of risks. Needs to be kept in sync with the PAF Calculator's
    write_paf_for_sev_calculator function.

    This function reads the pre-conversion PAF draws written by the python version of
    the PAF Calculator.

    Args:
        rei_id: REI to read draws for
        location_id: location to read draws for
        year_ids: years to read draws for
        n_draws: number of draws to return

    Returns:
        DataFrame of PAF draws.

    Raises:
        FileNotFoundError if the expected PAF draw file does not exist.
    """
    input_file = (
    )
    df = pd.read_csv(input_file).query("year_id in @year_ids").reset_index(drop=True)
    return ihme_dimensions.dfutils.resample(df, n_draws=n_draws)[
        ["location_id", "year_id", "age_group_id", "sex_id", "cause_id"]
        + [f"draw_{i}" for i in range(n_draws)]
    ]


def write_final_compare_version_id(version_id: int, compare_version_id: int) -> None:
    """Write final compare version id to a text file for later use."""
    with open(, "w") as file:
        file.write(str(compare_version_id))


def read_final_compare_version_id(version_id: int) -> Optional[int]:
    """Read final compare version id from expected text file.

    Returns None if file does not exist.
    """
    file_name = 
    if not file_name.exists():
        return None

    with open(file_name, "r") as file:
        return int(file.read())
