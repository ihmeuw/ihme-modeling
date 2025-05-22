import glob
import os
import pathlib
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import pandas as pd

import ihme_cc_averted_burden
from ihme_cc_cache import FileBackedCacheReader, FileBackedCacheWriter
from ihme_dimensions.lib import dfutils
from save_results.api.internal import StagedResult

from ihme_cc_paf_calculator.lib import constants, data_utils


def get_output_dir(
    rei_id: int, model_version_id: int, root_dir: str = constants.DEFAULT_ROOT_RUN_DIR
) -> pathlib.Path:
    """Returns the output directory for a run."""
    return 


def create_dirs(rei_id: int, output_dir: pathlib.Path, resume: bool, rei_set_id: int) -> None:
    """Create run-specific directory and any expected subdirs.

    Special handling for LBW/SG: create two additional subdirs, one for each child risk
    where intermediate PAF results will be stored. REIs in the Averted Burden REI set
    also create one additional subdir for the Unavertable counterfactual.
    """
    subdirs = list(asdict(constants.SubDirs()).values())
    if rei_id == constants.LBWSGA_REI_ID:
        subdirs += [constants.LBW_SUBDIR, constants.SG_SUBDIR]
    elif rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
        if rei_id == ihme_cc_averted_burden.get_all_rei_ids_for_drug(rei_id).avertable_rei_id:
            subdirs += [constants.UNAVERTABLE_SUBDIR]

    output_dir.mkdir(exist_ok=resume, parents=True)
    for subdir in subdirs:
        ().mkdir(exist_ok=resume, parents=True)


def read_settings(root_dir: pathlib.Path) -> constants.PafCalculatorSettings:
    """
    Read version-specific settings.

    Arguments:
        root_dir: path to root of a paf calculator run

    Returns:
        paf calculator settings
    """
    return constants.PafCalculatorSettings(**read_settings_dict(root_dir=root_dir))


def read_settings_dict(root_dir: pathlib.Path) -> Dict[str, Any]:
    """Read version-specific settings as a dictionary."""
    manifest_path = manifest_path_of_dir(root_dir)
    # Read cached settings of this run
    cache_reader = FileBackedCacheReader(manifest_path)
    return cache_reader.get(constants.CacheContents.SETTINGS)


def get(root_dir: pathlib.Path, key: str) -> Any:
    """
    Get a dataset from a paf calculator run's cache.

    Arguments:
        root_dir: path to root of a paf calculator run

    Returns:
        item in cache
    """
    manifest_path = manifest_path_of_dir(root_dir)
    cache_reader = FileBackedCacheReader(manifest_path)
    return cache_reader.get(key)


def get_location_specific(root_dir: pathlib.Path, key: str, location_id: int) -> Any:
    """
    Get a location-specific dataset from a paf calculator's cache.

    Arguments:
        root_dir: path to root of a paf calculator run

    Returns:
        item in cache
    """
    manifest_path = 
    cache_reader = FileBackedCacheReader(manifest_path)
    return cache_reader.get(key)


def manifest_path_of_dir(dir: pathlib.Path) -> pathlib.Path:
    """Given a directory, return the path to a manifest file in that directory."""
    return 


def write_version_settings(
    settings: constants.PafCalculatorSettings, root_dir: pathlib.Path
) -> None:
    """Writes settings to cache."""
    manifest_path = manifest_path_of_dir(root_dir)
    cache_writer = FileBackedCacheWriter(manifest_path)
    cache_writer.put(
        obj=asdict(settings),
        obj_name=constants.CacheContents.SETTINGS,
        relative_path=
    )


def write_version_settings_and_cache(
    settings: constants.PafCalculatorSettings,
    root_dir: pathlib.Path,
    rei_metadata: pd.DataFrame,
    mediator_rei_metadata: Optional[pd.DataFrame],
    cause_metadata: pd.DataFrame,
    age_metadata: pd.DataFrame,
    demographics: Dict[str, List[int]],
    me_ids: pd.DataFrame,
    rr_metadata: pd.DataFrame,
    paf_staged_mvid: StagedResult,
    paf_unmediated_staged_mvid: StagedResult,
    paf_lbw_staged_mvid: StagedResult,
    paf_sg_staged_mvid: StagedResult,
    paf_unavertable_staged_mvid: StagedResult,
    mediation_factors: pd.DataFrame,
    intervention_rei_metadata: Optional[pd.DataFrame] = None,
) -> None:
    """
    Writes all the settings and cached data that are general to the entire version/run.

    The items cached are:
        * run settings
        * all best models and corresponding MEs
        * rei metadata for the specific REI
        * mediator REI metadata for any mediators (if applicable)
        * intervention REI metadata for the intervention acting on the rei (if applicable)
        * cause metadata
        * age metadata
        * rr metadata
        * demographics
        * save_results.api.internal.StagedResult's for paf and, if applicable, paf_unmediated.
            For LBW/SG, there are two additional StagedResults for the two child risks.

    Arguments:
        settings: version-specific settings
        root_dir: path to root dir of a paf calculator run
        rei_metadata: dataframe of rei metadata for the specific REI
        mediator_rei_metadata: dataframe of rei metadata for any relevant mediators
        intervention_rei_metadata: dataframe of rei metadata for the relevant intervention
        cause_metadata: dataframe of cause metadata
        age_metadata: dataframe of age metadata
        demographics: dictionary of required demographics
        me_ids: dataframe of related MEs and corresponding best model versions
        rr_metadata: dataframe with relative risk model metadata
        paf_staged_mvid: A result of a call to create_model_version_id, to create a new
            model_version_id before saving. This result will be used to call
            save_results_risk_staged once the PAF calculation is complete.
        paf_unmediated_staged_mvid: A result of a call to create_model_version_id, to create
            a new model_version_id before saving. This result will be used to call
            save_results_risk_staged once the PAF calculation is complete. If there is no
            mediation, this is still saved, but with None entries for ME and MV IDs.
        paf_lbw_staged_mvid: A result of a call to create_model_version_id, to create
            a new model_version_id before saving. This result will be used to call
            save_results_risk_staged for low birth weight once the PAF calculation is
            complete. Applicable for LBW/SG. For other risks, this is still saved, but with
            None entries for ME and MV IDs.
        paf_sg_staged_mvid: A result of a call to create_model_version_id, to create
            a new model_version_id before saving. This result will be used to call
            save_results_risk_staged for short gestation once the PAF calculation is complete.
            Applicable for LBW/SG. For other risks, this is still saved, but with None entries
            for ME and MV IDs.
        mediation_factors: A DataFrame of total mediation factors for the risk. Will be
            empty for risks that have no mediated causes.

    Returns:
        None
    """
    write_version_settings(settings, root_dir)
    manifest_path = manifest_path_of_dir(root_dir)
    cache_writer = FileBackedCacheWriter(manifest_path)
    cache_writer.put(
        obj=asdict(paf_staged_mvid),
        obj_name=constants.CacheContents.PAF_MVID_STAGED_RESULT,
        relative_path=
    )
    cache_writer.put(
        obj=asdict(paf_unmediated_staged_mvid),
        obj_name=constants.CacheContents.PAF_UNMED_MVID_STAGED_RESULT,
        relative_path=
    )
    cache_writer.put(
        obj=asdict(paf_lbw_staged_mvid),
        obj_name=constants.CacheContents.PAF_LBW_MVID_STAGED_RESULT,
        relative_path=
    )
    cache_writer.put(
        obj=asdict(paf_sg_staged_mvid),
        obj_name=constants.CacheContents.PAF_SG_MVID_STAGED_RESULT,
        relative_path=
    )
    cache_writer.put(
        obj=asdict(paf_unavertable_staged_mvid),
        obj_name=constants.CacheContents.PAF_UNAVTB_MVID_STAGED_RESULT,
        relative_path=
    )
    cache_writer.put(
        obj=me_ids,
        obj_name=constants.CacheContents.MODEL_VERSIONS,
        relative_path=
        storage_format=
    )
    cache_writer.put(
        obj=rei_metadata,
        obj_name=constants.CacheContents.REI_METADATA,
        relative_path=
        storage_format=
    )
    if mediator_rei_metadata is not None:
        cache_writer.put(
            obj=mediator_rei_metadata,
            obj_name=constants.CacheContents.MEDIATOR_REI_METADATA,
            relative_path=
            storage_format=
        )
    if intervention_rei_metadata is not None:
        cache_writer.put(
            obj=intervention_rei_metadata,
            obj_name=constants.CacheContents.INTERVENTION_REI_METADATA,
            relative_path=
            storage_format=
        )
    cache_writer.put(
        obj=rr_metadata,
        obj_name=constants.CacheContents.RR_METADATA,
        relative_path=
        storage_format=
    )
    cache_writer.put(
        obj=demographics,
        obj_name=constants.CacheContents.DEMOGRAPHICS,
        relative_path=
    )
    cache_writer.put(
        obj=cause_metadata,
        obj_name=constants.CacheContents.CAUSE_METADATA,
        relative_path=
        storage_format=
    )
    cache_writer.put(
        obj=age_metadata,
        obj_name=constants.CacheContents.AGE_METADATA,
        relative_path=
        storage_format=
    )
    cache_writer.put(
        obj=mediation_factors,
        obj_name=constants.CacheContents.MEDIATION_FACTORS,
        relative_path=
        storage_format=
    )


def write_exposure_min_and_max_cache(
    root_dir: pathlib.Path, exposure_min_max: pd.DataFrame
) -> None:
    """Write exposure min and max to cache."""
    manifest_path = manifest_path_of_dir(root_dir)
    cache_writer = FileBackedCacheWriter(manifest_path)
    cache_writer.put(
        obj=exposure_min_max,
        obj_name=constants.CacheContents.EXPOSURE_MIN_MAX,
        relative_path=
        storage_format=
    )


def write_exposure_tmrel_cache(
    root_dir: pathlib.Path,
    location_id: int,
    exposure: pd.DataFrame,
    exposure_sd: Optional[pd.DataFrame],
    tmrel: Optional[pd.DataFrame],
    mediator_exposure: Optional[pd.DataFrame] = None,
    mediator_exposure_sd: Optional[pd.DataFrame] = None,
    mediator_tmrel: Optional[pd.DataFrame] = None,
    intervention_coverage: Optional[pd.DataFrame] = None,
) -> None:
    """
    Save location-specific exposure and tmrel draws for use in downstream tasks.
    Some continuous risks have mediator-specific exposure and tmrel draws to save
    as well

    Arguments:
        root_dir: root of a paf calculator run
        location_id: location to save data for
        exposure: draws of exposure
        exposure_sd: draws of exposure_sd (if risk is not categorical)
        tmrel: draws of tmrel (if risk is not categorical and uses tmrel draws)
        mediator_exposure: draws of mediator's exposure
        mediator_exposure_sd: draws of mediator's exposure_sd (if risk is not categorical)
        mediator_tmrel: draws of mediator's tmrel (if risk is not categorical
            and uses tmrel draws)
        intervention_coverage: draws of intervention coverage
    """
    this_cache_dir = _create_location_specific_output_directory(root_dir, location_id)
    manifest_file = manifest_path_of_dir(this_cache_dir)
    cache_writer = FileBackedCacheWriter(manifest_file)

    cache_writer.put(
        obj=exposure,
        obj_name=constants.LocationCacheContents.EXPOSURE,
        relative_path=
        storage_format=
    )
    if exposure_sd is not None:
        cache_writer.put(
            obj=exposure_sd,
            obj_name=constants.LocationCacheContents.EXPOSURE_SD,
            relative_path=
            storage_format=
        )
    if tmrel is not None:
        cache_writer.put(
            obj=tmrel,
            obj_name=constants.LocationCacheContents.TMREL,
            relative_path=
            storage_format=
        )
    if mediator_exposure is not None:
        cache_writer.put(
            obj=mediator_exposure,
            obj_name=constants.LocationCacheContents.MEDIATOR_EXPOSURE,
            relative_path=
            storage_format=
        )
    if mediator_exposure_sd is not None:
        cache_writer.put(
            obj=mediator_exposure_sd,
            obj_name=constants.LocationCacheContents.MEDIATOR_EXPOSURE_SD,
            relative_path=
            storage_format=
        )
    if mediator_tmrel is not None:
        cache_writer.put(
            obj=mediator_tmrel,
            obj_name=constants.LocationCacheContents.MEDIATOR_TMREL,
            relative_path=
            storage_format=
        )
    if intervention_coverage is not None:
        cache_writer.put(
            obj=intervention_coverage,
            obj_name=constants.LocationCacheContents.INTERVENTION_COVERAGE,
            relative_path=
            storage_format=
        )
    return


def validate_cache_contents(root_dir: pathlib.Path) -> None:
    """Validate that all expected items are present and readable in the cache. We don't
    validate the exposure_min_max dataframe because it is not present at the start of a
    workflow. Validation of RR metadata for mediators is conditional on whether we have
    2-stage mediators.
    """
    items_to_skip = [
        constants.CacheContents.EXPOSURE_MIN_MAX,
        constants.CacheContents.INTERVENTION_REI_METADATA,
    ]
    rr_metadata = get(root_dir, constants.CacheContents.RR_METADATA)
    mediators = rr_metadata[rr_metadata["source"] == "delta"]["med_id"].unique().tolist()
    if not mediators:
        items_to_skip.append(constants.CacheContents.MEDIATOR_REI_METADATA)

    manifest_path = manifest_path_of_dir(root_dir)
    cache_reader = FileBackedCacheReader(manifest_path)
    cache_reader.validate_from_list(
        [
            item
            for item in asdict(constants.CacheContents()).values()
            if item not in items_to_skip
        ],
        require_complete=False,
    )


def get_mediation_delta_for_salt_sbp(release_id: int, n_draws: int) -> pd.DataFrame:
    """Get mediation delta specifically for diet high in salt and SBP.

    Mediation delta for salt and high systolic blood pressure (SBP) is a function
    of the prevalence of hypertension, defined as blood pressure above 140 mmHG.
    Delta draws for salt and SBP account for this with an additional dimension 'sbp_shift'
    that signifies blood pressure under and above 140.

    Downsamples if n_draws < 1000.

    Used to calculate delta for salt and sbp.

    Returns:
        delta dataframe with columns: rei_id, med_id, age_group_id, sbp_shift, delta_unit,
        mean_delta, draw_0, ..., draw_n
    """
    return dfutils.resample(
        pd.read_csv(constants.SALT_SBP_DELTA_FILE.format(release_id=release_id)),
        n_draws=n_draws,
    )


def delete_exposure_tmrel_cache(root_dir: pathlib.Path, location_id: int) -> None:
    """
    Delete cached location-specific datasets after respective PAF calc task.

    Arguments:
        root_dir: root of a paf calculator run
        location_id: location to save data for
    """
    this_cache_dir = get_location_specific_output_directory(root_dir, location_id)
    manifest_file = manifest_path_of_dir(this_cache_dir)
    cache_writer = FileBackedCacheWriter(manifest_file)

    files_to_delete = asdict(constants.LocationCacheContents())
    for f in files_to_delete.values():
        if f in cache_writer:
            cache_writer.delete(f)


def write_paf(
    df: pd.DataFrame, root_dir: pathlib.Path, location_id: int, is_unmediated: bool = False
) -> None:
    """
    Validate finished location/cause-specific PAF draws, convert from morbidity/mortality
    to measure_id, and save for use in downstream task. PAFs in this tool are sharded by
    location and cause, so if our input data contains more than one cause we will save
    to multiple output files.

    Sets invalid PAF draws to the mean of valid PAF draws by row.

    Arguments:
        df: pafs to save
        root_dir: root of a paf calculator run
        location_id: location to save data for
        is_unmediated: whether the df to save represents unmediated PAFs
    """
    draw_columns = list(df.filter(like="draw_").columns)

    # Reset PAF draws > 1 and infinite/null PAF draws to the mean of valid PAF draws.
    df = data_utils.enforce_paf_in_range(df, draw_columns)

    df = data_utils.convert_to_measure_id(df)

    df = df[constants.PAF_INDEX_COLUMNS + draw_columns].sort_values(
        constants.PAF_INDEX_COLUMNS
    )

    this_cache_dir = get_location_specific_output_directory(root_dir, location_id)
    prefix = 
    for cause_id in df["cause_id"].unique():
        df.query(f"cause_id == {cause_id}").to_hdf(
        )
    return


def write_paf_for_lbwsg_child_risk(
    df: pd.DataFrame, root_dir: pathlib.Path, location_id: int, rei_id: int
) -> None:
    """Write PAF results for a LBW/SG child risk.

    Running LBW/SG creates PAF results for three risks: the joint (LBW/SG) and the two
    children (LBW and SG). The joint PAFs are saved normally with write_paf. The child PAFs
    are saved in their own subdir, so root_dir is altered, the location-specific directory
    is created within the subdir, and then write_paf is called.

    Arguments:
        df: pafs to save
        root_dir: root of a paf calculator run
        location_id: location to save data for
        rei_id: REI ID of the child risk. Expected to either be
            constants.LOW_BIRTH_WEIGHT_REI_ID or constants.SHORT_GESTATION_REI_ID
    """
    subdir = (
        constants.LBW_SUBDIR
        if rei_id == constants.LOW_BIRTH_WEIGHT_REI_ID
        else constants.SG_SUBDIR
    )
    root_dir = 
    _create_location_specific_output_directory(root_dir, location_id)

    write_paf(df, root_dir, location_id, is_unmediated=False)


def write_paf_for_unavertable(
    df: pd.DataFrame, root_dir: pathlib.Path, location_id: int
) -> None:
    """Wrapper around write_paf to write PAF results for a Unavertable intervention.

    Running Avertable intervention PAFs creates PAF results for two REIs: both
    the Avertable and Unavertable counterfactuals. The Avertable PAFs are saved
    normally with write_paf. The Unavertable PAFs are saved in their own subdir,
    so root_dir is altered, the location-specific directory is created within
    the subdir, and then write_paf is called.
    """
    root_dir = 
    _create_location_specific_output_directory(root_dir, location_id)

    write_paf(df, root_dir, location_id, is_unmediated=False)


def read_paf(
    root_dir: pathlib.Path, location_id: int, cause_id: int, is_unmediated: bool = False
) -> pd.DataFrame:
    """Reads PAF draws from location-specific output directory.

    Arguments:
        root_dir: root of a paf calculator run
        location_id: location to read data for
        cause_id: cause to read data for
        is_unmediated: whether the df to read represents unmediated PAFs
    """
    this_cache_dir = get_location_specific_output_directory(root_dir, location_id)
    prefix = 
    return pd.read_hdf()


def write_paf_for_sev_calculator(df: pd.DataFrame, rei_id: int, location_id: int) -> None:
    """Write PAFs for SEV Calculator to directly read in.

    Similar to write_paf, validate finished location/cause-specific PAF draws and
    convert from morbidity/mortality to measure_id.

    Sets invalid PAF draws to the mean of valid PAF draws by row.

    Saves files to constants.SEV_CALCULATOR_INPUT_DIR by location.

    Args:
        df: pafs to save with columns rei_id, cause_id, location_id, year_id, sex_id,
            age_group_id, measure_id, draw_0, ..., draw_n-1
        rei_id: REI to save data for
        location_id: location to save data for
    """
    # Make the SEV Calculator input directory if it doesn't already exist
    input_dir = pathlib.Path(constants.SEV_CALCULATOR_INPUT_DIR.format(rei_id=rei_id))
    os.makedirs(input_dir, exist_ok=True)

    draw_columns = list(df.filter(like="draw_").columns)

    # Reset PAF draws > 1 and infinite/null PAF draws to the mean of valid PAF draws.
    df = data_utils.enforce_paf_in_range(df, draw_columns)

    df = data_utils.convert_to_measure_id(df)

    df = df[constants.PAF_INDEX_COLUMNS + draw_columns].sort_values(
        constants.PAF_INDEX_COLUMNS
    )

    # SEV Calculator expects CSV files by location
    df.to_csv()


def delete_paf_draws(root_dir: pathlib.Path, file_pattern: str, demographics: dict) -> None:
    """Deletes PAF draws.

    Deletes PAF draw file for each location in demographics (most detailed) and cause found.
    Assumes PAF draw files exactly match with given locations.
    """
    for location_id in demographics["location_id"]:
        # Match all causes on file with * and make sure there's something
        files = 
        if not files:
            raise RuntimeError(
                "Internal error: no PAF draw files found to delete with pattern "
                f"{file_pattern.format(location_id=location_id, cause_id='*')}"
            )

        for file in files:
            pathlib.Path(file).unlink()


def get_location_specific_output_directory(
    root_dir: pathlib.Path, location_id: int
) -> pathlib.Path:
    """Get the location-specific output directory within root_dir.

    Does not guarantee existance of the directory.
    """
    return 


def _create_location_specific_output_directory(
    root_dir: pathlib.Path, location_id: int
) -> pathlib.Path:
    """Creates the location-specific output directory within root_dir.

    No-op if directory already exists.
    """
    location_specific_output_dir = get_location_specific_output_directory(
        root_dir, location_id
    )
    location_specific_output_dir.mkdir(exist_ok=True)
    return location_specific_output_dir


def get_paf_model_versions_from_settings(
    settings: constants.PafCalculatorSettings,
) -> List[int]:
    """Pull PAF model version IDs from a PafCalculatorSettings object."""
    return get_paf_model_versions_from_dict(vars(settings))


def get_paf_model_versions_from_dict(settings_dict: Dict[str, Any]) -> List[int]:
    """Pull PAF model version IDs from a settings dictionary, using the key pattern
    paf.*model_version_id. Only non-None PAF model version IDs are returned. Sort
    the return value for testing convenience.

    We should always have at least one valid PAF model version ID in the settings,
    so raise a RuntimeError if none are found.
    """
    paf_model_version_ids = [
        value
        for key, value in settings_dict.items()
        if re.search("paf.*model_version_id", key) is not None and value is not None
    ]

    if not paf_model_version_ids:
        raise RuntimeError(
            "Failed to parse any valid PAF model version IDs from settings dictionary "
            f"{settings_dict}."
        )

    return sorted(paf_model_version_ids)
