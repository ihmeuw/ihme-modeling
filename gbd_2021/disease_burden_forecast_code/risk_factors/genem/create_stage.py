from typing import Any, Dict, Iterable, List, Optional

from fhs_lib_database_interface.lib.fhs_lru_cache import fhs_lru_cache
from fhs_lib_file_interface.lib.version_metadata import VersionMetadata
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_orchestration_interface.lib import cluster_tools
from fhs_lib_year_range_manager.lib.year_range import YearRange
from jobmon.client.api import Tool
from jobmon.client.task import Task
from typeguard import typechecked

from fhs_lib_genem.lib.constants import JobConstants, OrchestrationConstants
from fhs_lib_genem.lib.model_restrictions import ModelRestrictions
from fhs_lib_genem.run.task import (
    get_arc_task,
    get_collect_submodels_task,
    get_model_weights_task,
    get_stagewise_mrbrt_task,
)

INTERCEPT_SHIFT_FROM_REFERENCE_DEFAULT = True


@typechecked
def create_genem_tasks(
    tool: Tool,
    cluster_project: str,
    entities: Iterable[str],
    intrinsic: Optional[Dict[str, bool]],
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    transform: str,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
    national_only: bool,
    scenario_quantiles: bool,
    model_restrictions: ModelRestrictions,
    subfolder: Optional[str],
    uncross_scenarios: bool,
    age_standardize: bool,
    remove_zero_slices: bool,
    rescale_ages: bool,
    log_level: Optional[str],
    intercept_shift_transform: str,
    intercept_shift_from_reference: bool = INTERCEPT_SHIFT_FROM_REFERENCE_DEFAULT,
    run_pv: bool = False,
    run_forecast: bool = False,
    run_model_weights: bool = False,
    run_collect_models: bool = False,
) -> List[Task]:
    """Create all the tasks for the genem."""
    validate_transform_specification(
        transform=transform, intercept_shift_transform=intercept_shift_transform
    )

    run_all_steps = not any([run_pv, run_forecast, run_model_weights, run_collect_models])

    if run_all_steps or run_pv:
        # run all genem submodels for predictive validity (pv) in parallel
        pv_tasks = forecast_submodels_stage(
            tool=tool,
            entities=entities,
            intrinsic=intrinsic,
            cluster_project=cluster_project,
            stage=stage,
            predictive_validity=True,
            versions=versions,
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            transform=transform,
            mrbrt_cov_stage1=mrbrt_cov_stage1,
            mrbrt_cov_stage2=mrbrt_cov_stage2,
            national_only=national_only,
            scenario_quantiles=scenario_quantiles,
            remove_zero_slices=remove_zero_slices,
            subfolder=subfolder,
            age_standardize=age_standardize,
            rescale_ages=rescale_ages,
            log_level=log_level,
        )
    else:
        pv_tasks = []

    if run_all_steps or run_forecast:
        # run all genem submodel forecasts in parallel
        forecast_tasks = forecast_submodels_stage(
            tool=tool,
            cluster_project=cluster_project,
            entities=entities,
            intrinsic=intrinsic,
            stage=stage,
            predictive_validity=False,
            versions=versions,
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            transform=transform,
            mrbrt_cov_stage1=mrbrt_cov_stage1,
            mrbrt_cov_stage2=mrbrt_cov_stage2,
            national_only=national_only,
            scenario_quantiles=scenario_quantiles,
            remove_zero_slices=remove_zero_slices,
            subfolder=subfolder,
            age_standardize=age_standardize,
            rescale_ages=rescale_ages,
            log_level=log_level,
        )
    else:
        forecast_tasks = []

    if run_all_steps or run_model_weights:
        # get model weights from pv results.
        # This step calculates weights for the collection step, using the predictive validity
        # calculated in the pv step.
        model_weights_tasks = model_weights_stage(
            tool=tool,
            cluster_project=cluster_project,
            out_version=versions.get_version_metadata(past_or_future="future", stage=stage),
            gbd_round_id=gbd_round_id,
            draws=draws,
            mrbrt_cov_stage1=mrbrt_cov_stage1,
            mrbrt_cov_stage2=mrbrt_cov_stage2,
            model_restrictions=model_restrictions,
            log_level=log_level,
        )
        for model_weights_task in model_weights_tasks:
            for pv_task in pv_tasks:
                model_weights_task.add_upstream(pv_task)
    else:
        model_weights_tasks = []

    if run_all_steps or run_collect_models:
        # collect draws from submodels to make genem
        # This step combines the forecasts generated in the forecast step, using the weights
        # calculated during the weights step.

        transform_spec = (
            transform if intercept_shift_transform == "none" else intercept_shift_transform
        )

        collect_models_tasks = collect_submodels_stage(
            tool=tool,
            entities=entities,
            cluster_project=cluster_project,
            stage=stage,
            versions=versions,
            gbd_round_id=gbd_round_id,
            years=years,
            transform=transform_spec,
            intercept_shift_from_reference=intercept_shift_from_reference,
            uncross_scenarios=uncross_scenarios,
            log_level=log_level,
        )
        for collect_models_task in collect_models_tasks:
            for forecast_task in forecast_tasks:
                collect_models_task.add_upstream(forecast_task)

            for model_weights_task in model_weights_tasks:
                collect_models_task.add_upstream(model_weights_task)
    else:
        collect_models_tasks = []

    return pv_tasks + forecast_tasks + model_weights_tasks + collect_models_tasks


@typechecked
def forecast_submodels_stage(
    tool: Tool,
    cluster_project: str,
    entities: Iterable[str],
    intrinsic: Optional[Dict[str, bool]],
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    transform: str,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
    predictive_validity: bool,
    national_only: bool,
    scenario_quantiles: bool,
    remove_zero_slices: bool,
    subfolder: Optional[str],
    age_standardize: bool,
    rescale_ages: bool,
    log_level: Optional[str],
) -> List[Task]:
    """Make tasks for genem submodels.

    The submodels are:
    1.) arc
    2.) mrbrt

    Args:
        tool: Jobmon tool to associate tasks with
        cluster_project: cluster project to run tasks under
        entities: the entities to forecast
        intrinsic: optional mapping of entities to intrinsic values. If not provided,
            its assumed that no entities are intrinsic.
        stage (str): stage to model
        versions (Versions): versions of all inputs, outputs and covariates (past and future).
        precalculated_version (str): version name of precalculated sevs.
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end.
        draws (int): number of draws to keep.
        transform (str): transformation to perform on data before modeling.
        mrbrt_cov_stage1 (str): The covariate name to be used in the MRBRT first stage.
        mrbrt_cov_stage2 (str): The covariate name to be used in the MRBRT second stage.
        predictive_validity (bool): whether this is a predictive-validity stage.
        national_only (bool): whether to only compute for nationals.
        scenario_quantiles (bool): If True, then use the scenario quantiles parameter in
            the stagewise-mrbrt model specification.
        subfolder (str): subfolder to read/write
        age_standardize (bool): whether to age-standardize the data before modeling
        rescale_ages (bool): whether to rescale during ARC age standardization. We are
            currently only setting this to true for the sevs pipeline.
        log_level: log_level to use for tasks

    Returns:
        List of tasks
    """
    subfolder = subfolder or ""

    tasks = []
    for entity in entities:
        entity_intrinsic = intrinsic[entity] if intrinsic else False

        tasks += genem_tasks(
            tool=tool,
            cluster_project=cluster_project,
            entity=entity,
            stage=stage,
            versions=versions,
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            transform=transform,
            mrbrt_cov_stage1=mrbrt_cov_stage1,
            mrbrt_cov_stage2=mrbrt_cov_stage2,
            predictive_validity=predictive_validity,
            national_only=national_only,
            scenario_quantiles=scenario_quantiles,
            remove_zero_slices=remove_zero_slices,
            intrinsic=entity_intrinsic,
            subfolder=OrchestrationConstants.SUBFOLDER if entity_intrinsic else subfolder,
            age_standardize=age_standardize,
            rescale_ages=rescale_ages,
            log_level=log_level,
        )

    return tasks


@typechecked
def genem_tasks(
    tool: Tool,
    cluster_project: str,
    entity: str,
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    transform: str,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
    predictive_validity: bool,
    national_only: bool,
    scenario_quantiles: bool,
    remove_zero_slices: bool,
    intrinsic: bool,
    subfolder: Optional[str],
    age_standardize: bool,
    rescale_ages: bool,
    log_level: Optional[str],
) -> List[Task]:
    """Make tasks for the genem submodels.

    Args:
        tool: Jobmon tool to associate tasks with
        cluster_project: cluster project to run tasks under
        entity (str): "all", or individual risk/cause-risk.
        stage (str): stage to forecast.
        versions (Versions): versions of all inputs, outputs and covariates (past and future).
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end.
        draws (int): number of draws to keep.
        transform (str): transformation to perform on data before modeling.
        mrbrt_cov_stage1 (str): The covariate name to be used in the MRBRT frst stage.
        mrbrt_cov_stage2 (str): The covariate name to be used in the MRBRT second stage.
        predictive_validity (bool): whether this is a predictive-validity job.
        national_only (bool): whether to only compute for nationals.
        scenario_quantiles (bool): If True, then use the scenario quantiles parameter in
            the stagewise-mrbrt model specification.
        intrinsic (bool): whether entity is "intrinsic" (SEV only).
        subfolder (Optional[str]): input/ouput data subfolder.
        age_standardize (bool): whether to age-standardize before modeling.
        rescale_ages (bool): whether to rescale during ARC age standardization. We are
            currently only setting this to true for the sevs pipeline.
        log_level: log_level to use for tasks

    Returns:
        List of tasks
    """
    model_name_map = make_model_name_map(
        mrbrt_cov_stage1=mrbrt_cov_stage1,
        mrbrt_cov_stage2=mrbrt_cov_stage2,
    )
    pv_forecast_start = years.past_end - (OrchestrationConstants.N_HOLDOUT_YEARS - 1)
    pv_years = YearRange(years.past_start, pv_forecast_start, years.past_end)

    arc_compute_resources = get_compute_resources(
        memory_gb=JobConstants.JOB_MEMORY,
        cluster_project=cluster_project,
        runtime=JobConstants.DEFAULT_RUNTIME,
    )

    mrbrt_compute_resources = get_compute_resources(
        memory_gb=JobConstants.MRBRT_MEM_GB,
        cluster_project=cluster_project,
        runtime=JobConstants.MRBRT_RUNTIME,
    )

    tasks = []
    arc_task = get_arc_task(
        tool=tool,
        compute_resources=arc_compute_resources,
        entity=entity,
        stage=stage,
        intrinsic=intrinsic,
        subfolder=subfolder,
        versions=versions,
        model_name=model_name_map["arc"],
        omega_min=OrchestrationConstants.OMEGA_MIN,
        omega_max=OrchestrationConstants.OMEGA_MAX,
        omega_step_size=OrchestrationConstants.OMEGA_STEP_SIZE,
        transform=transform,
        truncate=True,
        truncate_quantiles=OrchestrationConstants.ARC_TRUNCATE_QUANTILES,
        replace_with_mean=False,
        reference_scenario=OrchestrationConstants.ARC_REFERENCE_SCENARIO,
        years=pv_years if predictive_validity else years,
        gbd_round_id=gbd_round_id,
        cap_forecasts=False,
        cap_quantiles=None,
        national_only=national_only,
        age_standardize=age_standardize,
        rescale_ages=rescale_ages,
        predictive_validity=predictive_validity,
        remove_zero_slices=remove_zero_slices,
        log_level=log_level,
    )

    tasks.append(arc_task)

    for mrbrt_cov_stage2, mrbrt_name in model_name_map["mrbrt"].items():
        stagewise_mrbrt_task = get_stagewise_mrbrt_task(
            tool=tool,
            compute_resources_dict=mrbrt_compute_resources,
            years=pv_years if predictive_validity else years,
            gbd_round_id=gbd_round_id,
            versions=versions,
            model_name=mrbrt_name,
            stage=stage,
            entity=entity,
            draws=draws,
            mrbrt_cov_stage1=mrbrt_cov_stage1,
            mrbrt_cov_stage2=mrbrt_cov_stage2,
            omega_min=OrchestrationConstants.OMEGA_MIN,
            omega_max=OrchestrationConstants.OMEGA_MAX,
            step=OrchestrationConstants.OMEGA_STEP_SIZE,
            transform=transform,
            predictive_validity=predictive_validity,
            national_only=national_only,
            scenario_quantiles=scenario_quantiles,
            remove_zero_slices=remove_zero_slices,
            intrinsic=intrinsic,
            age_standardize=age_standardize,
            subfolder=subfolder,
            log_level=log_level,
        )
        tasks.append(stagewise_mrbrt_task)

    return tasks


@fhs_lru_cache(1)
def make_model_name_map(mrbrt_cov_stage1: str, mrbrt_cov_stage2: str) -> dict:
    """Determine arc/mrbrt model_names based on covariates.

    NOTE that "arc" maps to a single string model name, whereas "mrbrt" maps to a dict of
    model names.

    Args:
        mrbrt_cov_stage1 (str): The covariate name to be used in the MRBRT first stage.
        mrbrt_cov_stage2 (str): The covariate name to be used in the MRBRT second stage.

    Returns:
        (dict): dictionary mapping submodel type to version metadata.
    """
    mrbrt_map = {
        cov_stage2: f"{mrbrt_cov_stage1}_{cov_stage2}" for cov_stage2 in [mrbrt_cov_stage2]
    }
    return {"arc": "arc", "mrbrt": mrbrt_map}


@typechecked
def model_weights_stage(
    tool: Tool,
    cluster_project: str,
    out_version: VersionMetadata,
    gbd_round_id: int,
    draws: int,
    model_restrictions: ModelRestrictions,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
    log_level: Optional[str],
) -> List[Task]:
    """Generate the task for the model weights calculation."""
    model_name_map = make_model_name_map(
        mrbrt_cov_stage1=mrbrt_cov_stage1,
        mrbrt_cov_stage2=mrbrt_cov_stage2,
    )
    submodel_names = [model_name_map["arc"]] + list(model_name_map["mrbrt"].values())

    compute_resources = get_compute_resources(
        memory_gb=JobConstants.MODEL_WEIGHTS_MEM_GB,
        runtime=JobConstants.DEFAULT_RUNTIME,
        cluster_project=cluster_project,
    )

    task = get_model_weights_task(
        tool=tool,
        compute_resources=compute_resources,
        submodel_names=submodel_names,
        subfolder=OrchestrationConstants.SUBFOLDER,
        out_version=out_version,
        gbd_round_id=gbd_round_id,
        draws=draws,
        model_restrictions=model_restrictions,
        log_level=log_level,
    )
    return [task]


@typechecked
def collect_submodels_stage(
    tool: Tool,
    cluster_project: str,
    entities: Iterable[str],
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    transform: str,
    intercept_shift_from_reference: bool,
    uncross_scenarios: bool,
    log_level: Optional[str],
) -> List[Task]:
    """Generate tasks for collecting genem results.

    Args:
        tool: Jobmon tool to associate tasks with
        cluster_project: cluster project to run tasks under
        entities: entities to forecast.
        stage (str): stage to forecast.
        versions (Versions): versions with input and output versions.
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end.
        transform (str): name of transformation to use when intercept shifting.
        intercept_shift_from_reference (bool): If True, and we are in multi-scenario mode, then
            the intercept-shifting during the above `transform` is calculated from the
            reference scenario but applied to all scenarios; if False then each scenario will
            get its own shift amount.
        uncross_scenarios (bool): whether to fix crossed scenarios. This is currently only used
            for sevs and should be deprecated soon.
        remove_zero_slices (bool): If True, remove zero-slices along certain dimensions, when
            pre-processing inputs, and add them back in to outputs.
        log_level: log_level to use for tasks
    """
    compute_resources = get_compute_resources(
        memory_gb=JobConstants.COLLECT_SUBMODELS_MEM_GB,
        runtime=JobConstants.COLLECT_SUBMODELS_RUNTIME,
        cluster_project=cluster_project,
        cores=JobConstants.COLLECT_SUBMODELS_NUM_CORES,
    )

    tasks = []
    for rei in entities:
        task = get_collect_submodels_task(
            compute_resources=compute_resources,
            tool=tool,
            entity=rei,
            stage=stage,
            versions=versions,
            gbd_round_id=gbd_round_id,
            years=years,
            transform=transform,
            intercept_shift_from_reference=intercept_shift_from_reference,
            uncross_scenarios=uncross_scenarios,
            log_level=log_level,
        )
        tasks.append(task)

    return tasks


def get_compute_resources(
    memory_gb: int,
    cluster_project: str,
    runtime: str,
    cores: int = JobConstants.DEFAULT_CORES,
) -> Dict[str, Any]:
    """Return a dictionary containing keys & values required for Jobmon Task creation."""
    error_logs_dir, output_logs_dir = cluster_tools.get_logs_dirs()

    return dict(
        memory=f"{memory_gb}G",
        cores=cores,
        runtime=runtime,
        project=cluster_project,
        queue=JobConstants.DEFAULT_QUEUE,
        stderr=error_logs_dir,
        stdout=output_logs_dir,
    )


def validate_transform_specification(transform: str, intercept_shift_transform: str) -> None:
    """Ensure ``intercept_shift_transform`` is not none when ``transform`` is no-transform."""
    if transform == "no-transform" and intercept_shift_transform == "none":
        raise ValueError(
            "When ``--transform no-transform`` is set, ``--intercept-shift-transform`` cannot "
            "be ``none``."
        )
