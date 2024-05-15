from typing import Any, Dict, List, Optional, Tuple

from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.symlink_file import symlink_file_to_directory
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_genem.lib.model_restrictions import ModelRestrictions
from fhs_lib_genem.run.create_stage import create_genem_tasks
from fhs_lib_orchestration_interface.lib import cluster_tools
from fhs_lib_year_range_manager.lib.year_range import YearRange
from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.workflow import Workflow
from tiny_structured_logger.lib import fhs_logging
from typeguard import typechecked

from fhs_pipeline_sevs.lib.mediation import (
    get_intrinsic_sev_pairs,
    get_mediator_upstreams,
    get_sev_intrinsic_map,
)
from fhs_pipeline_sevs.run.task import (
    get_combine_future_mediator_task,
    get_compute_future_mediator_task,
    get_compute_past_intrinsic_sev_task,
)

MRBRT_COV_STAGE1 = "sdi"
MRBRT_COV_STAGE2 = "sdi"
TRANSFORM = "logit"

# Certain risks have SEVs forecasted via processes completely separate
# from the main forecasting pipeline; i.e. custom-made by other teams.
# These files reside in the --precalculated-version input directory
# specified in console.py.  "smoking_direct" is currently custom-made.
PRECOMPUTED_SEVS = ["smoking_direct"]

TIMEOUT = 260000  # giving the entire workflow 3 days to run

logger = fhs_logging.get_logger()


def create_workflow(
    wf_version: Optional[str],
    cluster_project: str,
    past_version: str,
    rr_version: str,
    past_sdi_version: str,
    future_sdi_version: str,
    out_version: str,
    precalculated_version: str,
    draws: int,
    years: YearRange,
    gbd_round_id: int,
    log_level: Optional[str],
    model_restrictions: ModelRestrictions,
    run_stage_1: bool = False,
    run_stage_2a: bool = False,
    run_stage_2b: bool = False,
    run_stage_3: bool = False,
    run_stage_4: bool = False,
    run_stage_5: bool = False,
    cluster_name: str = cluster_tools.identify_cluster(),
) -> Workflow:
    """Construct and return a workflow."""
    # if all flags are False, then run all stages. Otherwise, only run the stages which flags
    # are True
    run_all_stages = not any(
        [run_stage_1, run_stage_2a, run_stage_2b, run_stage_3, run_stage_4, run_stage_5]
    )
    # stages 2-5 are the ensemble model
    run_ensemble = any([run_all_stages, run_stage_2a, run_stage_2b, run_stage_3, run_stage_4])

    tool = Tool(name=TOOL_NAME)

    wf_args = f"{PIPELINE_NAME}_{cluster_tools.get_wf_version(wf_version)}"

    # Initialize new workflow.
    workflow = tool.create_workflow(
        name=PIPELINE_NAME,
        workflow_args=wf_args,
        default_cluster_name=cluster_name,
    )

    if run_all_stages or run_stage_1:
        # (1) compute past intrinsic SEVs
        stage_1_tasks = compute_past_intrinsic_sevs_stage(
            tool=tool,
            cluster_project=cluster_project,
            acause="all",
            rei="all",
            sev_version=past_version,
            rr_version=rr_version,
            gbd_round_id=gbd_round_id,
            draws=draws,
            log_level=log_level,
        )
        workflow.add_tasks(stage_1_tasks)
    else:
        stage_1_tasks = []

    if run_ensemble:
        _symlink_precomputed(
            gbd_round_id=gbd_round_id,
            precalculated_version=precalculated_version,
            out_version=out_version,
        )

        intrinsic_map = get_sev_intrinsic_map(gbd_round_id)
        ensemble_tasks = create_genem_tasks(
            tool=tool,
            cluster_project=cluster_project,
            entities=intrinsic_map.keys(),
            intrinsic=intrinsic_map,
            stage="sev",
            versions=Versions(
                "FILEPATH",
                "FILEPATH",
                "FILEPATH",
                "FILEPATH",
            ),
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            transform=TRANSFORM,
            intercept_shift_transform="none",
            mrbrt_cov_stage1=MRBRT_COV_STAGE1,
            mrbrt_cov_stage2=MRBRT_COV_STAGE2,
            national_only=False,
            scenario_quantiles=True,
            subfolder=None,
            uncross_scenarios=True,
            age_standardize=True,
            remove_zero_slices=True,
            rescale_ages=True,
            model_restrictions=model_restrictions,
            log_level=log_level,
            run_pv=run_stage_2a,
            run_forecast=run_stage_2b,
            run_model_weights=run_stage_3,
            run_collect_models=run_stage_4,
        )
    else:
        ensemble_tasks = []

    for ensemble_task in ensemble_tasks:
        for task_1 in stage_1_tasks:
            ensemble_task.add_upstream(task_1)

    workflow.add_tasks(ensemble_tasks)

    if run_all_stages or run_stage_5:
        # (5) compute future mediator total sevs
        stage_5_tasks = future_mediator_stage(
            tool=tool,
            cluster_project=cluster_project,
            acause="all",
            rei="all",
            sev_version=out_version,
            past_sev_version=past_version,
            rr_version=rr_version,
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            log_level=log_level,
        )

        for task_5 in stage_5_tasks:
            for ensemble_task in ensemble_tasks:
                task_5.add_upstream(ensemble_task)

        workflow.add_tasks(stage_5_tasks)

    workflow.bind()

    return workflow


def run_pipeline(
    wf_version: Optional[str],
    cluster_project: str,
    past_version: str,
    rr_version: str,
    past_sdi_version: str,
    future_sdi_version: str,
    out_version: str,
    precalculated_version: str,
    draws: int,
    years: YearRange,
    gbd_round_id: int,
    log_level: Optional[str],
    model_restrictions: ModelRestrictions,
    run_stage_1: bool = False,
    run_stage_2a: bool = False,
    run_stage_2b: bool = False,
    run_stage_3: bool = False,
    run_stage_4: bool = False,
    run_stage_5: bool = False,
    cluster_name: str = cluster_tools.identify_cluster(),
) -> Tuple[str, Workflow]:
    """Construct and run a workflow."""
    workflow = create_workflow(
        wf_version=wf_version,
        cluster_project=cluster_project,
        past_version=past_version,
        rr_version=rr_version,
        past_sdi_version=past_sdi_version,
        future_sdi_version=future_sdi_version,
        out_version=out_version,
        precalculated_version=precalculated_version,
        draws=draws,
        years=years,
        gbd_round_id=gbd_round_id,
        log_level=log_level,
        model_restrictions=model_restrictions,
        run_stage_1=run_stage_1,
        run_stage_2a=run_stage_2a,
        run_stage_2b=run_stage_2b,
        run_stage_3=run_stage_3,
        run_stage_4=run_stage_4,
        run_stage_5=run_stage_5,
        cluster_name=cluster_name,
    )
    workflow.run(seconds_until_timeout=TIMEOUT, resume=True)
    logger.info(
        "Workflow ended",
        bindings=dict(status=workflow.workflow_id),
    )

    return workflow


@typechecked()
def compute_past_intrinsic_sevs_stage(
    tool: Tool,
    cluster_project: str,
    acause: str,
    rei: str,
    gbd_round_id: int,
    sev_version: str,
    rr_version: str,
    draws: int,
    log_level: Optional[str],
) -> List[Task]:
    """Make tasks for computing past intrinsic sevs."""
    # Get all the cause-mediator pairs from mediation matrix
    pairs = get_intrinsic_sev_pairs(gbd_round_id)

    # Now filter the pairs based on provided acause/rei inputs
    if acause != "all":
        pairs = filter(lambda x: x[0] == acause, pairs)
    if rei != "all":
        pairs = filter(lambda x: x[1] == rei, pairs)

    compute_resources = get_compute_resources(
        memory_gb=int(BASE_MEMORY_GB + JOB_MEMORY_GB_PER_DRAW * draws / 1000),
        cluster_project=cluster_project,
        runtime="16:00:00",
    )

    tasks = []
    for task_acause, task_rei in pairs:
        task = get_compute_past_intrinsic_sev_task(
            tool=tool,
            compute_resources=compute_resources,
            acause=task_acause,
            rei=task_rei,
            gbd_round_id=gbd_round_id,
            sev_version=sev_version,
            rr_version=rr_version,
            draws=draws,
            log_level=log_level,
        )
        tasks.append(task)

    return tasks


@typechecked()
def future_mediator_stage(
    tool: Tool,
    cluster_project: str,
    acause: str,
    rei: str,
    gbd_round_id: int,
    sev_version: str,
    past_sev_version: str,
    rr_version: str,
    years: YearRange,
    draws: int,
    log_level: Optional[str],
) -> List[Task]:
    """Depending on the given acause and rei, computes/exports either.

    1.) all pairs for a given acause
    2.) all pairs for a given rei
    3.) all cause-risk pairs


    Args:
        tool: Jobmon tool to associate tasks with
        cluster_project: cluster project to run tasks under
        acause (str): acause.
        rei (str): risk j fomr (1).
        gbd_round_id (int): gbd round id.
        sev_version (str): version of future SEV.
        past_sev_version (str): version of past SEV.
        rr_version (str): version of past RR.
        years (YearRange): past_start:forecast_start:forecast_end.
        draws (int): number of draws kept in process.
        log_level: log_level to use for tasks
    """
    # Get all the cause-mediator pairs from mediation matrix
    pairs = get_intrinsic_sev_pairs(gbd_round_id)

    # Now filter the pairs based on provided acause/rei inputs
    if acause != "all":
        pairs = filter(lambda x: x[0] == acause, pairs)
    if rei != "all":
        pairs = filter(lambda x: x[1] == rei, pairs)

    # first set up all the compute/combine tasks for the cause-mediator pairs
    mediator_upstreams_dict = get_mediator_upstreams(gbd_round_id)

    compute_tasks_by_risk = {}
    combine_tasks_by_risk = {}
    for mediator in mediator_upstreams_dict.keys():
        acauses = [x[0] for x in pairs if x[1] == mediator]

        if acauses:
            compute_tasks, combine_task = _isev_to_sev_batch_tasks(
                tool=tool,
                cluster_project=cluster_project,
                acauses=acauses,
                rei=mediator,
                gbd_round_id=gbd_round_id,
                sev_version=sev_version,
                past_sev_version=past_sev_version,
                rr_version=rr_version,
                years=years,
                draws=draws,
                log_level=log_level,
            )
            compute_tasks_by_risk[mediator] = compute_tasks
            combine_tasks_by_risk[mediator] = combine_task

    mediators = compute_tasks_by_risk.keys()

    for med in mediators:  # these are mediators
        for med_2 in mediators:  # another mediator
            if med_2 != med and med_2 in mediator_upstreams_dict[med]:
                for task in compute_tasks_by_risk[med]:
                    task.add_upstream(combine_tasks_by_risk[med_2])

    tasks = sum(list(compute_tasks_by_risk.values()), []) + list(
        combine_tasks_by_risk.values()
    )

    return tasks


@typechecked()
def _isev_to_sev_batch_tasks(
    tool: Tool,
    cluster_project: str,
    acauses: List[str],
    rei: str,
    gbd_round_id: int,
    sev_version: str,
    past_sev_version: str,
    rr_version: str,
    years: YearRange,
    draws: int,
    log_level: Optional[str],
) -> Tuple[List[Task], Task]:
    """Compute SEVs for a given set of cause-risk iSEVs.

    Given cause-risk iSEV pairs, perform the following:
    1.) make a task to compute the cause-risk SEVs
    2.) make a task to combine the cause-risk SEVs into their risk SEVs, held on (1)

    Args:
        tool: Jobmon tool to associate tasks with
        cluster_project: cluster project to run tasks under
        acauses (List[str]): List of acauses to make cause-risk-specific
            compute_future_mediator tasks for
        rei (str): REI to create combine_future_mediator task for
        gbd_round_id (int): gbd round id.
        sev_version (str): version of future SEV.
        past_sev_version (str): version of past SEV.
        rr_version (str): version of past RR.
        years (YearRange): past_start:forecast_start:forecast_end.
        draws (int): number of draws kept in process.
        log_level: log_level to use for tasks

    Returns:
        Tuple[List[Task], Task]: the cause-risk-specific compute_future_mediator tasks, and
            the risk-specific combine_future_mediator task -- in that order
    """
    compute_resources = get_compute_resources(
        memory_gb=int(BASE_MEMORY_GB + JOB_MEMORY_GB_PER_DRAW * draws / 1000),
        runtime="16:00:00",
        cluster_project=cluster_project,
    )

    compute_tasks = []
    for acause in acauses:
        compute_task = get_compute_future_mediator_task(
            tool=tool,
            compute_resources=compute_resources,
            acause=acause,
            rei=rei,
            gbd_round_id=gbd_round_id,
            sev_version=sev_version,
            past_sev_version=past_sev_version,
            rr_version=rr_version,
            years=years,
            draws=draws,
            log_level=log_level,
        )

        compute_tasks.append(compute_task)

    combine_task = get_combine_future_mediator_task(
        tool=tool,
        compute_resources=compute_resources,
        rei=rei,
        gbd_round_id=gbd_round_id,
        past_sev_version=past_sev_version,
        sev_version=sev_version,
        rr_version=rr_version,
        years=years,
        log_level=log_level,
    )

    for compute_task in compute_tasks:
        combine_task.add_upstream(compute_task)

    return compute_tasks, combine_task


def _symlink_precomputed(
    gbd_round_id: int, precalculated_version: str, out_version: str
) -> None:
    """Symlink all the SEVs that have been computed via other processes.

    Args:
        gbd_round_id (int): gbd round id.
        precalculated_version (str): version where pre-computed SEVs are.
        out_version (str): the final output version for all the SEVs.
    """
    for entity in PRECOMPUTED_SEVS:
        try:
            precomputed_dir = FBDPath(
                gbd_round_id=gbd_round_id,
                past_or_future="future",
                stage="sev",
                version=precalculated_version,
            )

            out_dir = precomputed_dir.set_version(out_version)

            symlink_file_to_directory(precomputed_dir / (entity + ".nc"), out_dir)

        except FileExistsError:
            logger.info(f"{entity}.nc already exists.")

        except FileNotFoundError:
            raise FileNotFoundError(f"{entity}.nc not found.")


def get_compute_resources(
    memory_gb: int,
    cluster_project: str,
    runtime: str,
    cores: int = DEFAULT_CORES,
) -> Dict[str, Any]:
    """Return a dictionary containing keys & values required for Jobmon Task creation."""
    error_logs_dir, output_logs_dir = cluster_tools.get_logs_dirs()

    return dict(
        memory=f"{memory_gb}G",
        cores=cores,
        runtime=runtime,
        project=cluster_project,
        queue=DEFAULT_QUEUE,
        stderr=error_logs_dir,
        stdout=output_logs_dir,
    )
