from pathlib import Path

import click

from jobmon.core.constants import WorkflowRunStatus

from ihme_cc_paf_calculator.lib import constants, dbio, io_utils, logging_utils, workflow

logger = logging_utils.module_logger(__name__)


@click.command
@click.option(
    "model_root_dir",
    "--model_root_dir",
    type=str,
    required=True,
    help="Path to a model root directory.",
)
@click.option(
    "conda_env",
    "--conda_env",
    type=str,
    required=True,
    help=(
        "Full path to conda environment for launching tasks, for example "
    ),
)
def _launch_workflow(model_root_dir: str, conda_env: str) -> None:
    """Click command wrapper, creates and launches a jobmon workflow."""
    launch_workflow(model_root_dir=model_root_dir, conda_env=conda_env)


def launch_workflow(model_root_dir: str, conda_env: str) -> None:
    """Creates and launches a jobmon workflow."""
    model_root_dir = Path(model_root_dir)

    # Get the config settings
    io_utils.validate_cache_contents(model_root_dir)
    settings = io_utils.read_settings(model_root_dir)
    demographics = io_utils.get(model_root_dir, constants.CacheContents.DEMOGRAPHICS)
    rr_metadata = io_utils.get(model_root_dir, constants.CacheContents.RR_METADATA)
    rei_metadata = io_utils.get(model_root_dir, constants.CacheContents.REI_METADATA)
    mvids = io_utils.get(model_root_dir, constants.CacheContents.MODEL_VERSIONS)
    exposure_me_ids = (
        mvids.loc[mvids["draw_type"] == "exposure"]["modelable_entity_id"].unique().tolist()
    )
    is_continuous = (
        rei_metadata["rei_calculation_type"].iat[0] == constants.CalculationType.CONTINUOUS
    )

    workflow_manager = workflow.WorkflowManager(
        conda_env=conda_env,
        settings=settings,
        location_ids=demographics["location_id"],
        cause_ids=rr_metadata["cause_id"].unique().tolist(),
        exposure_me_ids=exposure_me_ids,
        is_continuous=is_continuous,
    )
    logger.info(f"resume: {settings.resume}")
    logger.info("Creating workflow")
    wf = workflow_manager.create_workflow()
    wf.bind()
    logger.info(f"Jobmon GUI {constants.JOBMON_URL.format(workflow_id=wf.workflow_id)}")
    logger.info("Running workflow")
    # 16 days (long.q max runtime) = 1382400 seconds
    workflow_run_status = wf.run(seconds_until_timeout=1382400, resume=settings.resume)
    if workflow_run_status != WorkflowRunStatus.DONE:
        # If the workflow run did not finish successfully as DONE, mark any PAF models with
        # submitted status as failed.
        dbio.fail_submitted_paf_models(settings=settings)


if __name__ == "__main__":
    _launch_workflow()
