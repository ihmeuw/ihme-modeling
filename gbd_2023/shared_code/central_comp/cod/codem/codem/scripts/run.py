"""
Main script for running CODEm functions for one
instance of a CODEm model. This is an executable
called by the jobmon tasks.
"""

import json
import logging
import os
import shutil
import sys

from jobmon.core.constants import WorkflowStatus
from jobmon.core.exceptions import WorkflowAlreadyComplete

from codem.data.parameters import get_model_parameters
from codem.joblaunch.args import get_args
from codem.joblaunch.CODEmStep import StepTaskGenerator
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.joblaunch.run_utils import change_model_status, update_code_version
from codem.joblaunch.step_profiling import inspect_parameters, inspect_submodels
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import ModelerAlert, setup_logging
from codem.reference.paths import ModelPaths, cleanup_files

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main script for running a CODEm model."""
    args = get_args()

    model_version_id = args.model_version_id
    conn_def = args.conn_def
    old_covariates_mvid = args.old_covariates_mvid
    debug_mode = args.debug_mode
    additional_resources = args.step_resources
    additional_resources = additional_resources.replace("j", "{")
    additional_resources = additional_resources.replace("z", "}")
    additional_resources = json.loads(additional_resources.replace("^", '"'))

    log_directory = setup_logging(model_version_id, step_id=None, conn_def=conn_def)

    alerts = ModelerAlert(model_version_id=model_version_id, conn_def=conn_def)
    alerts.alert(f"Logging here {log_directory}")

    update_code_version(model_version_id, conn_def=conn_def)
    model_parameters = get_model_parameters(
        model_version_id=model_version_id, conn_def=conn_def, update=True
    )

    # Models with Global model version type should not exclude any locations.
    if (model_parameters["model_version_type_id"] == 1) & (
        model_parameters["locations_exclude"] != ""
    ):
        excluded_locs_msg = (
            f"{model_parameters['model_version_type']} CODEm models must run across the full "
            "location hierarchy. Please relaunch this model removing excluded locations or "
            "as a different model version type!"
        )
        logger.info(excluded_locs_msg)
        alerts.alert(excluded_locs_msg)
        change_model_status(model_version_id, status=7, conn_def=conn_def)
        sys.exit(1)

    codem_wf_0 = CODEmWorkflow(
        name=f"cod_{model_version_id}_inputs",
        description=f"cod_{model_version_id}_inputs",
        project=model_parameters["cluster_project"],
        model_version_id=model_version_id,
        cause_id=model_parameters["cause_id"],
        acause=model_parameters["acause"],
        user=model_parameters["inserted_by"],
    )

    codem_wf_1 = CODEmWorkflow(
        name=f"cod_{model_version_id}_cov_ko",
        description=f"cod_{model_version_id}_cov_ko",
        project=model_parameters["cluster_project"],
        model_version_id=model_version_id,
        cause_id=model_parameters["cause_id"],
        acause=model_parameters["acause"],
        user=model_parameters["inserted_by"],
    )

    codem_wf_2 = CODEmWorkflow(
        name=f"cod_{model_version_id}_ensemble",
        description=f"cod_{model_version_id}_ensemble",
        project=model_parameters["cluster_project"],
        model_version_id=model_version_id,
        cause_id=model_parameters["cause_id"],
        acause=model_parameters["acause"],
        user=model_parameters["inserted_by"],
    )

    step_generator = StepTaskGenerator(
        model_version_id=model_version_id,
        conn_def=conn_def,
        old_covariates_mvid=old_covariates_mvid,
        debug_mode=debug_mode,
        additional_resources=additional_resources,
    )

    paths = ModelPaths(model_version_id=model_version_id, conn_def=conn_def)
    logger.info(f"Saving the job parameters to {paths.JOB_METADATA}.")
    if os.path.isfile(paths.JOB_METADATA):
        with open(paths.JOB_METADATA, "r") as json_file:
            logger.info("Reading inputs json.")
            inputs_info = json.load(json_file)
            inputs_info.update(inspect_parameters(parameters=model_parameters))
            logger.info(f"{inputs_info}")
    else:
        inputs_info = inspect_parameters(parameters=model_parameters)

    with open(paths.JOB_METADATA, "w") as outfile:
        json.dump(inputs_info, outfile)

    input_data = step_generator.generate(
        step_id=STEP_IDS["InputData"], inputs_info=inputs_info
    )

    wf_0 = codem_wf_0.get_workflow()
    wf_0.add_task(input_data)

    try:
        logger.info("Running Input data workflow.")
        # 1 day of runtime
        exit_status = wf_0.run(resume=True, seconds_until_timeout=86400)
        if exit_status == WorkflowStatus.DONE:
            logger.info("Input data workflow finished successfully.")
            alerts.alert("Input data workflow finished successfully.")
            shutil.rmtree(codem_wf_0.stderr, ignore_errors=True)
        else:
            logger.info(
                f"Input data workflow returned exit status {exit_status}, "
                f"see workflow logs {codem_wf_0.stderr}."
            )
            alerts.alert("The Input data workflow failed. Please submit a ticket!")
            change_model_status(
                model_version_id=model_version_id, status=7, conn_def=conn_def
            )
            sys.exit(1)
    except WorkflowAlreadyComplete:
        logger.info("Workflow already complete for input data. Skipping.")

    logger.info("Reading job metadata.")
    with open(paths.JOB_METADATA, "r") as json_file:
        inputs_info = json.load(json_file)

    generate_knockouts = step_generator.generate(
        step_id=STEP_IDS["GenerateKnockouts"],
        inputs_info=inputs_info,
        resource_scales={"memory": 0.5, "runtime": 0.1},
    )
    wf_1 = codem_wf_1.get_workflow()
    wf_1.add_task(generate_knockouts)

    if old_covariates_mvid:
        old_model_version = ModelPaths(
            model_version_id=old_covariates_mvid, conn_def=conn_def
        )
        logger.info(
            f"Skipping covariate selection, using {old_covariates_mvid} for old covariates."
        )
        alerts.alert(
            f"Skipping covariate selection, using covariates from model version ID "
            f"{old_covariates_mvid}"
        )
        if (
            not (
                os.path.isfile(old_model_version.COVARIATE_FILES["ln_rate"])
                and os.path.isfile(old_model_version.COVARIATE_FILES["lt_cf"])
            )
        ) or (
            (os.path.isfile(old_model_version.COVARIATE_FILES_NO_SELECT["ln_rate"]))
            and (os.path.isfile(old_model_version.COVARIATE_FILES_NO_SELECT["lt_cf"]))
        ):
            logger.info(
                f"Previous model version ID {old_covariates_mvid} is either incomplete or did "
                f"not select any covariates."
            )
            alerts.alert(
                f"Previous model version ID {old_covariates_mvid} is either incomplete or did "
                f"not select any covariates. Please relaunch this model with covariate "
                f"selection or a different previous model version!"
            )
            change_model_status(
                model_version_id=model_version_id, status=7, conn_def=conn_def
            )
            sys.exit(1)
        shutil.copy(
            old_model_version.COVARIATE_FILES["ln_rate"], paths.COVARIATE_FILES["ln_rate"]
        )
        shutil.copy(
            old_model_version.COVARIATE_FILES["lt_cf"], paths.COVARIATE_FILES["lt_cf"]
        )
    else:
        logger.info("Need to run covariate selection.")
        covariate_selection_tasks = []
        for outcome in ["ln_rate", "lt_cf"]:
            covariate_selection_tasks.append(
                step_generator.generate(
                    step_id=STEP_IDS["CovariateSelection"],
                    inputs_info=inputs_info,
                    additional_args={"outcome": outcome},
                    resource_scales={"memory": 1, "runtime": 1},
                )
            )
        wf_1.add_tasks(covariate_selection_tasks)

    try:
        logger.info("Running Covariate selection + knockouts workflow.")
        # 3 days of runtime
        exit_status = wf_1.run(resume=True, seconds_until_timeout=259200)
        if exit_status == WorkflowStatus.DONE:
            logger.info("Covariate selection + knockouts workflow finished successfully.")
            alerts.alert("Covariate selection + knockouts finished successfully.")
            shutil.rmtree(codem_wf_1.stderr, ignore_errors=True)
        else:
            logger.info(
                "Covariate selection + knockouts workflow returned exit status "
                f"{exit_status}, see workflow logs {codem_wf_1.stderr}."
            )
            alerts.alert(
                "The Covariate selection + knockouts workflow failed. Please submit a ticket!"
            )
            change_model_status(
                model_version_id=model_version_id, status=7, conn_def=conn_def
            )
            sys.exit(1)
    except WorkflowAlreadyComplete:
        logger.info(
            "Workflow already complete for Covariate selection + knockouts. Skipping."
        )

    if (os.path.isfile(paths.COVARIATE_FILES_NO_SELECT["ln_rate"])) and (
        os.path.isfile(paths.COVARIATE_FILES_NO_SELECT["lt_cf"])
    ):
        logger.info(
            "No covariates selected for either ln_rate or lt_cf. Please "
            "try different covariates."
        )
        alerts.alert(
            "No covariates were selected for this model. "
            "Please relaunch the model with different covariates!"
        )
        shutil.copy(paths.COVARIATE_FILES_NO_SELECT["ln_rate"], codem_wf_1.stderr)
        shutil.copy(paths.COVARIATE_FILES_NO_SELECT["lt_cf"], codem_wf_1.stderr)
        change_model_status(model_version_id=model_version_id, status=7, conn_def=conn_def)
        sys.exit(1)

    logger.info("Updating job metadata parameters.")
    with open(paths.JOB_METADATA, "r") as json_file:
        logger.info("Reading inputs after covariate selection json.")
        inputs_info = json.load(json_file)
        logger.info(f"{inputs_info}")
    inputs_info.update(inspect_submodels(paths))
    with open(paths.JOB_METADATA, "w") as outfile:
        logger.info("Writing inputs after covariate selection json.")
        logger.info(f"{inputs_info}")
        json.dump(inputs_info, outfile)

    logger.info("Reading job metadata.")
    with open(paths.JOB_METADATA, "r") as json_file:
        inputs_info = json.load(json_file)

    linear_model_builds = step_generator.generate(
        step_id=STEP_IDS["LinearModelBuilds"],
        inputs_info=inputs_info,
        resource_scales={"memory": 1, "runtime": 1},
    )
    read_linear_models = step_generator.generate(
        step_id=STEP_IDS["ReadLinearModels"],
        inputs_info=inputs_info,
        upstream_tasks=[linear_model_builds],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    linear_pv = step_generator.generate(
        step_id=STEP_IDS["LinearPV"],
        inputs_info=inputs_info,
        upstream_tasks=[read_linear_models],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    read_spacetime_models = step_generator.generate(
        step_id=STEP_IDS["ReadSpacetimeModels"],
        inputs_info=inputs_info,
        upstream_tasks=[linear_model_builds],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    apply_spacetime_smoothing = step_generator.generate(
        step_id=STEP_IDS["ApplySpacetimeSmoothing"],
        inputs_info=inputs_info,
        upstream_tasks=[read_spacetime_models],
        resource_scales={"memory": 1, "runtime": 0.5},
    )
    apply_gp_smoothing = step_generator.generate(
        step_id=STEP_IDS["ApplyGPSmoothing"],
        inputs_info=inputs_info,
        upstream_tasks=[apply_spacetime_smoothing],
        resource_scales={"memory": 1, "runtime": 0.5},
    )
    spacetime_pv = step_generator.generate(
        step_id=STEP_IDS["SpacetimePV"],
        inputs_info=inputs_info,
        upstream_tasks=[apply_gp_smoothing],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    optimal_psi = step_generator.generate(
        step_id=STEP_IDS["OptimalPSI"],
        inputs_info=inputs_info,
        upstream_tasks=[linear_pv, spacetime_pv],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    linear_draws = step_generator.generate(
        step_id=STEP_IDS["LinearDraws"],
        inputs_info=inputs_info,
        upstream_tasks=[optimal_psi],
        resource_scales={"memory": 0.3, "runtime": 0.5},
    )
    gpr_draws = step_generator.generate(
        step_id=STEP_IDS["GPRDraws"],
        inputs_info=inputs_info,
        upstream_tasks=[optimal_psi],
        resource_scales={"memory": 0.3, "runtime": 0.5},
    )
    ensemble_predictions = step_generator.generate(
        step_id=STEP_IDS["EnsemblePredictions"],
        inputs_info=inputs_info,
        upstream_tasks=[optimal_psi, linear_draws, gpr_draws],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    write_results = step_generator.generate(
        step_id=STEP_IDS["WriteResults"],
        inputs_info=inputs_info,
        upstream_tasks=[ensemble_predictions],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    diagnostics = step_generator.generate(
        step_id=STEP_IDS["Diagnostics"],
        inputs_info=inputs_info,
        upstream_tasks=[ensemble_predictions],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )
    email = step_generator.generate(
        step_id=STEP_IDS["Email"],
        inputs_info=inputs_info,
        upstream_tasks=[write_results, diagnostics],
        resource_scales={"memory": 0.3, "runtime": 0.25},
    )

    wf_2 = codem_wf_2.get_workflow()
    wf_2.add_tasks(
        [
            linear_model_builds,
            read_linear_models,
            linear_pv,
            linear_draws,
            read_spacetime_models,
            apply_spacetime_smoothing,
            apply_gp_smoothing,
            spacetime_pv,
            gpr_draws,
            optimal_psi,
            ensemble_predictions,
            write_results,
            diagnostics,
            email,
        ]
    )
    try:
        alerts.alert("Initiating the ensemble workflow.")
        logger.info("Running the rest of the ensemble workflow.")
        # 7 days of runtime
        exit_status = wf_2.run(resume=True, seconds_until_timeout=604800)
        if exit_status == WorkflowStatus.DONE:
            logger.info("The ensemble workflow successfully completed.")
            alerts.alert("The model has successfully completed!")
            shutil.rmtree(codem_wf_2.stderr)
        else:
            logger.info(
                f"The ensemble workflow failed, returning exit status {exit_status}, "
                f"see workflow logs {codem_wf_2.stderr}."
            )
            alerts.alert("The ensemble workflow failed. Please submit a ticket!")
            change_model_status(
                model_version_id=model_version_id, status=7, conn_def=conn_def
            )
            sys.exit(1)

    except WorkflowAlreadyComplete:
        logger.info("The ensemble workflow has already completed!")

    logger.info(f"Marking model version ID {model_version_id} as complete in the database.")
    change_model_status(model_version_id=model_version_id, status=1, conn_def=conn_def)
    logger.info("Cleaning up files...")
    cleanup_files(model_version_id=model_version_id, conn_def=conn_def)
    logger.info("Finished.")


if __name__ == "__main__":
    main()
