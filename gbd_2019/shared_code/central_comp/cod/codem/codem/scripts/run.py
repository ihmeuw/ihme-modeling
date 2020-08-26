"""
Master script for running CODEm functions for one
instance of a CODEm model. This is an executable
called by the jobmon tasks.
"""
import sys
import subprocess
import os
import logging
import json
from jobmon.client.swarm.workflow.workflow import WorkflowAlreadyComplete

from codem.joblaunch.CODEmStep import StepTaskGenerator
from codem.joblaunch.args import get_args
from codem.joblaunch.run_utils import log_branch, change_model_status
from codem.metadata.step_metadata import STEP_IDS
from codem.joblaunch.step_profiling import inspect_parameters, inspect_submodels
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.reference.log_config import setup_logging, ModelerAlert
from codem.reference.paths import cleanup_files, ModelPaths
from codem.data.parameters import get_model_parameters

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():

    args = get_args()

    model_version_id = args.model_version_id
    db_connection = args.db_connection
    old_covariates_mvid = args.old_covariates_mvid
    debug_mode = args.debug_mode
    additional_resources = args.step_resources
    additional_resources = json.loads(additional_resources.replace('^', '"'))

    branch = log_branch(model_version_id, db_connection)
    log_directory = setup_logging(model_version_id, step_id=None)

    alerts = ModelerAlert(model_version_id=model_version_id, db_connection=db_connection)
    alerts.alert(f"This model is running on branch {branch}")
    alerts.alert(f"You can find your logs here {log_directory}")

    wf_0 = CODEmWorkflow(name=f'cod_{model_version_id}_inputs',
                         description=f'cod_{model_version_id}_cov_ko',
                         resume=True, reset_running_jobs=True)
    wf_1 = CODEmWorkflow(name=f'cod_{model_version_id}_inputs',
                         description=f'cod_{model_version_id}_cov_ko',
                         resume=True, reset_running_jobs=True)
    wf_2 = CODEmWorkflow(name=f'cod_{model_version_id}_ensemble',
                         description=f'cod_{model_version_id}_ensemble',
                         resume=True, reset_running_jobs=True)

    step_generator = StepTaskGenerator(model_version_id=model_version_id,
                                       db_connection=db_connection,
                                       old_covariates_mvid=old_covariates_mvid,
                                       debug_mode=debug_mode,
                                       additional_resources=additional_resources)

    model_parameters = get_model_parameters(
        model_version_id=model_version_id,
        db_connection=db_connection,
        update=True
    )

    paths = ModelPaths(model_version_id=model_version_id,
                       acause=model_parameters['acause'])

    old_model_version = ModelPaths(model_version_id=old_covariates_mvid,
                                   acause=model_parameters['acause'])

    logger.info(f"Saving the job parameters to {paths.JOB_METADATA}.")
    if os.path.isfile(paths.JOB_METADATA):
        with open(paths.JOB_METADATA, 'r') as json_file:
            logger.info("Reading inputs json.")
            inputs_info = json.load(json_file)
            inputs_info.update(inspect_parameters(parameters=model_parameters))
            logger.info(f"{inputs_info}")
    else:
        inputs_info = inspect_parameters(parameters=model_parameters)

    with open(paths.JOB_METADATA, 'w') as outfile:
        json.dump(inputs_info, outfile)

    input_data = step_generator.generate(
        step_id=STEP_IDS['InputData'],
        inputs_info=inputs_info
    )

    wf_0.add_task(input_data)
    try:
        logger.info("Running input data workflow.")
        exit_status = wf_0.run()
        if exit_status:
            logger.info(f"Input data workflow returned exit status {exit_status}")
            alerts.alert("The input data workflow failed. Please submit a ticket!")
            change_model_status(model_version_id=model_version_id, status=7, db_connection=db_connection)
            sys.exit(exit_status)
        else:
            logger.info("Input data workflow finished successfully.")
            alerts.alert("Input data workflow finished successfully.")
    except WorkflowAlreadyComplete:
        logger.info("Workflow already complete for input data. Skipping.")
        pass

    logger.info("Reading job metadata.")
    with open(paths.JOB_METADATA, 'r') as json_file:
        inputs_info = json.load(json_file)

    generate_knockouts = step_generator.generate(
        step_id=STEP_IDS['GenerateKnockouts'],
        inputs_info=inputs_info,
        resource_scales={'m_mem_free': 0.5,
                         'max_runtime_seconds': 0.1}
    )
    wf_1.add_task(generate_knockouts)

    if old_covariates_mvid:
        logger.info(f'Using {old_covariates_mvid} for old covariates.')
        alerts.alert(f"Skipping: using {old_covariates_mvid}s "
                     f"covariates for this models covariates instead")
        subprocess.call('cp ' + old_model_version.COVARIATE_FILES['ln_rate'] + ' ' +
                        paths.COVARIATE_FILES['ln_rate'], shell=True)
        subprocess.call('cp ' + old_model_version.COVARIATE_FILES['lt_cf'] + ' ' +
                        paths.COVARIATE_FILES['lt_cf'], shell=True)
    else:
        logger.info("Need to run covariate selection.")
        covariate_selection_tasks = []
        for outcome in ['ln_rate', 'lt_cf']:
            covariate_selection_tasks.append(
                step_generator.generate(
                    step_id=STEP_IDS['CovariateSelection'],
                    inputs_info=inputs_info,
                    additional_args={'outcome': outcome},
                    resource_scales={'m_mem_free': 1,
                                     'max_runtime_seconds': 1}
                )
            )
        wf_1.add_tasks(covariate_selection_tasks)

    try:
        logger.info("Running covariate selection + knockouts workflow.")
        exit_status = wf_1.run()
        if exit_status:
            logger.info(f"Covariate selection + knockouts returned exit status {exit_status}")
            alerts.alert("The covariate selection workflow failed. Please submit a ticket!")
            change_model_status(model_version_id=model_version_id, status=7, db_connection=db_connection)
            sys.exit(exit_status)
        else:
            logger.info("The covariate selection + knockouts workflow was successful.")
            alerts.alert("The covariate selection + knockouts workflow was successful.")
    except WorkflowAlreadyComplete:
        logger.info("Workflow already complete for covariate selection + knockouts. Skipping.")
        pass
    if (os.path.isfile(paths.COVARIATE_FILES_NO_SELECT['ln_rate'])) and \
            (os.path.isfile(paths.COVARIATE_FILES_NO_SELECT['lt_cf'])):
        logger.info("No covariates selected for either ln_rate or lt_cf. Please"
                    "try different covariates.")
        alerts.alert("No covariates were selected for this model. "
                     "Please relaunch the model with different covariates!")
        raise RuntimeError("There were no covariates selected at all.")

    logger.info("Updating job metadata parameters.")
    with open(paths.JOB_METADATA, 'r') as json_file:
        logger.info("Reading inputs after covariate selection json.")
        inputs_info = json.load(json_file)
        logger.info(f"{inputs_info}")
    inputs_info.update(inspect_submodels(paths))
    with open(paths.JOB_METADATA, 'w') as outfile:
        logger.info("Writing inputs after covariate selection json.")
        logger.info(f"{inputs_info}")
        json.dump(inputs_info, outfile)

    logger.info("Reading job metadata.")
    with open(paths.JOB_METADATA, 'r') as json_file:
        inputs_info = json.load(json_file)

    linear_model_builds = step_generator.generate(
        step_id=STEP_IDS['LinearModelBuilds'],
        inputs_info=inputs_info,
        resource_scales={'m_mem_free': 1,
                         'max_runtime_seconds': 1}
    )
    read_linear_models = step_generator.generate(
        step_id=STEP_IDS['ReadLinearModels'],
        inputs_info=inputs_info,
        upstream_tasks=[
            linear_model_builds
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    linear_pv = step_generator.generate(
        step_id=STEP_IDS['LinearPV'],
        inputs_info=inputs_info,
        upstream_tasks=[
            read_linear_models
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    read_spacetime_models = step_generator.generate(
        step_id=STEP_IDS['ReadSpacetimeModels'],
        inputs_info=inputs_info,
        upstream_tasks=[
            linear_model_builds
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    apply_spacetime_smoothing = step_generator.generate(
        step_id=STEP_IDS['ApplySpacetimeSmoothing'],
        inputs_info=inputs_info,
        upstream_tasks=[
            read_spacetime_models
        ],
        resource_scales={'m_mem_free': 1,
                         'max_runtime_seconds': 0.5}
    )
    apply_gp_smoothing = step_generator.generate(
        step_id=STEP_IDS['ApplyGPSmoothing'],
        inputs_info=inputs_info,
        upstream_tasks=[
            apply_spacetime_smoothing
        ],
        resource_scales={'m_mem_free': 1,
                         'max_runtime_seconds': 0.5}
    )
    spacetime_pv = step_generator.generate(
        step_id=STEP_IDS['SpacetimePV'],
        inputs_info=inputs_info,
        upstream_tasks=[
            apply_gp_smoothing
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    optimal_psi = step_generator.generate(
        step_id=STEP_IDS['OptimalPSI'],
        inputs_info=inputs_info,
        upstream_tasks=[
            linear_pv,
            spacetime_pv
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    linear_draws = step_generator.generate(
        step_id=STEP_IDS['LinearDraws'],
        inputs_info=inputs_info,
        upstream_tasks=[
            optimal_psi
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.5}
    )
    gpr_draws = step_generator.generate(
        step_id=STEP_IDS['GPRDraws'],
        inputs_info=inputs_info,
        upstream_tasks=[
            optimal_psi
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.5}
    )
    ensemble_predictions = step_generator.generate(
        step_id=STEP_IDS['EnsemblePredictions'],
        inputs_info=inputs_info,
        upstream_tasks=[
            optimal_psi,
            linear_draws,
            gpr_draws
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    write_results = step_generator.generate(
        step_id=STEP_IDS['WriteResults'],
        inputs_info=inputs_info,
        upstream_tasks=[
            ensemble_predictions
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    diagnostics = step_generator.generate(
        step_id=STEP_IDS['Diagnostics'],
        inputs_info=inputs_info,
        upstream_tasks=[
            ensemble_predictions
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )
    email = step_generator.generate(
        step_id=STEP_IDS['Email'],
        inputs_info=inputs_info,
        upstream_tasks=[
            write_results,
            diagnostics
        ],
        resource_scales={'m_mem_free': 0.3,
                         'max_runtime_seconds': 0.25}
    )

    wf_2.add_tasks([linear_model_builds, read_linear_models, linear_pv, linear_draws,
                    read_spacetime_models, apply_spacetime_smoothing, apply_gp_smoothing, spacetime_pv, gpr_draws,
                    optimal_psi, ensemble_predictions, write_results, diagnostics, email])
    alerts.alert("Initiating the ensemble workflow.")
    logger.info("Running the rest of the ensemble workflow.")
    exit_status = wf_2.run()

    if exit_status:
        logger.info(f"The ensemble workflow failed, returning exit status {exit_status}")
        alerts.alert("The ensemble workflow failed. Please submit a ticket!")
        change_model_status(model_version_id=model_version_id, status=7, db_connection=db_connection)
        sys.exit(exit_status)
    else:
        logger.info("The ensemble workflow successfully completed.")
        alerts.alert("The model has successful completed!!")
        logger.info(f"Changing model version {model_version_id} to complete in the database.")
        change_model_status(model_version_id=model_version_id, status=1, db_connection=db_connection)
        logger.info("Cleaning up files...")
        cleanup_files(model_version_id=model_version_id, acause=model_parameters['acause'])
        logger.info("Finished.")


if __name__ == '__main__':
    main()
