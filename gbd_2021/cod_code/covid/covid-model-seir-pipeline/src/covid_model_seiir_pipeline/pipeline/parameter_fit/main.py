from covid_shared import cli_tools, ihme_deps
from loguru import logger

from covid_model_seiir_pipeline.pipeline.parameter_fit.specification import FitSpecification
from covid_model_seiir_pipeline.pipeline.parameter_fit.workflow import FitWorkflow
from covid_model_seiir_pipeline.pipeline.parameter_fit.data import FitDataInterface


def do_parameter_fit(app_metadata: cli_tools.Metadata,
                     fit_specification: FitSpecification,
                     preprocess_only: bool):
    logger.info(f'Starting beta fit for version {fit_specification.data.output_root}.')

    data_interface = FitDataInterface.from_specification(fit_specification)

    # Grab canonical location list from arguments
    hierarchy = data_interface.load_hierarchy_from_primary_source(
        location_set_version_id=fit_specification.data.location_set_version_id,
        location_file=fit_specification.data.location_set_file
    )
    location_ids = data_interface.filter_location_ids(hierarchy)

    data_interface.make_dirs(scenario=list(fit_specification.scenarios))
    data_interface.save_specification(fit_specification)
    data_interface.save_location_ids(location_ids)
    data_interface.save_hierarchy(hierarchy)

    if not preprocess_only:
        fit_wf = FitWorkflow(fit_specification.data.output_root,
                             fit_specification.workflow)
        n_draws = data_interface.get_n_draws()

        fit_wf.attach_tasks(n_draws=n_draws,
                            scenarios=fit_specification.scenarios)
        try:
            fit_wf.run()
        except ihme_deps.WorkflowAlreadyComplete:
            logger.info('Workflow already complete')
