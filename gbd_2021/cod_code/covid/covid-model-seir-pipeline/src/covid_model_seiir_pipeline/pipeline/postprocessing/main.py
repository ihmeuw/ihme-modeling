from covid_shared import cli_tools, ihme_deps
from loguru import logger

from covid_model_seiir_pipeline.pipeline.postprocessing.specification import PostprocessingSpecification
from covid_model_seiir_pipeline.pipeline.postprocessing.data import PostprocessingDataInterface
from covid_model_seiir_pipeline.pipeline.postprocessing.workflow import PostprocessingWorkflow
from covid_model_seiir_pipeline.pipeline.postprocessing import model


def do_postprocessing(app_metadata: cli_tools.Metadata,
                      postprocessing_specification: PostprocessingSpecification,
                      preprocess_only: bool):
    logger.info(f'Starting postprocessing for version {postprocessing_specification.data.output_root}.')

    data_interface = PostprocessingDataInterface.from_specification(postprocessing_specification)

    data_interface.make_dirs(scenario=postprocessing_specification.data.scenarios)
    data_interface.save_specification(postprocessing_specification)

    if not preprocess_only:
        workflow = PostprocessingWorkflow(postprocessing_specification.data.output_root,
                                          postprocessing_specification.workflow)
        known_covariates = list(model.COVARIATES)
        modeled_covariates = set(data_interface.get_covariate_names(postprocessing_specification.data.scenarios))
        unknown_covariates = modeled_covariates.difference(known_covariates + ['intercept'])
        if unknown_covariates:
            logger.warning("Some covariates that were modeled have no postprocessing configuration. "
                           "Postprocessing will produce no outputs for these covariates. "
                           f"Unknown covariates: {list(unknown_covariates)}")

        measures = [*model.MEASURES, *model.COMPOSITE_MEASURES,
                    *model.MISCELLANEOUS, *modeled_covariates.intersection(known_covariates)]
        workflow.attach_tasks(measures, postprocessing_specification.data.scenarios)

        try:
            workflow.run()
        except ihme_deps.WorkflowAlreadyComplete:
            logger.info('Workflow already complete')
