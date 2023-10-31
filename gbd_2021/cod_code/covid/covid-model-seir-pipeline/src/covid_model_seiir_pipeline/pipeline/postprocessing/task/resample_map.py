from pathlib import Path

import click
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.postprocessing.data import PostprocessingDataInterface
from covid_model_seiir_pipeline.pipeline.postprocessing.specification import (
    PostprocessingSpecification,
    POSTPROCESSING_JOBS,
)
from covid_model_seiir_pipeline.pipeline.postprocessing.model import resampling, loaders


logger = cli_tools.task_performance_logger


def run_resample_map(postprocessing_version: str) -> None:
    logger.info('Building resampling map', context='setup')
    postprocessing_spec = PostprocessingSpecification.from_path(
        Path(postprocessing_version) / static_vars.POSTPROCESSING_SPECIFICATION_FILE
    )
    workflow_spec = postprocessing_spec.workflow.task_specifications[POSTPROCESSING_JOBS.resample]
    resampling_params = postprocessing_spec.resampling
    data_interface = PostprocessingDataInterface.from_specification(postprocessing_spec)

    logger.info('Loading resampling data', context='read')
    deaths = loaders.load_deaths(resampling_params.reference_scenario,
                                 data_interface,
                                 workflow_spec.num_cores)

    logger.info('Computing resampling map', context='compute')
    deaths = pd.concat(deaths, axis=1)
    resampling_map = resampling.build_resampling_map(deaths, resampling_params)

    logger.info('Writing resampling map', context='write')
    data_interface.save_resampling_map(resampling_map)

    logger.report()


@click.command()
@cli_tools.with_task_postprocessing_version
@cli_tools.add_verbose_and_with_debugger
def resample_map(postprocessing_version: str,
                 verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_resample_map, logger, with_debugger)
    run(postprocessing_version=postprocessing_version)


if __name__ == '__main__':
    resample_map()
