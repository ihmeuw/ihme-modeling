from covid_shared import cli_tools, ihme_deps
from loguru import logger

from covid_model_seiir_pipeline.lib import io
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import DiagnosticsSpecification
from covid_model_seiir_pipeline.pipeline.diagnostics.workflow import DiagnosticsWorkflow


def do_diagnostics(app_metadata: cli_tools.Metadata,
                   diagnostics_specification: DiagnosticsSpecification,
                   preprocess_only: bool):
    logger.info(f'Starting diagnostics for version {diagnostics_specification.data.output_root}.')

    diagnostics_root = io.DiagnosticsRoot(diagnostics_specification.data.output_root)
    io.dump(diagnostics_specification.to_dict(), diagnostics_root.specification())

    if not preprocess_only:
        workflow = DiagnosticsWorkflow(diagnostics_specification.data.output_root,
                                       diagnostics_specification.workflow)
        grid_plot_jobs = [grid_plot_spec.name for grid_plot_spec in diagnostics_specification.grid_plots]
        compare_csv = bool(diagnostics_specification.cumulative_deaths_compare_csv)
        scatters_jobs = [scatters_spec.name for scatters_spec in diagnostics_specification.scatters]

        workflow.attach_tasks(grid_plot_jobs, compare_csv, scatters_jobs)

        try:
            workflow.run()
        except ihme_deps.WorkflowAlreadyComplete:
            logger.info('Workflow already complete')
