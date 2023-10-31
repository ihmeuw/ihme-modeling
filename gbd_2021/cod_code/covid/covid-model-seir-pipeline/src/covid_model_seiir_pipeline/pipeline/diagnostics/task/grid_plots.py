import functools
import multiprocessing
from pathlib import Path
import tempfile

import click
import pandas as pd

import tqdm

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import (
    DiagnosticsSpecification,
)
from covid_model_seiir_pipeline.pipeline.diagnostics import model


logger = cli_tools.task_performance_logger


def run_grid_plots(diagnostics_version: str, name: str, progress_bar: bool) -> None:
    """Make the grid plots!"""
    logger.info(f'Starting grid plots for version {diagnostics_version}, name {name}.', context='setup')
    diagnostics_spec = DiagnosticsSpecification.from_path(
        Path(diagnostics_version) / static_vars.DIAGNOSTICS_SPECIFICATION_FILE
    )
    grid_plot_spec = [spec for spec in diagnostics_spec.grid_plots if spec.name == name].pop()

    # Plot version objects are specific to a version and scenario and
    # encapsulate all the data and metadata necessary to plot that particular
    # version/scenario combo.
    logger.info('Building plot versions')
    plot_versions = model.make_plot_versions(grid_plot_spec.comparators, model.COLOR_MAP)

    # We'll use a local cache to hang onto data so we can use multiprocessing
    # to make the plots without doing a bunch of I/O over the network
    # drives.  We don't want to copy all of outputs though, since we just
    # need draws for a couple of things and draws make up almost all the
    # outputs.
    cache_draws = ['beta_scaling_parameters', 'coefficients']

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root = Path(temp_dir_name)
        logger.info('Building data cache', context='shared_io')
        data_cache = root / 'data_cache'
        data_cache.mkdir()
        # Copy all the summary & miscellaneous data and a subset of draws
        # from the seir-outputs to local storage.
        for plot_version in plot_versions:
            plot_version.build_cache(data_cache, cache_draws)

        # We'll use this to store location specific pdfs before collation.
        plot_cache = root / 'plot_cache'
        plot_cache.mkdir()

        # Fixme: this is a bit brittle as it requires some ordering constraints
        #   on the user side to get the expected results.
        logger.info('Loading locations', context='setup')
        hierarchy = plot_versions[-1].load_output_miscellaneous('hierarchy', is_table=True).reset_index()
        deaths = plot_versions[-1].load_output_summaries('daily_deaths').reset_index()
        modeled_locs = hierarchy.loc[hierarchy.location_id.isin(deaths.location_id.unique()),
                                     ['location_id', 'location_name']]
        locs_to_plot = [model.Location(loc[1], loc[2]) for loc in modeled_locs.itertuples()]

        logger.info('Starting plots', context='make_plots')
        _runner = functools.partial(
            model.make_grid_plot,
            plot_versions=plot_versions,
            date_start=pd.to_datetime(grid_plot_spec.date_start),
            date_end=pd.to_datetime(grid_plot_spec.date_end),
            output_dir=plot_cache,
        )
        _runner(locs_to_plot[111])
        num_cores = diagnostics_spec.workflow.task_specifications['grid_plots'].num_cores
        with multiprocessing.Pool(num_cores) as pool:
            list(tqdm.tqdm(pool.imap(_runner, locs_to_plot), total=len(locs_to_plot), disable=not progress_bar))

        logger.info('Collating plots', context='merge_plots')
        output_path = Path(diagnostics_spec.data.output_root) / f'grid_plots_{grid_plot_spec.name}.pdf'
        model.merge_pdfs(plot_cache, output_path, hierarchy)

        logger.report()


@click.command()
@cli_tools.with_task_diagnostics_version
@cli_tools.with_name
@cli_tools.with_progress_bar
@cli_tools.add_verbose_and_with_debugger
def grid_plots(diagnostics_version: str, name: str,
               progress_bar: bool, verbose: int, with_debugger: bool):
    """Produce grid plots corresponding to the configuration associated with NAME"""
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_grid_plots, logger, with_debugger)
    run(diagnostics_version=diagnostics_version,
        name=name,
        progress_bar=progress_bar)


if __name__ == '__main__':
    grid_plots()
