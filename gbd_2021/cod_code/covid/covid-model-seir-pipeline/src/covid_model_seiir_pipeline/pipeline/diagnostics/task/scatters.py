from pathlib import Path

import click
import matplotlib.lines as mlines
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import pandas as pd
import tqdm

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import (
    DiagnosticsSpecification,
    ScattersAxisSpecification,
)
from covid_model_seiir_pipeline.pipeline.postprocessing import (
    PostprocessingSpecification,
    PostprocessingDataInterface,
)
import matplotlib.pyplot as plt

COLOR_MAP = ['#7F3C8D', '#11A579',
             '#3969AC', '#F2B701',
             '#E73F74', '#80BA5A',
             '#E68310', '#008695',
             '#CF1C90', '#f97b72',
             '#4b4b8f', '#A5AA99'].__getitem__

AX_LABEL_FONTSIZE = 16
TITLE_FONTSIZE = 24
FIG_SIZE = (20, 8)
GRID_SPEC_MARGINS = {'top': 0.92, 'bottom': 0.08}

logger = cli_tools.task_performance_logger


def run_scatters(diagnostics_version: str, name: str, progress_bar: bool) -> None:
    logger.info(f'Starting scatters for version {diagnostics_version}, name {name}.', context='setup')

    diagnostics_spec = DiagnosticsSpecification.from_path(
        Path(diagnostics_version) / static_vars.DIAGNOSTICS_SPECIFICATION_FILE
    )
    scatters_spec = [spec for spec in diagnostics_spec.scatters if spec.name == name].pop()

    logger.info('Loading plotting data.', context='read')
    pp_spec = PostprocessingSpecification.from_path(
        Path(scatters_spec.x_axis.version) / static_vars.POSTPROCESSING_SPECIFICATION_FILE
    )
    pp_di = PostprocessingDataInterface.from_specification(pp_spec)
    
    if pp_di.is_counties_run():
        logger.info('No scatters for counties')
        return
    hierarchy = pp_di.load_hierarchy()
    name_map = hierarchy.set_index('location_id').location_ascii_name

    deaths_x = get_deaths(scatters_spec.x_axis)
    deaths_y = get_deaths(scatters_spec.y_axis)

    logger.info('Processing inputs.', context='transform')
    plotting_data = pd.concat([deaths_x, deaths_y], axis=1)
    pc = np.abs((deaths_y - deaths_x) / deaths_x)

    plotting_data['Above 25'] = pc > 0.25
    plotting_data['Above 45'] = pc > 0.45
    plotting_data['location_name'] = name_map.reindex(plotting_data.index)

    with PdfPages(f'{diagnostics_version}/cumulative_deaths_scatters_{name}.pdf') as pdf:
        make_scatter_pages(plotting_data, hierarchy, deaths_x.name, deaths_y.name, progress_bar, pdf)

    logger.report()


def get_deaths(axis_spec: ScattersAxisSpecification):
    pp_spec = PostprocessingSpecification.from_path(
        Path(axis_spec.version) / static_vars.POSTPROCESSING_SPECIFICATION_FILE
    )
    pp_di = PostprocessingDataInterface.from_specification(pp_spec)
    data_date = pp_di.load_full_data_unscaled().reset_index().date.max()
    data_date = data_date if not axis_spec.date else pd.Timestamp(axis_spec.date)

    deaths = pp_di.load_output_summaries(axis_spec.scenario, 'cumulative_deaths').reset_index()
    deaths = (deaths
              .loc[deaths.date == data_date]
              .set_index('location_id')['mean']
              .rename(f'{axis_spec.label.replace("_", " ").title()} {str(data_date.date())}'))

    return deaths


def make_ax_plot(ax, data, xlabel, ylabel, threshold_col, color):
    line_data = [0.0, 1.25 * data[xlabel].max()]
    ax.plot(line_data, line_data, color='k', linewidth=3)
    if threshold_col:
        t_data = data[data[threshold_col]]
        data = data[~data[threshold_col]]
    else:
        t_data = pd.DataFrame(columns=data.columns)

    ax.scatter(data[xlabel], data[ylabel], color='k')
    ax.scatter(t_data[xlabel], t_data[ylabel], color=color)
    for i, row in t_data.iterrows():
        ax.annotate(row.location_name, (row[xlabel] + 0.005 * line_data[1], row[ylabel] + 0.005 * line_data[1]),
                    color=color, fontsize=12)

    ax.set_xlim(*line_data)
    ax.set_xlabel(xlabel, fontsize=AX_LABEL_FONTSIZE)
    ax.set_ylim(*line_data)
    ax.set_ylabel(ylabel, fontsize=AX_LABEL_FONTSIZE)


def make_legend_handles(threshold_col, color):
    if threshold_col:
        colors_and_labels = [('k', f"{threshold_col.replace('Above', 'Below')}%"), (color, f"{threshold_col}%")]
    else:
        colors_and_labels = [('k', 'Data')]
    handles = [mlines.Line2D([], [], color=color, label=label, linestyle='', marker='o') for color, label in
               colors_and_labels]
    return handles


def make_scatter_page(data, fig_title, xlabel, ylabel, threshold_col='', pdf=None):
    fig = plt.figure(figsize=FIG_SIZE)
    gs = fig.add_gridspec(
        nrows=1, ncols=2,
        wspace=0.2,
    )
    gs.update(**GRID_SPEC_MARGINS)
    color = COLOR_MAP(0) if threshold_col == 'Above 25' else COLOR_MAP(1)

    ax_normal = fig.add_subplot(gs[0])
    make_ax_plot(ax_normal, data, xlabel, ylabel, threshold_col, color)

    log_data = data.copy()
    log_xlabel, log_ylabel = f'{xlabel} (log)', f'{ylabel} (log)'
    log_data[[log_xlabel, log_ylabel]] = np.log10(log_data[[xlabel, ylabel]])
    ax_log = fig.add_subplot(gs[1])
    make_ax_plot(ax_log, log_data, log_xlabel, log_ylabel, threshold_col, color)

    fig.suptitle(fig_title, fontsize=TITLE_FONTSIZE)
    fig.legend(handles=make_legend_handles(threshold_col, color),
               loc='lower center',
               bbox_to_anchor=(0.5, 0),
               fontsize=AX_LABEL_FONTSIZE,
               frameon=False,
               ncol=2 if threshold_col else 1)

    if pdf is not None:
        pdf.savefig(fig)
        plt.close(fig)
    else:
        plt.show()


def make_scatter_pages(plotting_data, hierarchy, xlabel, ylabel, progress_bar: bool = False, pdf=None):
    filters_labels_and_thresholds = [
        ([1], 'Global', 'Above 45'),
        (plotting_data.index.isin(hierarchy[hierarchy.level == 3].location_id.tolist()), 'National', 'Above 45')
    ]
    agg_locations = (hierarchy[(hierarchy.most_detailed == 0) & (hierarchy.level >= 2)]
                     .sort_values(['level', 'sort_order'])
                     .location_id
                     .tolist())
    name_map = hierarchy.set_index('location_id').location_ascii_name
    for agg_location in agg_locations:
        child_locs = hierarchy[hierarchy.parent_id == agg_location].location_id.tolist()
        filters_labels_and_thresholds.append((
            plotting_data.index.isin(child_locs), name_map.loc[agg_location], 'Above 25'
        ))

    for loc_filter, label, threshold_col in tqdm.tqdm(filters_labels_and_thresholds, disable=not progress_bar):
        make_scatter_page(
            plotting_data.loc[loc_filter],
            f'Cumulative Deaths Compare {label}',
            xlabel, ylabel,
            threshold_col=threshold_col,
            pdf=pdf
        )


@click.command()
@cli_tools.with_task_diagnostics_version
@cli_tools.with_name
@cli_tools.with_progress_bar
@cli_tools.add_verbose_and_with_debugger
def scatters(diagnostics_version: str, name: str,
             progress_bar: bool, verbose: int, with_debugger: bool):
    """Produce scatters corresponding to the configuration associated with NAME"""
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_scatters, logger, with_debugger)
    run(diagnostics_version=diagnostics_version,
        name=name,
        progress_bar=progress_bar)


if __name__ == '__main__':
    scatters()
