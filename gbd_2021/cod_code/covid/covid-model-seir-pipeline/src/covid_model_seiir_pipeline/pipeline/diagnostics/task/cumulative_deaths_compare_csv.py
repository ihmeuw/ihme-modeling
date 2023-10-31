import itertools
from pathlib import Path

import click
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.postprocessing import (
    PostprocessingSpecification,
    PostprocessingDataInterface,
)
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import (
    DiagnosticsSpecification,
)
from covid_model_seiir_pipeline.pipeline.diagnostics.model.pdf_merger import (
    get_locations_dfs,
)

logger = cli_tools.task_performance_logger


def run_cumulative_deaths_compare_csv(diagnostics_version: str) -> None:
    logger.info(f'Starting cumulative death compare csv for version {diagnostics_version}', context='setup')
    diagnostics_spec = DiagnosticsSpecification.from_path(
        Path(diagnostics_version) / static_vars.DIAGNOSTICS_SPECIFICATION_FILE
    )
    cumulative_death_spec = diagnostics_spec.cumulative_deaths_compare_csv

    labels = [label for c in cumulative_death_spec.comparators for label in c.scenarios.values()]
    if 'reference' not in labels or 'public_reference' not in labels:
        raise ValueError('Must have at least a "reference" and "public_reference" result label.')

    logger.info('Reading utility data', context='read')
    postprocessing_spec = PostprocessingSpecification.from_path(
        Path(cumulative_death_spec.comparators[-1].version) / static_vars.POSTPROCESSING_SPECIFICATION_FILE
    )
    postprocessing_data_interface = PostprocessingDataInterface.from_specification(postprocessing_spec)
    hierarchy = postprocessing_data_interface.load_hierarchy()
    countries = hierarchy[hierarchy.level == 3].location_id.tolist()
    population = postprocessing_data_interface.load_population()
    population = (population
                  .loc[(population.sex_id == 3) & (population.age_group_id == 22)]
                  .set_index('location_id')
                  .population)
    sorted_locs = get_locations_dfs(hierarchy)
    other_hierarchies = [
        '/ihme/covid-19/seir-outputs/agg-hierarchies/who_plus_palestine.csv',
        '/ihme/covid-19/seir-outputs/agg-hierarchies/who_euro.csv',
        '/ihme/covid-19/seir-outputs/agg-hierarchies/world_bank.csv',
        '/ihme/covid-19/seir-outputs/agg-hierarchies/eu_minus_uk.csv',
    ]
    for h_file in other_hierarchies:
        h = pd.read_csv(h_file)
        h = (h[~h.location_id.isin(hierarchy.location_id)])
        hierarchy = hierarchy.append(h)
        sorted_locs.extend(h.location_id.tolist())

    logger.info('Collating deaths', context='transform')
    data = []
    max_date = pd.Timestamp('2000-01-01')

    for comparator in cumulative_death_spec.comparators:
        postprocessing_spec = PostprocessingSpecification.from_path(
            Path(comparator.version) / static_vars.POSTPROCESSING_SPECIFICATION_FILE
        )
        postprocessing_data_interface = PostprocessingDataInterface.from_specification(postprocessing_spec)
        full_data = postprocessing_data_interface.load_full_data_unscaled()
        max_date = max(max_date, full_data.reset_index().date.max())

        for (scenario, label), measure in itertools.product(comparator.scenarios.items(), ['deaths', 'unscaled_deaths']):
            df = postprocessing_data_interface.load_output_summaries(scenario, measure=f'cumulative_{measure}')
            suffix = '_unscaled' if 'unscaled' in measure else ''
            df = df['mean'].rename(f'{label}{suffix}')
            data.append(df)

    logger.info('Producing cumulative deaths compare csv', context='transform')
    data = pd.concat(data, axis=1)
    sorted_locs_subset = [l for l in sorted_locs if l in data.reset_index().location_id.unique()]
    data = data.loc[sorted_locs_subset].reset_index()
    data['location_name'] = data.location_id.map(hierarchy.set_index('location_id').location_ascii_name)

    dates = [max_date] + [pd.Timestamp(date) for date in cumulative_death_spec.dates]
    date_labels = [date.strftime("%b%d_%Y") for date in dates]
    final_data = []
    for date, label in zip(dates, date_labels):
        date_data = (data[data.date == date]
                     .set_index(['location_id', 'location_name'])
                     .drop(columns='date'))
        date_data.columns = [f'{label}_{c}'for c in date_data.columns]
        final_data.append(date_data)

    final_data = pd.concat(final_data, axis=1)
    col_order = [f'{date_label}_{scenario_label}{suffix}'
                 for suffix, date_label, scenario_label in itertools.product(['', '_unscaled'], date_labels, labels)]
    assert sorted(col_order) == sorted(final_data.columns.tolist()), 'Column mismatch'
    pub_ref_col, ref_col, ref_unscaled_col = [f'{date_labels[0]}_{s}'
                                              for s in ['public_reference', 'reference', 'reference_unscaled']]
    final_data['diff_prev_current'] = final_data[ref_col] - final_data[pub_ref_col]
    final_data['pct_diff_prev_current'] = final_data['diff_prev_current'] / final_data[pub_ref_col]
    final_data = final_data[['diff_prev_current', 'pct_diff_prev_current'] + col_order]

    logger.info('Producing rates csv', context='transform')
    population = population.reindex(final_data.reset_index(level='location_name').index)
    final_data_rates = final_data.loc[:, col_order]
    final_data_rates = (final_data_rates
                        .divide(population.values, axis=0)
                        .reset_index())
    final_data_rates = final_data_rates.loc[final_data_rates.notnull().all(axis=1)]
    final_data = final_data.reset_index()

    logger.info('Producing demographics csvs', context='transform')
    country_level = final_data.location_id.isin(countries)
    top_20 = (final_data
              .loc[country_level, ['location_id', 'location_name', ref_col, ref_unscaled_col]]
              .sort_values(ref_col, ascending=False)
              .iloc[:20])
    top_20_rates = (final_data_rates
                    .loc[country_level, ['location_id', 'location_name', ref_col, ref_unscaled_col]]
                    .sort_values(ref_col, ascending=False)
                    .iloc[:20])

    logger.info('Writing results', context='write')
    final_data.to_csv(f'{diagnostics_version}/cumulative_death_compare.csv', index=False)
    final_data_rates.to_csv(f'{diagnostics_version}/cumulative_death_rate_compare.csv', index=False)
    top_20.to_csv(f'{diagnostics_version}/top_20_cumulative_death.csv', index=False)
    top_20_rates.to_csv(f'{diagnostics_version}/top_20_cumulative_death_rate.csv', index=False)

    logger.report()


@click.command()
@cli_tools.with_task_diagnostics_version
@cli_tools.add_verbose_and_with_debugger
def cumulative_deaths_compare_csv(diagnostics_version: str,
                                  verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_cumulative_deaths_compare_csv, logger, with_debugger)
    run(diagnostics_version=diagnostics_version)


if __name__ == '__main__':
    cumulative_deaths_compare_csv()
