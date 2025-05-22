from pathlib import Path

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from covid_gbd_model.paths import TOTAL_COVID_ASDR_PATH


def scatters(inputs_root: Path, model_root: Path, total_covid_asdr_path: Path = TOTAL_COVID_ASDR_PATH):
    colors = [
        'lightcoral',
        'firebrick',
        'orange',
        'olivedrab',
        'lightseagreen',
        'royalblue',
        'darkorchid',
    ]
    shapes = {
        1: 's',
        2: '^',
    }
    plot_years = [2020, 2021, 2022, 2023]

    location_metadata = pd.read_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata = location_metadata.loc[location_metadata['level'] == 3].sort_values('sort_order')

    gbd2023 = pd.read_parquet(model_root / 'processed' / 'raked_summaries.parquet')
    gbd2023 = gbd2023.loc[:, :, 27, [1, 2]].reset_index('age_group_id', drop=True).loc[:, 'mean_value']
    gbd2021 = pd.read_csv(total_covid_asdr_path)
    gbd2021 = gbd2021.set_index(['location_id', 'year_id', 'sex_id']).loc[:, 'covid_asdr']

    plot_data = pd.concat([gbd2023.rename('gbd2023'), gbd2021.rename('gbd2021')], axis=1)
    plot_data = plot_data.loc[location_metadata['location_id'], plot_years, :]

    sns.set_style('whitegrid')
    fig, ax = plt.subplots(2, 2, figsize=(12, 8))
    plot_sets = location_metadata.groupby('super_region_name')['location_id'].apply(lambda x: x.to_list())
    for i, year_id in enumerate(plot_years):
        idx = int(i >= 2), i % 2
        for c_i, (super_region_name, location_ids) in enumerate(plot_sets.items()):
            color = colors[c_i]
            for sex_id, sex in [(1, 'Males'), (2, 'Females')]:
                ax[idx].scatter(
                    plot_data.loc[location_ids, year_id, sex_id].loc[:, 'gbd2021'] * 1e5,
                    plot_data.loc[location_ids, year_id, sex_id].loc[:, 'gbd2023'] * 1e5,
                    color=color,
                    marker=shapes[sex_id],
                    s=40,
                    alpha=0.25,
                )
                if i == 0 and c_i == 0:
                    ax[idx].scatter(
                        np.nan,
                        np.nan,
                        color='darkgrey',
                        marker=shapes[sex_id],
                        s=40,
                        alpha=0.5,
                        label=sex,
                    )
            if i == 0:
                ax[0, 0].scatter(
                    np.nan,
                    np.nan,
                    color=color,
                    marker='o',
                    s=40,
                    alpha=0.5,
                    label=super_region_name.replace(', and', ',\nand'),
                )
        ax[idx].set_title(year_id)
        ax[idx].set_xlabel('GBD 2021')
        ax[idx].set_ylabel('GBD 2023')

        axis_limit = (plot_data.loc[:, year_id, :].loc[:, ['gbd2021', 'gbd2023']] * 1e5).stack().max() * 1.1
        ax[idx].plot((0, axis_limit), (0, axis_limit), color='darkgrey')
        ax[idx].set_xlim(0, axis_limit)
        ax[idx].set_ylim(0, axis_limit)

    ax[0, 0].legend(fontsize=7)
    fig.suptitle('National age-standardized death rates (per 100,000)')
    fig.tight_layout()

    fig.savefig(model_root / 'plots' / 'scatters.pdf')
    plt.close(fig)
