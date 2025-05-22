import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import numpy as np

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import cm

from covid_gbd_model.paths import OUTPUT_ROOT, SHAPEFILE_PATH
from covid_gbd_model.variables import MODEL_YEARS, RELEASE_ID


def mapper(
    inputs_root: Path, model_root: Path,
    model_years: List[int] = MODEL_YEARS[:-1],
    shapefile_path: Path = SHAPEFILE_PATH,
    compare_version_id: Optional[int] = None,
):
    location_metadata = pd.read_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata = location_metadata.loc[location_metadata['level'] == 3].sort_values('sort_order')
    loc_ids = location_metadata.loc[location_metadata['level'] == 3, 'location_id'].rename('loc_id')

    if compare_version_id is None:
        pred = pd.read_parquet(model_root / 'processed' / 'raked_summaries.parquet')
        pred = pred.loc[:, :, 27, 3].loc[:, 'mean_value']
        measures = ['mx', 'infections', 'ifr', 'idr']
    else:
        import db_queries
        pred = db_queries.get_outputs(
            'cause',
            release_id=RELEASE_ID,
            compare_version_id=compare_version_id,
            cause_id=1048,
            location_id=loc_ids.to_list(),
            year_id=model_years,
            age_group_id=27,
            sex_id=3,
            measure_id=1,
            metric_id=3,
        )
        pred = (
            pred
            .set_index(['location_id', 'year_id'])
            .loc[:, 'val']
            .rename('mean_value')
        )
        measures = ['mx_codcorrect']

    data = pd.read_parquet(model_root / 'data' / 'data.parquet')
    if data['idr'].min() < -1 and data['idr'].max() == 0:
        data['idr'] = np.exp(data['idr'])
    data = data.groupby(['location_id', 'year_id'])[['idr', 'inf_ancestral']].mean()

    data = pd.concat([data, pred], axis=1).loc[loc_ids, model_years, :].sort_index()
    data.index.names = ['loc_id', 'year_id']

    map_data = gpd.read_file(shapefile_path)
    map_data = map_data.loc[map_data['loc_id'].isin(loc_ids)]

    for measure in measures:
        if measure.startswith('mx'):
            year_bins = {
                2020: list(range(20, 220, 20)),
                2021: list(range(50, 550, 50)),
                2022: list(range(20, 220, 20)),
                2023: list(range(10, 110, 10)),
            }
            plot_data = (data['mean_value'] * 1e5).copy()
            label = 'Age-standardized deaths per 100,000 people'
        elif measure == 'infections':
            year_bins = {
                2020: list(range(10, 110, 10)),
                2021: list(range(20, 220, 20)),
                2022: list(range(20, 220, 20)),
                2023: list(range(30, 330, 30)),
            }
            plot_data = (data['inf_ancestral'] * 100).copy()
            label = 'Infections (%)'
        elif measure == 'ifr':
            year_bins = {
                2020: list(range(200, 2200, 200)),
                2021: list(range(200, 2200, 200)),
                2022: list(range(20, 220, 20)),
                2023: list(range(10, 110, 10)),
            }
            plot_data = ((data['mean_value'] * 1e5) / data['inf_ancestral']).copy()
            label = 'Age-standardized deaths per 100,000 infections'
        elif measure == 'idr':
            year_bins = {
                2020: list(range(1, 10, 1)),
                2021: list(range(2, 22, 2)),
                2022: list(range(10, 110, 10)),
                2023: list(range(10, 110, 10)),
            }
            plot_data = (data['idr'] * 100).copy()
            label = 'IDR (%)'
        else:
            raise ValueError(f'Invalid measure: {measure}')
        with PdfPages(model_root / 'plots' / f'{measure}_maps.pdf') as pdf:
            for model_year in model_years:
                # print(f'{measure}, {model_year}')
                year_map_data = map_data.merge(plot_data.loc[:, model_year].rename('plot_var').reset_index(), how='left')
                bins = year_bins[model_year]

                for i in range(1, len(bins) + 2):
                    b = [0] + bins + [np.inf]
                    s = b[i-1]
                    e = b[i]
                    n = year_map_data.loc[year_map_data['plot_var'].between(s, e)].shape[0]
                    # print(f'    {s} to {e}: {n} locations')
                cmap = cm.get_cmap('Spectral')
                colors = list(reversed([cmap(c) for c in np.linspace(0, 1, len(bins) + 1)]))

                bin_labels = [f'Under {bins[0]}']
                for i in range(len(bins) - 1):
                    bin_labels += [f'{bins[i]} to {bins[i+1]}']
                bin_labels += [f'{bins[-1]} and above']

                fig, ax = plt.subplots(2, 1, gridspec_kw={'height_ratios': [6, 1]}, figsize=(13, 7), dpi=200)
                year_map_data.plot(ax=ax[0], column='plot_var', cmap='Spectral_r',
                                scheme='user_defined', classification_kwds={'bins':bins},
                                edgecolor='black', linewidth=0.2,
                                missing_kwds={'color':'lightgrey'},
                                rasterized=True,
                                )
                ax[0].set_axis_off()
                ax[0].get_figure()
                ax[0].axis('off')

                for i, (bin_label, color) in enumerate(zip(bin_labels, colors)):
                    ax[1].bar(i, np.nan, color=color, label=bin_label)
                ax[1].axis('off')
                ax[1].legend(bbox_to_anchor=[0.5, 0.5], loc='center', ncol=int(np.ceil(len(bin_labels) / 2)))

                fig.suptitle(f'{label}, {model_year}')
                fig.tight_layout()
                pdf.savefig(fig)
                plt.close()
        del bins


if __name__ == '__main__':
    mapper(
        inputs_root=OUTPUT_ROOT / 'modeling' / sys.argv[1] / 'inputs',
        model_root=OUTPUT_ROOT / 'modeling' / sys.argv[1] / 'spliced_model',
        compare_version_id=int(sys.argv[2]),
    )
