import os
from typing import List, Tuple
import tqdm
import multiprocessing
import tempfile
from pathlib import Path

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
from pypdf import PdfWriter

from onemod.utils import get_handle

from covid_gbd_model.paths import TOTAL_COVID_ASDR_PATH


def prepare_data(model_directory: str, obs_measure: str, age_metadata: pd.Series) -> pd.DataFrame:
    dataif, config = get_handle(model_directory)
    data = (
        pd.merge(
            left=(
                dataif.load_data(
                    columns=config.ids + [config.obs]
                )
            ),
            right=(
                dataif.load_spxmod(
                    "predictions.parquet",
                    columns=config.ids + [config.pred]
                )
                .rename(columns={config.pred: "spxmod"})
            ),
            on=config.ids,
        )
        .merge(
            right=(
                dataif.load_kreg(
                    "predictions.parquet",
                    columns=config.ids + [config.pred]
                )
                .rename(columns={config.pred: "kreg"})
            ),
            on=config.ids,
            how="right",
        )
    )
    data = data.rename(columns={config.obs: 'obs'})
    if os.path.exists(f'{model_directory}/data/unadj_data.parquet'):
        data = data.merge(
            pd.read_parquet(f'{model_directory}/data/unadj_data.parquet').loc[:, config.ids + [config.obs]].rename(columns={config.obs: 'unadj_obs'}),
            on=config.ids
        )
    else:
        data['unadj_obs'] = np.nan
    if obs_measure == 'ifr':
        data = data.merge(
            pd.read_parquet(f'{model_directory}/data/data.parquet').loc[:, config.ids + ['inf_ancestral']],
            on=config.ids
        )
        data.loc[:, ['obs', 'unadj_obs', 'spxmod', 'kreg']] = (
            data.loc[:, ['obs', 'unadj_obs', 'spxmod', 'kreg']]
            .multiply(data['inf_ancestral'], axis=0)
        )
    data = data.set_index(config.ids).loc[:, ['obs', 'unadj_obs', 'spxmod', 'kreg']]

    processed = pd.read_parquet(f'{model_directory}/processed/raked_summaries.parquet')
    data = data.join(processed, how='outer').join(age_metadata)

    return data


def prepare_asdr(model_directory: str, total_covid_asdr_path: Path = TOTAL_COVID_ASDR_PATH):
    gbd2023_asdr = pd.read_parquet(f'{model_directory}/processed/raked_summaries.parquet')
    gbd2023_asdr = gbd2023_asdr.loc[:, :, 27, [1, 2]].reset_index('age_group_id', drop=True).loc[:, 'mean_value']

    gbd2021_asdr = pd.read_csv(total_covid_asdr_path)
    gbd2021_asdr = (
        gbd2021_asdr
        .set_index(['location_id', 'year_id', 'sex_id'])
        .loc[gbd2023_asdr.index, 'covid_asdr']
        # .loc[:, year_ids, :]
    )
    asdr = pd.concat([gbd2023_asdr.rename('gbd2023'), gbd2021_asdr.rename('gbd2021')], axis=1)

    return asdr


def set_share_axes(axs: List, target=None, sharex=False, sharey=False):
    if target is None:
        target = axs.flat[0]
    for ax in axs.flat:
        if sharex:
            target._shared_axes['x'].join(target, ax)
        if sharey:
            target._shared_axes['y'].join(target, ax)
    if sharex and axs.ndim > 1:
        for ax in axs[:-1,:].flat:
            ax.xaxis.set_tick_params(which='both', labelbottom=False, labeltop=False)
            ax.xaxis.offsetText.set_visible(False)
    if sharey and axs.ndim > 1:
        for ax in axs[:,1:].flat:
            ax.yaxis.set_tick_params(which='both', labelleft=False, labelright=False)
            ax.yaxis.offsetText.set_visible(False)


def location_plot(inputs: Tuple):
    location_id, location_aes, data, asdr, tmp_plot_path = inputs
    sex_aes = {1: ('Males', ('indianred', 'firebrick')), 2: ('Females', ('mediumpurple', 'rebeccapurple'))}

    sns.set_style('whitegrid')
    fig, ax = plt.subplots(2, 3, figsize=(12, 8))
    set_share_axes(ax[0,:], sharey=True)
    set_share_axes(ax[1,:2], sharey=True)
    for sex_id, (sex_label, colors) in sex_aes.items():
        for i, year_id in enumerate(data.index.get_level_values('year_id').unique().tolist()):
            idx = int(i >= 3), i % 3

            plot_data = np.log(data.loc[year_id, :, sex_id].set_index('age_mid').sort_index())

            plot_data.reset_index().plot(
                kind='scatter',
                ax=ax[idx],
                x='age_mid',
                y='obs',
                color=colors[0],
                edgecolors=colors[1],
                s=50,
                alpha=0.8,
                marker='o',
            )
            plot_data.reset_index().plot(
                kind='scatter',
                ax=ax[idx],
                x='age_mid',
                y='unadj_obs',
                color=colors[0],
                edgecolors=colors[1],
                s=30,
                alpha=0.4,
                marker='s',
            )
            plot_data['spxmod'].plot(
                ax=ax[idx],
                color=colors[1],
                alpha=0.6,
                linestyle='--',
            )
            plot_data['kreg'].plot(
                ax=ax[idx],
                color=colors[1],
                alpha=0.8,
                linestyle='-',
            )
            ax[idx].fill_between(
                x=plot_data.index,
                y1=plot_data['lower_value'],
                y2=plot_data['upper_value'],
                color=colors[0],
                alpha=0.4,
            )

            ax[idx].set_xlabel('Age')
            ax[idx].set_ylabel('ln(csmr)')
            ax[idx].set_title(year_id)
        i += 1
        idx = int(i >= 3), i % 3
        (asdr.loc[location_id, :, sex_id, :].loc[:, 'gbd2023'] * 1e5).plot(
            ax=ax[idx],
            color=colors[1],
            alpha=0.8,
            linestyle='-',
            label=sex_label,
        )
        if location_id != 1:
            (asdr.loc[location_aes['parent_id'], :, sex_id, :].loc[:, 'gbd2023'] * 1e5).plot(
                ax=ax[idx],
                color=colors[1],
                alpha=0.6,
                linestyle=':',
                label='',
            )
        (asdr.loc[location_id, :, sex_id, :].loc[:, 'gbd2021'] * 1e5).plot(
            ax=ax[idx],
            color=colors[0],
            alpha=0.6,
            linestyle='--',
            label='',
        )
        ax[idx].set_ylabel('Rate per 100,000')
        ax[idx].set_xlabel('Year')
        ax[idx].set_title('Age-standardized over time')
    if location_id != 1:
        ax[idx].plot(
            np.nan, np.nan,
            color='darkgrey',
            alpha=0.6,
            linestyle=':',
            label=location_aes['parent_name'],
        )
    ax[idx].plot(
        np.nan, np.nan,
        color='darkgrey',
        alpha=0.6,
        linestyle='--',
        label='GBD 2021',
    )
    ax[idx].legend()

    fig.suptitle(f"{location_aes['location_name']} ({location_id})")
    fig.tight_layout()
    if tmp_plot_path is None:
        fig.show()
    else:
        fig.savefig(f'{tmp_plot_path}/{location_id}.pdf')
        plt.close(fig)


def pdf_merger(inpdfs: List[str], location_names: List[str], parent_names: List[str], levels: List[int], outpdf: str):
    assert all([i.endswith('.pdf') for i in inpdfs]), 'Not all files passed into `pdfs` are actual PDFs.'
    assert outpdf.endswith('.pdf'), 'Provided output file is not a PDF.'

    exists = [os.path.exists(inpdf) for inpdf in inpdfs]
    pages = pd.Series(exists).replace(False, np.nan).cumsum().bfill().astype(int)
    pages = (pages - 1).to_list()

    merger = PdfWriter()
    merger_tracker = {}
    for inpdf, location_name, page, parent_name, level in zip(inpdfs, location_names, pages, parent_names, levels):
        if os.path.exists(inpdf):
            merger.append(inpdf)
        if parent_name in location_names and level > 0:
            if parent_name == location_name:
                merger_tracker[location_name] = merger.add_outline_item(f'{location_name} ', page, merger_tracker[parent_name])
            else:
                merger_tracker[location_name] = merger.add_outline_item(location_name, page, merger_tracker[parent_name])
        else:
            merger_tracker[location_name] = merger.add_outline_item(location_name, page)

    merger.write(outpdf)
    merger.close()


def grid_plots(metadata_directory: str, model_directory: str, obs_measure: str, verbose: bool = False):
    age_metadata = pd.read_parquet(f'{metadata_directory}/age_metadata.parquet')
    age_metadata = age_metadata.set_index('age_group_id').loc[:, 'age_mid']

    data = prepare_data(model_directory, obs_measure, age_metadata)
    asdr = prepare_asdr(model_directory)

    location_metadata = pd.read_parquet(f'{metadata_directory}/location_metadata.parquet')
    location_metadata = (
        location_metadata
        .loc[:, ['location_id', 'level', 'location_name', 'parent_id']]
    )
    location_metadata = (
        location_metadata
        .merge(
            location_metadata.drop(['level', 'parent_id'], axis=1).rename(columns={'location_id': 'parent_id', 'location_name': 'parent_name'}),
            how='left'
        )
        .loc[location_metadata['location_id'].isin(data.index.get_level_values('location_id').unique())]
    )
    location_dict = location_metadata.set_index('location_id').T.to_dict()

    with tempfile.TemporaryDirectory() as tmp_plot_path:
        inputs = []
        for location_id, location_aes in location_dict.items():
            location_data = data.loc[location_id]
            if location_id == 1:
                l_p = [1]
            else:
                l_p = [location_id, location_aes['parent_id']]
            location_asdr = asdr.loc[l_p]
            inputs.append(
                (location_id, location_aes, location_data, location_asdr, tmp_plot_path)
            )

        with multiprocessing.Pool(25) as pool:
            _ = list(tqdm.tqdm(pool.imap(location_plot, inputs), total=len(inputs), disable=not verbose))

        inpdfs = []
        location_names = []
        parent_names = []
        levels = []
        outpdf = f'{model_directory}/plots/grid_plots.pdf'
        for location_id, location_aes in location_dict.items():
            inpdfs.append(f'{tmp_plot_path}/{location_id}.pdf')
            location_names.append(location_aes['location_name'])
            parent_names.append(location_aes['parent_name'])
            levels.append(location_aes['level'])
        pdf_merger(
            inpdfs=inpdfs,
            location_names=location_names,
            parent_names=parent_names,
            levels=levels,
            outpdf=outpdf,
        )
