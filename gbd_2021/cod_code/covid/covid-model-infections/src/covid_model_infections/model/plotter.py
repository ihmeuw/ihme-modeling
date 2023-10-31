from typing import Dict, Tuple, List
from pathlib import Path
from loguru import logger

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import seaborn as sns

MEASURE_COLORS = {
    'deaths':{'light':'indianred', 'dark':'darkred'},
    'cases':{'light':'mediumseagreen', 'dark':'darkgreen'},
    'hospitalizations':{'light':'dodgerblue', 'dark':'navy'}
}

DATE_LOCATOR = mdates.AutoDateLocator(maxticks=10)
DATE_FORMATTER = mdates.ConciseDateFormatter(DATE_LOCATOR, show_offset=False)


def get_dates(input_data: Dict, output_data: Dict, output_draws: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
    input_dates = [v['cumul'].reset_index()['date'] for k, v in input_data.items()]
    output_dates = [v['infections_cumul'][0].reset_index()['date'] for k, v in output_data.items()]
    draw_dates = [output_draws.reset_index()['date']]
    dates = pd.concat(input_dates + output_dates + draw_dates)
    start_date = dates.min() - pd.Timedelta(days=7)
    end_date = dates.max() + pd.Timedelta(days=7)
    
    return start_date, end_date


def plotter(plot_dir: Path, location_id: int, location_name: str,
            input_data: Dict,
            sero_data: pd.DataFrame, ratio_model_inputs: Dict,
            cross_variant_immunity: List[int], escape_variant_prevalence: pd.Series,
            output_data: Dict, smooth_infections: pd.Series, output_draws: pd.DataFrame,
            population: float,
            measures: List[str] = ['cases', 'hospitalizations', 'deaths']):
    start_date, end_date = get_dates(input_data, output_data, output_draws)
    
    n_cols = 3
    n_rows = 12
    widths = [2, 1, 2]
    heights = [1] * n_rows
    
    sns.set_style('whitegrid')
    fig = plt.figure(figsize=(16, 9), constrained_layout=True)
    gs = fig.add_gridspec(n_rows, n_cols, width_ratios=widths, height_ratios=heights)
    
    # line1 = plt.Line2D((0.41, 0.41),(0., 0.975), color='darkgrey', linewidth=2)
    # line2 = plt.Line2D((0.65, 0.65),(0., 0.975), color='darkgrey', linewidth=2)
    # fig.add_artist(line1)
    # fig.add_artist(line2)
    
    for i, measure in enumerate(measures):
        daily_ax = fig.add_subplot(gs[i*4:i*4+4, 0])
        cumul_ax = fig.add_subplot(gs[i*4:i*4+2, 1])
        if measure in list(input_data.keys()):
            if i == 0:
                daily_title = 'Daily'
                cumul_title = 'Cumulative (in thousands)'
            else:
                daily_title = None
                cumul_title = None
            daily_title = None
            cumul_title = None
            data_plot(daily_ax, measure.capitalize(), 'Daily',
                      input_data[measure]['daily'][1:], output_data[measure]['daily'][1:],
                      MEASURE_COLORS[measure]['light'], MEASURE_COLORS[measure]['dark'],
                      start_date, end_date, measure==measures[-1])

            data_plot(cumul_ax, None, 'Cumulative',
                      input_data[measure]['cumul'], output_data[measure]['cumul'],
                      MEASURE_COLORS[measure]['light'], MEASURE_COLORS[measure]['dark'],
                      start_date, end_date)
        else:
            daily_ax.axis('off')
            cumul_ax.axis('off')
    
    ratio_names = {'deaths':'IFR', 'hospitalizations':'IHR', 'cases':'IDR'}
    for i, measure in enumerate(measures):
        ratio_ax = fig.add_subplot(gs[i*4+2:i*4+4, 1])
        if measure in list(input_data.keys()):
            adj_ratio = smooth_infections.copy()
            adj_ratio.index += pd.Timedelta(days=int(np.mean(input_data[measure]['lags'])))
            adj_ratio = (output_data[measure]['daily'] * input_data[measure]['scalar'].mean()) / adj_ratio
            adj_ratio = adj_ratio.dropna()
            ratio_data = pd.concat([input_data[measure]['ratio'].groupby(level=1).mean(),
                                    input_data[measure]['daily']], axis=1).dropna()['ratio']
            ratio_data_fe = pd.concat([input_data[measure]['ratio'].groupby(level=1).mean(),
                                       input_data[measure]['daily']], axis=1).dropna()['ratio_fe']
            ratio_plot_range = pd.concat([ratio_data, ratio_data_fe, ratio_model_inputs[measure]['ratio_mean']])
            ratio_plot_range = ratio_plot_range.replace((-np.inf, np.inf), np.nan).dropna()
            ratio_plot_range_min = ratio_plot_range.min()
            ratio_plot_range_max = ratio_plot_range.max()
            ratio_plot_lims = (max(0, ratio_plot_range_min - ratio_plot_range_max * 0.2),
                               min(1, ratio_plot_range_max + ratio_plot_range_max * 0.2))
            # if ratio_names[measure] == 'IFR':
            #     adj_ratio[adj_ratio < ratio_plot_lims[0]] = np.nan
            #     adj_ratio[adj_ratio > ratio_plot_lims[1]] = np.nan
            # elif ratio_names[measure] == 'IHR':
            #     adj_ratio[adj_ratio < ratio_plot_lims[0]] = np.nan
            #     adj_ratio[adj_ratio > ratio_plot_lims[1]] = np.nan
            # elif ratio_names[measure] == 'IDR':
            #     adj_ratio[adj_ratio < 0] = np.nan
            #     adj_ratio[adj_ratio > 1] = np.nan
            # else:
            #     raise ValueError('Unexpected ratio present in plotting.')
            ratio_plot(ratio_ax,
                       ratio_plot_lims,
                       ratio_names[measure],
                       ratio_data,
                       ratio_data_fe,
                       adj_ratio,
                       ratio_model_inputs[measure],
                       MEASURE_COLORS[measure]['light'],
                       MEASURE_COLORS[measure]['dark'],
                       start_date, end_date, measure==measures[-1])
        else:
            ratio_ax.axis('off')
    
    model_measures = [m for m in measures if m in list(output_data.keys())]
    #whitespace_top = fig.add_subplot(gs[0:1, 2])
    #whitespace_top.axis('off')
    gs[i*4:i*4+4, 0]
    dailymodel_ax = fig.add_subplot(gs[0:4, 2])
    infection_daily_data = {mm: pd.concat(output_data[mm]['infections_daily'], axis=1).dropna().mean(axis=1)[1:] 
                            for mm in model_measures}
    model_plot(dailymodel_ax, 'Infections', 'Daily infections', infection_daily_data, None,
               smooth_infections.dropna()[1:],
               output_draws.dropna()[1:], start_date, end_date, False)
    #whitespace_mid = fig.add_subplot(gs[5:7, 2])
    #whitespace_mid.axis('off')
    
    cumul_infections_measures = {mm: (pd.concat(output_data[mm]['infections_cumul'], axis=1).dropna().mean(axis=1) / population) * 100 
                                 for mm in model_measures}
    cumul_infections_draws = output_draws.cumsum().dropna()
    cumul_infections_point = smooth_infections.cumsum().dropna()
    
    cumulinfmodel_ax = fig.add_subplot(gs[4:8, 2])
    model_plot(cumulinfmodel_ax, None, 'Cumulative infections (%)',
               cumul_infections_measures,
               sero_data,
               (cumul_infections_point / population) * 100,
               (cumul_infections_draws / population) * 100,
               start_date, end_date, False)
    
    expand_dates = [date for date in pd.date_range('2019-11-01', end_date) 
                    if not date in escape_variant_prevalence.index]
    if expand_dates:
        date_idx = pd.Index(expand_dates, name='date')
        if isinstance(escape_variant_prevalence, pd.Series):
            escape_variant_prevalence = pd.concat([pd.Series(np.nan, index=date_idx, name=escape_variant_prevalence.name),
                                                   escape_variant_prevalence])
            escape_variant_prevalence = escape_variant_prevalence.fillna(method='bfill').fillna(method='ffill')
        elif escape_variant_prevalence.empty:
            escape_variant_prevalence = pd.Series(np.nan, index=date_idx, name='escape_variant_prevalence')
    cumul_infected_measures = {mm: pd.concat(output_data[mm]['infections_daily'], axis=1).dropna().mean(axis=1)
                               for mm in model_measures}
    cumul_infected_measures = {mm: (calc_infected(mm_data,
                                                  escape_variant_prevalence.loc[mm_data.index].values,
                                                  np.mean(cross_variant_immunity),
                                                  population,).dropna() / population) * 100 
                               for mm, mm_data in cumul_infected_measures.items()}
    cumul_infected_draws = calc_infected(output_draws,
                                         escape_variant_prevalence.loc[output_draws.index].to_frame().values,
                                         np.array(cross_variant_immunity),
                                         population,).dropna()
    cumul_infected_point = calc_infected(smooth_infections,
                                         escape_variant_prevalence.loc[smooth_infections.index].values,
                                         np.mean(cross_variant_immunity),
                                         population,).dropna()
    del output_draws, smooth_infections
    
#     if not daily_reinfection_rr.empty:
#         sero_data = sero_data.join(daily_reinfection_rr, how='left')
#         sero_data['inflation_factor'] = sero_data['inflation_factor'].fillna(1)
#         sero_data['seroprev_mean_no_vacc_waning'] /= sero_data['inflation_factor']
#         del sero_data['inflation_factor']

    cumulpropmodel_ax = fig.add_subplot(gs[8:12, 2])
    model_plot(cumulpropmodel_ax, None, 'Cumulative infected (%)',
               cumul_infected_measures,
               None, # sero_data,
               (cumul_infected_point / population) * 100,
               (cumul_infected_draws / population) * 100,
               start_date, end_date, True)
    #whitespace_bottom = fig.add_subplot(gs[11:12, 2])
    #whitespace_bottom.axis('off')

    fig.suptitle(f'{location_name} ({location_id})', fontsize=20)
    plt.tight_layout()
    if plot_dir is not None:
        plt.switch_backend('pdf')
        fig.savefig(plot_dir / f'{location_id}.pdf')
        plt.close(fig)
    else:
        plt.show()
        

def calc_infected(daily_infections, escape_variant_prevalence, cross_variant_immunity, population):
    non_ev_infections = daily_infections * (1 - escape_variant_prevalence)
    ev_infections = daily_infections * escape_variant_prevalence
    repeat_infections = (1 - cross_variant_immunity) * (non_ev_infections.cumsum() / population).clip(0, 1) * ev_infections
    first_infections = daily_infections - repeat_infections
    cumul_infected = first_infections.cumsum()

    return cumul_infected


def data_plot(ax, title, ylabel, raw_data, smooth_data, clight, cdark, start_date, end_date, include_xticks=False):
    ax.scatter(raw_data.index, raw_data,
               c=clight, edgecolors=cdark, alpha=0.4)
    ax.plot(raw_data, color=clight, alpha=0.2)
    ax.plot(smooth_data, color=cdark, alpha=1.)

    if title:
        ax.set_title(title, loc='left', fontsize=16)
    ax.set_ylabel(ylabel)
    ax.set_xlim(start_date, end_date)
    # if include_xticks:
    #     ax.tick_params('x', labelrotation=60)
    ax.xaxis.set_major_locator(DATE_LOCATOR)
    ax.xaxis.set_major_formatter(DATE_FORMATTER)
    if not include_xticks:
        ax.set_xticklabels([])
    
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)


def ratio_plot(ax, ylims, ylabel, ratio_data, ratio_data_fe, adj_ratio, ratio_input_data,
               clight, cdark, start_date, end_date, include_xticks=True):
    ax.plot(ratio_data, color=cdark, alpha=0.8)
    ax.plot(ratio_data_fe, linestyle='--', color=cdark, alpha=0.8)
    ax.plot(adj_ratio, color=clight, alpha=0.8)
    
    ax.scatter(ratio_input_data.loc[ratio_input_data['is_outlier'] == 0].index,
               ratio_input_data.loc[ratio_input_data['is_outlier'] == 0, 'ratio_mean'],
               color=cdark, alpha=0.8, marker='o', facecolors='none')
    ax.scatter(ratio_input_data.loc[ratio_input_data['is_outlier'] == 1].index,
               ratio_input_data.loc[ratio_input_data['is_outlier'] == 1, 'ratio_mean'],
               color=cdark, alpha=0.8, marker='x')
    
    ax.set_ylabel(ylabel)
    ax.set_ylim(*ylims)
    ax.set_xlim(start_date, end_date)
    
        
    # if include_xticks:
    #     ax.tick_params('x', labelrotation=60)
    ax.xaxis.set_major_locator(DATE_LOCATOR)
    ax.xaxis.set_major_formatter(DATE_FORMATTER)
    if not include_xticks:
        ax.set_xticklabels([])
    
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)


def model_plot(ax, title, ylabel, measure_data, sero_data, smooth_infections, output_draws,
               start_date, end_date, include_xticks=False):
    if sero_data is not None:
        ax.scatter(sero_data.loc[sero_data['is_outlier'] == 1].index,
                   sero_data.loc[sero_data['is_outlier'] == 1, 'seroprevalence'] * 100,
                   s=80, c='maroon', edgecolors='maroon', alpha=0.45, marker='x')
        ax.scatter(sero_data.loc[sero_data['is_outlier'] == 0].index,
                   sero_data.loc[sero_data['is_outlier'] == 0, 'seroprevalence'] * 100,
                   s=80, c='darkturquoise', edgecolors='darkcyan', alpha=0.3, marker='s')
        ax.scatter(sero_data.loc[sero_data['is_outlier'] == 0].index,
                   sero_data.loc[sero_data['is_outlier'] == 0, 'seroprevalence_no_vacc'] * 100,
                   s=80, c='orange', edgecolors='darkorange', alpha=0.3, marker='^')
        ax.scatter(sero_data.loc[sero_data['is_outlier'] == 0].index,
                   sero_data.loc[sero_data['is_outlier'] == 0, 'sero_sample_mean'] * 100,
                   s=100, c='mediumorchid', edgecolors='darkmagenta', alpha=0.6, marker='o')

    ax.plot(output_draws.mean(axis=1), color='black', alpha=0.8)
    ax.plot(smooth_infections, color='black', linestyle=':', alpha=0.6)
    ax.fill_between(output_draws.index,
                    np.percentile(output_draws, 2.5, axis=1),
                    np.percentile(output_draws, 97.5, axis=1),
                    color='black', alpha=0.2)
    for m, md in measure_data.items():
        ax.plot(md, color=MEASURE_COLORS[m]['dark'], linestyle='--', alpha=0.6)
    if title:
        ax.set_title(title, loc='left', fontsize=16)
    ax.set_ylabel(ylabel)
    ax.set_xlim(start_date, end_date)
    # if include_xticks:
    #     ax.tick_params('x', labelrotation=60)
    ax.xaxis.set_major_locator(DATE_LOCATOR)
    ax.xaxis.set_major_formatter(DATE_FORMATTER)
    if not include_xticks:
        ax.set_xticklabels([])
    
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
