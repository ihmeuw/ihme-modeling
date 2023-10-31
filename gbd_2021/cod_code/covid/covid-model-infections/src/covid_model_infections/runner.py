import sys
from pathlib import Path
from typing import Dict, List
import dill as pickle
from loguru import logger
import functools
import multiprocessing
from tqdm import tqdm

import pandas as pd

from covid_shared import shell_tools, cli_tools

from covid_model_infections import data, cluster, model, aggregation
from covid_model_infections.pdf_merger import pdf_merger
from covid_model_infections.utils import SUB_LOCATIONS

MP_THREADS = 25

## TODO:
##     - holdouts


def build_directories(output_root: Path):
    logger.info('Creating directories.')
    model_in_dir = output_root / 'model_inputs'
    model_out_dir = output_root / 'model_outputs'
    plot_dir = output_root / 'plots'
    infections_draws_dir = output_root / 'infections_draws'
    shell_tools.mkdir(model_in_dir)
    shell_tools.mkdir(model_out_dir)
    shell_tools.mkdir(plot_dir)
    shell_tools.mkdir(infections_draws_dir)
    
    return model_in_dir, model_out_dir, plot_dir, infections_draws_dir
    
    
def prepare_input_data(app_metadata: cli_tools.Metadata,
                       rates_root: Path,
                       model_in_dir: Path,
                       n_draws: int,
                       fh: bool,
                       gbd: bool,
                       no_deaths: bool,):
    logger.info('Loading supplemental data.')
    hierarchy = data.load_hierarchy(rates_root, fh, gbd,)
    pop_data = data.load_population(rates_root)

    logger.info('Loading epi report data.')
    cumul_deaths, daily_deaths, deaths_manipulation_metadata = data.load_model_inputs(
        rates_root, hierarchy, 'deaths', fh,
    )
    cumul_hospital, daily_hospital, hospital_manipulation_metadata = data.load_model_inputs(
        rates_root, hierarchy, 'hospitalizations', fh,
    )
    cumul_cases, daily_cases, cases_manipulation_metadata = data.load_model_inputs(
        rates_root, hierarchy, 'cases', fh,
    )

    cumul_deaths, cumul_hospital, cumul_cases,\
    daily_deaths, daily_hospital, daily_cases = data.trim_leading_zeros(
        [cumul_deaths, cumul_hospital, cumul_cases],
        [daily_deaths, daily_hospital, daily_cases],
    )

    app_metadata.update({'data_manipulation': {
        'deaths': deaths_manipulation_metadata,
        'hospitalizations': hospital_manipulation_metadata,
        'cases': cases_manipulation_metadata,
    }})

    logger.info('Loading estimated ratios.')
    ifr = data.load_ifr(rates_root, n_draws,)
    ifr_rr = data.load_ifr_rr(rates_root, n_draws,)
    ifr_model_data = data.load_ifr_data(rates_root)
    cross_variant_immunity = data.load_cross_variant_immunity(rates_root, n_draws,)
    variant_risk_ratio = data.load_variant_risk_ratio(rates_root, n_draws,)
    ihr = data.load_ihr(rates_root, n_draws,)
    ihr_model_data = data.load_ihr_data(rates_root)
    idr = data.load_idr(rates_root, n_draws, (0., 1.),) # Assumes IDR has estimated floor already applied
    idr_model_data = data.load_idr_data(rates_root)
    
    logger.info('Loading total Covid scalars.')
    em_scalar_data = data.load_em_scalars(rates_root, n_draws,)

    logger.info('Loading durations for each draw.')
    durations = data.load_durations(rates_root, n_draws,)

    logger.info('Loading additional rates model outputs.')
    sero_data = data.load_sero_data(rates_root)
    test_data = data.load_test_data(rates_root)
    vaccine_data = data.load_vaccine_data(rates_root)
    escape_variant_prevalence = data.load_escape_variant_prevalence(rates_root)

    logger.info('Compiling model input data and writing intermediate files.')
    if no_deaths:
        logger.warning('Not using deaths to estimate infections.')
    model_data = {
        'no_deaths': no_deaths,
        'durations': durations,
        'daily_deaths': daily_deaths, 'cumul_deaths': cumul_deaths, 'ifr': ifr, 'ifr_rr': ifr_rr,
        'daily_hospital': daily_hospital, 'cumul_hospital': cumul_hospital, 'ihr': ihr,
        'daily_cases': daily_cases, 'cumul_cases': cumul_cases, 'idr': idr,
    }
    # TODO: centralize this information, is used elsewhere...
    estimated_ratios = {'deaths': ('ifr', [d['exposure_to_death'] for d in durations], ifr.copy()),
                        'hospitalizations': ('ihr', [d['exposure_to_admission'] for d in durations], ihr.copy()),
                        'cases': ('idr', [d['exposure_to_case'] for d in durations], idr.copy()),}
    model_data_path = model_in_dir / 'model_data.pkl'
    with model_data_path.open('wb') as file:
        pickle.dump(model_data, file, -1)
    cross_variant_immunity_path = model_in_dir / 'cross_variant_immunity.pkl'
    with cross_variant_immunity_path.open('wb') as file:
        pickle.dump(cross_variant_immunity, file, -1)
    for input_data, input_file in [(hierarchy, 'hierarchy'),
                                   (pop_data, 'pop_data'),
                                   (sero_data, 'sero_data'),
                                   (test_data, 'test_data'),
                                   (vaccine_data, 'vaccine_data'),
                                   (em_scalar_data, 'em_scalar_data'),
                                   (escape_variant_prevalence, 'escape_variant_prevalence'),
                                   (ifr_model_data, 'ifr_model_data'),
                                   (ihr_model_data, 'ihr_model_data'),
                                   (idr_model_data, 'idr_model_data'),]:
        input_data_path = model_in_dir / f'{input_file}.parquet'
        input_data.to_parquet(input_data_path)
        
    agg_plot_inputs = {
        'pop_data': pop_data,
        'sero_data': sero_data,
        'cross_variant_immunity': cross_variant_immunity,
        'escape_variant_prevalence': escape_variant_prevalence,
        'ifr_model_data': ifr_model_data,
        'ihr_model_data': ihr_model_data,
        'idr_model_data': idr_model_data,
    }
    
    reported_deaths = cumul_deaths.groupby(level=0).max()

    return app_metadata, hierarchy, estimated_ratios, variant_risk_ratio, agg_plot_inputs, durations, reported_deaths


def run_location_models(gbd: bool,
                        hierarchy: pd.DataFrame,
                        n_draws: int,
                        model_in_dir: Path,
                        model_out_dir: Path,
                        plot_dir: Path,
                        output_root: Path,):
    logger.info('Launching location-specific models.')
    most_detailed = hierarchy['most_detailed'] == 1
    location_ids = hierarchy.loc[most_detailed, 'location_id'].to_list()
    job_args_map = {
        location_id: [model.runner.__file__,
                      location_id, n_draws, str(model_in_dir), str(model_out_dir), str(plot_dir),]
        for location_id in location_ids if location_id not in SUB_LOCATIONS
    }
    if gbd:
        cluster.run_cluster_jobs('covid_loc_inf', output_root, job_args_map, 'gbd')
    else:
        cluster.run_cluster_jobs('covid_loc_inf', output_root, job_args_map, 'standard')
    
    
def collect_results(app_metadata: cli_tools.Metadata,
                    hierarchy: pd.DataFrame,
                    model_out_dir: Path,):
    most_detailed = hierarchy['most_detailed'] == 1
    location_ids = hierarchy.loc[most_detailed, 'location_id'].to_list()
    
    logger.info('Compiling infection draws.')
    infections_draws = []
    for draws_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith('_infections_draws.parquet')]:
        infections_draws.append(pd.read_parquet(draws_path))
    infections_draws = pd.concat(infections_draws)
    completed_modeled_location_ids = infections_draws.reset_index()['location_id'].unique().tolist()

    logger.info('Identifying failed models.')
    failed_model_location_ids = list(set(location_ids) - set(completed_modeled_location_ids))
    app_metadata.update({'failed_model_location_ids': failed_model_location_ids})
    if failed_model_location_ids:
        logger.debug(f'Models failed/missing for the following location_ids: {", ".join([str(l) for l in failed_model_location_ids])}')

    logger.info('Compiling other model data.')
    inputs = {}
    for inputs_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith('_input_data.pkl')]:
        with inputs_path.open('rb') as inputs_file:
            inputs.update(pickle.load(inputs_file))
    inputs = {l: inputs[l] for l in completed_modeled_location_ids}
    outputs = {}
    for outputs_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith('_output_data.pkl')]:
        with outputs_path.open('rb') as outputs_file:
            outputs.update(pickle.load(outputs_file))
    outputs = {l: outputs[l] for l in completed_modeled_location_ids}
    em_scalar_data = []
    for em_scalar_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith('_em_scalar_data.parquet')]:
        em_scalar_data.append(pd.read_parquet(em_scalar_path))
    em_scalar_data = pd.concat(em_scalar_data)
    em_scalar_data = (em_scalar_data
                      .reset_index()
                      .set_index(['draw', 'location_id'])
                      .sort_index()
                      .loc[:, 'em_scalar'])
    
    logger.info('Processing mean deaths.')
    deaths = {k:v['deaths']['daily'] for k, v in outputs.items() if 'deaths' in list(outputs[k].keys())}
    deaths = [pd.concat([v, pd.DataFrame({'location_id':k}, index=v.index)], axis=1).reset_index() for k, v in deaths.items()]
    deaths = pd.concat(deaths)
    deaths = (deaths
              .set_index(['location_id', 'date'])
              .sort_index()
              .loc[:, 'deaths'])

    return app_metadata, infections_draws, inputs, outputs, em_scalar_data, deaths


def aggregate_and_plot(hierarchy: pd.DataFrame,
                       fh: bool,
                       inputs: Dict,
                       outputs: Dict,
                       infections_draws: pd.DataFrame,
                       plot_dir: Path,
                       output_root: Path,
                       pop_data: pd.DataFrame,
                       sero_data: pd.DataFrame,
                       cross_variant_immunity: List,
                       escape_variant_prevalence: pd.DataFrame,
                       ifr_model_data: pd.DataFrame,
                       ihr_model_data: pd.DataFrame,
                       idr_model_data: pd.DataFrame,
                       measures: List = ['deaths', 'hospitalizations', 'cases']):
    logger.info('Aggregating inputs.')
    agg_inputs = aggregation.aggregate_md_data_dict(inputs.copy(), hierarchy, measures, 1)

    logger.info('Aggregating outputs.')
    agg_outputs = aggregation.aggregate_md_data_dict(outputs.copy(), hierarchy, measures, 1)

    logger.info('Aggregating final infections draws.')
    agg_infections_draws = aggregation.aggregate_md_draws(infections_draws.copy(), hierarchy, MP_THREADS)
    
    if not fh:
        logger.info(f"Getting regional average for the following locations {', '.join([str(sl) for sl in SUB_LOCATIONS])}.")
        sub_infections_draws = pd.concat([aggregation.fill_w_region(sub_location,
                                                                    infections_draws.append(agg_infections_draws).copy(),
                                                                    hierarchy.copy(), pop_data.copy()) 
                                          for sub_location in SUB_LOCATIONS])
    else:
        sub_infections_draws = pd.DataFrame()
    
    logger.info('Plotting aggregates.')
    plot_parent_ids = agg_infections_draws.reset_index()['location_id'].unique().tolist()
    for plot_parent_id in tqdm(plot_parent_ids, total=len(plot_parent_ids), file=sys.stdout):
        aggregation.plot_aggregate(
            plot_parent_id,
            agg_inputs[plot_parent_id].copy(),
            agg_outputs[plot_parent_id].copy(),
            agg_infections_draws.loc[plot_parent_id].copy(),
            hierarchy.copy(),
            pop_data.copy(),
            sero_data.copy(),
            cross_variant_immunity,
            escape_variant_prevalence.copy(),
            ifr_model_data.copy(),
            ihr_model_data.copy(),
            idr_model_data.copy(),
            plot_dir,
        )
        
    logger.info('Merging PDFs.')
    possible_pdfs = [f'{l}.pdf' for l in hierarchy['location_id']]
    existing_pdfs = [str(x).split('/')[-1] for x in plot_dir.iterdir() if x.is_file()]
    pdf_paths = [pdf for pdf in possible_pdfs if pdf in existing_pdfs]
    pdf_location_ids = [int(pdf_path[:-4]) for pdf_path in pdf_paths]
    pdf_location_names = [hierarchy.loc[hierarchy['location_id'] == location_id, 'location_name'].item() for location_id in pdf_location_ids]
    pdf_parent_ids = [hierarchy.loc[hierarchy['location_id'] == location_id, 'parent_id'].item() for location_id in pdf_location_ids]
    pdf_parent_names = [hierarchy.loc[hierarchy['location_id'] == parent_id, 'location_name'].item() for parent_id in pdf_parent_ids]
    pdf_levels = [hierarchy.loc[hierarchy['location_id'] == location_id, 'level'].item() for location_id in pdf_location_ids]
    pdf_paths = [str(plot_dir / pdf_path) for pdf_path in pdf_paths]
    pdf_out_path = output_root / f'past_infections_{str(output_root).split("/")[-1]}.pdf'
    pdf_merger(pdf_paths, pdf_location_names, pdf_parent_names, pdf_levels, str(pdf_out_path))
    
    return sub_infections_draws

    
def write_seir_inputs(model_out_dir: Path,
                      infections_draws_dir: Path,
                      output_root: Path,
                      hierarchy: pd.DataFrame,
                      infections_draws: pd.DataFrame,
                      em_scalar_data: pd.Series,
                      deaths: pd.Series,
                      estimated_ratios: Dict,
                      variant_risk_ratio: List,
                      sero_data: pd.DataFrame,
                      sub_infections_draws: pd.DataFrame,
                      n_draws: int,
                      durations: List,
                      reported_deaths: pd.Series,):
    most_detailed = hierarchy['most_detailed'] == 1
    location_ids = hierarchy.loc[most_detailed, 'location_id'].to_list()
    
    logger.info('Writing SEIR inputs - infections draw files.')
    infections_draws_cols = infections_draws.columns
    infections_draws = pd.concat([infections_draws, deaths], axis=1)
    infections_draws = infections_draws.sort_index()
    deaths = infections_draws['deaths'].copy()
    infections_draws = [infections_draws[infections_draws_col].copy() for infections_draws_col in infections_draws_cols]
    infections_draws = [pd.concat([infections_draw, (deaths * em_scalar_data.loc[n]).rename('deaths')], axis=1) 
                        for n, infections_draw in enumerate(infections_draws)]
    
    if not sub_infections_draws.empty:
        _, _, ifr = estimated_ratios['deaths']
        _get_sub_loc_deaths = functools.partial(
            aggregation.get_sub_loc_deaths,
            n_draws=n_draws,
            sub_infections_draws=sub_infections_draws.copy(),
            ifr=ifr.copy(),
            durations=durations.copy(),
            reported_deaths=reported_deaths.copy(),
        )
        with multiprocessing.Pool(MP_THREADS) as p:
            sub_deaths_scalar = list(tqdm(p.imap(_get_sub_loc_deaths, SUB_LOCATIONS),
                                          total=len(SUB_LOCATIONS), file=sys.stdout))
        sub_deaths = pd.concat([sds[0] for sds in sub_deaths_scalar]).sort_index()
        sub_em_scalar_data = pd.concat([sds[1] for sds in sub_deaths_scalar]).sort_index()
        del sub_deaths_scalar
        sub_infections_draws = [pd.concat([sub_infections_draws[infections_draws_col].copy(),
                                           sub_deaths[infections_draws_col].rename('deaths')], axis=1) 
                                 for infections_draws_col in infections_draws_cols]
        infections_draws = [pd.concat([i_d, s_i_d]).sort_index() for i_d, s_i_d in zip(infections_draws, sub_infections_draws)]
        em_scalar_data = pd.concat([em_scalar_data, sub_em_scalar_data]).sort_index()
    
    _inf_writer = functools.partial(
        data.write_infections_draws,
        infections_draws_dir=infections_draws_dir,
    )
    with multiprocessing.Pool(MP_THREADS) as p:
        infections_draws_paths = list(tqdm(p.imap(_inf_writer, infections_draws), total=n_draws, file=sys.stdout))

    for measure, (estimated_ratio, measure_durations, ratio_prior_estimates) in estimated_ratios.items():
        logger.info(f'Compiling {estimated_ratio.upper()} draws.')
        ratio_draws = []
        for draws_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith(f'_{estimated_ratio}_draws.parquet')]:
            ratio_draws.append(pd.read_parquet(draws_path))
        ratio_draws = pd.concat(ratio_draws)
        if estimated_ratio == 'ifr':
            ratio_rr_draws = []
            for draws_path in [result_path for result_path in model_out_dir.iterdir() if str(result_path).endswith(f'_{estimated_ratio}_rr_draws.parquet')]:
                ratio_rr_draws.append(pd.read_parquet(draws_path))
            ratio_rr_draws = pd.concat(ratio_rr_draws)

        logger.info(f'Filling {estimated_ratio.upper()} with original model estimate where we do not have a posterior.')
        ratio_draws_cols = ratio_draws.columns
        ratio_prior_estimates = (ratio_prior_estimates
                                 .reset_index()
                                 .loc[:, ['location_id', 'draw', 'date', 'ratio']])
        ratio_prior_estimates = pd.pivot_table(ratio_prior_estimates,
                                               index=['location_id', 'date'],
                                               columns='draw',
                                               values='ratio',)
        ratio_prior_estimates.columns = ratio_draws_cols
        ratio_locations = ratio_draws.reset_index()['location_id'].unique()
        ratio_prior_locations = ratio_prior_estimates.reset_index()['location_id'].unique()
        missing_locations = [l for l in ratio_prior_locations if l not in ratio_locations]
        missing_locations = [l for l in missing_locations if l in location_ids]
        ratio_prior_estimates = ratio_prior_estimates.loc[missing_locations]
        if len(ratio_prior_estimates) > 0:
            logger.info(f"Appending prior estimates for the following locations: "
                        f"{', '.join(ratio_prior_estimates.reset_index()['location_id'].astype(str).unique())}")
        ratio_draws = ratio_draws.append(ratio_prior_estimates)

        logger.info(f'Writing SEIR inputs - {estimated_ratio.upper()} draw files.')
        ratio_draws = ratio_draws.sort_index()
        if estimated_ratio == 'ifr':
            ifr_lr_rr = (ratio_rr_draws
                        .reset_index()
                        .loc[:, ['location_id', 'draw', 'date', 'ifr_lr_rr']])
            ifr_lr_rr = pd.pivot_table(ifr_lr_rr,
                                       index=['location_id', 'date'],
                                       columns='draw',
                                       values='ifr_lr_rr',)
            ifr_lr_rr.columns = ratio_draws_cols
            ifr_hr_rr = (ratio_rr_draws
                        .reset_index()
                        .loc[:, ['location_id', 'draw', 'date', 'ifr_hr_rr']])
            ifr_hr_rr = pd.pivot_table(ifr_hr_rr,
                                       index=['location_id', 'date'],
                                       columns='draw',
                                       values='ifr_hr_rr',)
            ifr_hr_rr.columns = ratio_draws_cols
            ratio_draws = [(ratio_draws[ratio_draws_col].copy(),
                            ifr_lr_rr[ratio_draws_col].copy(),
                            ifr_hr_rr[ratio_draws_col].copy(),)
                           for ratio_draws_col in ratio_draws_cols]
        else:
            ratio_draws = [[ratio_draws[ratio_draws_col].copy()] for ratio_draws_col in ratio_draws_cols]
        ratio_draws_dir = output_root / f'{estimated_ratio}_draws'
        shell_tools.mkdir(ratio_draws_dir)
        _ratio_writer = functools.partial(
            data.write_ratio_draws,
            estimated_ratio=estimated_ratio,
            durations=measure_durations,
            variant_risk_ratio=variant_risk_ratio,
            ratio_draws_dir=ratio_draws_dir,
        )
        with multiprocessing.Pool(MP_THREADS) as p:
            ratio_draws_paths = list(tqdm(p.imap(_ratio_writer, ratio_draws), total=n_draws, file=sys.stdout))

    logger.info('Writing serology data and EM scaling factor data.')
    em_path = output_root / 'em_data.csv'
    em_scalar_data = (infections_draws[0]
                      .reset_index()
                      .loc[:, ['location_id', 'date']]
                      .merge(em_scalar_data.reset_index(), how='left'))
    em_scalar_data['em_scalar'] = em_scalar_data['em_scalar'].fillna(1)
    em_scalar_data.to_csv(em_path, index=False)
    # em_scalar_data['date'] = em_scalar_data['date'].astype(str)
    # em_path = output_root / 'em_data.parquet'
    # em_scalar_data.to_parquet(em_path, engine='fastparquet', compression='gzip')
    sero_data['included'] = 1 - sero_data['is_outlier']
    sero_data = sero_data.rename(columns={'sero_sample_mean': 'value'})
    sero_data = sero_data.loc[:, ['included', 'value']]
    sero_path = output_root / 'sero_data.csv'
    sero_data.reset_index().to_csv(sero_path, index=False)
    # sero_data = sero_data.reset_index()
    # sero_data['date'] = sero_data['date'].astype(str)
    # sero_path = output_root / 'sero_data.parquet'
    # sero_data.to_parquet(sero_path, engine='fastparquet', compression='gzip')


def make_infections(app_metadata: cli_tools.Metadata,
                    rates_root: Path,
                    output_root: Path,
                    holdout_days: int,
                    n_draws: int,
                    fh: bool,
                    gbd: bool,
                    no_deaths: bool,):
    logger.info(f'Model run initiated -- {str(output_root)}.')
    if holdout_days > 0:
        raise ValueError('Holdout not yet implemented.')
    
    model_in_dir, model_out_dir, plot_dir, infections_draws_dir = build_directories(output_root)

    app_metadata, hierarchy, estimated_ratios, variant_risk_ratio, agg_plot_inputs, durations, reported_deaths = prepare_input_data(
        app_metadata, rates_root, model_in_dir, n_draws, fh, gbd, no_deaths,
    )

    run_location_models(
        gbd, hierarchy, n_draws, model_in_dir, model_out_dir, plot_dir, output_root,
    )
    
    app_metadata, infections_draws, inputs, outputs, em_scalar_data, deaths = collect_results(
        app_metadata, hierarchy, model_out_dir,
    )
    
    sub_infections_draws = aggregate_and_plot(
        hierarchy, fh, inputs, outputs, infections_draws, plot_dir, output_root, **agg_plot_inputs,
    )

    write_seir_inputs(
        model_out_dir, infections_draws_dir, output_root,
        hierarchy, infections_draws, em_scalar_data, deaths, estimated_ratios, variant_risk_ratio,
        agg_plot_inputs['sero_data'],
        sub_infections_draws, n_draws, durations, reported_deaths,
    )

    logger.info(f'Model run complete -- {str(output_root)}.')
