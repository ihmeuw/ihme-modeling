from pathlib import Path
import dill as pickle
from loguru import logger

import pandas as pd
import numpy as np

from covid_shared import cli_tools

from covid_historical_model.rates.pipeline import pipeline_wrapper
from covid_historical_model.etl.model_inputs import copy_model_inputs
from covid_historical_model.durations.durations import EXPOSURE_TO_SEROCONVERSION

##     - make comparison routine; plot all fits in cascade; explore how coefficients change down cascade
##     - best way to fill where we have no assay information
##     - bias covariates
##     - incompatible vaccination adj locs
##     - try trimming in certain levels (probably just global)?
##     - formalize test matching in `serology.apply_waning_adjustment`
##     - use fit to find tests where we have multiple? would be a little harder...
##     - mark model data NAs as outliers, drop that way (in general, make it clear what data is and is not included)
##     - remove unused model data in runner after modeling
##     - plotting (draws updates)


def main(app_metadata: cli_tools.Metadata, out_dir: Path,
         model_inputs_root: Path,
         vaccine_coverage_root: Path, variant_scaleup_root: Path,
         age_rates_root: Path,
         testing_root: Path,
         n_samples: int,
         excess_mortality: bool,
         gbd: bool,):
    logger.info(f'Model run initiated -- {str(out_dir)}.')
    
    ## copy over files from model inputs (IES will use these)
    copy_model_inputs(model_inputs_root, out_dir)
    
    ## run models
    pipeline_results, selected_combinations, \
    cross_variant_immunity_samples, variant_risk_ratio_samples, \
    reported_seroprevalence, reported_sensitivity_data, \
    escape_variant_prevalence, severity_variant_prevalence, \
    vaccine_coverage, em_data, hierarchy, population = pipeline_wrapper(
        out_dir,
        excess_mortality, gbd,
        vaccine_coverage_root, variant_scaleup_root,
        age_rates_root,
        testing_root,
        n_samples,
    )
    
    ## save IFR
    logger.info('Compiling IFR draws and other data.')
    ifr_draws = []
    for n, ifr_draw in [(n, pipeline_results[n]['ifr_results'].pred.rename('ifr').reset_index()) for n in range(n_samples)]:
        ifr_draw['draw'] = n
        ifr_draws.append(ifr_draw.loc[:, ['location_id', 'date', 'draw', 'ifr']])
    ifr_draws = pd.concat(ifr_draws).reset_index(drop=True)
    
    ifr_unadj_draws = []
    for n, ifr_unadj_draw in [(n, pipeline_results[n]['ifr_results'].pred_unadj.rename('ifr_unadj').reset_index()) for n in range(n_samples)]:
        ifr_unadj_draw['draw'] = n
        ifr_unadj_draws.append(ifr_unadj_draw.loc[:, ['location_id', 'date', 'draw', 'ifr_unadj']])
    ifr_unadj_draws = pd.concat(ifr_unadj_draws).reset_index(drop=True)
    
    ifr_fe_draws = []
    for n, ifr_fe_draw in [(n, pipeline_results[n]['ifr_results'].pred_fe.rename('ifr_fe').reset_index()) for n in range(n_samples)]:
        ifr_fe_draw['draw'] = n
        ifr_fe_draws.append(ifr_fe_draw.loc[:, ['location_id', 'date', 'draw', 'ifr_fe']])
    ifr_fe_draws = pd.concat(ifr_fe_draws).reset_index(drop=True)
    
    ifr_draws = ifr_draws.merge(ifr_unadj_draws)
    ifr_draws = ifr_draws.merge(ifr_fe_draws)
    del ifr_unadj_draws, ifr_fe_draws
    
    ifr_lr_draws = []
    for n, ifr_lr_draw in [(n, pipeline_results[n]['ifr_results'].pred_lr.rename('ifr_lr').reset_index()) for n in range(n_samples)]:
        ifr_lr_draw['draw'] = n
        ifr_lr_draws.append(ifr_lr_draw.loc[:, ['location_id', 'date', 'draw', 'ifr_lr']])
    ifr_lr_draws = pd.concat(ifr_lr_draws).reset_index(drop=True)
    
    ifr_hr_draws = []
    for n, ifr_hr_draw in [(n, pipeline_results[n]['ifr_results'].pred_hr.rename('ifr_hr').reset_index()) for n in range(n_samples)]:
        ifr_hr_draw['draw'] = n
        ifr_hr_draws.append(ifr_hr_draw.loc[:, ['location_id', 'date', 'draw', 'ifr_hr']])
    ifr_hr_draws = pd.concat(ifr_hr_draws).reset_index(drop=True)
    
    ifr_rr_draws = ifr_lr_draws.merge(ifr_hr_draws)
    del ifr_lr_draws, ifr_hr_draws
    ifr_rr_draws = ifr_rr_draws.merge(ifr_draws.loc[:, ['location_id', 'date', 'draw', 'ifr']])
    ifr_rr_draws['ifr_lr_rr'] = ifr_rr_draws['ifr_lr'] / ifr_rr_draws['ifr']
    ifr_rr_draws['ifr_hr_rr'] = ifr_rr_draws['ifr_hr'] / ifr_rr_draws['ifr']
    del ifr_rr_draws['ifr'], ifr_rr_draws['ifr_lr'], ifr_rr_draws['ifr_hr']
    
    ifr_model_data = pd.concat([pipeline_results[n]['ifr_results'].model_data for n in range(n_samples)])
    ifr_model_data = ifr_model_data.reset_index(drop=True)
    ifr_model_data = pd.concat([
        ifr_model_data.loc[:, ['data_id', 'location_id']].drop_duplicates().set_index('data_id'),
        ifr_model_data.groupby('data_id')['mean_death_date'].median(),
        ifr_model_data.groupby('data_id')['ifr'].mean().rename('ifr_mean'),
        ifr_model_data.groupby('data_id')['ifr'].std().rename('ifr_std'),
    ], axis=1)
    ifr_model_data['is_outlier'] = 0
    ifr_model_data = ifr_model_data.reset_index()
    ifr_model_data = ifr_model_data.rename(columns={'mean_death_date': 'date'})
    ifr_model_data = ifr_model_data.loc[:, ['data_id', 'location_id', 'date', 'ifr_mean', 'ifr_std', 'is_outlier']]
    
    ifr_age_stand = pipeline_results[0]['ifr_results'].age_stand_scaling_factor.reset_index()
    
    ifr_level_lambdas_draws = []
    for n, ifr_level_lambdas in [(n, pipeline_results[n]['ifr_results'].level_lambdas) for n in range(n_samples)]:
        ifr_level_lambdas = pd.DataFrame(ifr_level_lambdas).T
        ifr_level_lambdas.index.name = 'hierarchy_level'
        ifr_level_lambdas = ifr_level_lambdas.reset_index()
        ifr_level_lambdas['draw'] = n
        ifr_level_lambdas_draws.append(ifr_level_lambdas)
    ifr_level_lambdas_draws = pd.concat(ifr_level_lambdas_draws).reset_index(drop=True)

    ## save IHR -- we have LR/HR draws as well, could save them if they were to be of use
    logger.info('Compiling IHR draws and other data.')
    ihr_draws = []
    for n, ihr_draw in [(n, pipeline_results[n]['ihr_results'].pred.rename('ihr').reset_index()) for n in range(n_samples)]:
        ihr_draw['draw'] = n
        ihr_draws.append(ihr_draw.loc[:, ['location_id', 'date', 'draw', 'ihr']])
    ihr_draws = pd.concat(ihr_draws).reset_index(drop=True)
    
    ihr_fe_draws = []
    for n, ihr_fe_draw in [(n, pipeline_results[n]['ihr_results'].pred_fe.rename('ihr_fe').reset_index()) for n in range(n_samples)]:
        ihr_fe_draw['draw'] = n
        ihr_fe_draws.append(ihr_fe_draw.loc[:, ['location_id', 'date', 'draw', 'ihr_fe']])
    ihr_fe_draws = pd.concat(ihr_fe_draws).reset_index(drop=True)
    ihr_draws = ihr_draws.merge(ihr_fe_draws)
    del ihr_fe_draws
    
    ihr_model_data = pd.concat([pipeline_results[n]['ihr_results'].model_data for n in range(n_samples)])
    ihr_model_data = ihr_model_data.reset_index(drop=True)
    ihr_model_data = pd.concat([
        ihr_model_data.loc[:, ['data_id', 'location_id']].drop_duplicates().set_index('data_id'),
        ihr_model_data.groupby('data_id')['mean_hospitalization_date'].median(),
        ihr_model_data.groupby('data_id')['ihr'].mean().rename('ihr_mean'),
        ihr_model_data.groupby('data_id')['ihr'].std().rename('ihr_std'),
    ], axis=1)
    ihr_model_data['is_outlier'] = 0
    ihr_model_data = ihr_model_data.reset_index()
    ihr_model_data = ihr_model_data.rename(columns={'mean_hospitalization_date': 'date'})
    ihr_model_data = ihr_model_data.loc[:, ['data_id', 'location_id', 'date', 'ihr_mean', 'ihr_std', 'is_outlier']]
    
    ihr_age_stand = pipeline_results[0]['ihr_results'].age_stand_scaling_factor.reset_index()
    
    ihr_level_lambdas_draws = []
    for n, ihr_level_lambdas in [(n, pipeline_results[n]['ihr_results'].level_lambdas) for n in range(n_samples)]:
        ihr_level_lambdas = pd.DataFrame(ihr_level_lambdas).T
        ihr_level_lambdas.index.name = 'hierarchy_level'
        ihr_level_lambdas = ihr_level_lambdas.reset_index()
        ihr_level_lambdas['draw'] = n
        ihr_level_lambdas_draws.append(ihr_level_lambdas)
    ihr_level_lambdas_draws = pd.concat(ihr_level_lambdas_draws).reset_index(drop=True)
    
    ## save IDR
    logger.info('Compiling IDR draws and other data.')
    idr_draws = []
    for n, idr_draw in [(n, pipeline_results[n]['idr_results'].pred.rename('idr').reset_index()) for n in range(n_samples)]:
        idr_draw['draw'] = n
        idr_draws.append(idr_draw.loc[:, ['location_id', 'date', 'draw', 'idr']])
    idr_draws = pd.concat(idr_draws).reset_index(drop=True)
    
    idr_fe_draws = []
    for n, idr_fe_draw in [(n, pipeline_results[n]['idr_results'].pred_fe.rename('idr_fe').reset_index()) for n in range(n_samples)]:
        idr_fe_draw['draw'] = n
        idr_fe_draws.append(idr_fe_draw.loc[:, ['location_id', 'date', 'draw', 'idr_fe']])
    idr_fe_draws = pd.concat(idr_fe_draws).reset_index(drop=True)
    idr_draws = idr_draws.merge(idr_fe_draws)
    del idr_fe_draws
    
    idr_model_data = pd.concat([pipeline_results[n]['idr_results'].model_data for n in range(n_samples)])
    idr_model_data = idr_model_data.reset_index(drop=True)
    idr_model_data = pd.concat([
        idr_model_data.loc[:, ['data_id', 'location_id']].drop_duplicates().set_index('data_id'),
        idr_model_data.groupby('data_id')['mean_infection_date'].median(),
        idr_model_data.groupby('data_id')['idr'].mean().rename('idr_mean'),
        idr_model_data.groupby('data_id')['idr'].std().rename('idr_std'),
    ], axis=1)
    idr_model_data['is_outlier'] = 0
    idr_model_data = idr_model_data.reset_index()
    idr_model_data = idr_model_data.rename(columns={'mean_infection_date': 'date'})
    idr_model_data = idr_model_data.loc[:, ['data_id', 'location_id', 'date', 'idr_mean', 'idr_std', 'is_outlier']]
    
    idr_level_lambdas_draws = []
    for n, idr_level_lambdas in [(n, pipeline_results[n]['idr_results'].level_lambdas) for n in range(n_samples)]:
        idr_level_lambdas = pd.DataFrame(idr_level_lambdas).T
        idr_level_lambdas.index.name = 'hierarchy_level'
        idr_level_lambdas = idr_level_lambdas.reset_index()
        idr_level_lambdas['draw'] = n
        idr_level_lambdas_draws.append(idr_level_lambdas)
    idr_level_lambdas_draws = pd.concat(idr_level_lambdas_draws).reset_index(drop=True)
    
    ## save sero scaling factor
    reinfection_inflation_factor = pd.concat([pipeline_results[n]['cumul_reinfection_inflation_factor'] for n in range(n_samples)])
    reinfection_inflation_factor = reinfection_inflation_factor.groupby(['location_id', 'date'])['inflation_factor'].mean()
    reinfection_inflation_factor = reinfection_inflation_factor.reset_index()
    
    ## save serology
    logger.info('Compiling serology data.')
    seroprevalence = reported_seroprevalence.copy()
    seroprevalence = seroprevalence.rename(columns={'seroprevalence': 'seroprevalence_no_vacc',
                                                    'reported_seroprevalence': 'seroprevalence'})
    seroprevalence_samples = [pipeline_results[n]['idr_results'].seroprevalence for n in range(n_samples)]
    seroprevalence_samples = pd.concat([ss.groupby('data_id')['seroprevalence'].mean().rename(f'draw_{n}')
                                        for n, ss in enumerate(seroprevalence_samples)], axis=1)
    seroprevalence_samples = pd.concat([seroprevalence_samples.mean(axis=1).rename('sero_sample_mean'),
                                        seroprevalence_samples.std(axis=1).rename('sero_sample_std'),],
                                       axis=1).reset_index()
    seroprevalence = seroprevalence.merge(seroprevalence_samples, how='left')
    seroprevalence['infection_date'] = seroprevalence['date'] - pd.Timedelta(days=int(np.round(np.mean(EXPOSURE_TO_SEROCONVERSION))))
    seroprevalence = seroprevalence.rename(columns={'start_date': 'sero_start_date',
                                                    'date': 'sero_end_date'})
    
    ## save durations
    durations = [pipeline_results[n]['durations'] for n in range(n_samples)]
    
    ## save raw sensitivity curves
    raw_sensitivity_curve_draws = []
    for n, raw_sensitivity_curves in [(n, pipeline_results[n]['raw_sensitivity_curves'].copy()) for n in range(n_samples)]:
        raw_sensitivity_curves = raw_sensitivity_curves.reset_index()
        raw_sensitivity_curves['draw'] = n
        raw_sensitivity_curve_draws.append(raw_sensitivity_curves)
    raw_sensitivity_curve_draws = pd.concat(raw_sensitivity_curve_draws).reset_index(drop=True)
    
    ## save sensitivity
    sensitivity_draws = []
    for n, sensitivity in [(n, pipeline_results[n]['sensitivity'].copy()) for n in range(n_samples)]:
        sensitivity['draw'] = n
        sensitivity_draws.append(sensitivity)
    sensitivity_draws = pd.concat(sensitivity_draws).reset_index(drop=True)
    
    ## save testing
    testing = pipeline_results[0]['idr_results'].testing_capacity.reset_index()
    
    ## save variants
    variants = (pd.concat([escape_variant_prevalence, severity_variant_prevalence], axis=1)
                .reset_index())

    ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
    logger.info('Writing output files.')
    ## write outputs
    hierarchy.to_parquet(out_dir / 'hierarchy.parquet')
    population.reset_index().to_parquet(out_dir / 'population.parquet')
    
    with (out_dir / 'covariate_combinations.pkl').open('wb') as file:
        pickle.dump(selected_combinations, file, -1)
        
    with (out_dir / 'durations.pkl').open('wb') as file:
        pickle.dump(durations, file, -1)
        
    with (out_dir / 'cross_variant_immunity.pkl').open('wb') as file:
        pickle.dump(cross_variant_immunity_samples, file, -1)
        
    with (out_dir / 'variant_risk_ratio.pkl').open('wb') as file:
        pickle.dump(variant_risk_ratio_samples, file, -1)
    
    em_data.to_parquet(out_dir / 'excess_mortality.parquet')

    ifr_draws.to_parquet(out_dir / 'ifr_draws.parquet')
    ifr_rr_draws.to_parquet(out_dir / 'ifr_rr_draws.parquet')
    ifr_model_data.to_parquet(out_dir / 'ifr_model_data.parquet')
    ifr_age_stand.to_parquet(out_dir / 'ifr_age_stand_data.parquet')
    ifr_level_lambdas_draws.to_parquet(out_dir / 'ifr_level_lambdas_draws.parquet')

    ihr_draws.to_parquet(out_dir / 'ihr_draws.parquet')
    ihr_model_data.to_parquet(out_dir / 'ihr_model_data.parquet')
    ihr_age_stand.to_parquet(out_dir / 'ihr_age_stand_data.parquet')
    ihr_level_lambdas_draws.to_parquet(out_dir / 'ihr_level_lambdas_draws.parquet')

    idr_draws.to_parquet(out_dir / 'idr_draws.parquet')
    idr_model_data.to_parquet(out_dir / 'idr_model_data.parquet')
    idr_level_lambdas_draws.to_parquet(out_dir / 'idr_level_lambdas_draws.parquet')

    reinfection_inflation_factor.to_parquet(out_dir / 'reinfection_inflation_factor.parquet')
    
    ## write this as a csv, for data intake purposes
    seroprevalence.to_csv(out_dir / 'sero_data.csv', index=False)
    
    reported_sensitivity_data.to_parquet(out_dir / 'raw_sensitivity_data.parquet')
    raw_sensitivity_curve_draws.to_parquet(out_dir / 'raw_sensitivity_curves.parquet')
    sensitivity_draws.to_parquet(out_dir / 'sensitivity.parquet')
    
    testing.to_parquet(out_dir / 'testing.parquet')
    
    vaccine_coverage.reset_index().to_parquet(out_dir / 'vaccine_coverage.parquet')
    
    variants.to_parquet(out_dir / 'variants.parquet')

    logger.info(f'Model run complete -- {str(out_dir)}.')
