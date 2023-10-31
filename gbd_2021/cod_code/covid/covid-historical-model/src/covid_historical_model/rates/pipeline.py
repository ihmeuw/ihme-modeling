import os
import sys
from typing import List, Tuple, Dict
from pathlib import Path
import itertools
from loguru import logger
import dill as pickle
import shutil

import pandas as pd
import numpy as np

from covid_shared import shell_tools
from covid_shared.cli_tools.logging import configure_logging_to_terminal

from covid_historical_model.etl import model_inputs, estimates, db
from covid_historical_model.durations import durations
from covid_historical_model.rates import (
    serology,
    covariate_selection,
    ifr, ihr, idr,
    age_standardization,
    cvi, variant_severity
)
from covid_historical_model import cluster


def pipeline_wrapper(out_dir: Path,
                     excess_mortality: bool, gbd: bool,
                     vaccine_coverage_root: Path, variant_scaleup_root: Path,
                     age_rates_root: Path,
                     testing_root: Path,
                     n_samples: int,
                     day_0: pd.Timestamp = pd.Timestamp('2020-03-15'),
                     pred_start_date: pd.Timestamp = pd.Timestamp('2019-11-01'),
                     pred_end_date: pd.Timestamp = pd.Timestamp('2022-03-15'),
                     correlate_samples: bool = True,
                     bootstrap: bool = True,
                     verbose: bool = True,) -> Tuple:
    np.random.seed(15243)
    if verbose:
        logger.info('Loading variant, vaccine, and sero data.')
    hierarchy = model_inputs.hierarchy(out_dir)
    if gbd:
        gbd_hierarchy = model_inputs.hierarchy(out_dir, 'covid_gbd')
    else:
        gbd_hierarchy = model_inputs.hierarchy(out_dir, 'covid_modeling_plus_zaf')
    adj_gbd_hierarchy = model_inputs.validate_hierarchies(hierarchy.copy(), gbd_hierarchy.copy())
    population = model_inputs.population(out_dir)
    age_spec_population = model_inputs.population(out_dir, by_age=True)
    population_lr, population_hr = age_standardization.get_risk_group_populations(age_spec_population)
    shared = {
        'hierarchy': hierarchy,
        'gbd_hierarchy': gbd_hierarchy,
        'adj_gbd_hierarchy': adj_gbd_hierarchy,
        'population': population,
        'age_spec_population': age_spec_population,
        'population_lr': population_lr,
        'population_hr': population_hr,
    }
    
    escape_variant_prevalence = estimates.variant_scaleup(variant_scaleup_root, 'escape', verbose=verbose)
    severity_variant_prevalence = estimates.variant_scaleup(variant_scaleup_root, 'severity', verbose=verbose)
    vaccine_coverage = estimates.vaccine_coverage(vaccine_coverage_root, pred_end_date)
    reported_seroprevalence, seroprevalence_samples = serology.load_seroprevalence_sub_vacccinated(
        out_dir, hierarchy, vaccine_coverage.copy(), n_samples=n_samples,
        correlate_samples=correlate_samples, bootstrap=bootstrap,
        verbose=verbose,
    )
    reported_sensitivity_data, sensitivity_data_samples = serology.load_sensitivity(out_dir, n_samples,)
    durations_samples = durations.get_duration_dist(n_samples)
    cross_variant_immunity_samples = cvi.get_cvi_dist(n_samples)
    variant_risk_ratio_samples = variant_severity.get_variant_severity_rr_dist(n_samples)
    
    covariate_options = ['obesity', 'smoking', 'diabetes', 'ckd',
                         'cancer', 'copd', 'cvd', 'uhc', 'haq',]
    covariates = [db.obesity(adj_gbd_hierarchy),
                  db.smoking(adj_gbd_hierarchy),
                  db.diabetes(adj_gbd_hierarchy),
                  db.ckd(adj_gbd_hierarchy),
                  db.cancer(adj_gbd_hierarchy),
                  db.copd(adj_gbd_hierarchy),
                  db.cvd(adj_gbd_hierarchy),
                  db.uhc(adj_gbd_hierarchy) / 100,
                  db.haq(adj_gbd_hierarchy) / 100,]
    
    if verbose:
        logger.info('Identifying best covariate combinations and creating input data object.')
    test_combinations = []
    for i in range(len(covariate_options)):
        test_combinations += [list(set(cc)) for cc in itertools.combinations(covariate_options, i + 1)]
    test_combinations = [cc for cc in test_combinations if 
                        len([c for c in cc if c in ['uhc', 'haq']]) <= 1]
    selected_combinations = covariate_selection.covariate_selection(
        n_samples=n_samples, test_combinations=test_combinations,
        out_dir=out_dir, excess_mortality=excess_mortality,
        age_rates_root=age_rates_root, shared=shared,
        reported_seroprevalence=reported_seroprevalence,
        covariate_options=covariate_options,
        covariates=covariates,
        reported_sensitivity_data=reported_sensitivity_data,
        vaccine_coverage=vaccine_coverage,
        escape_variant_prevalence=escape_variant_prevalence,
        severity_variant_prevalence=severity_variant_prevalence,
        cross_variant_immunity_samples=cross_variant_immunity_samples,
        variant_risk_ratio_samples=variant_risk_ratio_samples,
        pred_start_date=pred_start_date,
        pred_end_date=pred_end_date,
        cutoff_pct=1.,
        durations={'sero_to_death': (int(round(np.mean(durations.EXPOSURE_TO_ADMISSION))) + 
                                     int(round(np.mean(durations.ADMISSION_TO_DEATH))) - 
                                     int(round(np.mean(durations.EXPOSURE_TO_SEROCONVERSION)))),
                   'exposure_to_death': (int(round(np.mean(durations.EXPOSURE_TO_ADMISSION))) + 
                                         int(round(np.mean(durations.ADMISSION_TO_DEATH)))),
                   'exposure_to_seroconversion': int(round(np.mean(durations.EXPOSURE_TO_SEROCONVERSION)))},
    )
    
    idr_covariate_options = [['haq'], ['uhc'], ['prop_65plus'], [],]
    idr_covariate_pool = np.random.choice(idr_covariate_options, n_samples)
    
    day_inflection_options = ['2020-06-01', '2020-07-01',
                              '2020-08-01', '2020-09-01',
                              '2020-10-01', '2020-11-01',
                              '2020-12-01', '2021-01-01',
                              '2021-02-01', '2021-03-01',]
    day_inflection_pool = np.random.choice(day_inflection_options, n_samples)
    day_inflection_pool = [pd.Timestamp(str(d)) for d in day_inflection_pool]
    
    inputs = {
        n: {
            'out_dir': out_dir,
            'orig_seroprevalence': seroprevalence,
            'shared': shared,
            'excess_mortality': excess_mortality,
            'sensitivity_data': sensitivity_data,
            'vaccine_coverage': vaccine_coverage,
            'escape_variant_prevalence': escape_variant_prevalence,
            'severity_variant_prevalence': severity_variant_prevalence,
            'age_rates_root': age_rates_root,
            'testing_root': testing_root,
            'day_inflection': day_inflection,
            'covariates': covariates,
            'covariate_list': covariate_list,
            'idr_covariate_list': idr_covariate_list,
            'cross_variant_immunity': cross_variant_immunity,
            'variant_risk_ratio': variant_risk_ratio,
            'durations': durations,
            'day_0': day_0,
            'pred_start_date': pred_start_date,
            'pred_end_date': pred_end_date,
            'verbose': verbose,
        }
        for n, (covariate_list, idr_covariate_list,
                seroprevalence, sensitivity_data,
                cross_variant_immunity, variant_risk_ratio,
                day_inflection, durations,)
        in enumerate(zip(selected_combinations, idr_covariate_pool,
                         seroprevalence_samples, sensitivity_data_samples,
                         cross_variant_immunity_samples, variant_risk_ratio_samples,
                         day_inflection_pool, durations_samples,))
    }
    
    if verbose:
        logger.info('Storing inputs and submitting sero-sample jobs.')
    inputs_path = out_dir / 'pipeline_inputs.pkl'
    with inputs_path.open('wb') as file:
        pickle.dump(inputs, file, -1)
    pipeline_dir = out_dir / 'pipeline_outputs'
    shell_tools.mkdir(pipeline_dir)
    job_args_map = {n: [__file__, n, inputs_path, pipeline_dir] for n in range(n_samples)}
    if gbd:
        cluster.run_cluster_jobs('covid_rates_pipeline', out_dir, job_args_map, 'gbd')
    else:
        cluster.run_cluster_jobs('covid_rates_pipeline', out_dir, job_args_map, 'standard')
    
    pipeline_results = {}
    for n in range(n_samples):
        with (pipeline_dir / f'{n}_outputs.pkl').open('rb') as file:
            outputs = pickle.load(file)
        pipeline_results.update(outputs)
    
    em_data = estimates.excess_mortailty_scalars(excess_mortality)
    
    return pipeline_results, selected_combinations, \
           cross_variant_immunity_samples, variant_risk_ratio_samples, \
           reported_seroprevalence, reported_sensitivity_data, \
           escape_variant_prevalence, severity_variant_prevalence, \
           vaccine_coverage, em_data, hierarchy, population


def pipeline(n: int,
             out_dir: Path,
             orig_seroprevalence: pd.DataFrame,
             shared: Dict,
             excess_mortality: bool,
             sensitivity_data: pd.DataFrame,
             vaccine_coverage: pd.DataFrame,
             escape_variant_prevalence: pd.Series,
             severity_variant_prevalence: pd.Series,
             age_rates_root: Path,
             testing_root: Path,
             day_inflection: pd.Timestamp,
             covariates: List[pd.Series],
             covariate_list: List[str],
             idr_covariate_list: List[str],
             cross_variant_immunity: float,
             variant_risk_ratio: float,
             durations: Dict,
             day_0: pd.Timestamp,
             pred_start_date: pd.Timestamp,
             pred_end_date: pd.Timestamp,
             verbose: bool,) -> Tuple:
    if verbose:
        logger.info('\n*************************************\n'
                    f"IFR ESTIMATION\n"
                    '*************************************')
    ifr_input_data = ifr.data.load_input_data(out_dir,
                                              excess_mortality, n,
                                              age_rates_root,
                                              shared.copy(),
                                              orig_seroprevalence.copy(),
                                              sensitivity_data.copy(),
                                              vaccine_coverage.copy(),
                                              escape_variant_prevalence.copy(), severity_variant_prevalence.copy(),
                                              covariates.copy(),
                                              cross_variant_immunity,
                                              variant_risk_ratio,
                                              verbose=verbose)
    ifr_results, adj_seroprevalence, raw_sensitivity_curves, sensitivity, \
    cumul_reinfection_inflation_factor, _ = ifr.runner.runner(
        input_data=ifr_input_data,
        day_inflection=day_inflection,
        covariate_list=covariate_list,
        durations=durations,
        day_0=day_0,
        pred_start_date=pred_start_date,
        pred_end_date=pred_end_date,
        verbose=verbose,
    )

    if verbose:
        logger.info('\n*************************************\n'
                    'IDR ESTIMATION\n'
                    '*************************************')
    
    idr_input_data = idr.data.load_input_data(out_dir,
                                              excess_mortality, n,
                                              testing_root,
                                              shared.copy(),
                                              adj_seroprevalence.copy(),
                                              vaccine_coverage.copy(),
                                              escape_variant_prevalence.copy(),
                                              covariates.copy(),
                                              cross_variant_immunity,
                                              verbose=verbose)
    idr_results = idr.runner.runner(idr_input_data,
                                    ifr_results.pred.copy(),
                                    idr_covariate_list,
                                    durations,
                                    pred_start_date,
                                    pred_end_date,
                                    verbose=verbose)
    
    if verbose:
        logger.info('\n*************************************\n'
                    'IHR ESTIMATION\n'
                    '*************************************')
    ihr_input_data = ihr.data.load_input_data(out_dir,
                                              excess_mortality, n,
                                              age_rates_root,
                                              shared.copy(),
                                              adj_seroprevalence.copy(),
                                              vaccine_coverage.copy(),
                                              escape_variant_prevalence.copy(),
                                              severity_variant_prevalence.copy(),
                                              covariates.copy(),
                                              cross_variant_immunity,
                                              variant_risk_ratio,
                                              verbose=verbose)
    ihr_results = ihr.runner.runner(ihr_input_data,
                                    ifr_results.pred.copy(),
                                    covariate_list,
                                    durations,
                                    day_0,
                                    pred_start_date,
                                    pred_end_date,
                                    verbose=verbose)
    
    if verbose:
        logger.info('\n*************************************\n'
                    'PIPELINE COMPLETE -- preparing storage and saving results \n'
                    '*************************************')
    pipeline_results = {
        'covariate_list': covariate_list,
        'idr_covariate_list': idr_covariate_list,
        'durations': durations,
        'seroprevalence': adj_seroprevalence,
        'raw_sensitivity_curves': raw_sensitivity_curves,
        'sensitivity': sensitivity,
        'cumul_reinfection_inflation_factor': cumul_reinfection_inflation_factor,
        'day_inflection': day_inflection,
        'ifr_results': ifr_results,
        'idr_results': idr_results,
        'ihr_results': ihr_results,
    }
    
    return pipeline_results


def main(n: int, inputs_path: str, pipeline_dir: str):
    os.environ['OMP_NUM_THREADS'] = cluster.OMP_NUM_THREADS
    os.environ['MKL_NUM_THREADS'] = cluster.MKL_NUM_THREADS

    with Path(inputs_path).open('rb') as file:
        inputs = pickle.load(file)[n]
    
    np.random.seed(123 * (n + 1))
    pipeline_outputs = pipeline(n, **inputs)
    
    with (Path(pipeline_dir) / f'{n}_outputs.pkl').open('wb') as file:
        pickle.dump({n: pipeline_outputs}, file)
    

if __name__ == '__main__':
    configure_logging_to_terminal(verbose=2)
    
    main(int(sys.argv[1]), sys.argv[2], sys.argv[3])
