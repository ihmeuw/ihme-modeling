import argparse
import json
import logging
import math
import os
from datetime import datetime
from typing import Dict, List

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import t

from db_tools import ezfuncs

from codem.joblaunch.profiling import START_DATE, query_jobs
from codem.joblaunch.resources import STEP_CORES
from codem.metadata.step_metadata import STEP_IDS, STEP_NAMES

matplotlib.use('Agg')
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)

RESOURCE_DIR = 'FILEPATH/codem/resource_predictions'
RESOURCE_FILE = os.path.join(RESOURCE_DIR, 'resource_file.txt')
JOBMON_PORTS = [10010, 10020, 10030]

MAX_MIN = 60 * 24 * 2
MIN_MIN = 1
DEFAULT_MIN = 60 * 3

MAX_GB = 480
MIN_GB = 1
DEFAULT_GB = 20


def import_resource_parameters():
    with open(RESOURCE_FILE, 'r') as f:
        d = json.loads(f.read())
    return d


class Parameters:
    """
    Parameters used in fitting the model.
    """
    # Prediction interval width
    ALPHA = 0.02
    # Outcome variables to predict
    MEASURE_VARS = ['ram_gb', 'runtime_min']
    # Predictors to use in each regression, by step
    STEP_VARIABLES = {
        'InputData':
            ['n_age_groups', 'global_model'],
        'GenerateKnockouts':
            ['n_data_points', 'n_demographic_rows'],
        'CovariateSelection':
            ['n_age_groups', 'n_demographic_rows', 'n_lvl1', 'n_lvl2', 'n_lvl3'],
        'LinearModelBuilds':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'ReadSpacetimeModels':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'ApplySpacetimeSmoothing':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'ApplyGPSmoothing':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'ReadLinearModels':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'SpacetimePV':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'LinearPV':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'OptimalPSI':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data', 'n_psi_values'],
        'LinearDraws':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'GPRDraws':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'EnsemblePredictions':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'WriteResults':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'Diagnostics':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data'],
        'Email':
            ['n_demographic_rows', 'num_submodels', 'n_data_points', 'n_time_series_with_data']
    }


def get_model_parameters(
    sge_ids: List[int], jobmon_ports: List[int]
) -> pd.DataFrame:
    """
    Get model parameters from the jobmon attributes table for a list of SGE job IDs.

    Args:
        sge_ids: SGE job IDs, or jobmon executor IDs
        jobmon_ports: jobmon ports used

    Returns:
        Dataframe of model runtime results
    """
    logging.info("Querying for the job parameters/attributes.")
    res = []
    for port in jobmon_ports:
        logging.info(f"Reading cached jobs from jobmon port {port}")
        result = pd.read_csv(f"{RESOURCE_DIR}/jobmon_{port}.csv")
        result = result.loc[result.job_number.isin(sge_ids)]
        res.append(result)
    logging.info(f"Reading jobs from active jobmon 'jobmon-codem'")
    result = ezfuncs.query(
        """
        SELECT DISTINCT ji.executor_id AS job_number, ja.value AS job_attributes
        FROM docker.job_instance ji
        LEFT JOIN docker.job_attribute ja ON ja.job_id = ji.job_id
        WHERE
            ja.attribute_type = 8
            AND ji.executor_id IN :executor_ids
        """, parameters={"executor_ids": sge_ids}, conn_def="jobmon-codem")
    res.append(result)
    results = pd.concat(res).drop_duplicates()
    return results


def get_step_data(step_name: str, all_data: pd.DataFrame):
    """
    Get data specific to a step pulling from the all_data data set
    from qpid.

    Args:
        step_name: name of CODEm step
        all_data: all_data used for predictions
    """
    predictors = Parameters.STEP_VARIABLES[step_name]
    step_string = f'_{STEP_IDS[step_name]:02d}'
    data = all_data[predictors + Parameters.MEASURE_VARS][all_data.job_name.str.contains(step_string)]
    return data, predictors


def fit_model(y: pd.DataFrame, X: pd.DataFrame):
    """
    Fit an OLS model using statsmodels.

    Args:
        y: outcome variable in np.array(1,)
        X: design matrix of predictors w/o intercept

    Returns:
        results dictionary to be used in make_all_predictions()
    """
    results = {}
    null_predictors = X.isnull().any(axis=1)
    X = X.loc[~null_predictors]
    y = y.loc[~null_predictors]
    results['predictors'] = X.columns.tolist()
    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    results['parameters'] = model.params.to_dict()
    results['df_resid'] = model.df_resid
    results['sigma2'] = (1 / model.df_resid) * sum((y - predictions) ** 2)
    results['sandwich'] = np.linalg.inv(np.matmul(np.array(X).T, np.array(X))).tolist()
    return results


def make_step_model(step_name: str, all_data: pd.DataFrame, outcome_variable: str):
    """
    Make one step model results with a specific outcome variable

    Args:
        step_name: name of CODEm step
        all_data: all data used for predictions
        outcome_variable:
    """
    logging.info(f"Making step model for {step_name} and {outcome_variable}")
    step_data, predictors = get_step_data(step_name=step_name, all_data=all_data)
    results = fit_model(y=step_data[outcome_variable], X=step_data[predictors])
    return results


def make_all_step_models(all_data: pd.DataFrame, step_names: Dict[str, int]):
    """
    Make all step-specific and outcome-variable specific models and put the
    results into a dictionary that we can access later on. Run this periodically
    to update the predictions.

    Args:
        all_data: relevant data set pulled from qpid
        step_names: dictionary of CODEm steps
    """
    logging.info("Making all step models.")
    all_results = {}
    for step_name in step_names:
        all_results[step_name] = {}
        for outcome_variable in Parameters.MEASURE_VARS:
            all_results[step_name][outcome_variable] = make_step_model(
                step_name=step_name,
                all_data=all_data,
                outcome_variable=outcome_variable
            )
    return all_results


def get_all_data_for_predictions(start_date=START_DATE):
    """
    Get all data to make predictions.

    Args:
        start_date: datetime object representing date to pull predictions up to
    """
    complete = query_jobs(start_date=start_date)
    params = get_model_parameters(
        sge_ids=complete.job_number.tolist(),
        jobmon_ports=JOBMON_PORTS
    )

    all_data = complete.merge(params, on='job_number')
    attributes = all_data['job_attributes'].apply(lambda x: pd.Series(json.loads(x)))
    all_data = pd.concat([all_data, attributes], axis=1)
    all_data.drop(['job_attributes'], inplace=True, axis=1)
    all_data['n_covariates'] = all_data['n_lvl1'].values + all_data['n_lvl2'].values + all_data['n_lvl3'].values
    print(len(all_data))
    all_data = all_data.loc[~all_data.global_model.isnull()]
    print(len(all_data))
    all_data['global_model'] = all_data['global_model'].astype(int)
    all_data = all_data.loc[all_data.holdout_number == 20]
    return all_data


def run_resource_prediction_models(
    output_dir: str, step_names: Dict[str, int] = STEP_IDS, save: bool = True
):
    """
    Run the full resource prediction model for a list of step names.

    Args:
        output_dir: output directory to save resource file to
        step_names: dictionary of step IDs to names
        save: bool to save
    """
    logging.info("Running the resource predictions model.")
    all_data = get_all_data_for_predictions()
    all_models = make_all_step_models(all_data=all_data, step_names=step_names)

    if save:
        logging.info("Writing the json file for all resource predictions.")
        with open(os.path.join(output_dir, "resource_file.txt"), 'w') as outfile:
            json.dump(all_models, outfile)

    return all_data, all_models


def plot_predictions(all_models: pd.DataFrame, all_data: pd.DataFrame, plot_dir: str):
    """
    Make plots of the predicted v. reality of how much memory
    and runtime something took.

    Args:
        all_models: all models object from run_resource_prediction_models
        all_data: all of the input data
        plot_dir: directory to save plots to

    Returns:
        None, saves set of step graphs to plot_dir
    """
    for step in all_models:
        step_data, predictors = get_step_data(step_name=step, all_data=all_data)
        for outcome in all_models[step]:
            logging.info(f"Making predictions + plots for {step} and {outcome}")
            result_data = all_models[step][outcome]
            X = step_data[predictors]
            y = step_data[outcome]
            null_predictors = X.isnull().any(axis=1)
            X = X.loc[~null_predictors]
            y = y.loc[~null_predictors]
            preds = make_all_predictions(X=X, results=result_data)
            fig, ax = plt.subplots()
            ax.scatter(y=preds, x=y)
            x = np.linspace(*ax.get_xlim())
            ax.plot(x, x)
            ax.set_xlabel(f"observed {outcome}")
            ax.set_ylabel(f"predicted {outcome}")
            ax.set_title(f"Predictions for {step}")
            matplotlib.pyplot.savefig(fname=os.path.join(plot_dir, f"{step}_{outcome}.png"))
            plt.show()
            plt.close(fig)


def make_all_predictions(X: pd.DataFrame, results: Dict[str, int]):
    """
    Makes predictions at the (1-ALPHA/2) prediction interval upper limit
    for a design matrix X which holds all of the job attributes,
    and a model results dictionary that needs keys for:

        - parameters (coefficients of the linear model + intercept)
        - df_resid (degrees of freedom for residuals)
        - sigma2 (mean squared error)
        - sandwich (the (X^T X)^{-1} matrix)

    Args:
        X: new data to predict on
        results: dictionary of model results
        np.array of predictions with shape X.shape[0]
    """
    X = np.array(sm.add_constant(X, has_constant='add'), dtype=float)
    betas = np.array(list(results['parameters'].values()))
    ones = np.ones(X.shape[0])
    t_value = t.ppf(q=(1 - Parameters.ALPHA / 2), df=results['df_resid'])
    sandwich = np.array(results['sandwich'])

    predictions = np.matmul(X, betas)
    variance = results['sigma2'] * (ones + np.diag(
        np.matmul(np.matmul(X, sandwich), X.T)
    ))
    return predictions + t_value * variance ** 0.5


def get_step_prediction(resource_parameters: Dict[str, int], step_id: int, input_info: Dict[str, int]):
    """
    Get the step predictions given resource parameters and a step ID.
    Also use the input info for the actual job.

    Args:
        resource_parameters: dictionary of resource parameters
        step_id: ID of CODEm step
        input_info: dictionary of inputs parameters
    """
    step_parameters = resource_parameters[STEP_NAMES[step_id]]
    new_data = pd.DataFrame.from_dict(input_info, orient='index').T

    run_parameters = {}

    params = step_parameters['ram_gb']
    try:
        m_mem_free = make_all_predictions(
            X=new_data[params['predictors']], results=params
        )
        m_mem_free = m_mem_free * 1.1 # TODO
    except:
        logger.warning("Could not predict for m_mem_free.")
        logger.warning(f"Giving it the default of {DEFAULT_GB}.")
        m_mem_free = DEFAULT_GB
    if m_mem_free < MIN_GB:
        m_mem_free = MIN_GB
    elif m_mem_free > MAX_GB:
        m_mem_free = MAX_GB
    m_mem_free = str(math.ceil(m_mem_free)) + 'GB'

    run_parameters['m_mem_free'] = m_mem_free

    params = step_parameters['runtime_min']
    try:
        minutes = make_all_predictions(
            X=new_data[params['predictors']], results=params
        )
        minutes = minutes * 1.5 # TODO
    except:
        logger.warning("Could not predict for max_runtime_seconds.")
        logger.warning(f"Giving it the default of {DEFAULT_MIN}.")
        minutes = DEFAULT_MIN
    if minutes > MAX_MIN:
        minutes = MAX_MIN
    elif minutes < MIN_MIN:
        minutes = MIN_MIN
    else:
        pass
    run_parameters['max_runtime_seconds'] = int(minutes * 60)

    run_parameters['num_cores'] = int(STEP_CORES[STEP_NAMES[step_id]])
    run_parameters['queue'] = 'all.q'
    for k, v in run_parameters.items():
        logger.info(f"Task gets {k}: {v}")
    return run_parameters


def main(output_dir: str):
    """Main function to generate resource predictions file and graphs of
    predictions to an output directory. Directory should be saved in format:
    FILEPATH/codem/resource_predictions/archive/{current date}

    Args:
        output_dir: directory to save graphs and predictions file to
    """
    if not os.path.isdir(output_dir):
        logging.info(f"Creating directory at {output_dir}")
        os.mkdir(output_dir)
    logging.info("Generating resource predictions")
    all_data, all_models = run_resource_prediction_models(output_dir=output_dir)
    logging.info("Generating graphs of predictions")
    plot_predictions(
        all_models=all_models,
        all_data=all_data,
        plot_dir=output_dir
    )
    logging.info("Done!")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dir', type=str, required=False,
        default=os.path.join(RESOURCE_DIR, "archive", str(datetime.today().date()))
    )
    args = parser.parse_args()
    return args.output_dir


if __name__ == '__main__':
    main(parse_arguments())

