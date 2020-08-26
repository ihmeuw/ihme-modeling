import numpy as np
from scipy.stats import t
import sqlalchemy as sql
import pandas as pd
import statsmodels.api as sm
import json
import logging
import os
from datetime import datetime
import matplotlib
import math
import matplotlib.pyplot as plt

from codem.joblaunch.profiling import pull_jobs
from codem.metadata.step_metadata import STEP_IDS, STEP_NAMES
from codem.joblaunch.resources import STEP_CORES

logger = logging.getLogger(__name__)

RESOURCE_DIR = 'FILEPATH'
RESOURCE_FILE = os.path.join(RESOURCE_DIR, 'resource_file.txt')
JOBMON_PORTS = [10010, 10020]
PROJECTS = ['proj_codem']

MAX_MIN = 60 * 24 * 2
MIN_MIN = 1
DEFAULT_MIN = 60*3

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


def query_jobmon_database(call, port):
    """
    Read the password credentials from a file on the cluster.
    """
    creds = "USERNAME:" + "USERNAME"
    connection = 'ADDRESS'
    DB = 'ADDRESS'
    engine = sql.create_engine(DB)
    df = pd.read_sql_query(call, engine)
    return df


def get_model_parameters(job_instance_ids, port):
    """
    Get model parameters from the jobmon attributes table
    for a list of job instance IDs.

    :param job_instance_ids:
    :param port: (int) jobmon port
    :return:
    """
    logger.info("Querying for the job parameters/attributes.")
    job_instance_ids = ', '.join([str(x) for x in job_instance_ids])
    q_string = (f"SELECT DISTINCT ji.executor_id AS job_number, ja.value AS job_attributes "
                f"FROM docker.job_instance ji "
                f"LEFT JOIN docker.job_attribute ja ON ja.job_id = ji.job_id "
                f"WHERE ja.attribute_type = 8 "
                f"AND ji.executor_id IN ({job_instance_ids})")
    res = []
    for p in port:
        logger.info(f"Using port {p}")
        result = query_jobmon_database(q_string, port=p)
        res.append(result)
    results = pd.concat(res)
    return results


def get_step_data(step_name, all_data):
    """
    Get data specific to a step pulling from the all_data data set
    from qpid.

    :param step_name: (str)
    :param all_data: pd.DataFrame
    :return:
    """
    predictors = Parameters.STEP_VARIABLES[step_name]
    step_string = f'_{STEP_IDS[step_name]:02d}'
    data = all_data[predictors + Parameters.MEASURE_VARS][all_data.job_name.str.contains(step_string)]
    return data, predictors


def fit_model(y, X):
    """
    Fit an OLS model using statsmodels.

    :param y: outcome variable in np.array(1,)
    :param X: design matrix of predictors w/o intercept
    :return: results dictionary to be used in make_all_predictions()
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


def make_step_model(step_name, all_data, outcome_variable):
    """
    Make one step model results with a specific outcome variable
    :param step_name: (str)
    :param all_data: pd.DataFrame
    :param outcome_variable: (str)
    :return:
    """
    logger.info(f"Making step model for {step_name} and {outcome_variable}")
    step_data, predictors = get_step_data(step_name=step_name, all_data=all_data)
    results = fit_model(y=step_data[outcome_variable], X=step_data[predictors])
    return results


def make_all_step_models(all_data, step_names):
    """
    Make all step-specific and outcome-variable specific models and put the
    results into a dictionary that we can access later on. Run this periodically
    to update the predictions.

    :param all_data: relevant data set pulled from qpid
    :param step_names:
    :return:
    """
    logger.info("Making all step models.")
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


def get_all_data_for_predictions(start_date=datetime(2019, 7, 9)):
    """
    Get all data to make predictions.
    :return:
    """
    df = pull_jobs(start_date=start_date, project=PROJECTS)
    complete = df.loc[(df.exit_status == 0) & (df.failed == 0)]
    params = get_model_parameters(
        job_instance_ids=complete.job_number.tolist(),
        port=JOBMON_PORTS
    )
    total = complete.merge(params, on='job_number')
    attributes = total['job_attributes'].apply(lambda x: pd.Series(json.loads(x)))
    total = pd.concat([total, attributes], axis=1)
    total.drop(['job_attributes'], inplace=True, axis=1)
    total['n_covariates'] = total['n_lvl1'].values + total['n_lvl2'].values + total['n_lvl3'].values
    print(len(total))
    total = total.loc[~total.global_model.isnull()]
    print(len(total))
    total['global_model'] = total['global_model'].astype(int)
    total = total.loc[total.holdout_number == 20]
    return total


def run_resource_prediction_models(step_names=STEP_IDS, save=True):
    """
    Run the full resource prediction model for a list of step names.

    :param step_names:
    :param save:
    :return:
    """
    logger.info("Running the resource predictions model.")
    total = get_all_data_for_predictions()
    all_models = make_all_step_models(all_data=total, step_names=step_names)

    if save:
        logger.info("Writing the json file for all resource predictions.")
        with open(RESOURCE_FILE, 'w') as outfile:
            json.dump(all_models, outfile)

    return total, all_models


def plot_predictions(all_models, all_data, plot_dir):
    """
    Make plots of the predicted v. reality of how much memory
    and runtime something took.

    :param all_models: all models object from run_resource_prediction_models
    :param all_data: all of the input data
    :param plot_dir:
    :return:
    """
    for step in all_models:
        step_data, predictors = get_step_data(step_name=step, all_data=all_data)
        for outcome in all_models[step]:
            logger.info(f"Making predictions + plots for {step} and {outcome}")
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
            matplotlib.pyplot.savefig(filename=os.path.join(plot_dir, f"{step}_{outcome}.png"))
            plt.show()
            plt.close(fig)


def make_all_predictions(X, results):
    """
    Makes predictions at the (1-ALPHA/2) prediction interval upper limit
    for a design matrix X which holds all of the job attributes,
    and a model results dictionary that needs keys for:

        - parameters (coefficients of the linear model + intercept)
        - df_resid (degrees of freedom for residuals)
        - sigma2 (mean squared error)
        - sandwich (the (X^T X)^{-1} matrix)

    :param X: new data to predict on
    :param results: dictionary of model results
    :return: np.array of predictions with shape X.shape[0]
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


def get_step_prediction(resource_parameters, step_id, input_info):
    """
    Get the step predictions given resource parameters and a step ID.
    Also use the input info for the actual job.
    :param resource_parameters: (dict)
    :param step_id: (int)
    :param input_info: (dict) dictionary of inputs parameters
    :return:
    """
    step_parameters = resource_parameters[STEP_NAMES[step_id]]
    new_data = pd.DataFrame.from_dict(input_info, orient='index').T

    run_parameters = {}

    params = step_parameters['ram_gb']
    try:
        m_mem_free = make_all_predictions(
            X=new_data[params['predictors']], results=params
        )
        m_mem_free = m_mem_free * 2
    except:
        logger.warning("Could not predict for m_mem_free.")
        logger.warning(f"Giving it the default of {DEFAULT_GB}.")
        m_mem_free = DEFAULT_GB
    if m_mem_free < MIN_GB:
        m_mem_free = MIN_GB
    elif m_mem_free > MAX_GB:
        m_mem_free = MAX_GB
    else:
        pass
    m_mem_free = str(math.ceil(m_mem_free)) + 'GB'

    run_parameters['m_mem_free'] = m_mem_free

    params = step_parameters['runtime_min']
    try:
        minutes = make_all_predictions(
            X=new_data[params['predictors']], results=params
        )
        minutes = minutes * 2
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

