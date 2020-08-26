"""
These functions are taken verbatim from the old registration code, and
all of them will be deleted once we stop saving things to files.
"""

import functools
import itertools

import numpy as np
import pandas as pd


def determine_n_parameter_sets(params_dict):
    """Takes in parameter settings related to a given run_id, looks at
    hyperparameter combinations delimiter in
    st_lambdaa, st_omega, and st_zeta
    to determine whether or not user specified multiple parameter sets
    to run as separate models for parameter selection

    Args
            * params_dict (dict) : Must have keys 'st_lambdaa', 'st_omega',
                                                    'st_zeta', 'gpr_scale'
            * holdouts (int) : Zero or positive integer specifying
                                            number of holdouts to run with
            * density_cutoffs (list) : List of data density cutoffs, ie
                                                    cutpoints at which a new hyperparameter
                                                    will be assigned for countries with
                                                    >= *cutpoint* country-years of data

    Returns:
            (int) designating the number of hyperparameter combinations being used
    """
    params = ['st_lambda', 'st_omega', 'st_zeta', 'gpr_scale']
    holdouts = params_dict['holdouts']
    if holdouts > 0:
        for i in params_dict.items():
            n_values = [len(val)
                        for key, val in params_dict.items() if key in params]

        n_param_sets = functools.reduce(lambda x, y: x * y, n_values)
    else:
        n_param_sets = 1

    return n_param_sets


def square_skeleton(components, names):
    """Cartesian product of four variables to make a square.

            Inputs:
            -components (list): list of LISTS to turn into dataframes and merge
            -names (list): names of output columns, in same order as componets

            Outputs: pandas dataframe with all combinations of each list entered in components """

    key = 'tmpkey'
    sqr = pd.DataFrame({key: 1}, index=[0])

    for i in range(0, len(components)):
        components[i] = pd.DataFrame({names[i]: components[i], key: 1})
        sqr = sqr.merge(components[i], on=key, how='outer')

    sqr.drop(columns=key, inplace=True)

    return(sqr)


def create_hyperparameter_grid(params_dict, density_cutoffs, run_type):
    """Create a grid of potential hyperparameters - if it's a KO run,
    split it up into separate parameters files and save. If not, split
    by data density and prepare for merge during spacetime. Ugh.

    Args
    * params_dict (dict) : Must have keys 'st_lambdaa', 'st_omega',
                                            'st_zeta', 'gpr_scale'
    * density_cutoffs (list) : List of data density cutoffs, ie
                                            cutpoints at which a new hyperparameter
                                            will be assigned for countries with
                                            >= *cutpoint* country-years of data
    * run_type (str) : string designating which kind of run this is
                                       MUST match one of the outputs in function
                                       determine_run_type

    Returns: (pandas dataframe) with all combinations of hyperparameters
                    given data density cutoffs and hyperparam dict"""

    cutoffs = density_cutoffs.copy()
    combo_params = ['st_lambda', 'st_omega', 'st_zeta', 'gpr_scale']
    run_type = run_type.name

    names = [key for key, val in params_dict.items() if key in combo_params]
    param_names = [k.rsplit('_')[-1] for k in names]
    param_components = [val for key,
                        val in params_dict.items() if key in combo_params]

    if ((run_type == 'in_sample_selection') | (run_type == 'oos_selection')):

        combos = square_skeleton(param_components, param_names)

    elif run_type == 'dd':
        names = ['density_cutoffs'] + param_names
        components = [cutoffs] + param_components
        combos = pd.DataFrame(dict(zip(names, components)))

    else:
        cutoffs = 0
        names = ['density_cutoffs'] + param_names
        components = [[cutoffs]] + param_components
        combos = pd.DataFrame(itertools.product(*components), columns=names)

    for param in param_names:
        combos[param] = combos[param].astype(float)
    return combos.drop_duplicates().reset_index(drop=True)


def set_up_hyperparam_system(run_root, combos, run_type):
    run_type = run_type.name
    path = f'{run_root}/parameters.h5'
    if ((run_type == 'in_sample_selection') or (run_type == 'oos_selection')):
        for i in list(range(0, combos.shape[0])):
            combos.iloc[i].to_hdf(path, f'parameters_{i}', index=False)
    else:
        combos.to_hdf(path, 'parameters_0', index=False)


def mark_outliers_for_viz(data_df):
    data_df['outlier_value'] = data_df['data']
    data_df.loc[data_df['is_outlier'] == 0, 'outlier_value'] = np.nan
    data_df.loc[data_df['is_outlier'] == 1, 'data'] = np.nan
    return data_df
