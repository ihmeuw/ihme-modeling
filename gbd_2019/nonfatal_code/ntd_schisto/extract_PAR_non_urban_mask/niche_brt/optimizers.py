#FILEPATH python3
# -*- coding: utf-8 -*-

# import system-level functions (interact R and python, set library path)
from sys import argv, path
path.append('FILEPATH')
# standard imports
from scipy import mean
from pandas import read_csv, DataFrame
# load optimizers
import skopt
# load learners
from sklearn.ensemble import (GradientBoostingRegressor,
                              GradientBoostingClassifier)
from xgboost.sklearn import (XGBRegressor,
                             XGBClassifier)
# load scoring_methods
from sklearn.model_selection import cross_val_score

## STANDARDIZING SPACE FUNCTION: 
##    convenience function for taking limits from .csv to python list
def space_maker (df):
    ls = []
    for i in range(0, len(df.columns)):
        ls += [tuple(df.iloc[:,i].dropna())]
    return ls

## FUNCTION FOR CALCULATING WEIGHTS OF BERNOULLI DISTRIBUTED DATA (i.e. PA)
def bernoulli (PA):
    weight_list = []
    denom = 0
    numer = sum(PA)
    for i in range(0, len(PA)):
        denom += 1-PA[i]
    for i in range(0, len(PA)):
        if PA[i] == 1:
            weight_list += [1]
        else:
            weight_list += [numer / denom]
    return weight_list

## HYPERPARAMETER OPTIMIZATION FUNCTION
##    note: skopt.gp_minimize() will return a NaN error if the space boundaries defined in range_file are identical
def optimize (X,
              y,
              range_file,
              learner_choice = 'brtR',
              optimizer = 'gp',
              cv_folds = 10,
              n_calls = 50):
    # read in ranges from range_file
    ranges = read_csv(range_file)
    # get number of features of input data
    n_features = X.shape[1]
    # make dataframe of limits parse-able by objective
    space = space_maker(ranges)
    # make weights
    weights = bernoulli(list(y))
    # make compatible search spaces and objective functions for each learner_choice
    if (learner_choice == 'brtR' or learner_choice == 'brtC'):
        space += [(1, n_features)]
        # define objective function to minimize <- skopt
        def objective (params):
            (learning_rate,
             max_depth,
             n_estimators,
             max_features) = params
            if (learner_choice == 'brtR'):
                learner = GradientBoostingRegressor()
            elif (learner_choice == 'brtC'):
                learner = GradientBoostingClassifier()
            learner.set_params(learning_rate = learning_rate,
                               loss = 'ls',
                               max_depth = max_depth,
                               max_features = max_features,
                               n_estimators = n_estimators,
                               subsample = 0.75)
            return -mean(cross_val_score(learner, X, y,
                                        cv=cv_folds, n_jobs=-1, scoring="neg_mean_absolute_error"))
    elif (learner_choice == 'xgbR' or learner_choice == 'xgbC'):
        # define objective function to minimize <- xgboost
        def objective (params):
            (learning_rate,
             max_depth,
             n_estimators) = params
            if (learner_choice == 'xgbR'):
                learner = XGBRegressor()
            elif (learner_choice == 'xgbC'):
                learner = XGBClassifier()
            learner.set_params(booster = 'gbtree',
                               learning_rate = learning_rate,
                               max_depth = max_depth,
                               n_estimators = n_estimators,
                               subsample = 0.75)
            return -mean(cross_val_score(learner, X, y, 
                                        cv=cv_folds, n_jobs=-1, scoring="neg_mean_absolute_error"))
    else:
        error_lrn = 'not an available choice, use: brtR, brtC, xgbR, or xgbC'
    # run fits: gaussian process, random forest, gradient-boosted regression trees (resp.)
    if (optimizer == 'gp'):
        best_pars = skopt.gp_minimize(objective, space, n_calls=n_calls)
    elif (optimizer == 'rf'):
        best_pars = skopt.forest_minimize(objective, space, n_calls=n_calls)
    elif (optimizer == 'brt'):
        best_pars = skopt.gbrt_minimize(objective, space, n_calls=n_calls)
    else:
        error_opt = 'not an available choice, use: gp, rf, or brt'  
    # return fit params conditional on no user type-o's
    if 'error_opt' in locals():
        print(error_opt)
        return None
    elif 'error_lrn' in locals():
        print(error_lrn)
        return None
    else:
        return best_pars.x

## DEFINE SYSTEM ARGS FROM R
bounds_file = argv[1]
data_file = argv[2]
optimizer = argv[3]
learner = argv[4]
cv_folds = int(argv[5])
n_calls = int(argv[6])
jobnum = argv[7]

## LOAD SEARCH SPACE LIMITS
lims = read_csv(bounds_file)
data = read_csv(data_file)
data = data.dropna(axis=0, how='any')
X_data = data.loc[:, data.columns[4]:data.columns[len(data.columns)-1]]
y_data = data.loc[:, data.columns[0]]

## RUN HYPERPARAMETER TUNING
par_fit = optimize(X_data,
                   y_data,
                   range_file = bounds_file,
                   learner_choice = learner,
                   optimizer = optimizer,
                   cv_folds = cv_folds,
                   n_calls = n_calls)

## SAVE TUNING RESULTS TO FILE
if (learner == 'brtR' or learner == 'brtC'):
    par_fit_dict = {'shrinkage': par_fit[0], 
                    'interaction.depth': par_fit[1], 
                    'n.trees': par_fit[2], 
                    'n.minobsinnode':par_fit[3]}
else:
    par_fit_dict = {'shrinkage': par_fit[0], 
                    'interaction.depth': par_fit[1], 
                    'n.trees': par_fit[2]}
    
par_fit_df = DataFrame(data = par_fit_dict, index=[0])
best_pars_file = 'FILEPATH'
par_fit_df.to_csv(best_pars_file, index=False)



