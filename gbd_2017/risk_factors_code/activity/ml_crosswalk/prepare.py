import matplotlib as mpl
import numpy as np
from sklearn.preprocessing import OneHotEncoder
mpl.use("Agg")  # make sure matplot lib is working non-interactively. This switches it to a different backend.
import pandas as pd
from ml_crosswalk.labels import SharedLabels
from ml_crosswalk.drives import drives
from ml_crosswalk.covariates import merge_covariates
import copy 
from ml_crosswalk.machine_learning_models import RandomForestModel, XGBoostModel
from ml_crosswalk.models import Metrics
import configparser

# TODO: where to put these global variables?
config = configparser.ConfigParser()
config.read('FILEPATH/config.ini')


def train_model(algorithm, matched, estimand, estimator, feature_cols, save_dir):
    """

    :param algorithm:
    :param matched:
    :param estimand:
    :param estimator:
    :param feature_cols:
    :param save_dir:
    :return:
    """
    if algorithm == 'rf':
        rf = RandomForestModel(matched, estimand, estimator, save_dir)
        rf.train(feature_cols, estimand)
        rf.print_importance_chart(save_dir)
        return rf 
    elif algorithm == 'xgb':
        xgb = XGBoostModel(matched, estimand, estimator, save_dir)
        xgb.train(feature_cols, estimand)
        # TODO: add importance chart
        return xgb


def run_model_and_graph(graphing_df, model, estimator_data, save_dir, algorithm):
    """

    :param graphing_df:
    :param model:
    :param estimator_data:
    :param save_dir:
    :param algorithm:
    :return:
    """
    print(graphing_df.head())

    # Calculate metrics and graph
    metrics_obj = Metrics(graphing_df=graphing_df, model=model, save_dir=save_dir, algorithm=algorithm)
    metrics_obj.get_indiv_metrics()
    metrics_obj.print_facet_plots()

    # Predict DR on all FAO data (not just matched) by feeding it into the model
    new_df = estimator_data

    print(new_df.describe())
    print(new_df.isnull().any())
    predictions = model.predict(new_df=new_df, save_dir=save_dir)
    
    return predictions 
    

def merge_data(raw, topic, categorical_cols, estimand, estimator, me_name_df, feature_cols, required_cols,
               preserved_cols, cov_dir=None, covariates=None):
    """
    Manipulates the data to convert categories and optionally merge with covariates.

    #TODO add assumptions about parameters

    :param raw:
    :param topic:
    :param categorical_cols:
    :param estimand:
    :param estimator:
    :param me_name_df:
    :param feature_cols:
    :param required_cols:
    :param preserved_cols:
    :param cov_dir:
    :param covariates:
    :return:
    """

    df = raw.copy(deep=True)
    df = df[~df.location_id.isnull()]

    # Convert dtypes
    df['me_name'] = df['me_name'].astype('category')
    df['location_id'] = df['location_id'].astype('int')

    # Merge missing data
    super_regions_df = SharedLabels().super_regions()
    df = df.merge(super_regions_df, how='left')
    df = df.merge(me_name_df, on='me_name', how='left')

    _update_feature_cols(feature_cols)
    _update_categorical_cols(categorical_cols)

    if feature_cols is not None:  # TODO: why would it be none? We just appended to it
        desired_cols = feature_cols + required_cols + preserved_cols + ['data']
        desired_cols = list(set(desired_cols))
        if 'location_id' not in desired_cols:
            desired_cols.append('location_id')  # Strict location requirement

        print(df.head())
        # Subset
        df = df[desired_cols]

    if covariates:
        merged = merge_covariates(df, covariates, cov_dir)

    # TODO -- refactor. This is hard coded and inflexible. Consider using 'get_covariate_estimates' directly
        for c in covariates:
            feature_cols.append(c)
    else:
        merged = df

    print('\n\n')
    print('After preparing but before matching the data, the columns are:')
    _print_merge_report(merged)

    return merged


def get_estimator_data(merged_df, estimator, feature_cols):
    """

    :param merged_df:
    :param estimator:
    :param feature_cols:
    :return:
    """

    estimator_data = merged_df[merged_df.estimator == estimator.lower()]

    feature_cols.append('{}_data'.format(estimator.lower()))

    estimator_data = estimator_data.rename(columns={'data': '{}_data'.format(estimator.lower())})
    estimator_data = estimator_data.drop('estimator', axis=1)
    return estimator_data


def match_data(merged_df, estimator, estimator_data, estimand, save_dir, og_feature_cols, required_cols, covariates):
    """
    Creates a matched dataset from 'prepare's output of shared location, sex, year, and ages
        between estimand and estimator.

    :param merged_df:
    :param estimator:
    :param estimator_data:
    :param estimand:
    :param save_dir:
    :param og_feature_cols:
    :param required_cols:
    :param covariates:
    :return: a wide Pandas DataFrame with no missing values in the feature or target columns
    """
    print("\n")
    print("Merged df shape: ")
    print(merged_df.shape)

    # reshape to wide
    print("Estimator is: " + estimator)
    print("Estimator data shape: ")
    print(estimator_data.shape)

    estimand_data = merged_df[merged_df.estimator == estimand.lower()]
    estimand_data = estimand_data.rename(columns={'data': estimand.lower() + '_data'})  # gold standard
    estimand_data = estimand_data.drop('estimator', axis=1)
    print("Estimand is: " + estimand)
    print("Estimand data shape: ")
    print(estimand_data.shape)

    join_cols = list(set(og_feature_cols + required_cols + covariates)) + ['me_name_cat']
    join_cols.remove('estimator')

    if estimator == estimand:
        join_cols.append(estimand.lower() + '_data')

    print("The join cols are: ")
    print(join_cols)
    print(estimator_data.head())
    print(estimand_data.head())
    cleaned_df = estimator_data.merge(estimand_data, on=join_cols, how='inner', suffixes=['_' + estimator.lower(),
                                                                                          '_' + estimand.lower()])

    print("Cleaned df shape: ")
    print(cleaned_df.shape)
    print(cleaned_df.head())
    print(cleaned_df.isnull().any())

    print(estimator_data.columns)
    print(estimand_data.columns)

    print('\n\n')
    print('After matching, the columns are:')
    _print_merge_report(cleaned_df)
    print('\n\n')

    # file_path = save_dir + 'matched_data.csv'
    # print('Saving matched data set to ', file_path)
    # cleaned_df.to_csv(file_path)

    return cleaned_df


def _update_feature_cols(feature_cols, covariates=None):
    """
    Adds covariates to feature cols, if any

    :param feature_cols:
    :param covariates:
    :return:
    """
    if not covariates:
        feature_cols.append('me_name_cat')
        feature_cols.remove('me_name')
    else:
        for c in covariates:
            feature_cols.append(c)


def _update_categorical_cols(categorical_cols):
    """

    :param categorical_cols:
    :return:
    """
    categorical_cols.append('me_name_cat')
    categorical_cols.remove('me_name')


def build_graphing_dataset(model, me_name_df, og_categorical_cols):
    """
    """

    X_test = copy.deepcopy(model.X_test)
    print("X_test shape: ")
    print(X_test.shape)

    # De-code binary variables
    # Sometimes the case occurs where we have only one me_name_cat but multiple super region ids
    for col in og_categorical_cols:
        # TODO: fix this hard-coded biz with me_name.
        if col == 'me_name':
            col += '_cat'
        if col not in X_test.columns:
            col_array = X_test.filter(like=col + '_', axis=1).apply(pd.to_numeric)
            col_array = col_array.idxmax(axis=1)
            col_array = col_array.str.replace('(.*_)+', '')
            col_array = col_array.astype('int')

            print("Here are our {} IDs: ".format(col))
            print(col_array.head())
            print("\n\n")

            X_test = pd.concat([col_array, X_test], axis=1)
            X_test = X_test.rename(columns={X_test.columns[0]: col})

    print("The shape of the new X_test is: ")
    print(X_test.shape)
    print(X_test.columns)

    graphing_df = pd.DataFrame({'super_region_id': X_test['super_region_id'].values,
                                'me_name_cat': X_test['me_name_cat'].values,
                                'actual': model.y_test,
                                'predicted': model.predicted_test})
    print("Graphing df shape: ")
    print(graphing_df.shape)

    print(graphing_df.columns)
    super_regions_df = SharedLabels().super_regions().drop('location_id', axis=1).drop_duplicates()
    graphing_df = graphing_df.merge(super_regions_df, how='left')
    print("new shape:")
    print(graphing_df.shape)

    graphing_df = graphing_df.merge(me_name_df)
    print("new shape: ")
    print(graphing_df.shape)

    graphing_df = graphing_df.sort_values(by='super_region_id')
    print(graphing_df.head())
    print("Graphing df shape: ")
    print(graphing_df.shape)

    return graphing_df


def _print_merge_report(df):
    """
    Prints unique super regions, locations, years, and age groups. Useful in debugging.

    :param df:
    :return:
    """
    print('\n\n')
    print(df.columns)
    print('\n\n')
    print('The super regions present are:')
    print(sorted(df.super_region_id.unique()))
    print('\n\n')
    print('There are {} location IDs'.format(len(df.location_id.unique().tolist())))
    print('\n\n')
    print('The years present are:')
    print(sorted(df.year_id.unique()))
    print('\n\n')
    print('The age groups present are:')
    print(sorted(df.age_group_id.unique()))
    print('\n')


def one_hot_encoder(matched_df, feature_cols, categorical_cols):
    """

    :param matched_df:
    :param feature_cols:
    :param categorical_cols:
    :return: a one-hot encoded Pandas DataFrame
    """
    onehot_encoder = OneHotEncoder(sparse=False)
    oh_df = matched_df  # copy
    print(categorical_cols)
    for c in categorical_cols:
        oh_df = oh_df[pd.notnull(oh_df[c])]  # TODO find why these sometimes exist
    new_cols = []

    for col in categorical_cols:
        vals = matched_df[col].unique()
        vals = np.sort(vals)

        if len(vals) > 2:
            # oh_df = oh_df.drop(col, axis=1)
            oh_cols = ['{}_{}'.format(str(col), v) for v in vals]
            for new_col in oh_cols:
                new_cols.append(new_col)

            onehot_encoded = onehot_encoder.fit_transform(matched_df[[str(col)]])
            temp_df = pd.DataFrame(onehot_encoded, columns=oh_cols)
            oh_df = pd.concat([oh_df, temp_df], axis=1)
            feature_cols.remove(col)
            categorical_cols.remove(col)

    for nc in new_cols:
        feature_cols.append(nc)
        categorical_cols.append(nc)

    print('\n\n')
    print('After one-hot encoding, the columns are:')
    print(oh_df.columns)
    print('\n\n')
    print('Feature columns are ' + str(feature_cols))
    return oh_df
