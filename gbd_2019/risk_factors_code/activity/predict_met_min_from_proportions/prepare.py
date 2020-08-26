import matplotlib as mpl
import numpy as np
from sklearn.preprocessing import OneHotEncoder
mpl.use("Agg")  # make sure matplot lib is working non-interactively. This switches it to a different backend.
import pandas as pd
from ml_crosswalk.labels import SharedLabels
from ml_crosswalk.drives import Drives
from ml_crosswalk.covariates import merge_covariates
import copy 
from ml_crosswalk.machine_learning_models import RandomForestModel, XGBoostModel
from ml_crosswalk.models import Metrics
import configparser


config = configparser.ConfigParser()
config.read(Drives().h + 'ml_crosswalk/src/ml_crosswalk/config.ini')


def train_model(algorithm, matched, estimand, estimator, feature_cols, save_dir):
    """
    Creates a Machine Learning Model object using the given algorithm. Trains a model and returns the object.

    :param algorithm: a string depicting the abbreviated machine learning model to be used.
    :param matched: a pandas DataFrame containing only matched data between the estimator and estimand
    :param estimand: a string depicting the estimand used for analysis
    :param estimator: a string depicting the estimator used for analysis
    :param feature_cols: an array of strings depicting which columns on the DataFrames are features to be used in the
                         algorithm
    :param save_dir: a string depicting a local file path for writing
    :returns: a Machine Learning Model object that has undergone training
    """
    if algorithm == 'rf':
        rf = RandomForestModel(matched, estimand, estimator, save_dir)
        rf.train(feature_cols, estimand)
        rf.print_importance_chart(save_dir)
        return rf 
    elif algorithm == 'xgb':
        xgb = XGBoostModel(matched, estimand, estimator, save_dir)
        xgb.train(feature_cols, estimand)
        return xgb


def run_model_and_graph(graphing_df, model, estimator_data, save_dir, algorithm):
    """
    Passes the Machine Learning Model object to a Metrics object. The Metrics object calculates metrics
    (i.e. OOS error and Pearson correlation) for every modelable entity and saves them to disk within the given save
    directory. It also prints to disk scatter plots for OOS correlations between the target data and estimator data.

    :param graphing_df: a pandas DataFrame prepared for graphing
    :param model: a Machine Learning Model object that contains the trained model and results
    :param estimator_data: a pandas DataFrame containing only the original estimator data
    :param save_dir: a string depicting a local file path for writing
    :param algorithm: a string denoting which (abbreviated) machine learning model is being used for graphing
    :return predictions: a pandas DataFrame that has the results of a DataFrame being passed through the Machine Learning model.
             It contains newly predicted data.
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
    

def merge_data(raw, categorical_cols, me_name_df, feature_cols, required_cols,
               preserved_cols, cov_dir=None, covariates=None):
    """
    Manipulates the data to convert categories and optionally merge with covariates.

    :param raw: a pandas dataFrame with clean, unprocessed data
    :param categorical_cols: an array of strings depicting which columns of raw contain categorical data
    :param me_name_df: a pandas dataFrame that maps the modelable entity name to a code from the config.ini file
    :param feature_cols: a list of strings depicting which columns of raw contain feature data
    :param required_cols: a list of strings depicting which columns of raw are required for modeling
    :param preserved_cols: a list of strings depicting which columns of raw should be preserved throughout the modeling
                           process
    :param cov_dir: (optional) a directory that points to the location where the covariate flat files are stored
    :param covariates: (optional) a list of strings depicting which covariates to merge onto the new dataFrame
    :return merged: a pandas DataFrame that contains newly categorical data and (optional) covariate columns
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

    if feature_cols is not None:
        desired_cols = feature_cols + required_cols + preserved_cols + ['data']
        desired_cols = list(set(desired_cols))
        if 'location_id' not in desired_cols:
            desired_cols.append('location_id')  # Strict location requirement

        print(df.head())
        # Subset
        df = df[desired_cols]

    if covariates:
        merged = merge_covariates(df, covariates, cov_dir)

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
    Parses all estimator data from the merged_df by subsetting for rows where the value of the 'estimator' column
    matches the current estimator (from the config.ini file)

    :param merged_df: a pandas DataFrame of one-hot encoded, covariate-merged data
    :param estimator: a string depicting which values in the 'estimator' column of merged_df should be kept
    :param feature_cols: an array of strings depicting which columns of merged_df contain feature data
    :return estimator_data: a pandas DataFrame containing only the data to be matched with the estimand data for
                            model training
    """

    estimator_data = merged_df[merged_df.estimator == estimator.lower()]

    feature_cols.append('{}_data'.format(estimator.lower()))

    estimator_data = estimator_data.rename(columns={'data': '{}_data'.format(estimator.lower())})
    estimator_data = estimator_data.drop('estimator', axis=1)
    return estimator_data


def match_data(merged_df, estimator, estimator_data, estimand, og_feature_cols, required_cols, covariates):
    """
    Creates a matched dataFrame of shared location, sex, year, and ages between the given estimand and given estimator.

    :param merged_df: a pandas dataFrame of one-hot encoded, covariate-merged data
    :param estimator: a string depicting the estimator data source
    :param estimator_data: a pandas dataFrame consisting of only estimator data (returned from get_estimator_data())
    :param estimand: a string depicting the estimand data source
    :param og_feature_cols: an array of strings depicting which columns of merged_df are the original (pre-transformed)
                            feature columns
    :param required_cols: an array of strings depicting which columns of merged_df are required for modeling
    :param covariates: an array of strings depicting the covariates that were merged onto merged_df
    :return cleaned_df: a wide Pandas DataFrame resulting from an inner join between estimator and estimand data with no missing
             values in the feature or target columns
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
    Adds covariates to feature cols, if any.

    :param feature_cols: a list of strings depicting which columns on the DataFrame are features for the model
    :param covariates: a list of strings depicting which covariates are to be pulled in and merged with the data
    """
    if not covariates:
        feature_cols.append('me_name_cat')
        feature_cols.remove('me_name')
    else:
        for c in covariates:
            feature_cols.append(c)


def _update_categorical_cols(categorical_cols):
    """
    Replaces the `me_name` column in the categorical columns list with the updated `me_name_cat`.

    :param categorical_cols: a list of strings depicting which columns on the DataFrame contain categorical data (and
                             thus need to be one-hot encoded)
    """
    categorical_cols.append('me_name_cat')
    categorical_cols.remove('me_name')


def build_graphing_dataset(model, me_name_df, og_categorical_cols):
    """
    Back-transforms one-hot encoded data to a long DataFrame for plotting Out-of-sample correlation.

    :param model: an object that inherits from MachineLearningModel
    :param me_name_df: a pandas DataFrame mapping the modelable entity codes to the actual names
    :param og_categorical_cols: an array of strings depicting which columns on the original data set are categorical
    :return graphing_df: a pandas DataFrame of data prepared for graphing.
    """

    X_test = copy.deepcopy(model.X_test)
    print("X_test shape: ")
    print(X_test.shape)

    # De-code binary variables
    # Sometimes the case occurs where we have only one me_name_cat but multiple super region ids
    for col in og_categorical_cols:
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

    :param df: a pandas DataFrame of recently merged data.
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


def one_hot_encoder(merged_df, feature_cols, categorical_cols):
    """
    One-hot encodes data to prepare categorical columns for modeling.

    :param merged_df: a pandas DataFrame that has already gone through the merging process
                      of optionally adding covariates
    :param feature_cols: a list of strings depicting which columns of merged_df contain feature data
    :param categorical_cols: an array of strings depicting which columns of merged_df contain categorical data
    :return oh_df: a pandas DataFrame with categorical columns now one-hot encoded (and thus with sparser data)
    """
    onehot_encoder = OneHotEncoder(sparse=False)
    oh_df = merged_df  # copy
    print(categorical_cols)
    for c in categorical_cols:
        oh_df = oh_df[pd.notnull(oh_df[c])]
    new_cols = []

    for col in categorical_cols:
        vals = merged_df[col].unique()
        vals = np.sort(vals)

        if len(vals) > 2:
            oh_cols = ['{}_{}'.format(str(col), v) for v in vals]
            for new_col in oh_cols:
                new_cols.append(new_col)

            onehot_encoded = onehot_encoder.fit_transform(merged_df[[str(col)]])
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
