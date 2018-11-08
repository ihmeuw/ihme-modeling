import os
import pandas as pd
import sys
import ast
import configparser
from ml_crosswalk.prepare import match_data, merge_data, get_estimator_data, train_model
from ml_crosswalk.prepare import run_model_and_graph, build_graphing_dataset
from ml_crosswalk.drives import drives
from ml_crosswalk.labels import SharedLabels, get_save_dir
from ml_crosswalk.models import plot_predictions_vs_estimator_data, make_st_gpr_plots, Metrics
from ml_crosswalk.prepare import one_hot_encoder
from ml_crosswalk.validation import _validate_preserved_cols, _validate_algorithm, _validate_matched_dataset
from ml_crosswalk.validation import _validate_config_file, _validate_input_columns
from ml_crosswalk.qsub_hybrid_parallel import predict_draws_in_parallel


def step_one_training(topic, version, algorithm, covariates, estimator, estimand, feature_cols, me_name_df,
                      categorical_cols):
    """ Matches data, performs one-hot encoding if necessary, trains a machine-learning model, and runs
    the model and graphs. Returns several objects along the way. """

    save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)

    print('Currently working on predicting {} data using {} data.\n'.format(estimand, estimator))

    merged = merge_data(raw, topic=topic,
                        estimator=estimator,
                        me_name_df=me_name_df,
                        feature_cols=feature_cols,
                        categorical_cols=categorical_cols,
                        covariates=covariates,
                        estimand=estimand,
                        preserved_cols=preserved_cols,
                        required_cols=required_columns,
                        cov_dir='FILEPATH')

    oh_merged = one_hot_encoder(matched_df=merged, feature_cols=feature_cols,
                                categorical_cols=categorical_cols)

    print(feature_cols)
    estimator_data = get_estimator_data(merged_df=oh_merged,
                                        estimator=estimator,
                                        feature_cols=feature_cols)
    if estimator.lower() != estimand.lower():
        matched = match_data(merged, estimator, estimator_data, estimand, save_dir, og_feature_cols, required_columns,
                             covariates)
    else:  # Sometimes we are training the data on ALL the estimand data without any estimator data
        matched = estimator_data
        feature_cols.remove(estimator.lower() + '_data')  # Without removing this column, our target is in our features

    # Skip if no common data points
    _validate_matched_dataset(matched, estimand, estimator)

    model = train_model(matched=matched, algorithm=algorithm, estimator=estimator,
                        estimand=estimand, feature_cols=feature_cols,
                        save_dir=save_dir)

    graphing_df = build_graphing_dataset(model, me_name_df, og_categorical_cols)

    predictions = run_model_and_graph(estimator_data=estimator_data, graphing_df=graphing_df,
                                          model=model, algorithm=algorithm,
                                          save_dir=save_dir)

    print("Raw shape: ")
    print(raw.shape)
    print("Merged shape: ")
    print(merged.shape)
    print("oh_merged shape: ")
    print(oh_merged.shape)
    print("Estimator data shape: ")
    print(estimator_data.shape)
    print("Matched shape: ")
    print(matched.shape)
    print(matched.year_id.unique())
    print(matched.age_group_id.unique())
    print("X_train shape: ")
    print(model.X_train.shape)
    print("X_test shape: ")
    print(model.X_test.shape)
    print("Graphing df shape: ")
    print(graphing_df.shape)
    print("Predictions shape: ")
    print(predictions.shape)
    print("\n")

    return {'merged': merged, 'estimator_data': estimator_data, 'matched': matched, 'model': model,
            'predictions': predictions, 'graphing_df': graphing_df}


def step_two_crosswalking(topic, version, raw, sups, estimators_array, estimators, estimands, me_name_df):
    """ Does the essential crosswalking part of this library. """

    estimand_data = raw.copy(deep=True)
    print(raw.head())
    estimand_data = estimand_data[estimand_data.estimator == estimands[0].lower()]
    estimand_data = estimand_data.rename(columns={'data': estimands[0].lower() + '_data'})
    estimand_data = estimand_data.merge(sups)
    estimand_data['estimator'] = estimands[0].lower()
    print("The estimand data are: ")
    print(estimand_data.head())

    if estimand_data.shape[0] == 0:
        raise ValueError("Oops! No estimand found in the original data set. Did you mean to look for a match for " +
                         estimands[0] + "?")

    cols = og_feature_cols + preserved_cols + ['{}_data'.format(estimands[0].lower())] + required_columns
    cols = list(set(cols))
    estimand_data = estimand_data[cols]

    estimators_array[0]['predictions']['estimator'] = estimators[0].lower()
    estimators_array[0]['predictions'] = estimators_array[0]['predictions'].merge(me_name_df)
    print("The predictions data are: ")
    print(estimators_array[0]['predictions'].head())

    total = estimators_array[0]['predictions'].rename(columns={'{}_prediction'.format(algorithm):
                                                               estimands[0].lower() + '_data'})

    print("Checking for null values in " + estimators[0].lower() + '_data')
    print(total[estimators[0].lower() + '_data'].head())
    if total[estimators[0].lower() + '_data'].isnull().any():
        raise ValueError("Oops! You have missing values in the " + estimators[0].lower() + "_data column!")

    assert (len(list(set(total.columns)))) == len(list(total.columns))
    assert len(list(set(estimand_data.columns))) == len(list(estimand_data.columns))
    total = total.append(estimand_data)

    if len(estimators) > 2 or (len(estimators) > 1 and estimands[0].lower() != estimators[1].lower()):
        total = combine_all(estimators_array, estimands, estimators, topic, estimand_data, me_name_df, total)

    print(total.head())
    print(total.describe())
    print(total[[estimands[0].lower() + '_data', estimators[0].lower() + '_data']].head())

    # Get save directory
    save_dir = get_save_dir(topic=topic, estimand=estimands[0], version=version, estimator=None)
    file_path = save_dir + '{}_combined_predictions.csv'.format(algorithm)

    print("Printing all of the predictions (combined) to " + file_path)
    print("\n\n")
    total.to_csv(file_path)
    return total


def combine_all(estimators_array, estimands, estimators, topic, estimand_data, me_name_df, total):
    """

    :param estimators_array:
    :param estimands:
    :param estimators:
    :param topic:
    :param estimand_data:
    :param me_name_df:
    :return:
    """

    for i in range(1, len(estimators_array)):
        if estimators_array[i]['model']:
            estimand = estimands[0].lower()
            print('Estimand is ' + estimand)
            first_estimator = estimators[0].lower()
            print('First estimator is ' + first_estimator)
            second_estimator = estimators[i].lower()
            print('Second estimator is ' + second_estimator)
            print('First target col is ' + estimand)
            second_estimand = estimands[i].lower()
            print('Second target col is ' + second_estimand)
            first_model = estimators_array[0]['model']
            second_predictions = estimators_array[i]['predictions']

            save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=second_estimator, version=version)
            save_dir += 'through_' + second_estimand.lower() + '/'

            if not os.path.exists(save_dir):
                try:
                    os.makedirs(save_dir)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

            new_df = second_predictions.rename(columns={'{}_prediction'.format(algorithm):
                                                        first_estimator.lower() + '_data'})
            print(new_df.head())

            new_predictions = first_model.predict(new_df=new_df, save_dir=save_dir)
            print(new_predictions.columns)

            estimand_data = estimand_data.rename(columns={estimand.lower() + '_data': 'actual'})
            print(estimand_data.columns)

            join_cols = list(set(og_feature_cols + required_columns))
            join_cols.remove('estimator')

            new_graphing_df = new_predictions.merge(estimand_data, how='inner', on=join_cols,
                                                    suffixes=('_predictions', '_actual'))

            super_regions_df = SharedLabels().super_regions()
            super_regions_df = super_regions_df.drop('location_id', axis=1)
            new_graphing_df = new_graphing_df.merge(super_regions_df)

            new_graphing_df = new_graphing_df.rename(columns={'{}_prediction'.format(algorithm): 'predicted'})
            print(new_graphing_df.head())
            print(new_graphing_df.shape)

            first_model.estimator = second_estimator

            metrics_obj = Metrics(new_graphing_df, algorithm=algorithm, model=first_model, save_dir=save_dir)
            metrics_obj.get_indiv_metrics()
            metrics_obj.print_facet_plots()

            new_predictions['estimator'] = estimators[i].lower()
            new_predictions = new_predictions.rename(columns={'{}_prediction'.format(algorithm):
                                                               estimand.lower() + '_data'})

            new_predictions = new_predictions.merge(me_name_df)
            print(new_predictions.columns)
            print(total.columns)

            total = total.append(new_predictions)
    return total


def predict_on_unseen_data(topic, estimator, estimand, version, og_feature_cols, og_categorical_cols,
                           me_name_df, covariates, required_columns, estimators_object, path_to_new_data):
    """
    Generate predictions on unseen data

    :param feature_cols:
    :param categorical_cols:

    :return:
    """

    feature_cols = list(og_feature_cols)
    categorical_cols = list(og_categorical_cols)
    unseen_raw = pd.read_csv(drives().j + paths_to_new_data[i])

    save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)
    if len(paths_to_new_data) > 1:
        save_dir += 'draws/'

        if not os.path.exists(save_dir):
            try:
                os.makedirs(save_dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    if 'data' not in unseen_raw.columns:
        unseen_raw['data'] = None

    print(unseen_raw.shape)
    preserved_cols = unseen_raw.columns.tolist()
    unseen_merged = merge_data(unseen_raw, topic=topic,
                               estimator=estimator,
                               me_name_df=me_name_df,
                               feature_cols=feature_cols,
                               categorical_cols=categorical_cols,
                               estimand=estimand,
                               covariates=covariates,
                               preserved_cols=preserved_cols,
                               required_cols=required_columns,
                               cov_dir='FILEPATH')
    print(unseen_merged.shape)

    oh_unseen_merged = one_hot_encoder(matched_df=unseen_merged, feature_cols=feature_cols,
                                       categorical_cols=categorical_cols)

    print(oh_unseen_merged.shape)

    unseen_estimator_data = get_estimator_data(merged_df=oh_unseen_merged, estimator=estimator,
                                               feature_cols=feature_cols)
    print(unseen_estimator_data.shape)
    print(unseen_estimator_data.isnull().any())
    print(unseen_estimator_data.head())
    print(estimator)
    print(estimand)
    print(estimators_object['model'].estimator)
    print(estimators_object['model'].estimand)

    if estimators_object['model'].estimator.lower() != estimators_object['model'].estimand.lower():
        unseen_estimator_data = unseen_estimator_data[unseen_estimator_data[estimator.lower() + '_data'].notnull()]
    # assert estimator == estimators_object['model'].estimator
    # assert estimand == estimators_object['model'].estimand
    print(unseen_estimator_data.shape)

    new_preds = estimators_object['model'].predict(new_df=unseen_estimator_data, save_dir=save_dir, unseen=True,
                                                   draw_number=i)
    print(new_preds.columns.tolist())
    print(new_preds[estimator.lower() + '_data'].head())


def predict_second_data_on_first_model(topic, estimators_objects_dict, estimators, estimands, i):
    for j in range(0, len(estimators)):
        if estimands[i] != estimators[j]:
            model = estimators_objects_dict[i]['model']
            matched_second_data = estimators_objects_dict[j]['matched']

            # Fake it till you make it
            # matched_second_data = matched_second_data.rename(columns={estimators[j].lower() + '_data':
            #                                                          estimands[i].lower() + '_data'})
            save_dir = get_save_dir(topic, estimands[i], estimators[j])
            model.predict(new_df=matched_second_data, save_dir=save_dir, unseen=False)
    return


if __name__ == '__main__':
    args = sys.argv[1:]
    dr = drives()
    j_drive = dr.j
    h_drive = dr.h
    labs = SharedLabels()

    _validate_config_file()

    config = configparser.ConfigParser()
    config.read('FILEPATH/config.ini')
    # Debug arguments
    for i in range(0, len(args)):
        print('Arg {} is: '.format(i))
        print(args[i])

    if len(args) > 3:
        # Read arguments from sys input
        topic = args[0]
        version = args[1]
        og_feature_cols = ast.literal_eval(args[2])
        og_categorical_cols = ast.literal_eval(args[3])
        covariates = ast.literal_eval(args[4])
        preserved_cols = ast.literal_eval(args[5])
        algorithm = args[6]
        unseen_data = ast.literal_eval(args[7])

    else:
        i = 0
        topic = args[0]
        version = ast.literal_eval(config[topic]['version'])[i]
        og_feature_cols = ast.literal_eval(config[topic]['og_feature_cols'])[i]
        og_categorical_cols = ast.literal_eval(config[topic]['og_categorical_cols'])[i]
        covariates = ast.literal_eval(config[topic]['covariates'])[i]
        preserved_cols = ast.literal_eval(config[topic]['preserved_cols'])[i]
        algorithm = ast.literal_eval(config['DEFAULT']['algorithm'])[int(args[1])]
        unseen_data = ast.literal_eval(args[2])

    # Get me name codebook
    try:
        me_name_codebook = ast.literal_eval(config[topic]['me_name_codebook'])

    except UserWarning('No codebook created for multiple MEs in the {} topic yet.\n'
                       'Please update the `config.ini` file with information for this topic'.format(topic)):
        me_name_codebook = {0: ''}

    me_name_df = pd.DataFrame.from_dict(data=me_name_codebook, orient='index').reset_index()
    me_name_df.columns = ['me_name', 'me_name_cat']

    # Create path
    path_to_file = config[topic]['path_to_file']
    data_path = j_drive + path_to_file
    raw = pd.read_csv(data_path, encoding="ISO-8859-1")
    # raw = raw.sample(n=500000)  # for debugging only

    # Validate inputs
    required_columns = ast.literal_eval(config['DEFAULT']['required_columns'])
    _validate_input_columns(raw, required_columns)
    _validate_preserved_cols(preserved_cols, og_feature_cols)
    _validate_algorithm(algorithm)

    # Sometimes we concatenate files where not all of them have super regions.
    for i in ['super_region_id', 'super_region_name']:
        if i in raw.columns:
            raw = raw.drop(i, axis=1)

    # Define variables for looping
    estimators = ast.literal_eval(config[topic]['estimators'])
    estimands = ast.literal_eval(config[topic]['estimands'])

    print("Estimators are: " + str(estimators))
    print("Estimands are: " + str(estimands))

    # loop over estimators
    estimators_objects_dict = []
    for i in range(len(estimators)):
        arr = step_one_training(topic, version, algorithm, covariates, estimators[i], estimands[i],
                                list(og_feature_cols), me_name_df, list(og_categorical_cols))
        estimators_objects_dict.append(arr)

    if estimands[0].lower() == estimators[0].lower():  # Assume they will match at every index
        for i in range(len(estimands)):
            predict_second_data_on_first_model(topic, estimators_objects_dict, estimators, estimands, i)

    else:
        # Predict with new data
        total = step_two_crosswalking(topic, version, raw, SharedLabels().super_regions(),
                                      estimators_objects_dict[:],
                                      estimators, estimands, me_name_df)

        print("Step two was successful.")
        print(total.head())

        # Plot estimated vs original
        years_start = ast.literal_eval(config[topic]['years_start'])
        years_end = ast.literal_eval(config[topic]['years_end'])
        for start, end in zip(years_start, years_end):
            plot_predictions_vs_estimator_data(topic=topic, version=version, sups=labs.super_regions(),
                                               year_start=start, year_end=end, estimator=estimators[0],
                                               estimand=estimands[0],
                                               predictions=estimators_objects_dict[0]['predictions'],
                                               algorithm=algorithm)

        # Make st-gpr style plots
        if total.empty:
            total = estimators_objects_dict[0]['predictions']
            total = total.rename(columns={'{}_prediction'.format(algorithm): 'predicted',
                                          estimators[0].lower() + '_data': 'actual'})
            total[estimands[0]] = total['predicted']
            total['estimator'] = estimators[0].lower()

        me_names = total.me_name.unique().tolist()
        location_ids = ast.literal_eval(config['DEFAULT']['selected_location_ids'])

        for location_id in location_ids:
            for me in me_names:
                make_st_gpr_plots(topic=topic, version=version, estimand=estimands[0], algorithm=algorithm,
                                  sups=labs.super_regions(), loc_id=location_id, me=me, total=total)

    # Optionally predict on new data
    if unseen_data:
        print("\n")
        print("Now predicting on new (unseen) data.")

        for i in range(len(estimators)):
            if estimators[0].lower() != estimands[0].lower():

                paths_to_new_data = str(config[topic]['paths_to_new_data'])
                paths_to_new_data = eval(paths_to_new_data)
                for j in range(len(paths_to_new_data)):  # This is serialized and takes forever for all 1000 draws
                    if len(paths_to_new_data) > 1:
                        # Launch a subprocess
                        predict_draws_in_parallel(config, topic, version, algorithm, estimators[i],
                                                  estimands[i], estimators_objects_dict[i])
                    else:
                        predict_on_unseen_data(topic, estimators[i], estimands[i], version,
                                           list(og_feature_cols), list(og_categorical_cols),
                                           me_name_df, covariates, required_columns, estimators_objects_dict[i],
                                           paths_to_new_data[j])
            else:
                for j in range(len(estimators)):
                    if estimators[i].lower() != estimators[j].lower():
                        paths_to_new_data = str(config[topic]['paths_to_new_data'])
                        paths_to_new_data = eval(paths_to_new_data)
                        for k in range(len(paths_to_new_data)):
                            predict_on_unseen_data(topic, estimators[j], estimands[i], version,
                                                   list(og_feature_cols), list(og_categorical_cols),
                                                   me_name_df, covariates, required_columns,
                                                   estimators_objects_dict, paths_to_new_data[k])

