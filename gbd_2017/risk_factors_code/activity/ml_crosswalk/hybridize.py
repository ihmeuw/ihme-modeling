import pandas as pd
# import os
from ml_crosswalk.labels import get_save_dir, SharedLabels, drives
from ml_crosswalk.models import make_st_gpr_plots, plot_predictions_vs_estimator_data
import configparser
import ast
import sys

dr = drives()
j_drive = dr.j

args = sys.argv[1:]
labs = SharedLabels()

config = configparser.ConfigParser()
config.read('FILEPATH/config.ini')

topic = args[0]
version = args[1]
unseen_predictions = ast.literal_eval(args[2])
# og_feature_cols = ast.literal_eval(args[2])
# og_categorical_cols = ast.literal_eval(args[3])
# covariates = ast.literal_eval(args[4])
# preserved_cols = ast.literal_eval(args[5])

algorithm = 'hybrid'
estimands = ast.literal_eval(config[topic]['estimands'])
estimators = ast.literal_eval(config[topic]['estimators'])
other_algorithms = ast.literal_eval(config['DEFAULT']['algorithm'])
hybrid_preserved_columns = ast.literal_eval(config[topic]['hybrid_preserved_columns'])

years_start = 1980
years_end = 2000

path_to_file = config[topic]['path_to_file']


def get_total(other_algorithms, csv_name, target_col, estimator, estimator_col, common_cols, save_dir):
    """
    Reads in the other algorithms' predictions to create a combined data set.

    :param other_algorithms:
    :param csv_name:
    :param estimator:
    :param target_col:
    :param common_cols:
    :param hybrid_preserved_columns:
    :return:
    """

    assert estimator_col.lower() != target_col.lower()
    total = pd.DataFrame()

    for j in range(len(other_algorithms)):

        file_path = save_dir + '{}_'.format(other_algorithms[j]) + csv_name + '.csv'
        print("Loading the predictions at " + file_path)
        preds = pd.read_csv(file_path)

        print(preds.columns.tolist())
        assert len(set(preds.columns.tolist())) == len(preds.columns.tolist())

        preds['model'] = '{}'.format(other_algorithms[j])

        print("\n")
        print("Preds shape is: ")
        print(preds.shape)
        print("\n")
        print("Preds columns are: ")
        print(preds.columns.tolist())
        print(preds.head())
        print("Estimator is " + estimator)

        if 'combined' not in csv_name:
            preds = preds.rename(columns={'{}_prediction'.format(other_algorithms[j]): target_col})
            preds['estimator'] = estimator

            for df in [preds, total]:
                print("Checking for duplicates in " + estimator + " predictions.")
                print(df.columns.tolist())
                assert len(list(set(df.columns))) == len(list(df.columns))

            total = total.append(preds)

        else:
            index_cols = common_cols + hybrid_preserved_columns
            index_cols = list(set(index_cols))
            preds = preds[index_cols + [target_col, estimator_col]]
            preds = preds.rename(columns={target_col: other_algorithms[j]})

            if total.shape[0] == 0:
                total = preds
            else:
                total = total.merge(preds, on=index_cols, suffixes=['_{}'.format(other_algorithms[j-1]),
                                                                    '_{}'.format(other_algorithms[j])])
    return total


def get_mean(topic, other_algorithms, csv_name, target_col, estimator_col, common_cols, total, estimators, estimands):
    """
    Concatenate the predictions data frames from the algorithms used in the previous step of the program.
    Takes the simple mean of the various predictions.

    :param topic:
    :param other_algorithms:
    :param csv_name:
    :param target_col:
    :param estimator:
    :param estimator_col:
    :param common_cols:
    :return: a Pandas DataFrame of the hybridized predictions
    """

    assert len(list(set(total.columns))) == len(list(total.columns))
    print("Total's shape is: ")
    print(total.shape)
    print(total.head())
    print(total.isnull().any())

    index_cols = common_cols + [estimator_col]
    index_cols = list(set(index_cols))
    print("\n")
    print("Index cols are: " + str(index_cols))

    print(total.estimator.unique())

    if 'combined' not in csv_name:
        print(estimator_col == target_col)
        print(total.columns.tolist())

        if estimators[0] != estimands[0]:
            total = total.dropna(subset=[estimator_col])

        index_cols.remove(estimator_col)
        target_data = total.pivot_table(values=target_col,
                                        index=index_cols,
                                        columns='model').reset_index()

        # else:
        #     total = total.sort_values(['location_id', 'age_group_id', 'sex_id', 'year_id'])
        #     print(total.head())
        #     print(target_col)
        #     index_cols.remove(estimator_col)
        #
        #     target_data = total.pivot(values=target_col,
        #                               index=index_cols + ['mean_highactive', 'mean_inactive'],
        #                               columns='model').reset_index()
        #     assert target_data.shape[0] > 0

    else:
        estimand_data = total[total.estimator == estimand]
        print(estimand_data.head())

        total = total[total.estimator != estimand]
        estimator_data = total.filter(like=estimator_col).mean(axis=1)

        # target_data[estimator_col][target_data[estimator_col].isnull()] =
        target_data = pd.concat([total, estimator_data], axis=1)

        target_data.columns = total.columns.tolist() + [estimator_col]

        print(target_data.columns.tolist())
        print(estimand_data.columns.tolist())
        target_data = target_data.append(estimand_data)

    print(target_data.isnull().any())

    print(target_data.head())
    new_target_col = target_data[other_algorithms].mean(axis=1)
    new_target_data = pd.concat([target_data, new_target_col], axis=1)
    new_target_data.columns = target_data.columns.tolist() + [target_col]

    print(new_target_data.isnull().any())
    if new_target_data.shape[0] == 0:
        raise ValueError("You have no data!")

    for alg in other_algorithms:
        if new_target_data[alg].isnull().any():
            raise ValueError("Oops! You have a missing value in the '" + alg + "' column!")

    if new_target_data[target_col].isnull().any():
        raise ValueError("Oops! You have a missing value in the target column (" + target_col + ").")

    print(new_target_data.shape)
    new_target_data.head()
    new_target_data = new_target_data.merge(labs.super_regions())

    print("Target col is " + target_col)
    print("Estimator col is " + estimator_col)
    print(new_target_data.columns)
    print(new_target_data.head())

    # Here are our hybrid predictions
    new_target_data['hybrid_prediction'] = new_target_data[target_col]
    new_target_data = new_target_data.sort_values(by=['me_name', 'super_region_id'])

    if 'unseen' not in csv_name:
        if 'combined' not in csv_name:
            file_path = save_dir + "hybridized_predictions.csv"

        else:
            file_path = save_dir + 'hybridized_combined_predictions.csv'
            # for i in estimators:
            #     if i not in new_target_data.me_name.unique():
            #         raise ValueError("You're missing estimator " + i)

        print(new_target_data.shape)
        print("Saving hybridized predictions to " + file_path)
        new_target_data.to_csv(file_path)
    return new_target_data


def pull_unseen(file_path):

    if unseen_predictions:
        print("\n")
        print("Working on hybridizing unseen data.\n")
        new_common_cols = common_cols + hybrid_preserved_columns
        print("Old common cols are " + str(common_cols))
        print("Hybrid cols to preserve are " + str(hybrid_preserved_columns))

        paths_to_new_data = str(config[topic]['paths_to_new_data'])
        paths_to_new_data = eval(paths_to_new_data)

        for draw in range(len(paths_to_new_data)):

            if len(ast.literal_eval(config[topic]['estimands'])) > 2:
                save_dir = get_save_dir(topic=topic, estimand=estimand, version=version)
                unseen_csv_name = 'unseen_combined_predictions_draw{}'.format(draw)

            else:
                save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)
                unseen_csv_name = 'unseen_predictions_draw{}'.format(draw)

            if len(paths_to_new_data) > 1:
                save_dir += 'draws/'

            new_total = get_total(other_algorithms, unseen_csv_name, target_col, estimator, estimator_col,
                                  new_common_cols, save_dir)
            print(new_total.head())
            print("Estimator col is " + estimator_col)

            new_predictions = get_mean(topic, other_algorithms, unseen_csv_name, target_col, estimator_col,
                                           new_common_cols, new_total, estimators, estimands)
            print(new_predictions.columns.tolist())
            print(new_predictions.isnull().any())
            print(new_predictions.head())

            new_predictions = new_predictions.drop(target_col, axis=1)
            for alg in other_algorithms:
                new_predictions = new_predictions.drop(alg, axis=1)

            new_predictions = new_predictions.rename(columns={'hybrid_prediction': 'value'})

            file_path = save_dir + 'hybridized_unseen_predictions_draw{}.csv'.format(draw)
            print("Printing hybridized unseen predictions to " + file_path)
            new_predictions.to_csv(file_path)


if __name__ == '__main__':

    common_cols = ['location_id', 'sex_id', 'age_group_id', 'year_id', 'me_name', 'me_name_cat',
                   'super_region_id', 'estimator']

    if len(estimators) > 1 and estimators[0].lower() != estimands[0].lower():
        if estimators[1].lower() == estimands[0].lower():
            upper_range = 2
        else:
            upper_range = 1

        total = pd.DataFrame()
        for i in range(0, upper_range):

            estimator = estimators[i]
            estimand = estimands[i]

            if (len(ast.literal_eval(config[topic]['estimands'])) > 2 or estimators[1] != estimands[0]) and \
                    estimators[0].lower() != estimands[0].lower():
                save_dir = get_save_dir(topic=topic, estimand=estimand, version=version)
                csv_name = 'combined_predictions'

            else:
                save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)
                csv_name = 'predictions'

            estimator_col = estimator.lower() + '_data'
            target_col = estimand.lower() + '_data'
            total = total.append(get_total(other_algorithms, csv_name, target_col, estimators[i], estimator_col, common_cols,
                              save_dir))

        print(total.estimator.unique())

        pivot_data = get_mean(topic, other_algorithms, csv_name, target_col, estimator_col,
                              common_cols, total, estimators, estimands)

        # Plot estimated vs original
        years_start = ast.literal_eval(config[topic]['years_start'])
        years_end = ast.literal_eval(config[topic]['years_end'])
        for start, end in zip(years_start, years_end):
            plot_predictions_vs_estimator_data(topic=topic, version=version, sups=labs.super_regions(),
                                               year_start=start, year_end=end, estimator=estimator,
                                               estimand=estimand,
                                               predictions=pivot_data,
                                               algorithm=algorithm)

        # St-gpr style plots
        me_names = pivot_data.me_name.unique().tolist()
        location_ids = ast.literal_eval(config['DEFAULT']['selected_location_ids'])

        for loc_id in location_ids:
            for me in me_names:
                make_st_gpr_plots(topic, version, labs.super_regions(), estimand, loc_id, me, pivot_data, algorithm)

        file_path = save_dir + csv_name

        pull_unseen(file_path)

    else:
        for i in range(0, len(estimators)):
            total = pd.DataFrame()
            for j in range(0, len(estimators)):
                if estimators[i].lower() != estimands[j].lower():

                    estimand = estimands[i]
                    estimator = estimators[j]

                    save_dir = get_save_dir(topic=topic, estimator=estimator, estimand=estimand, version=version)
                    csv_name = 'predictions'

                    estimator_col = estimator.lower() + '_data'
                    target_col = estimand.lower() + '_data'

                    print("Estimator col is " + estimator_col)
                    print("Target col is " + target_col)

                    total = total.append(get_total(other_algorithms, csv_name, target_col, estimators[i], estimator_col,
                                                   common_cols, save_dir))
            print(total.estimator.unique())
            print(total.describe())
            print(total.isnull().any())
            pivot_data = get_mean(topic, other_algorithms, csv_name, target_col, estimator_col,
                                          common_cols, total, estimators, estimands)

            file_path = save_dir + csv_name
            pull_unseen(file_path)



        #
        # paths_to_new_data = str(config[topic]['paths_to_new_data'])
        # paths_to_new_data = eval(paths_to_new_data)
        #

        #
        #     new_data = pd.read_csv(drives().j + paths_to_new_data[i])
        #     print(new_data.head())
        #     new_data = new_data[new_data.estimator == estimand.lower()]
        #     new_data = new_data[new_data.data.notnull()]
        #     new_data = new_data.rename(columns={'data': 'value'})
        #
        #     for_upload = pd.concat([new_predictions, new_data])
        #     print(for_upload.shape)
        #     print(new_predictions.shape)
        #     assert for_upload.shape[0] > new_predictions.shape[0]
        #
        #     print(for_upload.head())
        #
        #     for_upload.to_csv(save_dir + 'hybridized_unseen_and_original.csv')

