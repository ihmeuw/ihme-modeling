import pandas as pd
from ml_crosswalk.labels import get_save_dir, SharedLabels, Drives
from ml_crosswalk.models import make_st_gpr_plots, plot_predictions_vs_estimator_data
import configparser
import ast
import sys

dr = Drives()
j_drive = dr.j

args = sys.argv[1:]
labs = SharedLabels()

config = configparser.ConfigParser()
config.read(Drives().h + 'FILEPATH/config.ini')

topic = args[0]
version = args[1]
unseen_predictions = ast.literal_eval(args[2])

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

    :param other_algorithms: an array of strings detailing the abbreviated names of the algorithms included
                             in this run
    :param csv_name: a partial file path name that leads to the CSV where predictions are stored
    :param estimator: a string representing the name of the primary variable we're using to predict the target
                      i.e. our "silver standard" data
    :param target_col: a string representing the column name of the target column of the machine learning model
    :param common_cols: an array of strings denoting which columns are common between the two data frames
    :param hybrid_preserved_columns: an array of strings depicting the columns to preserve from the original data frame
                                     that don't necessarily have to do with the underlying analysis.
    :return: a Pandas DataFrame comprised of the combined predictions across the other algorithms
    """

    assert estimator_col.lower() != target_col.lower()
    total = pd.DataFrame()

    for j in range(len(other_algorithms)):

        file_path = save_dir + '{}_'.format(other_algorithms[j]) + csv_name + '.csv'
        preds = pd.read_csv(file_path)

        assert len(set(preds.columns.tolist())) == len(preds.columns.tolist())

        preds['model'] = '{}'.format(other_algorithms[j])

        preds = preds.rename(columns={'{}_prediction'.format(other_algorithms[j]): target_col})
        preds['estimator'] = estimator

        for df in [preds, total]:
            assert len(list(set(df.columns))) == len(list(df.columns))

        total = total.append(preds)
        print(total.columns.tolist())

    return total
    

def get_mean(other_algorithms, csv_name, target_col, estimator_col, common_cols, total, estimators, estimands):
    """
    Concatenate the predictions data frames from the algorithms used in the previous step of the program.
    Takes the simple mean of the various predictions.

    :param other_algorithms: an array of strings detailing the abbreviated names of the algorithms included
                             in this run
    :param csv_name: a partial file path name that leads to the CSV where predictions are stored
    :param target_col: a string representing the column name of the target column of the machine learning model
    :param estimator: a string representing the name of the primary variable we're using to predict the target
                      i.e. our "silver standard" data
    :param estimator_col: the estimator + '_data'
    :param common_cols: an array of strings denoting which columns are common between the two data frames
    :return: a Pandas DataFrame of the hybridized predictions
    """

    index_cols = common_cols + [estimator_col]
    index_cols = list(set(index_cols))

    target_data = total.pivot_table(values=target_col,
                                    index=index_cols,
                                    columns='model').reset_index()

    new_target_col = target_data[other_algorithms].mean(axis=1)
    new_target_data = pd.concat([target_data, new_target_col], axis=1)
    new_target_data.columns = target_data.columns.tolist() + [target_col]

    if new_target_data.shape[0] == 0:
        raise ValueError("You have no data!")

    for alg in other_algorithms:
        if new_target_data[alg].isnull().any():
            raise ValueError("Oops! You have a missing value in the '" + alg + "' column!")

    if new_target_data[target_col].isnull().any():
        raise ValueError("Oops! You have a missing value in the target column (" + target_col + ").")

    new_target_data.head()
    new_target_data = new_target_data.merge(labs.super_regions())

    # Here are our hybrid predictions
    new_target_data['hybrid_prediction'] = new_target_data[target_col]
    new_target_data = new_target_data.sort_values(by=['me_name', 'super_region_id'])

    if 'unseen' not in csv_name:
        file_path = save_dir + "hybridized_predictions.csv"

        new_target_data.to_csv(file_path, index = False)
    return new_target_data
    
    
    
def pull_unseen(file_path):

    if unseen_predictions:
        new_common_cols = common_cols + hybrid_preserved_columns

        paths_to_new_data = str(config[topic]['paths_to_new_data'])
        paths_to_new_data = eval(paths_to_new_data)

        for draw in range(len(paths_to_new_data)):
            save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)
            unseen_csv_name = 'unseen_predictions_draw{}'.format(draw)

            if len(paths_to_new_data) > 1:
                save_dir += 'draws/'

            new_total = get_total(other_algorithms, unseen_csv_name, target_col, estimator, estimator_col,
                                  new_common_cols, save_dir)

            new_predictions = get_mean(other_algorithms, unseen_csv_name, target_col, estimator_col,
                                           new_common_cols, new_total, estimators, estimands)

            new_predictions = new_predictions.drop(target_col, axis=1)
            for alg in other_algorithms:
                new_predictions = new_predictions.drop(alg, axis=1)

            new_predictions = new_predictions.rename(columns={'hybrid_prediction': 'value', 'seq': 'crosswalk_parent_seq'})

            file_path = save_dir + 'hybridized_unseen_predictions_draw{}.csv'.format(draw)
            new_predictions.to_csv(file_path, index = False)
            
    
    
if __name__ == '__main__':

    common_cols = ['location_id', 'sex_id', 'age_group_id', 'year_id', 'me_name', 'me_name_cat',
                   'super_region_id', 'estimator']
    csv_name = 'predictions'

    upper_range = 2

    total = pd.DataFrame()
    for i in range(0, upper_range):  # Predicts for IPAQ, then predicts for GPAQ

        estimator = estimators[i]
        estimand = estimands[i]

        save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)

        estimator_col = estimator.lower() + '_data'
        target_col = estimand.lower() + '_data'
        total = get_total(other_algorithms, csv_name, target_col, estimators[i], estimator_col, common_cols,
                          save_dir)

        pivot_data = get_mean(other_algorithms, csv_name, target_col, estimator_col,
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
