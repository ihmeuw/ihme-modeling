################################################################################
## DESCRIPTION: Predicts LPA METs using algorithms from run_topic.py ##
## INPUTS: Crosswalk topic and version, three lists of column names as strings, 
##    strings of estimator, estimand, and algorithm, list of data filepaths as 
##    string, pickle file of estimation coefficients ##
## OUTPUTS: CSV of MET predictions ##
## AUTHOR:  ##
## DATE: 30 October 2019 ##
################################################################################

# Load dependencies
import ast
import configparser
import os
import sys
import pickle
import pandas as pd

from ml_crosswalk.prepare import match_data, merge_data, get_estimator_data, train_model
from ml_crosswalk.prepare import run_model_and_graph, build_graphing_dataset
from ml_crosswalk.drives import Drives
from ml_crosswalk.labels import SharedLabels, get_save_dir
from ml_crosswalk.models import plot_predictions_vs_estimator_data, make_st_gpr_plots, Metrics
from ml_crosswalk.prepare import one_hot_encoder
from ml_crosswalk.validation import _validate_preserved_cols, _validate_algorithm, _validate_matched_dataset
from ml_crosswalk.validation import _validate_config_file, _validate_input_columns
from ml_crosswalk.qsub_hybrid_parallel import predict_draws_in_parallel



if __name__ == '__main__':
    # Drives
    dr = Drives()
    j_drive = dr.j
    h_drive = dr.h
    labs = SharedLabels()
    
    # Validate and read in config file
    _validate_config_file()

    config = configparser.ConfigParser()
    config.read(Drives().h + 'FILEPATH/config.ini')
    
    # Parse arguments
    args = sys.argv[1:]
    for i in range(0, len(args)):
        print('Arg {} is: '.format(i))
        print(args[i])
    topic = args[0]
    version = args[1]
    og_feature_cols = ast.literal_eval(args[2])
    og_categorical_cols = ast.literal_eval(args[3])
    covariates = ast.literal_eval(args[4])
    estimator = args[5]
    estimand = args[6]
    algorithm = args[7]
    paths_to_new_data = eval(args[8])
    
    # Read in or calculate necessary additional data
    required_columns = ast.literal_eval(config['DEFAULT']['required_columns'])
    
    try:
        me_name_codebook = ast.literal_eval(config[topic]['me_name_codebook'])

    except UserWarning('No codebook created for multiple MEs in the {} topic yet.\n'
                       'Please update the `config.ini` file with information for this topic'.format(topic)):
        me_name_codebook = {0: ''}

    me_name_df = pd.DataFrame.from_dict(data=me_name_codebook, orient='index').reset_index()
    me_name_df.columns = ['me_name', 'me_name_cat']
    
    with open("FILEPATH/v" + version + "/estimators_obj_" + algorithm + "_" + estimator + ".pkl", 'rb') as f:
        estimators_object = pickle.load(f)
    draw_num = int(os.getenv("SGE_TASK_ID")) - 1
    print(draw_num)
    path_to_new_data = paths_to_new_data[draw_num]
    print(path_to_new_data)

    feature_cols = list(og_feature_cols)
    categorical_cols = list(og_categorical_cols)
    unseen_raw = pd.read_csv(path_to_new_data)

    # Get output directory filepath
    save_dir = get_save_dir(topic=topic, estimand=estimand, estimator=estimator, version=version)
    if len(paths_to_new_data) > 1:
        save_dir += 'draws/'

        if not os.path.exists(save_dir):
            try:
                os.makedirs(save_dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    # Add data column to unseen_raw if it's not already present
    if 'data' not in unseen_raw.columns:
        unseen_raw['data'] = None

    # Reformat unseen_raw and merge relevant additional columns
    print(unseen_raw.shape)
    preserved_cols = unseen_raw.columns.tolist()
    unseen_merged = merge_data(unseen_raw,
                               me_name_df=me_name_df,
                               feature_cols=feature_cols,
                               categorical_cols=categorical_cols,
                               covariates=covariates,
                               preserved_cols=preserved_cols,
                               required_cols=required_columns,
                               cov_dir=h_drive + 'FILEPATH')
    print(unseen_merged.shape)

    # One-hot encode unseen_merged to fit with estimator coefficients
    oh_unseen_merged = one_hot_encoder(merged_df=unseen_merged, feature_cols=feature_cols,
                                       categorical_cols=categorical_cols)

    print(oh_unseen_merged.shape)

    # Filter unseen dataset to only the rows with the desired questionnaire
    unseen_estimator_data = get_estimator_data(merged_df=oh_unseen_merged, estimator=estimator,
                                               feature_cols=feature_cols)
    print(unseen_estimator_data.shape)
    print(unseen_estimator_data.isnull().any())
    print(unseen_estimator_data.head())
    print(estimator)
    print(estimand)
    print(estimators_object)
    print(type(estimators_object))

    # Filter out rows without values to predict on
    if estimators_object['model'].estimator.lower() != estimators_object['model'].estimand.lower():
        unseen_estimator_data = unseen_estimator_data[unseen_estimator_data[estimator.lower() + '_data'].notnull()]
    print(unseen_estimator_data.shape)

    # Run prediction and output results
    new_preds = estimators_object['model'].predict(new_df=unseen_estimator_data, save_dir=save_dir, unseen=True, draw_number = draw_num)
    print(new_preds.columns.tolist())
    print(new_preds[estimator.lower() + '_data'].head())
