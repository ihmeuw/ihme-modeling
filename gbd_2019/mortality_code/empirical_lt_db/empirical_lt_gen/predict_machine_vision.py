from machine_vision_elt import (
    load_plot_set,
    make_outlier_ids,
    prep_image,
    prep_model,
    run_model,
    collect_predictions,
    accuracy_and_loss,
    save_predictions,
    load_plot_set_one_dir,
    predict_many)

from tensorflow import keras
import argparse

"""
Predict outlier status of ELTs from a machine vision model that has already been run.
For the sake of time, we do not need to re-run the model every time we run the
    mortality pipeline.
"""

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--ihme_loc_id', type=str, required=True,
                    action='store', help='ihme_loc_id')
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='version_id')

args = parser.parse_args()
ihme_loc_id = args.ihme_loc_id
version_id = args.version_id

# set directories
root_dir = 'FILEPATH/empirical-lt-dir/'
dir = '{}/{}/machine_vision'.format(root_dir, version_id)

# current best model, change path if a new model is run
model_path = 'FILEPATH/output'

# predict
predict_many(model_path, dir, ihme_loc_id)

# END
