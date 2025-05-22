import pandas as pd
import sklearn as sk
from sklearn.svm import SVC
import matplotlib
import matplotlib.pyplot as plt
import pickle
from covid_correction_helper_functions import *
from covid_correction_settings import get_covid_correction_settings

def get_and_add_labels(df, SETTINGS=get_covid_correction_settings()):
    labels = labels = pd.read_csv(SETTINGS['TRAINING_LABELS'])
    labels['cause_id'] = labels.cause_id.astype(str)
    df = merge_n_check_left(df, labels)   
    return df

def generate_models(df, SETTINGS=get_covid_correction_settings()):
    for model_year in SETTINGS['COVID_YEARS']:
        model_columns = get_svm_columns(model_year)
        training_df = df[model_columns]
        training_df = training_df[~training_df[f'tag_{model_year}'].isnull()]
        train_x = training_df[model_columns].drop(f'tag_{model_year}', axis=1)
        train_y = training_df[f'tag_{model_year}']
        max_training_size = int(train_x.shape[0] * .75) - 1
        training_steps = list(range(10,max_training_size, int(max_training_size*.15)))
        training_steps = training_steps + [max_training_size]
        # gridsearch used to find best parameters
        model_up = SVC(random_state=42, kernel="rbf", C=5000, tol=.001, max_iter=20000000, probability=True)
        model_up.fit(train_x, train_y.replace(-1, 0))
        # gridsearch used to find best parameters
        model_down = SVC(random_state=42, kernel="rbf", C=5000, tol=.001, max_iter=20000000, probability=True)
        model_down.fit(train_x, train_y.replace(1, 0).replace(-1,1))
        with open(SETTINGS['MODEL_UP'].format(year=model_year),'wb') as f:
            pickle.dump(model_up,f)

        with open(SETTINGS['MODEL_DOWN'].format(year=model_year),'wb') as f:
            pickle.dump(model_down,f)

        df.to_csv(SETTINGS['TRAINING_LABEL_SNAPSHOT'],index=False)


if __name__ == "__main__":
    SETTINGS = get_covid_correction_settings()
    check_and_make_dir('VERSION_FOLDER')
    populate_list_of_locations_with_2020_data()
    iso3s = get_list_of_active_iso3s()
    df = get_and_format_claude_data_for_predictions(iso3s)
    df = create_features(df)
    df = get_and_add_labels(df)
    check_and_make_dir('SVM_FIGURE_FOLDER')
    generate_models(df)
    