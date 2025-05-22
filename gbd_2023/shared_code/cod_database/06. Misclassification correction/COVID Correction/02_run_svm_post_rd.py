import pandas as pd
import sklearn as sk
import pickle
from sklearn.model_selection import learning_curve
from sklearn.svm import SVC
import numpy as np
from covid_correction_helper_functions import *
from covid_correction_settings import get_covid_correction_settings

def remove_predictions_for_years_without_data(df, SETTINGS=get_covid_correction_settings()):
    for year in SETTINGS['COVID_YEARS']:
        df[f'prediction_{year}_down'] = df[f'prediction_{year}_down'].where(df[f'{year}'] != -0.00001, np.nan, axis=0)
        df[f'prediction_{year}_up'] = df[f'prediction_{year}_up'].where(df[f'{year}'] != -0.00001, np.nan, axis=0)
    return df


def generate_predictions(df, SETTINGS=get_covid_correction_settings()):
    final_columns = ['location_id', 'cause_id', 'sex_id', 'age_group','super_region_name']
    for model_year in SETTINGS['COVID_YEARS']:
        model_columns = get_svm_columns(model_year)
        model_columns.remove(f'tag_{model_year}')
        with open(SETTINGS['MODEL_UP'].format(year=model_year), 'rb') as f:
            model_up = pickle.load(f)
        with open(SETTINGS['MODEL_DOWN'].format(year=model_year), 'rb') as f:
            model_down = pickle.load(f)
        df[f'prediction_{model_year}_up'] = model_up.predict(df[model_columns])
        df[f'proba_{model_year}_up'] = model_up.predict_proba(df[model_columns])[:,1]

        df[f'prediction_{model_year}_down'] = model_down.predict(df[model_columns])
        df[f'proba_{model_year}_down'] = model_down.predict_proba(df[model_columns])[:,1]
        final_columns = final_columns + [f'prediction_{model_year}_up']
        final_columns = final_columns + [f'prediction_{model_year}_down']
        final_columns = final_columns + [f'proba_{model_year}_up']
        final_columns = final_columns + [f'proba_{model_year}_down']

    df = remove_predictions_for_years_without_data(df)

    df.to_csv(SETTINGS['DATAFRAME_WITH_SVM_FEATURES_post_rd'], index=False)

    df[final_columns].to_csv(SETTINGS['DATAFRAME_WITH_SVM_PREDICTIONS_post_rd'], index=False)


if __name__ == "__main__":
    SETTINGS = get_covid_correction_settings()

    iso3s = get_list_of_active_iso3s()

    df = get_and_format_claude_data_for_predictions(iso3s, claude_phase="redistribution")

    df = create_features(df)

    generate_predictions(df)
