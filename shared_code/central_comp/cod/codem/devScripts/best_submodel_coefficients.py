import sys
import pandas as pd
import numpy as np
import os
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.Ensemble import Ensemble
from codem.Ensemble import load_codem_model
pd.set_option('chained', None)


def get_top_submodel_covariates(model_version_id, db_connection):
    """
    Get the submodel with the best OOS PV aand return the coefficient
    estimates of the model.

    :param model_version_id: int
        Valid model version id to pull results from
    :return: data frame
        Data frame with coefficient information
    """
    codem_model = load_codem_model(model_version_id, db_connection)
    df_rank = codem_model.submodel_rank()
    submodel_id = df_rank["submodel_version_id"][0]
    submodel_class = codem_model.submodel_components(submodel_id)
    df_submodel = submodel_class.fix_eff.copy()
    df_submodel.reset_index(inplace=True)
    df_submodel["model_version_id"] = model_version_id
    df_submodel["submodel_version_id"] = submodel_id
    df_submodel["std_error"] = np.diag(submodel_class.vcov)**.5
    return df_submodel

if __name__ == "__main__":
    save_location = sys.argv[1]  # first argument is the location where you want to save
    db_connection = sys.argv[2]  # next argument is the database server to connect to
    model_version_id_list = sys.argv[3:]  # all other arguments are the model version ids
    df = pd.DataFrame()
    for mvi in model_version_id_list:
        df = df.append(get_top_submodel_covariates(mvi, db_connection))
    df.to_csv(save_location, index=False)
