import numpy as np

import codem.data.queryStrings as QS
from codem.reference import db_connect


def write_submodel(model_version_id, submodel_type_id, submodel_dep_id, weight,
                   rank, db_connection):
    """
    (int, int, int, float, int) -> int

    Write a submodel to the table and get the id back
    """
    call = QS.submodel_query_str.format(model_version_id, submodel_type_id,
                                        submodel_dep_id, weight, rank)
    db_connect.query(call, db_connection)
    call = QS.submodel_get_id.format(model_version_id, rank)
    submodel_id_df = db_connect.query(call, db_connection)
    submodel_id = submodel_id_df["submodel_version_id"][0]
    return submodel_id


def write_submodel_covariate(submodel_id, list_of_covariate_ids, db_connection):
    for cov in list_of_covariate_ids:
        call = QS.submodel_cov_write_str.format(submodel_id, cov)
        db_connect.query(call, db_connection)


def write_model_pv(tag, value, model_version_id, db_connection):
    call = QS.pv_write.format(tag, value, model_version_id)
    db_connect.query(call, db_connection)


def write_model_output(df_true, model_version_id, sex_id, db_connection):
    df = df_true.copy()
    df["sex_id"] = sex_id
    columns = ["draw_%d" % i for i in range(1000)]
    df[columns] = df[columns].values / df["envelope"].values[..., np.newaxis]
    df["mean_cf"] = df[columns].mean(axis=1)
    df["lower_cf"] = df[columns].quantile(.025, axis=1)
    df["upper_cf"] = df[columns].quantile(.975, axis=1)
    c = ["mean_cf", "lower_cf", "upper_cf", "year_id", "age_group_id", "sex_id", "location_id"]
    df = df[c]
    df["model_version_id"] = model_version_id
    db_connect.write_df_to_sql(df, db="cod", table="model",
                               connection=db_connection)


def truncate_draws(mat, percent=95):
    """
    :param mat:     array where rows correspond to observations and columns draw
    :param percent: a value between 0 and 100 corresponding to the amount of
                    data to keep
    :return:        array where row data outside row percentile has been
                    replaced with the mean.
    """
    assert 0 < percent < 100, "percent is out of range"
    low_bound = (100. - float(percent)) / 2.
    hi_bound = 100. - low_bound
    matrix = np.copy(mat)
    row_lower_bound = np.percentile(matrix, low_bound, axis=1)
    row_upper_bound = np.percentile(matrix, hi_bound, axis=1)
    replacements = ((matrix.T < row_lower_bound).T |
                    (matrix.T > row_upper_bound).T)
    replacements[matrix.std(axis=1) < 10**-5, :] = False
    masked_matrix = np.ma.masked_array(matrix, replacements)
    row_mean_masked = np.mean(masked_matrix, axis=1)
    row_replacements = np.where(replacements)[0]
    matrix[replacements] = row_mean_masked[row_replacements]
    return matrix
