import sqlalchemy as sql
import pandas as pd
import sys, os
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.db_connect import query

def best_models(db_connection):
    """
    Pull the best models from the cod database.
    """
    call = '''
        SELECT
        cause_id, locations_exclude, model_version_type_id, sex_id,
        age_start, age_end, pv_rmse_in, pv_rmse_out, pv_trend_in,
        pv_trend_out, pv_coverage_in, pv_coverage_out
        FROM
        cod.model_version
        WHERE
        model_version_id IN (
            SELECT
                child_id # the children of hybrids
            FROM
                cod.model_version_relation
            WHERE parent_id IN (SELECT
                                    model_version_id
                                FROM
                                    cod.model_version
                                WHERE
                                    model_version_type_id=3 # hybrid model
                                    AND is_best=1 # is best
                                    AND date_inserted > "2015-6-11" # past date
                                )
        )
        OR
        model_version_id IN (SELECT model_version_id
                             FROM cod.model_version
                             WHERE
                             model_version_type_id!=3 # not hybrid model
                             AND is_best=1 # is best
                             AND date_inserted > "2015-6-11" # past date
        )
    '''
    data = query(call, db_connection)
    return data


def cause_list(db_connection):
    """
    Pull the cause list from the database.
    """
    call = '''
        SELECT cause_id, cause_name FROM shared.cause;
    '''
    data = query(call, db_connection)
    data = data.rename(columns={"cause_name": "Cause"})
    return data


def age_list(db_connection):
    """
    Pull the age list from the database.
    """
    call = '''
        SELECT age_group_id, age_group_alternative_name as age_name
        FROM shared.age_group;
    '''
    data = query(call, db_connection)
    return data


def codem_oos_pv_table(db_connection):
    """
    Create codem tabel based off the template found in the 2013 paper.
    """
    df = best_models(db_connection)
    df = df[df.pv_rmse_in.notnull()]
    df = df.merge(cause_list(), on=["cause_id"], how="left")
    rename_start = {"age_group_id": "age_start", "age_name":"Age Start"}
    rename_end = {"age_group_id": "age_end", "age_name":"Age End"}
    df = df.merge(age_list(db_connection).rename(columns=rename_start),
                                                on=["age_start"], how="left")
    df = df.merge(age_list(db_connection).rename(columns=rename_end),
                                                on=["age_end"], how="left")
    df["Sex"] = df.sex_id.map(lambda x: "Male" if x == 1 else "Female")
    df["Regions"] = df.model_version_type_id.map(lambda x: " [Data Rich]" if x == 2 else "")
    df["is_africa"] = df.locations_exclude.map(lambda x: "4 31 " in x) & (df.cause_id == 345)
    df["not_africa"] = (df.is_africa != True) & (df.cause_id == 345)
    df["malaria_note1"] = df.is_africa.map(lambda x: " [Africa]" if x else "")
    df["malaria_note2"] = df.not_africa.map(lambda x: " [Non-Africa]" if x else "")
    df["Cause"] = df["Cause"] + df["Regions"] + df["malaria_note1"] + df["malaria_note2"]
    pv_rename = {"pv_rmse_in": "RMSE In", "pv_rmse_out": "RMSE out", "pv_trend_in": "Trend In",
                 "pv_trend_out": "Trend Out", "pv_coverage_in": "Coverage In",
                 "pv_coverage_out": "Coverage Out"}
    df = df.rename(columns=pv_rename)
    df.sort_values(by=["cause_id", "Sex", "Cause"], inplace=True)
    keep = ["Cause", "Sex", "Age Start", "Age End", "RMSE In", "RMSE out", "Trend In",
            "Trend Out", "Coverage In", "Coverage Out"]
    df.reset_index(drop=True, inplace=True)
    return df[keep]


if __name__ == "__main__":
    db_connection = sys.argv[1]
    f = "INSERT_PATH_TO_TABLE"
    df = codem_oos_pv_table(db_connection)
    df.to_csv(f, index=False)
