#!INSERT PYTHON PATH HERE
import sys
import os
# access the main codem repo
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
import codem.db_connect as db_connect


def show_model_status(model_version_id, db_conn="INSERT_DATABASE_NAME"):
    """
    Pulls the logs from the DB of a model run.
    """
    call = '''
    SELECT model_version_log_entry, date_inserted
    FROM cod.model_version_log WHERE model_version_id = {}
    ORDER BY date_inserted DESC
    '''.format(model_version_id)
    df = db_connect.query(call, db_conn)
    return df


if __name__ == "__main__":
    import argparse
    import time
    parser = argparse.ArgumentParser(description='Run CODEM centrally')
    parser.add_argument('-db', '--db_connection', type=str,
                        help='the db server to connect to', required=True)
    parser.add_argument('-m', '--model_version_id', type=int, required=True,
                        help='model version id number of codem run')

args = parser.parse_args()

print show_model_status(args.model_version_id, args.db_connection)
