from elmo import run
import pandas as pd
import numpy as np
import os
from db_tools import ezfuncs as ez
import argparse



def create_path(row):
    path = "FILEPATH".format(c=row.acause,b=row.bundle_id,r=row.request_id) 
    return path


def get_path(bundle_id, temp_dir):
    conn_def = 'epi'
    '''Pulls the request_id for the bundle_id that was downloaded most recently'''
    query = ('SELECT b1.request_id, b1.bundle_id, b1.request_type_id, '
                'b1.request_status_id, b1.date_inserted, b2.cause_id, c1.acause '
                'FROM {DATABASE} b1 '
                'INNER JOIN '
                '{DATABASE} b2 on b2.bundle_id=b1.bundle_id '
                'INNER JOIN '
                '{DATABASE} c1 on c1.cause_id=b2.cause_id '
                'WHERE b1.date_inserted=( '
                'SELECT max(date_inserted) as max_date '
                'FROM {DATABASE} b3 '
                'WHERE b1.bundle_id={} and b3.bundle_id=b1.bundle_id and '
                'request_type_id=2 and request_status_id=5);'.format(bundle_id))
    seq_df = ez.query(query=query, conn_def=conn_def)
    seq_df['path'] = seq_df.apply(create_path, axis=1)

    seq_df.to_csv(os.path.join(temp_dir, 
        "download_path_bundle_{}.csv".format(bundle_id)), index=False, 
        encoding='utf-8')


def download_data(bundle_id, temp_dir):
    '''If export=True, an excel file will be saved in the bundle's download
    folder with the current state of the bundle data'''
    df = run.get_epi_data(bundle_id, export=True)
    if not df.empty: get_path(bundle_id, temp_dir)


if __name__ == "__main__":
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_id", help="which bundle to use", type=int)
    parser.add_argument("temp_dir", help="directory to save stuff")
    args = vars(parser.parse_args())

    download_data(bundle_id=args["bundle_id"], temp_dir=args["temp_dir"])
