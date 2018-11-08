import pandas as pd
import os
from elmo import run
import glob
import argparse
from datetime import datetime
import re

def get_date_stamp():
    # get datestamp for output file
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:10]
    date = date_regex.sub('_', date_unformatted)
    return date

def upload_data(df, bundle, destination_file):
    if not df.empty:
        print destination_file
        df.to_excel(destination_file, index=False, sheet_name="extraction")
        validate = run.validate_input_sheet(bundle, destination_file, 
            "FILEPATH")
        assert (validate['status'].item()=='passed')
        report = run.upload_epi_data(bundle, destination_file)
        #assert nothing in the report is wrong
        assert (report['request_status'].item()=='Successful')


def upload_maternal_epi_bundle(in_dir, acause, bundle):
    to_upload_dir = ("FILEPATH".format(acause,bundle))
    if not os.path.exists(to_upload_dir):
        os.makedirs(to_upload_dir)
    to_upload = []
    for f in glob.glob(os.path.join(in_dir, "*.csv")):
        df = pd.read_csv(f)
        to_upload.append(df)
    upload_me = pd.concat(to_upload)

    # grab current data in bundle, delete it all, then upload new data
    # make sure that export=True so that a copy of the current data is saved
    # before modification
    
    # delete data
    epi = run.get_epi_data(bundle, export=True)
    epi = epi[['bundle_id','seq']]
    destination_file = '{}/delete_all_{}.xlsx'.format(to_upload_dir,get_date_stamp())
    upload_data(epi, bundle, destination_file)

    # upload new data
    destination_file = '{}/LBA_upload_{}.xlsx'.format(to_upload_dir,get_date_stamp())
    upload_data(upload_me, bundle, destination_file)


if __name__ == "__main__":
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("in_dir", help="directory where files are stored")
    parser.add_argument("acause", help="the acause of the bundle we are uploading")
    parser.add_argument("bundle", help="the bundle we are uploading",type=int)
    args = vars(parser.parse_args())

    upload_maternal_epi_bundle(in_dir=args["in_dir"], acause=args["acause"], bundle=args["bundle"])
