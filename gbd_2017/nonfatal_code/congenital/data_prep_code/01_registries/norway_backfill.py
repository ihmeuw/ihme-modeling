import argparse 
import pandas as pd
import numpy as np
import os
from elmo.run import validate_input_sheet, upload_epi_data
from datetime import datetime
import re
from ihme_dimensions import dimensionality, gbdize
import sys

reload(sys)
sys.setdefaultencoding('utf8')

def get_date_stamp():
    # get datestamp for output file
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:10]
    date = date_regex.sub('_', date_unformatted)
    return date

def get_eurocat_nids():
    #list of NIDs that correspond  to EUROCAT data
    eurocat_nids = [159930, 159937, 128835 , 159941, 163937, 159924, 159938, 
        159926, 159927, 159928, 159929, 159931, 159932, 159933, 159934, 159935, 
        159936, 159939, 159940, 159942, 163938, 163939, 159925, 128835]
    return eurocat_nids

def upload(bundle_id, destination_file, error_path):
    validate = validate_input_sheet(bundle_id, destination_file, error_path)
    assert (validate['status'].item()=='passed')
    status_df = upload_epi_data(bundle_id, destination_file)
    assert (status_df['request_status'].item()=='Successful')
    return status_df

def pop_weight(df, population):
    #pop weight 
    keeps = ['location_id','age_start','sex','year_start','pop_weight']
    # merging on year_start, Norway EUROCAT data was extracted as single years
    weighted = pd.merge(df, population[keeps], on=[x for x in keeps if x != 'pop_weight'], 
        how='left', indicator=True)
    assert (weighted._merge=='both').all()
    weighted.drop('_merge', axis=1, inplace=True)
    for value in ['cases', 'effective_sample_size', 'sample_size']:
        weighted.loc[:,'{}'.format(value)] = weighted['{}'.format(value)] * weighted['pop_weight']
    return weighted

def backfill(df, norway_id, code_dir, loc_meta):
    #backfill
    data_cols = ['cases', 'effective_sample_size', 'sample_size']
    data_dct = {'data_cols': data_cols}
    index_cols = list(set(df.columns) - set(data_cols))
    index_cols.remove('location_id')
    norway_subs = loc_meta.loc[loc_meta.parent_id==norway_id, 'location_id'].tolist()
    index_dct = {
        tuple(index_cols): list(set(
            tuple(x) for x in df[index_cols].values)),
        'location_id': norway_subs
        }
    gbdizer = gbdize.GBDizeDataFrame(
        dimensionality.DataFrameDimensions(index_dct, data_dct))
    backfilled = gbdizer.fill_location_from_nearest_parent(df, location_set_id=35, 
        gbd_round_id=5)
    return backfilled

def run(bundle_id, cause_name, code_dir, fname):
    latest_downloads = pd.read_csv(os.path.join(code_dir, fname))
    filepath = latest_downloads.loc[latest_downloads.bundle_id==bundle_id,'path'].item()
    df = pd.read_excel(filepath, sheet_name='extraction')
    
    norway_id = 90
    eurocat_nids = get_eurocat_nids()
    eurocat = df.loc[(df.nid.isin(eurocat_nids))|(df.underlying_nid.isin(eurocat_nids)),:]
    eurocat = eurocat.loc[eurocat.location_id==norway_id,:]
    loc_meta = pd.read_csv(os.path.join(code_dir, 'location_metadata.csv'))
    population = pd.read_csv(os.path.join(code_dir, 'norway_population.csv'))

    backfilled = backfill(eurocat, norway_id, code_dir, loc_meta)
    weighted = pop_weight(backfilled, population)
    
    #clean up for upload
    clean = weighted.loc[weighted.location_id!=norway_id,:]
    clean.loc[:, 'seq'] = np.nan
    clean = pd.merge(clean,loc_meta[['location_id', 'location_name']], 
        on='location_id', suffixes=('', '_y'))
    clean.loc[:, 'location_name'] = clean['location_name_y']
    clean.drop('location_name_y', axis=1, inplace=True)
    outdir = "FILEPATH"
    destination_file = os.path.join(outdir,
        'norway_eurocat_backfill_{}_{}.xlsx'.format(bundle_id,get_date_stamp()))
    # make error_path two directories up from the download file
    error_path = os.path.dirname(os.path.dirname(os.path.dirname(filepath)))
    clean.to_excel(destination_file, sheet_name="extraction", index=False, 
        encoding='utf8')
    upload(bundle_id, destination_file, error_path)

#############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_id", 
        help="the bundle we're going to upload to", type=int)
    parser.add_argument("cause_name", 
        help="the acause associated with the bundle we're working with")
    parser.add_argument("code_dir", 
        help="directory where csv containing the paths to all the downloaded bundle files lives")
    parser.add_argument("fname", help="name of final csv file")
    args = vars(parser.parse_args())
    
    # call function
    run(bundle_id=args['bundle_id'], cause_name=args['cause_name'], 
        code_dir=args['code_dir'], fname=args['fname'])