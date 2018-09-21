import pandas as pd
import os
import subprocess
import glob
import datetime
import argparse
from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles

##############################################################
## Begins formatting HALE data, and splits it into single
## and multi year dfs. Calls the format_outputs function
##############################################################
def generate_outputs(inputs, single_out, multi_out):
    sums = glob.glob('%s/*summary.csv' % inputs)

    df = pd.DataFrame()

    for file_name in sums:
        add = pd.read_csv(file_name)
        df = df.append(add)

    df['measure_id'] = 28
    df['metric_id'] = 5

    single_year = df.loc[df['year_id'] < 3000]
    multi_year = df.loc[df['year_id'] > 3000]

    format_outputs(single_year, single_out, ['year_id'])

    years = [9999, 10000, 10001]
    start = [1990, 1990, 2005]
    end = [2016, 2005, 2016]
    zipped = zip(years, start, end)

    mult_df = pd.DataFrame()

    for year, start, end in zipped:
        year_df = multi_year.loc[multi_year['year_id'] == year]
        year_df['year_start_id'] = start
        year_df['year_end_id'] = end
        year_df = year_df.drop('year_id', axis = 1)
        mult_df = mult_df.append(year_df)

    format_outputs(mult_df, multi_out, ['year_end_id', 'year_start_id'])

##############################################################
## Continues formatting the single and multi dfs, and
## outputs them to CSVs
##############################################################
def format_outputs(df, output, years):
    sort = ['measure_id', 'location_id',
            'sex_id', 'age_group_id', 'metric_id']
    for col in years:
        sort.insert(1, col)
    df = df.sort(sort, axis=0)
    df = df.rename(columns = {'val_HALE':'val',
                              'lower_HALE':'lower',
                              'upper_HALE':'upper'})
    order = ['measure_id', 'location_id', 'sex_id', 'age_group_id',
             'metric_id', 'val', 'upper', 'lower']
    for col in years:
        order.insert(1, col)
    df = df[order]
    df = df.set_index('measure_id')
    df.to_csv(output)

##############################################################
## Loads data into specified db using db_tools Infiles
## object
##############################################################
def load_data(stagedir, conn, table_name):
    sesh = get_session(conn_def=conn)

    infiler = Infiles(table=table_name,
                      schema='gbd',
                      session=sesh)

    csv = '%s/%s.csv' % (stagedir, table_name)
    print('beginning infile')
    start_time = str(datetime.datetime.now().time())

    infiler.infile(path=csv, commit=True)

    print('done with infiles at {} (started {})'.format(
            str(datetime.datetime.now().time()), start_time))
    return None

##############################################################
## Run upload process
##############################################################

def run_upload(version, inputs, envr, root_dir):
    if envr == 'test':
        conn = 'gbd-test'
    elif envr == 'prod':
        conn = 'gbd'

    csv_params = ['chmod', '777', '%s' % inputs]
    print(csv_params)
    subprocess.check_output(csv_params)

    single = 'output_hale_single_year_v%s' % version
    multi = 'output_hale_multi_year_v%s' % version
    single_out = '%s/%s.csv' % (inputs, single)
    multi_out = '%s/%s.csv' % (inputs, multi)

    print(version, inputs, single, multi, single_out, multi_out)

    generate_outputs(inputs, single_out, multi_out)

    for out in [single_out, multi_out]:
        csv_params = ['chmod', '777', '%s' % out]
        print(csv_params)
        subprocess.check_output(csv_params)

    load_data(inputs, conn, single)
    load_data(inputs, conn, multi)

    #If upload was successful, clear the inputs dir
    params = glob.glob("%s/PATH/*" % root_dir)
    for f in params:
        os.remove(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    def_dir = "/PATH"
    base_dir = "/PATH"
    parser.add_argument("version",
                        help="version",
                        dUSERt=391,
                        type=int)
    parser.add_argument("inputs",
                        help="inputs",
                        dUSERt=def_dir,
                        type=str)
    parser.add_argument("environment",
                        help="environment",
                        dUSERt="test",
                        type=str)
    parser.add_argument("root_dir",
                        help="root directory",
                        dUSERt=base_dir,
                        type=str)
    args = parser.parse_args()
    version = args.version
    inputs = args.inputs
    envr = args.environment
    root_dir = args.root_dir

    run_upload(version, inputs, envr, root_dir)
