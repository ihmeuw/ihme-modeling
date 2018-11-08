import pandas as pd
import subprocess
import glob
import datetime
import argparse
from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles

def generate_outputs(inputs, single_out, multi_out):
    ##############################################################
    ## Begins formatting HALE data, and splits it into single
    ## and multi year dfs. Calls the format_outputs function
    ##############################################################
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
    start = [1990, 1990, 2007]
    end = [2017, 2007, 2017]
    zipped = list(zip(years, start, end))

    mult_df = pd.DataFrame()

    for year, start, end in zipped:
        year_df = multi_year.loc[multi_year['year_id'] == year]
        year_df['year_start_id'] = start
        year_df['year_end_id'] = end
        year_df = year_df.drop('year_id', axis = 1)
        mult_df = mult_df.append(year_df)
	
    format_outputs(mult_df, multi_out, ['year_end_id', 'year_start_id'])

def format_outputs(df, output, years):
    ##############################################################
    ## Continues formatting the single and multi dfs, and
    ## outputs them to CSVs
    ##############################################################	
    sorter = ['measure_id', 'location_id',
            'sex_id', 'age_group_id', 'metric_id']
    for col in years:
        sorter.insert(1, col)
    df = df.sort_values(sorter, axis=0)
    df = df.rename(columns = {'HALE_mean':'val',
                              'HALE_lower':'lower',
                              'HALE_upper':'upper'})
    order = ['measure_id', 'location_id', 'sex_id', 'age_group_id',
             'metric_id', 'val', 'upper', 'lower']
    for col in years:
        order.insert(1, col)
    df = df[order]			
    df = df.set_index('measure_id')
    df.to_csv(output)

def load_data(stagedir, conn, table_name):
    ##############################################################
    ## Loads data into specified db using db_tools Infiles
    ## object
    ##############################################################
    sesh = get_session(conn_def=conn)

    infiler = Infiles(table=table_name,
                      schema='gbd',
                      session=sesh)
	
    csv = '%s/%s.csv' % (stagedir, table_name)
    print('beginning infile')
    start_time = str(datetime.datetime.now().time())

    infiler.infile(path=csv, commit=True, with_replace=True)

    print('done with infiles at {} (started {})'.format(
            str(datetime.datetime.now().time()), start_time))
    return None

def run_upload(version, inputs, envr):
    ##############################################################
    ## Run upload process
    ##############################################################
    if envr == 'test':
        conn = 'gbd-test'
    elif envr == 'prod':
        conn = 'gbd'

    csv_params = ['chmod', '777', '%s' % inputs]
    print(csv_params)
    subprocess.check_output(csv_params)
			
    single = 'TABLE_NAME'
    multi = 'TABLE_NAME'
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

    touch_params = ['touch', '{}/upload_v{}_done'.format(inputs, version)]
    subprocess.check_output(touch_params)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    def_dir = "DIRECTORY"
    parser.add_argument("--hale_version",
                        help="version",
                        default=744,
                        type=int)
    parser.add_argument("--hale_dir",
                        help="inputs directory",
                        default=def_dir,
                        type=str)
    parser.add_argument("--envr",
                        help="environment",
                        default="test",
                        type=str)
    args = parser.parse_args()
    version = args.hale_version
    inputs = args.hale_dir
    envr = args.envr
	
    run_upload(version, inputs, envr)
			
