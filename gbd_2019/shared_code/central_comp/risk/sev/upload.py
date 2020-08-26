import sys
import glob
import pandas as pd
import os
import subprocess

from gbd_outputs_versions import GBDProcessVersion
from db_tools.loaders import Infiles
from db_tools.ezfuncs import get_session


# read in all summary files and save
def upload(sev_version_id, process_version_id, table_type):

    file_pattern = "FILEPATH/sev/{v}/summaries/{tt}_*.csv".format(v=sev_version_id, tt=table_type)
    data = []
    for f in glob.glob(file_pattern):
        data.append(pd.read_csv(f))
    data = pd.concat(data)
    data.fillna(0, inplace=True)
    id_cols = [c for c in data if c.endswith('_id')]
    data[id_cols] = data[id_cols].astype(int)
    table_name = "output_sev_{}_v{}".format(table_type, process_version_id.gbd_process_version_id)
    data_dir = "FILEPATH/sev/{v}/summaries/{tt}".format(v=sev_version_id, tt=table_type)
    os.mkdir(data_dir)

    if table_type == 'single_year':
        data = data[['measure_id', 'year_id', 'location_id', 'sex_id',
               'age_group_id', 'rei_id', 'metric_id', 'val',
               'upper', 'lower']].sort_values(
                   ['measure_id', 'year_id', 'location_id',
                    'sex_id', 'age_group_id', 'rei_id', 'metric_id'])
        year_ids = data['year_id'].drop_duplicates().tolist()
        for yr in year_ids:
            file = os.path.join(data_dir, 'single_year_{}.csv'.format(int(yr)))
            data.loc[data['year_id'] == yr].to_csv(file, index=False)
            os.chmod(file, 0o775)
    else:
        data = data[['measure_id', 'year_start_id',
                 'year_end_id', 'location_id', 'sex_id',
                 'age_group_id', 'rei_id', 'metric_id', 'val',
                 'upper', 'lower']].sort_values(
                     ['measure_id', 'year_start_id',
                      'year_end_id', 'location_id',
                      'sex_id', 'age_group_id', 'rei_id', 'metric_id'])
        year_ids = data[['year_start_id','year_end_id']].drop_duplicates().to_dict('split')['data']
        for yr in year_ids:
            file = os.path.join(data_dir, 'multi_year_{}_{}.csv'.format(int(yr[0]),int(yr[1])))
            data.loc[(data['year_start_id'] == yr[0]) & (data['year_end_id'] == yr[1])].to_csv(file, index=False)
            os.chmod(file, 0o775)

    sesh = get_session(conn_def='gbd', connectable=True)
    infiler = Infiles(table=table_name, schema='gbd', session=sesh)
    file_list = sorted(os.listdir(data_dir))
    for f in file_list:
        print(f)
        infiler.infile(os.path.join(data_dir, f), with_replace=False, commit=True)

# parse args
sev_version_id = int(sys.argv[1])
paf_version_id = int(sys.argv[2])
gbd_round_id = int(sys.argv[3])
decomp_step = sys.argv[4]

# set up process version id
git_hash = subprocess.check_output(['git', '--git-dir=FILEPATH/sev/.git',
    '--work-tree=FILEPATH', 'rev-parse', 'HEAD']).strip()
process_version_id = GBDProcessVersion.add_new_version(
   gbd_process_id=14,
   gbd_process_version_note='SEV v{}'.format(sev_version_id),
   code_version=git_hash,
   gbd_round_id=gbd_round_id,
   decomp_step=decomp_step,
   metadata={2: paf_version_id})

# upload
for table_type in ['single_year', 'multi_year']:
    upload(sev_version_id, process_version_id, table_type)
process_version_id.mark_best()
