import sys
import glob
import pandas as pd
import os
import subprocess

from gbd_outputs_versions import GBDProcessVersion


# read in all summary files and save
def upload(sev_version_id, process_version_id, table_type):
    file_pattern = "FILEPATH/{v}/summaries/{tt}_*.csv".format(
        v=sev_version_id, tt=table_type)
    data = []
    for f in glob.glob(file_pattern):
        data.append(pd.read_csv(f))
    data = pd.concat(data)
    data.fillna(0, inplace=True)
    table_name = "output_sev_{}_v{}".format(
        table_type, process_version_id.gbd_process_version_id)
    data_dir = "FILEPATH/{v}/summaries/{tt}".format(
        v=sev_version_id, tt=table_type)
    os.mkdir(data_dir)
    process_version_id.save_for_upload(data, data_dir, table_name=table_name)
    process_version_id.upload_to_table(table_name)

# parse args
sev_version_id = int(sys.argv[1])
paf_version_id = int(sys.argv[2])
gbd_round_id = int(sys.argv[3])

# set up process version id
git_hash = subprocess.check_output(['git', '--git-dir=FILEPATH/sev/.git',
    '--work-tree=FILEPATH/sev/', 'rev-parse', 'HEAD']).strip()
process_version_id = GBDProcessVersion.add_new_version(
   gbd_process_id=14,
   gbd_process_version_note='SEV {}'.format(sev_version_id),
   code_version=git_hash,
   gbd_round_id=gbd_round_id,
   metadata={2: paf_version_id})

# upload
for table_type in ['single_year', 'multi_year']:
    upload(sev_version_id, process_version_id, table_type)
process_version_id.mark_best()
