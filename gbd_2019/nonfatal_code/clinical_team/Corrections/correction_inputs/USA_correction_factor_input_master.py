# coding: utf-8
"""
Master script to send out each individual year and state to process HCUP

Note: if you want to make any major demographic changes to the input data
ie change the age groups or locations you'll need to look into the
/gbd2015_source_prep scripts
"""
import sys
import glob
import subprocess
import getpass

user = getpass.getuser()

run_id = sys.argv[1]
run_id = run_id.replace("\r", "")

# get a list of files to process
files = glob.glob(FILEPATH)

for f in files:
    location_id = f[-7:-4]  # loc id is stored at the end of each file
    if location_id == "527":
        print("this is cali, loc id {}".format(location_id))
        slots = 30
    else:
        print("not cali, loc id {}".format(location_id))
        slots = 15
    qsub = HCUP_QSUB
    subprocess.call(qsub, shell=True)
