# coding: utf-8
"""
Master script to send out each individual year for NZL

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


for year in range(2000, 2016, 1):
    m_mem_free = 20
    fthread = 8

    qsub = QSUB
    subprocess.call(qsub, shell=True)
