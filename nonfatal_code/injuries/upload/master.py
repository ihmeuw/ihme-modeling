######################################################################
## Purpose: Master Script to submit injuries-como custom aggregations
##			for incidence and prevalence. Will submit a job array with
##			a predefined number of tasks, or the full set of tasks for
##			production. Set this in the tester flag below.
######################################################################

# parallel settings
tester = False

# import packages
import pandas as pd
import numpy as np
import os
import sys
import errno
import db_queries

# set filepaths and parameters
root_j_dir = "FILEPATH"
root_tmp_dir = "FILEPATH"
date = "DATE"
code_dir = "FILEPATH"
in_dir = os.path.join(root_j_dir, "FILEPATH")
out_dir = os.path.join("FILEPATH")
ndraws = 1000

# get demographics to loop over
demographics = pd.read_csv("FILEPATH")

# define the number of tasks to submit. if tester, then will only submit 2 tasks
# if production mode, will submit all tasks from the demographics
if tester:
	tasks = 10000
else:
	tasks = len(demographics.index)

# define function to submit the array job
def submit_array_job(root_j_dir, root_tmp_dir, date, code_dir, in_dir, out_dir, ndraws):
	print "Submitting array"
	"""Submit the QSUB"""
	""" Base Settings for QSUB"""
	base = "qsub -cwd -P proj_injuries -N injuries_como_first10K -t 1:{N} -j y -pe multi_slot {slots} {shell} {script} ".format(
		N = tasks,
		slots = 7,
		script = "FILEPATH.py",
		shell = "FILEPATH.sh"
		)
	""" Additional Arguments to Pass to Child Script"""
	args = "{root_j_dir} {root_tmp_dir} {date} {code_dir} {in_dir} {out_dir} {ndraws}".format(
		root_j_dir = root_j_dir,
		root_tmp_dir = root_tmp_dir,
		date = date,
		code_dir = code_dir,
		in_dir = in_dir,
		out_dir = out_dir,
		ndraws = ndraws)
	os.popen(base + args)

# GO!
submit_array_job(root_j_dir, root_tmp_dir, date, code_dir, in_dir, out_dir, ndraws)

# end of script