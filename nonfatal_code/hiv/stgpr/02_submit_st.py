import os
import pandas as pd
from cluster_utils import submitter
from st_gpr.hierarchies import dbtrees
import getpass

# Filepath settings
user = getpass.getuser()
if user == "":
    code_dir = FILEPATH
else: 
    code_dir = FILEPATH

location_set_version_id = 153
runfile = "%s/run_st.py" %code_dir
stdir = FILEPATH
paramdir = FILEPATH
params_file = "%s/params.csv" %paramdir

slots = 10
memory = 20

# Get model params
params = pd.read_csv(params_file)


for l in params.location_id.unique():

    if not os.path.isfile('%s/%s.csv' % (stdir, l)):
        lparams = params[params.location_id == l]
        lambdaa = lparams['lambda'].values[0]
        omega = lparams.omega.values[0]
        zeta = lparams.zeta.values[0]

        if l == 43:
            submitter.submit_job(
                    runfile,
                    'hivst%s' % l,
                    slots=slots,
                    memory=memory,
                    parameters=[l, lambdaa, omega, zeta],
                    project='proj_hiv',
                    stderr = FILEPATH)
        else: 
            submitter.submit_job(
                    runfile,
                    'hivst%s' % l,
                    slots=slots,
                    memory=memory,
                    parameters=[l, lambdaa, omega, zeta],
                    project='proj_hiv')
