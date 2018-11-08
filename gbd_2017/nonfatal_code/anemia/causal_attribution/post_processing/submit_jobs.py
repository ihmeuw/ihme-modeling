"""
This part of the code is responsible to queueing jobs that need be run.
"""

import os
import pandas as pd
import subprocess


# define locals
username = USERNAME
anemia_cause = 'hiv' #either 'hiv' or 'pud' or 'gastritis'
out_dir = (FILEPATH).format(username=username, cause=anemia_cause)
year_list = [1990, 1995, 2000, 2005, 2010, 2017]

# load in the info table, set up directories, and create year_list to
# parallelize across
work_dir = os.path.dirname(os.path.realpath(__file__))
info_df = pd.read_csv(work_dir + '/info_table_{}.csv'.format(anemia_cause))
new_me_id_list = info_df['proportion_me'].tolist()


if not os.path.exists(out_dir):
    os.makedirs(out_dir)
for new_me_id in new_me_id_list:
    dir = '{0}/{1}'.format(out_dir, new_me_id)
    if not os.path.exists(dir):
        os.makedirs(dir)

year_string = [str(i) for i in year_list]
unlisted_years = " ".join(year_string)

# submit compute job for each me_id and form a string of job names
job_string = ''
for year in year_list:
    job_name = "make_{0}_props_{1}".format(anemia_cause, year)
    job_string = job_string + ',' + job_name
    call = ('qsub -cwd -P proj_anemia -N {0} -pe multi_slot 4'
            ' -l mem_free=8'
            ' -o FILEPATH'
            ' -e FILEPATH cluster_shell.sh'
            ' compute_props.py {1} {2} {4}'.format(job_name, year, out_dir,
                                                   username, anemia_cause))
    print(call)
    subprocess.call(call, shell=True)

# submit save result jobs after compute jobs finished
for new_me_id in new_me_id_list:
    save_dir = '{0}/{1}'.format(out_dir, new_me_id)

    call = ('qsub  -hold_jid {0} -cwd -P proj_anemia'
            ' -pe multi_slot 35'
            ' -l mem_free=70'
            ' -o FILEPATH'
            ' -e FILEPATH -N save_{1}'
            ' cluster_shell.sh save.py {1} {2} {4}'.format(job_string, new_me_id,
                                                       save_dir, username, unlisted_years))
    print(call)
    subprocess.call(call, shell=True)


