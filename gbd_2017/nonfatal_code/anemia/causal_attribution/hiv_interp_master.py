import pandas as pd
import subprocess

from db_queries import get_location_metadata

locs = get_location_metadata(location_set_id=35)
locs = locs[locs['most_detailed']==1]
locs = locs.location_id.unique().tolist()



job_string = ''

#First submit the interpolation jobs
for loc in locs:
    job_name = "hiv_interp_{}".format(loc)
    job_string = job_string + ',' + job_name
    call = ('qsub -l mem_free=8.0G -pe multi_slot 4'
                    ' -cwd -P proj_anemia -o'
                    ' FILEPATH'
                    ' -e FILEPATH -N {0}'
                    ' cluster_shell.sh'
                    ' hiv_interp.py'
                    ' {1}'.format(job_name, str(int(loc))))
    print(call)
    subprocess.call(call, shell=True)

#Once the interpolations finish, save results
for meid in [16317, 16318, 16319, 16320]:
    call = ('qsub  -hold_jid {0} -cwd -P proj_anemia'
        ' -pe multi_slot 40'
        ' -l mem_free=80'
        ' -o FILEPATH'
        ' -e FILEPATH -N save_{1}'
        ' cluster_shell.sh interp_save.py {1}'.format(job_string, meid))
    print(call)
    subprocess.call(call, shell=True)