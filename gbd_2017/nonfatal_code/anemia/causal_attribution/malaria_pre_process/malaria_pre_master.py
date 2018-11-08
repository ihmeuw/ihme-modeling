import pandas as pd
import subprocess

from db_queries import get_location_metadata

locs = get_location_metadata(location_set_id=35)
locs = locs[locs['most_detailed']==1]
locs = locs.location_id.unique().tolist()

job_string = ''

#First submit the interpolation jobs
for loc in locs:
    job_name = "malaria_pre_{}".format(loc)
    job_string = job_string + ',' + job_name
    call = ('qsub -l mem_free=8.0G -pe multi_slot 4'
                    ' -cwd -P proj_anemia -o'
                    ' FILEPATH'
                    ' -e FILEPATH -N {0}'
                    ' cluster_shell.sh'
                    ' subtract_clinical_malaria.py'
                    ' {1}'.format(job_name, str(int(loc))))
    print(call)
    subprocess.call(call, shell=True)

#Once the new draws exist, save results
for meid in [19390, 19394]:
    call = ('qsub  -hold_jid {0} -cwd -P proj_anemia'
        ' -pe multi_slot 40'
        ' -l mem_free=80'
        ' -o FILEPATH'
        ' -e FILEPATH -N save_{1}'
        ' cluster_shell.sh malaria_save.py {1}'.format(job_string, meid))
    print(call)
    subprocess.call(call, shell=True)