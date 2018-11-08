
import pandas as pd
import subprocess

from db_queries import get_location_metadata

locs = get_location_metadata(location_set_id=22)
locs = locs.location_id.unique().tolist()

covid = 442
covname = 'vitadef_prev_agestd'
meid = 2510
measid = 5

for loc in locs:
    job_name = "covariate_{}".format(loc)
    call = ('qsub -l mem_free=10.0G -pe multi_slot 5'
                    ' -cwd -P PROJECT -o'
                    ' FILEPATH'
                    ' -e FILEPATH -N {0}'
                    ' FILEPATH'
                    ' dismod_to_cov.py'
                    ' {1} {2} {3} {4} {5}'.format(job_name,
                        str(int(loc)),
                        str(int(measid)),
                        str(int(meid)),
                        str(int(covid)),
                        str(covname)))
    subprocess.call(call, shell=True)