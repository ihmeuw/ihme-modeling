import pandas as pd
import subprocess

meids = [2484, 2488, 2501, 2505]


for meid in meids:
    job_name = "hw_save_{}".format(meid)
    call = ('qsub -pe multi_slot 35'
                    ' FILEPATH -N {0}'
                    ' {1}'.format(job_name, str(int(meid))))
    subprocess.call(call, shell=True)
