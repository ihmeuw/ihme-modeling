import pandas as pd
import subprocess

meids = [2484, 2488, 2501, 2505]


for meid in meids:
    job_name = "hw_save_{}".format(meid)
    call = ('qsub -l mem_free=70.0G -pe multi_slot 35'
                    ' -cwd -P proj_anemia -o'
                    ' FILEPATH'
                    ' -e FILEPATH -N {0}'
                    ' cluster_shell.sh'
                    ' save_parallel.py'
                    ' {1}'.format(job_name, str(int(meid))))
    subprocess.call(call, shell=True)