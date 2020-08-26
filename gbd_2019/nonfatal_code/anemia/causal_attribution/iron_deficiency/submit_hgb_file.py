import pandas as pd
import subprocess

job_name = "hgb_file_1"
call = ('qsub -l m_mem_free=120G -l fthread=15, -l h_rt=03:00:00, -q all.q'
                    ' -cwd -P proj_anemia -o'
		    ' -N {0} FILEPATH'.format(job_name)
subprocess.call(call, shell=True)
