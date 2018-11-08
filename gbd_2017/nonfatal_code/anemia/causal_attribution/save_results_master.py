import pandas as pd
import subprocess


meids = pd.read_excel("FILEPATH/in_out_meid_map.xlsx", "out_meids")
meids = meids.filter(like='modelable_entity').values.flatten()
meids = meids.tolist()


for meid in meids:
    job_name = "ca_sv_{}".format(meid)
    call = ('qsub -l mem_free=70.0G -pe multi_slot 35'
                    ' -cwd -P proj_anemia -o'
                    ' FILEPATH'
                    ' -e FILEPATH -N {0}'
                    ' cluster_shell.sh'
                    ' save_custom.py'
                    ' {1}'.format(job_name, str(int(meid))))
    subprocess.call(call, shell=True)