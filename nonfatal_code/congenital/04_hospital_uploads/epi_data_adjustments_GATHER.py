import pandas as pd
import numpy as np
import subprocess

#pull in congenital bundle/model/cause mapping file
map_file = "{FILEPATH}"
map_df = pd.read_excel(map_file, header=0)
#drop null rows
map_df = map_df.loc[~map_df.fullmod_bundle.isnull()]
cause_bundle_pairs = list(zip(map_df.cause, map_df.fullmod_bundle))

for cause, bundle in cause_bundle_pairs:
    bundle = int(bundle)
    job_name = "epi_adjust_{b}_{c}".format(b=bundle, c=cause)
    call = ('qsub -l mem_free=6.0G -pe multi_slot 3'
                    ' -cwd -P proj_custom_models
                    ' -o {FILEPATH}'
                    ' -e {FILEPATH} -N {0}'
                    ' cluster_shell.sh'
                    ' v6_hospital_and_v3_marketscan_reupload.py'
                    ' {1} {2}'.format(job_name, str(int(bundle)), cause))
    subprocess.call(call, shell=True)
