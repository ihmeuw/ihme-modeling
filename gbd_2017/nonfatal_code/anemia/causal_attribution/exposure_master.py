import subprocess

from db_queries import get_location_metadata

locs = get_location_metadata(location_set_id=35, gbd_round_id=5)
locs = locs[locs['most_detailed']==1]
locs = locs.location_id.unique().tolist()

for loc in locs:
    job_name = "iron_rf_{}".format(loc)
    call = ('qsub -l mem_free=10.0G -pe multi_slot 5'
                    ' -cwd -P proj_custom_models -o'
                    ' FILEPATH'
                    ' -e /FIELPATH -N {0}'
                    ' cluster_shell.sh'
                    ' exposures_wo_draws.py'
                    ' {1}'.format(job_name, str(int(loc))))
    subprocess.call(call, shell=True)