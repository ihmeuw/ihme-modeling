from db_tools import dbapis, query_tools
import subprocess
import time

# create enginer
enginer = dbapis.engine_factory()

query = ('''
         SELECT
            *
         FROM
            cod.model_version
         WHERE
            cause_id = 366 AND model_version_type_id = 3 AND is_best = 1''')
while True:
    codem_df = query_tools.query_2_df(query,
                                      engine=enginer.engines["cod_prod"])
    if len(codem_df) == 1:
        break
    else:
        print "CODEm model still running"
        time.sleep(600)

call = ('qsub -cwd -P proj_custom_models -o FILEPATH '
        '-e FILEPATH -pe multi_slot 10 '
        '-N master_maternal cluster_shell.sh 00_master_maternal.py')
subprocess.call(call, shell=True)
