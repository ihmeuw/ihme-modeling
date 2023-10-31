import shutil
import sys

from codem.joblaunch.CODEmWorkflow import CODEmWorkflow

from hybridizer.joblaunch.HybridTask import HybridTask


def main():
    user, global_model_version_id, \
            datarich_model_version_id, db_connection = sys.argv[1:5]

    if db_connection == 'ADDRESS':
        conn_def = 'codem'
    elif db_connection in ['ADDRESS', 'ADDRESS']:
        conn_def = 'codem-test'
    else:
        raise RuntimeError("Need to pass a valid database connection!")

    ht = HybridTask(user=user,
                    global_model_version_id=global_model_version_id,
                    datarich_model_version_id=datarich_model_version_id,
                    conn_def=conn_def)

    wf = CODEmWorkflow(name=f"cod_hybrid_{ht.model_version_id}",
                       description=f"cod_hybrid_{ht.model_version_id}",
                       reset_running_jobs=True,
                       resume=True)
    wf.add_task(ht)
    exit_status = wf.run()
    if exit_status:
        raise RuntimeError(
            f"The hybridizer workflow failed, returning exit status {exit_status}, "
            f"see workflow logs {wf.stderr}."
        )
    else:
        shutil.rmtree(wf.stderr)


if __name__ == '__main__':
    main()
