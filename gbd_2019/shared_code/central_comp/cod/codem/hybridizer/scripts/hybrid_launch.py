import sys
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from hybridizer.joblaunch.HybridTask import HybridTask
from hybridizer import emails
from hybridizer.database import update_model_status, acause_from_id
from hybridizer.log_utilities import get_log_dir


def main():
    user, global_model_version_id, \
            developed_model_version_id, db_connection = sys.argv[1:5]

    if db_connection == 'ADDRESS':
        conn_def = 'ADDRESS'
    elif db_connection == 'ADDRESS':
        conn_def = 'ADDRESS'
    else:
        raise RuntimeError("Need to pass a valid database connection!")

    ht = HybridTask(user=user,
                    global_model_version_id=global_model_version_id,
                    developed_model_version_id=developed_model_version_id,
                    conn_def=conn_def)
    log_dir = get_log_dir(model_version_id=ht.model_version_id,
                          acause=acause_from_id(model_version_id=ht.model_version_id,
                                                conn_def=conn_def))
    wf = CODEmWorkflow(name="cod_hybrid_{}".format(ht.model_version_id),
                       description="codem_hybridizer_{}".format(ht.model_version_id),
                       reset_running_jobs=True,
                       resume=True)
    wf.add_task(ht)
    exit_status = wf.run()
    if exit_status:
        update_model_status(ht.model_version_id, 7, conn_def)
        emails.send_failed_email(ht.model_version_id, global_model_version_id,
                                 developed_model_version_id, user, log_dir,
                                 ht.gbd_round_id, conn_def)
    else:
        update_model_status(ht.model_version_id, 1, conn_def)
        emails.send_success_email(ht.model_version_id, global_model_version_id,
                                  developed_model_version_id, user,
                                  ht.gbd_round_id, conn_def)


if __name__ == '__main__':
    main()
