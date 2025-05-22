import shutil
import sys

from gbd import conn_defs
from jobmon.core.constants import WorkflowStatus

from hybridizer.joblaunch.HybridTask import HybridTask
from hybridizer.joblaunch.HybridWorkflow import HybridWorkflow


def main() -> None:
    """Create and run a jobmon workflow for a CODEm hybrid, main entry point for CoDViz."""
    user, global_model_version_id, datarich_model_version_id, db_host = sys.argv[1:5]
    user = str(user)
    global_model_version_id = int(global_model_version_id)
    datarich_model_version_id = int(datarich_model_version_id)
    db_host = str(db_host)

    if db_host == "SERVER":
        conn_def = conn_defs.CODEM
    elif db_host in ["SERVER", "SERVER"]:
        # CODV-1631: CoDViz uses old cod-db-t01 but may convert to cod-dev-db, support both
        conn_def = conn_defs.CODEM_TEST
    else:
        raise RuntimeError(
            f"Invalid database host {db_host} provided by CoDViz, please file a ticket "
            f"with Central Comp."
        )

    ht = HybridTask(
        user=user,
        global_model_version_id=global_model_version_id,
        datarich_model_version_id=datarich_model_version_id,
        conn_def=conn_def,
    )
    hybrid_wf = HybridWorkflow(
        name=f"cod_hybrid_{ht.model_version_id}",
        description=f"cod_hybrid_{ht.model_version_id}",
        model_version_id=ht.model_version_id,
        cause_id=ht.cause_id,
        acause=ht.acause,
        user=user,
    )
    wf = hybrid_wf.get_workflow()
    ht = ht.get_task()
    wf.add_task(ht)

    exit_status = wf.run()
    if exit_status != WorkflowStatus.DONE:
        raise RuntimeError(
            f"The hybridizer workflow failed, returning exit status {exit_status}, "
            f"see workflow logs {hybrid_wf.stderr}."
        )
    else:
        shutil.rmtree(hybrid_wf.stderr)


if __name__ == "__main__":
    main()
