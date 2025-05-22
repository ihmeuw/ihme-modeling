import logging
import os
from argparse import ArgumentParser, Namespace

from hierarchies.dbtrees import loctree

from cascade_ode import __version__  # noqa: F401
from cascade_ode.legacy import constants, upload
from cascade_ode.legacy.argument_parser import cascade_parser, inverse_parser
from cascade_ode.legacy.constants import CLIArgs
from cascade_ode.legacy.dag import get_cascade_jobmon_tool, make_dag
from cascade_ode.legacy.db import execute_select
from cascade_ode.legacy.importer import get_model_version
from cascade_ode.legacy.patch_io import setup_io_patches
from cascade_ode.legacy.run_all import prepare_directories
from cascade_ode.legacy.settings import load as load_settings
from cascade_ode.legacy.setup_logger import setup_logger

log = logging.getLogger(__name__)
log.info("driver started")

# Get configuration options
settings = load_settings()


def get_user_from_model_version(mvid: int) -> str:
    """Given a model version id, return a username.

    Uses ENVIRONMENT_NAME environment variable to determine prod/dev db.
    """
    query = """
    SELECT inserted_by
    FROM epi.model_version mv
    WHERE model_version_id=:mvid
    LIMIT 1;
    """
    df = execute_select(query=query, params={"mvid": mvid})
    user = df["inserted_by"].iloc[0]

    return user


class Driver(object):
    def __init__(self, mvid):
        """
        Makes directories in preparation for launching cascade. Then launches
        Jobmon to manage rest of cascade.

        Args:
            mvid (int): model version of DisMod job
        """
        self.mvid = mvid
        self.logdir = 
        self.mvm = get_model_version(mvid)
        if self.mvm.empty:
            raise RuntimeError(f"Model version for {mvid} is empty.")
        self.run_cv = self.mvm.cross_validate_id.values[0] == 1
        self.meid = self.mvm.modelable_entity_id.values[0]

        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.project = "proj_tb"
            self.is_tb = True
        else:
            self.project = "proj_dismod"
            self.is_tb = False

        os.makedirs(self.logdir, exist_ok=True)
        try:
            os.chmod(self.logdir, 0o775)
        except Exception:
            pass

    def build_jobmon_workflow(self, identifier=None, extra_arguments=None):
        """
        Returns Jobmon workflow that represents cascade job dag.

        Args:
            identifier (str): A unique string to identify this workflow
                for JobMon. Running twice with the same string will restart
                a workflow.
            extra_arguments (List[str]): Command-line arguments to add to
                every UGE Job specified in Jobmon.
        Returns:
            jobmon.Workflow: With all Jobmon tasks created.
        """
        extra_arguments = extra_arguments if extra_arguments else list()
        cv_iters = None if not self.run_cv else list(range(11))

        lsvid = self.mvm.location_set_version_id.values[0]
        release_id = self.mvm.release_id.values[0]
        lt = loctree(location_set_version_id=lsvid, release_id=release_id)

        tool = get_cascade_jobmon_tool()

        jobdag = make_dag(
            mvid=self.mvid, loctree=lt, cv_iter=cv_iters, add_arguments=extra_arguments
        )

        env = settings["env_variables"]["ENVIRONMENT_NAME"]
        identifier = identifier if identifier else f"dismod_{self.mvid}_{env}"
        workflow_attributes = {
            "username": get_user_from_model_version(mvid=self.mvid),
            "model_version_id": self.mvid,
            "modelable_entity_id": self.meid,
        }
        wf = tool.create_workflow(
            workflow_args=identifier,
            name=f"dismod_{self.mvid}_{env}",
            description=self.mvm.description.values[0],
            default_cluster_name=constants.ClusterNames.SLURM,
            workflow_attributes=workflow_attributes,
        )
        wf.set_default_compute_resources_from_dict(
            cluster_name=constants.ClusterNames.SLURM,
            dictionary={
                "project": self.project,
                "stderr": self.logdir,
                "stdout": self.logdir,
            },
        )

        # since we're looping through the dict and mutating each JobNode
        # to contain a reference to a PythonTask, we require the jobdag dict
        # to be sorted such that we've already visited all upstream tasks of
        # any given node.
        for _, dagnode in jobdag.items():
            dagnode.add_job(wf, jobdag, self.mvm)

        return wf


def parse_args(args=None) -> Namespace:
    parser = get_arg_parser()
    return parser.parse_args(args=args)


def get_arg_parser() -> ArgumentParser:
    parser = cascade_parser(
        description="Launch/relaunch the DisMod cascade from the global level."
    )
    parser.add_argument(
        CLIArgs.MVID.FLAG, type=CLIArgs.MVID.TYPE, help=CLIArgs.MVID.DESCRIPTION
    )
    parser.add_argument(CLIArgs.WORKFLOW.FLAG, help=CLIArgs.WORKFLOW.DESCRIPTION)
    return parser


def main() -> None:
    """Parses command line to start Jobmon and run the DisMod cascade."""
    args = parse_args()
    info_level = 1
    dirs = prepare_directories(args.mvid, create_directories=False)
    logging_filepath = 
    setup_logger(logging_filepath, level=args.quiet - args.verbose + info_level)
    log = logging.getLogger(__name__)
    log.debug("main started")
    setup_io_patches(args.no_upload)

    mvid = args.mvid
    add_arguments = inverse_parser(args)

    try:
        upload.update_model_status(mvid, upload.RUNNING)
        upload.set_commit_hash(mvid)
        driver = Driver(mvid)
        wf = driver.build_jobmon_workflow(
            identifier=args.workflow, extra_arguments=add_arguments
        )
        wf_status = wf.run(resume=True, seconds_until_timeout=1210000)
    except Exception:
        wf_status = "uncaught_exception"
        log.exception("error in main driver with args {}".format(str(args)))
        upload.update_model_status(mvid, upload.FAILED)
        raise

    if wf_status != "D":
        upload.update_model_status(mvid, upload.FAILED)
        log.error(f"Workflow error, status: {wf_status}. Closing.")


if __name__ == "__main__":
    main()
