import subprocess
import logging
import os

from hierarchies.dbtrees import loctree
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus

from cascade_ode import __version__
from cascade_ode import upload
from cascade_ode import drill
from cascade_ode import sge
from cascade_ode.argument_parser import cascade_parser, inverse_parser
from cascade_ode.patch_io import setup_io_patches
from cascade_ode.importer import get_model_version
from cascade_ode.run_all import prepare_directories
from cascade_ode.setup_logger import setup_logger
from cascade_ode.settings import load as load_settings
from cascade_ode.demographics import Demographics
from cascade_ode.dag import make_dag

log = logging.getLogger(__name__)
log.info("driver started")

# Get configuration options
settings = load_settings()


class Driver(object):

    def __init__(self, mvid):
        '''
        Makes directories in preparation for launching cascade. Then launches
        jobmon to manage rest of cascade.

        Args:
            mvid (int): model version of dismod job
        '''
        self.mvid = mvid
        self.logdir = '{}/{}'.format(settings['log_dir'], self.mvid)
        self.mvm = get_model_version(mvid)
        if self.mvm.empty:
            raise RuntimeError(f"Model version for {mvid} is empty.")
        self.run_cv = (self.mvm.cross_validate_id.values[0] == 1)
        self.meid = self.mvm.modelable_entity_id.values[0]

        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.project = "proj_tb"
            self.is_tb = True
        else:
            self.project = "proj_dismod"
            self.is_tb = False

        try:
            os.makedirs(self.logdir)
        except Exception:
            pass
        try:
            os.chmod(self.logdir, 0o775)
        except Exception:
            pass

    def build_jobmon_workflow(self, identifier=None, extra_arguments=None):
        """
        Returns jobmon workflow that represents cascade job dag.

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

        demo = Demographics(self.mvid)
        lsvid = self.mvm.location_set_version_id.values[0]
        lt = loctree(
            location_set_id=demo.LOCATION_SET_ID,
            location_set_version_id=lsvid,
            gbd_round_id=demo.gbd_round_id)

        desc = self.mvm.description.values[0]

        jobdag = make_dag(
            mvid=self.mvid, loctree=lt, cv_iter=cv_iters,
            add_arguments=extra_arguments
        )

        env = settings['env_variables']['ENVIRONMENT_NAME']
        identifier = identifier if identifier else f"dismod_{self.mvid}_{env}"
        wf = Workflow(
            workflow_args=identifier,
            name=f"dismod_{self.mvid}_{env}",
            resume=True,
            description=desc,
            project=self.project,
            stderr=self.logdir,
            stdout=self.logdir,
            seconds_until_timeout=1210000)

        # since we're looping through the dict and mutating each JobNode
        # to contain a reference to a PythonTask, we require the jobdag dict
        # to be sorted such that we've already visited all upstream tasks of
        # any given node.
        for jobname, dagnode in jobdag.items():
            dagnode.add_job(wf, jobdag, self.mvm)

        return wf


def parse_args(args=None):
    parser = cascade_parser(
        "Launch/relaunch the dismod cascade from the global level"
    )
    parser.add_argument("mvid", type=int)
    parser.add_argument(
        "--workflow",
        help=("unique identifier for workflow. Lets you rerun a model "
              "by giving it a new identifier for Jobmon. "
              "Jobmon won't let you run the same job twice so this "
              "makes it a different job, in Jobmon's eyes.")
    )
    return parser.parse_args(args)


def main():
    '''
    Parses command line to start jobmon and run the dismod cascade.
    '''
    args = parse_args()
    info_level = 1
    dirs = prepare_directories(args.mvid, create_directories=False)
    logging_filepath = '%s/%s' % (
        dirs['model_logdir'], f'{args.mvid}_driver.log')
    setup_logger(
        logging_filepath, level=args.quiet - args.verbose + info_level)
    log = logging.getLogger(__name__)
    log.debug("main started")
    setup_io_patches(args.no_upload)

    mvid = args.mvid
    add_arguments = inverse_parser(args)

    wf_status = None

    try:
        upload.update_model_status(mvid, upload.RUNNING)
        try:
            commit_hash = sge.get_commit_hash(
                dir='%s/..' % drill.this_path)
        except subprocess.CalledProcessError:
            commit_hash = __version__
        upload.set_commit_hash(mvid, commit_hash)
        driver = Driver(mvid)
        wf = driver.build_jobmon_workflow(
            identifier=args.workflow, extra_arguments=add_arguments
        )
        wf_status = wf.execute()
    except Exception:
        log.exception("error in main driver with args {}".format(
            str(args)))
        upload.update_model_status(mvid, upload.FAILED)

    if wf_status != DagExecutionStatus.SUCCEEDED:
        upload.update_model_status(mvid, upload.FAILED)


if __name__ == '__main__':
    main()
