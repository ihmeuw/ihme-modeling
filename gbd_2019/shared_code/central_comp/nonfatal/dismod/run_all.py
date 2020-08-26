import os
from functools import lru_cache
import pandas as pd

from cascade_ode.argument_parser import cascade_parser, inverse_parser
from cascade_ode.patch_io import setup_io_patches
from cascade_ode.settings import load as load_settings
from cascade_ode import db
from cascade_ode.setup_logger import setup_logger
from cascade_ode import sge

# Set default file mask to readable-for all users
os.umask(0o0002)

# Load settings from file
settings = load_settings(check_for_custom_conda=True)


@lru_cache()
def prepare_directories(mvid, create_directories=True):
    """Defines, creates, and returns directories for cascade outputs and logs

    Args:
        mvid (int): model_version_id associated with given cascade model run.
            Used to create directory paths.
        create_directories (bool): boolean to specify weather or not to create
            given directories. Specify 'False' to only return directory strings.
    """

    logdir = '%s/%s' % (settings['log_dir'], mvid)
    root_dir = '%s/%s' % (settings['cascade_ode_out_dir'], mvid)
    model_logdir = '%s/%s' % (root_dir, 'logs')

    if create_directories:
        for dir_string in (logdir, root_dir, model_logdir):
            create_dir(dir_string, mode=0o775)

    return {
        'logdir': logdir,
        'root_dir': root_dir,
        'model_logdir': model_logdir
    }


def submit_driver(
        mvid, project, dirs, extra_arguments=None, driver_arguments=None):
    """ Submit the cascade driver job. This job will run jobmon, which manages
    all subsequent cascade jobs.

    Args:
        mvid (str): model version ID
        project (str): The name of the proj, eg. ``proj_dismod``
        dirs (dict): Dictionary of directory locations.
        extra_arguments (List[str]): command-line arguments to add to
            every Jobmon job.
        driver_arguments (List[str]): command-line arguments just for
            the driver.py job that gets launched here.
    """
    logdir = dirs['logdir']
    gfile = os.path.join(settings['code_dir'], "driver.py")
    jobname = 'dm_%s_driver' % mvid
    slots, memory, _runtime = sge.cluster_limits('driver', mvm=None)
    extra_arguments = extra_arguments if extra_arguments else list()
    driver_arguments = driver_arguments if driver_arguments else list()

    sge.qsub_w_retry(
        gfile,
        jobname,
        jobtype='python',
        project=project,
        slots=slots,
        memory=memory,
        parameters=[mvid] + driver_arguments + extra_arguments,
        conda_env=settings['conda_env'],
        environment_variables=settings['env_variables'],
        prepend_to_path=os.path.join(settings['conda_root'], 'bin'),
        stderr='%s/%s.error' % (logdir, jobname))


def get_meid(mvid):
    ''' Given a model_version_id, return its modelable_entity_id. Uses
    ENVIRONMENT_NAME environment variable to determine which database to read
    from'''
    sett = load_settings()
    eng = db.get_engine(conn_def='epi',
                        env=sett['env_variables']['ENVIRONMENT_NAME'])
    meid = pd.read_sql("""
        SELECT modelable_entity_id FROM epi.model_version
        WHERE model_version_id = %s""" % mvid, eng)
    try:
        return meid.values[0][0]
    except IndexError:
        raise RuntimeError(f"No meid for {mvid} in environment "
                           f"{sett['env_variables']['ENVIRONMENT_NAME']} "
                           f"to host {eng.host}")


def parse_args(args=None):
    parser = cascade_parser("Launch dismod model")
    parser.add_argument("mvid", type=int, help="model version id")
    parser.add_argument(
        "--workflow",
        help=("unique identifier for workflow. Lets you rerun a model "
              "by giving it a new identifier for Jobmon. "
              "Jobmon won't let you run the same job twice so this "
              "makes it a different job, in Jobmon's eyes.")
    )
    return parser.parse_args(args)


def main():
    ''' Main entry point to launching a dismod model via Epi-Viz. Reads
    model_version_id from command line arguments, creates directories, and
    qsubs cascade job'''
    args = parse_args()
    try:
        default_debug_level = -1
        dirs = prepare_directories(args.mvid)
        setup_io_patches(args.no_upload)
        meid = get_meid(args.mvid)
        if meid in [9422, 7695, 1175, 10352, 9309]:
            project = "proj_tb"
        else:
            project = "proj_dismod"

        logging_filepath = '%s/%s' % (
            dirs['model_logdir'], f'{args.mvid}_run_all.log')
        setup_logger(
            logging_filepath,
            level=args.quiet - args.verbose + default_debug_level)

        add_arguments = inverse_parser(args)
        if args.workflow:
            driver_arguments = ["--workflow", args.workflow]
        else:
            driver_arguments = list()
        submit_driver(
            args.mvid, project, dirs, add_arguments, driver_arguments)
    except Exception:
        if args.pdb:
            # This invokes a live pdb debug session when an uncaught
            # exception makes it here.
            import pdb
            import traceback

            traceback.print_exc()
            pdb.post_mortem()
        else:
            raise


def create_dir(dir_string, mode):
    """
    Makes a directory at the given path with the specified filemode.

    Args:
        dir_string (str): New directory to create.
            (ex: '/path/to/dir/' where /path/to/ already exists)
        mode (str): User Mask permission string to assign given directory.
            (ex: '0o775')
    """
    try:
        os.makedirs(dir_string, mode=mode)
    except Exception:
        pass


if __name__ == '__main__':
    main()
