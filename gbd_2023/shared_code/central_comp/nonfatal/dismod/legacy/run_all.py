import os
from argparse import ArgumentParser, Namespace
from functools import lru_cache

import pandas as pd

from cascade_ode.legacy import cluster as legacy_cluster
from cascade_ode.legacy import db
from cascade_ode.legacy.argument_parser import cascade_parser, inverse_parser
from cascade_ode.legacy.constants import CLIArgs
from cascade_ode.legacy.patch_io import setup_io_patches
from cascade_ode.legacy.settings import load as load_settings
from cascade_ode.legacy.setup_logger import setup_logger
from cascade_ode.lib import cluster

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

    logdir = 
    root_dir = 
    model_logdir = 

    if create_directories:
        for dir_string in (logdir, root_dir, model_logdir):
            create_dir(dir_string, mode=0o775)

    return {"logdir": logdir, "root_dir": root_dir, "model_logdir": model_logdir}


def submit_driver(mvid, project, dirs, extra_arguments=None, driver_arguments=None):
    """Submit the cascade driver job. This job will run jobmon, which manages
    all subsequent cascade jobs.

    Args:
        mvid (str): model version ID
        project (str): The name of the proj, eg. ``proj_dismod``
        dirs (dict): Dictionary of directory locations.
        extra_arguments (List[str]): command-line arguments to add to
            every Jobmon job.
        driver_arguments (List[str]): command-line arguments just for
            the job that gets launched here.
    """
    logdir = dirs["logdir"]
    gfile = 
    jobname = "dm_%s_driver" % mvid
    slots, memory, _runtime = legacy_cluster.cluster_limits("driver", mvm=None)
    extra_arguments = extra_arguments if extra_arguments else list()
    driver_arguments = driver_arguments if driver_arguments else list()

    cluster.submit(
        gfile,
        jobname,
        account=project,
        partition="long.q",
        threads=slots,
        memory_gb=memory,
        runtime="384:00:00",
        parameters=[mvid] + driver_arguments + extra_arguments,
        conda_env=settings["conda_env"],
        environment_variables=settings["env_variables"],
        prepend_to_path=,
        stderr=logdir,
    )


def get_meid(mvid):
    """Given a model_version_id, return its modelable_entity_id. Uses
    ENVIRONMENT_NAME environment variable to determine which database to read
    from"""
    sett = load_settings()
    eng = db.get_engine(conn_def="epi", env=sett["env_variables"]["ENVIRONMENT_NAME"])
    meid = pd.read_sql(
        """
        SELECT modelable_entity_id FROM epi.model_version
        WHERE model_version_id = %s"""
        % mvid,
        eng,
    )
    try:
        return meid.values[0][0]
    except IndexError:
        raise RuntimeError(
            f"No meid for {mvid} in environment "
            f"{sett['env_variables']['ENVIRONMENT_NAME']} "
            f"to host {eng.host}"
        )


def parse_args(args=None) -> Namespace:
    parser = get_arg_parser()
    return parser.parse_args(args=args)


def get_arg_parser() -> ArgumentParser:
    parser = cascade_parser(description="Launch DisMod model.")
    parser.add_argument("mvid", type=int, help="model version ID")
    parser.add_argument(CLIArgs.WORKFLOW.FLAG, help=CLIArgs.WORKFLOW.DESCRIPTION)
    return parser


def main() -> None:
    """Main entry point to launching a DisMod model via Epi-Viz. Reads
    model_version_id from command-line arguments, creates directories, and
    submits cascade job."""
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

        logging_filepath = 
        setup_logger(logging_filepath, level=args.quiet - args.verbose + default_debug_level)

        add_arguments = inverse_parser(args)
        if args.workflow:
            driver_arguments = [CLIArgs.WORKFLOW.FLAG, args.workflow]
        else:
            driver_arguments = list()
        submit_driver(args.mvid, project, dirs, add_arguments, driver_arguments)
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
        mode (str): User Mask permission string to assign given directory.
            (ex: '0o775')
    """
    try:
        os.makedirs(dir_string, mode=mode)
    except Exception:
        pass


if __name__ == "__main__":
    main()
