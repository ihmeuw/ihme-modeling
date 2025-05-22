import concurrent.futures
import gc
import logging
import os
import sys
from argparse import ArgumentParser, Namespace

from cascade_ode.legacy.argument_parser import cascade_parser
from cascade_ode.legacy.constants import CLIArgs
from cascade_ode.legacy.dag import check_error_msg_for_sigkill
from cascade_ode.legacy.distribute_children import distribute
from cascade_ode.legacy.drill import Cascade, Cascade_loc
from cascade_ode.legacy.io import datastore
from cascade_ode.legacy.patch_io import setup_io_patches
from cascade_ode.legacy.run_all import prepare_directories
from cascade_ode.legacy.setup_logger import setup_logger

# Set default file mask to readable-for all users
os.umask(0o0002)


def run_location(args):
    """Meant to be called in parallel using multiprocessing. Run
    DisMod.

    Args:
        args (tuple): The arguments are packed, but there are two,
            a proxy to a dictionary and this location ID.

    Returns:
        Tuple of location_id and either a string error message or integer 0,
        representing no error
    """
    shared, loc_id = args
    gc.collect()
    sex_id = shared["sex_id"]
    year = shared["year"]
    full_timespan = shared["full_timespan"]
    args = shared["args"]
    cascade = shared["cascade"]
    cl_parent = shared["cl_parent"]

    # Each subprocess has its own imports, so patches need to be redone.
    setup_io_patches(args.no_upload)
    if args.debug:
        if full_timespan:
            cl = Cascade_loc(
                loc_id,
                sex_id,
                year,
                cascade,
                timespan=50,
                parent_loc=cl_parent,
                feature_flags=args,
            )
        else:
            cl = Cascade_loc(
                loc_id, sex_id, year, cascade, parent_loc=cl_parent, feature_flags=args
            )
        cl.initialize()
        cl.run_dismod()
        cl.summarize_posterior()
        cl.draw()
        cl.predict()
        return cl.datastore, 0
    else:
        try:
            if full_timespan:
                cl = Cascade_loc(
                    loc_id,
                    sex_id,
                    year,
                    cascade,
                    timespan=50,
                    parent_loc=cl_parent,
                    feature_flags=args,
                )
            else:
                cl = Cascade_loc(
                    loc_id, sex_id, year, cascade, parent_loc=cl_parent, feature_flags=args
                )
            cl.initialize()
            cl.run_dismod()
            cl.summarize_posterior()
            cl.draw()
            cl.predict()
            _die_if_orphaned()
            return cl.datastore, 0
        except Exception as e:
            logging.exception("Failure running location {}".format(loc_id))
            return loc_id, str(e)


def parse_args(args=None) -> Namespace:
    parser = get_arg_parser()
    return parser.parse_args(args=args)


def get_arg_parser() -> ArgumentParser:
    parser = cascade_parser(description="Launch initial global DisMod model.")
    parser.add_argument(
        CLIArgs.MVID.FLAG, type=CLIArgs.MVID.TYPE, help=CLIArgs.MVID.DESCRIPTION
    )
    parser.add_argument("location_id", type=int, help="parent location ID")
    parser.add_argument("sex", type=str, help="male or female population")
    parser.add_argument("year_id", type=int, help="year for population")
    parser.add_argument("cv_iter", type=int, help="cross-validation")
    parser.add_argument(
        "debug",
        type=str,
        nargs="?",
        help=(
            "If this is specified, and it is the "
            "string 'debug', this code runs in serial "
            "instead of in parallel."
        ),
    )

    return parser


def main() -> None:
    """Read command-line arguments to run DisMod for all child location IDs of
    given location IDs.
    """
    args = parse_args()
    mvid = args.mvid
    location_id = args.location_id
    sex = args.sex
    y = args.year_id
    cv_iter = args.cv_iter
    if args.debug not in {None, "debug"}:
        raise AttributeError(f"Debug flag should be off or 'debug' but is {args.debug}.")

    dirs = prepare_directories(mvid, create_directories=False)
    logging_filepath = 
    setup_logger(logging_filepath, level=args.quiet - args.verbose)
    log = logging.getLogger(__name__)
    log.info(
        "Starting cascade mvid {} loc {} sex {} year {} cv_iter {}".format(
            mvid, location_id, sex, y, cv_iter
        )
    )

    setup_io_patches(args.no_upload)

    sex_dict = {"male": 0.5, "female": -0.5}
    sex_id = sex_dict[sex]

    log.info("Creating cascade")
    cascade = Cascade(mvid, reimport=False, cv_iter=cv_iter, feature_flags=args)
    log.info("Done with cascade")

    year_split_lvl = cascade.model_version_meta.fix_year.values[0] - 1
    lt = cascade.loctree
    this_lvl = lt.get_nodelvl_by_id(location_id)
    log.info("Generating cascade loc")
    if location_id == 1:
        cl_parent = Cascade_loc(
            location_id, 0, 2000, cascade, reimport=False, feature_flags=args
        )
    else:
        cl_parent = Cascade_loc(
            location_id, sex_id, y, cascade, reimport=False, feature_flags=args
        )
    cl_parent.initialize()
    log.info("Done generating cascade loc")

    full_timespan = this_lvl < (year_split_lvl - 1)

    # Run child locations
    arglist = []
    for child_loc in lt.get_node_by_id(location_id).children:
        arglist.append(child_loc.id)

    shared_to_children = dict(
        cascade=cascade,
        cl_parent=cl_parent,
        sex_id=sex_id,
        year=y,
        full_timespan=full_timespan,
        args=args,
    )

    if args.debug:
        "..... RUNNING IN SINGLE PROCESS DEBUG MODE ....."
        try:
            res = [run_location((shared_to_children, work)) for work in arglist]
        except Exception:
            res = list()
            if args.pdb:
                # This invokes a live pdb debug session when an uncaught
                # exception makes it here.
                import pdb
                import traceback

                traceback.print_exc()
                pdb.post_mortem()
            else:
                raise
    else:
        res = distribute(run_location, shared_to_children, arglist)
        log.info("Done running")

    write_or_raise(res, cl_parent, mvid, location_id, sex, y, cv_iter)


def write_or_raise(results, cl_parent, mvid, location_id, sex, y, cv_iter) -> None:
    """Try to process results of parallel DisMod submodels. If any DisMod
    submodels failed, raise an error.

    We potentially exit with 137 exit code, to propagate jobmon failures.
    """
    log = logging.getLogger(__name__)

    errors = []
    ok = accumulate_errors(results, errors)
    try:
        datastore.save_datastores(ok, cl_parent.out_dir, batchsize=30_000)
    except concurrent.futures.process.BrokenProcessPool:
        log.error(
            (
                "Process pool died abruptly. Assuming sigkill due to OOM killer."
                " Returning exit code 137 for jobmon resource retry"
            )
        )
        sys.exit(137)
    except RuntimeError as e:
        if "StopIteration" in str(e):
            # process errors
            pass
        else:
            raise

    if len(errors) == 0:
        log.info("No errors found")
    else:
        num_errors = len(errors)
        error_msg = "; ".join(errors)
        log.error(
            f"Found {num_errors} errors for {CLIArgs.MVID.FLAG} {mvid} loc {location_id} "
            f"sex {sex} year {y} cv_iter {cv_iter}: {error_msg}"
        )
        if check_error_msg_for_sigkill(error_msg):
            log.error(
                '"Signals.SIGKILL: 9" found in error_msg. Returning exit code '
                " 137 for jobmon resource retry."
            )
            sys.exit(137)
        elif "Cannot allocate memory" in error_msg:
            log.error(
                "Submodel could not allocate memory. Returning exit code "
                "137 for jobmon resource retry."
            )
            sys.exit(137)
        else:
            raise RuntimeError("Dismod kernel failures.")


def accumulate_errors(results, errors):
    """A generator that yields successful submodels. Any failed models
    have their errors appended to an errors list and then stop. We fail fast
    to avoid waiting for all submodels to finish in the event of a failure.
    """
    for elem, err_msg in results:
        if err_msg:
            errors.append(f"{elem}: {err_msg}")
            raise StopIteration
        else:
            yield elem


def _die_if_orphaned():
    """
    If this job has exceeded memory, the parent process may be the one to
    recieve the sigkill signal. In linux, if the parent process is killed then
    the child process is reparented to the init process (1). This function
    checks if the parent process is 1, and if so it will exit. This avoids an
    issue where the subprocess worker tries to return data to the dead parent
    process, which causes a deadlock.
    """
    parent_process_id = os.getppid()
    if parent_process_id == 1:
        logging.exception("Orphaned child process detected; dying.")
        # Note that os._exit is important -- sys.exit will not
        # resolve our hanging issue because standard process cleanup
        # involves communicating with the parent process, which is
        # dead.
        os._exit(1)


if __name__ == "__main__":
    main()
