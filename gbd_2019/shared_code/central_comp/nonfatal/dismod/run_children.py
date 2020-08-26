import gc
import logging
import os
import sys

import concurrent.futures

from cascade_ode.argument_parser import cascade_parser
from cascade_ode.dag import check_error_msg_for_sigkill
from cascade_ode.distribute_children import distribute
from cascade_ode.drill import Cascade, Cascade_loc
from cascade_ode.patch_io import setup_io_patches
from cascade_ode.run_all import prepare_directories
from cascade_ode.setup_logger import setup_logger

# Set default file mask to readable-for all users
os.umask(0o0002)


def run_location(args):
    '''Meant to be called in parallel using multiprocessing. Run
    dismod.

    Args:
        args (tuple): The arguments are packed, but there are two,
            a proxy to a dictionary and this location ID.

    Returns:
        Tuple of location_id and either a string error message or integer 0,
        representing no error
    '''
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
                loc_id, sex_id, year, cascade, timespan=50,
                parent_loc=cl_parent,
                feature_flags=args)
        else:
            cl = Cascade_loc(
                loc_id, sex_id, year, cascade,
                parent_loc=cl_parent,
                feature_flags=args)
        cl.initialize()
        cl.run_dismod()
        cl.summarize_posterior()
        cl.draw()
        cl.predict()
        return loc_id, 0
    else:
        try:
            if full_timespan:
                cl = Cascade_loc(
                    loc_id, sex_id, year, cascade, timespan=50,
                    parent_loc=cl_parent,
                    feature_flags=args)
            else:
                cl = Cascade_loc(
                    loc_id, sex_id, year, cascade,
                    parent_loc=cl_parent,
                    feature_flags=args)
            cl.initialize()
            cl.run_dismod()
            cl.summarize_posterior()
            cl.draw()
            cl.predict()
            return loc_id, 0
        except Exception as e:
            logging.exception("Failure running location {}".format(loc_id))
            return loc_id, str(e)


def parse_args(args=None):
    parser = cascade_parser("Launch initial global dismod model")
    parser.add_argument("mvid", type=int, help="model version ID")
    parser.add_argument("location_id", type=int, help="parent location ID")
    parser.add_argument("sex", type=str, help="male or female population")
    parser.add_argument("year_id", type=int, help="year for population")
    parser.add_argument("cv_iter", type=int, help="cross-validation")
    parser.add_argument("debug", type=str, nargs="?",
                        help=("If this is specified, and it is the "
                              "string 'debug', this code runs in serial "
                              "instead of in parallel.")
                        )
    return parser.parse_args(args)


def main():
    '''Read command line arguments to run dismod for all child location ids of
    given location ids.
    '''
    args = parse_args()
    mvid = args.mvid
    location_id = args.location_id
    sex = args.sex
    y = args.year_id
    cv_iter = args.cv_iter
    if args.debug not in {None, "debug"}:
        raise AttributeError(
            f"Debug flag should be off or 'debug' but is {args.debug}.")

    dirs = prepare_directories(mvid, create_directories=False)
    logging_filepath = '%s/%s' % (
            dirs['model_logdir'],
            f'{mvid}_{location_id}_{sex}_{y}_{cv_iter}_child.log')
    setup_logger(
        logging_filepath, level=args.quiet - args.verbose)
    log = logging.getLogger(__name__)
    log.info(
        "Starting cascade mvid {} loc {} sex {} year {} cv_iter {}".format(
            mvid, location_id, sex, y, cv_iter))

    setup_io_patches(args.no_upload)

    sex_dict = {'male': 0.5, 'female': -0.5}
    sex_id = sex_dict[sex]

    log.info("Creating cascade")
    cascade = Cascade(mvid, reimport=False, cv_iter=cv_iter,
                      feature_flags=args)
    log.info("Done with cascade")

    year_split_lvl = cascade.model_version_meta.fix_year.values[0] - 1
    lt = cascade.loctree
    this_lvl = lt.get_nodelvl_by_id(location_id)
    log.info("Generating cascade loc")
    if location_id == 1:
        cl_parent = Cascade_loc(
            location_id, 0, 2000, cascade, reimport=False, feature_flags=args)
    else:
        cl_parent = Cascade_loc(
            location_id, sex_id, y, cascade, reimport=False,
            feature_flags=args)
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
        '..... RUNNING IN SINGLE PROCESS DEBUG MODE .....'
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

    try:
        errors = ['%s: %s' % (str(r[0]), r[1]) for r in res if r[1] != 0]
    except concurrent.futures.process.BrokenProcessPool:
        log.error((
            "Process pool died abruptly. Assuming sigkill due to OOM killer."
            " Returning exit code 137 for jobmon resource retry"))
        sys.exit(137)

    if len(errors) == 0:
        log.info("No errors found")
    else:
        num_errors = len(errors)
        error_msg = "; ".join(errors)
        log.error(
            "Found {} errors for mvid {} loc {} sex {} year {} cv_iter"
            "{}: {}".format(num_errors, mvid, location_id, sex, y, cv_iter,
                            error_msg))
        if check_error_msg_for_sigkill(error_msg):
            log.error(
                '"Signals.SIGKILL: 9" found in error_msg. Returning exit code '
                ' 137 for jobmon resource retry.')
            sys.exit(137)
        else:
            raise RuntimeError('Dismod kernel failures.')


if __name__ == '__main__':
    main()
