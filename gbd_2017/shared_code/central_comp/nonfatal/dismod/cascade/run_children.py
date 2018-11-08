import logging
import sys
from drill import Cascade, Cascade_loc
from setup_logger import setup_logger
import multiprocessing as mp
import gc
import os

# Set default file mask to readable-for all users
os.umask(0o0002)


def run_loc(args):
    '''Meant to be called in parallel using multiprocessing. Run
    dismod.

    Args:
        args(Tuple[int, int, int, Bool, Bool]): tuple of
            (location_id, sex_id, year_id, full_timespan, debug)

    Returns:
        Tuple of location_id and either a string error message or integer 0,
        representing no error
    '''
    gc.collect()
    loc_id, sex_id, year, full_timespan, debug = args
    if debug:
        if full_timespan:
            cl = Cascade_loc(loc_id, sex_id, year, cascade, timespan=50,
                             parent_loc=cl_parent)
        else:
            cl = Cascade_loc(loc_id, sex_id, year, cascade,
                             parent_loc=cl_parent)
        cl.run_dismod()
        cl.summarize_posterior()
        cl.draw()
        cl.predict()
        return loc_id, 0
    else:
        try:
            if full_timespan:
                cl = Cascade_loc(loc_id, sex_id, year, cascade, timespan=50,
                                 parent_loc=cl_parent)
            else:
                cl = Cascade_loc(loc_id, sex_id, year, cascade,
                                 parent_loc=cl_parent)
            cl.run_dismod()
            cl.summarize_posterior()
            cl.draw()
            cl.predict()
            return loc_id, 0
        except Exception as e:
            logging.exception("Failure running location {}".format(loc_id))
            return loc_id, str(e)


def main():
    '''Read command line arguments to run dismod for all child location ids of
    given location ids.

    Args:
        mvid(int): model version id
        location_id(int): parent location id
        sex(str): one of 'male'/'female'
        year_id(int): year id
        debug(str, optional): If specified and value == 'debug', will run
            in serial instead of in parallel
    '''
    mvid = int(sys.argv[1])
    location_id = int(sys.argv[2])
    sex = sys.argv[3]
    y = int(sys.argv[4])
    cv_iter = int(sys.argv[5])

    setup_logger()
    log = logging.getLogger(__name__)
    log.info(
        "Starting cascade mvid {} loc {} sex {} year {} cv_iter {}".format(
            mvid, location_id, sex, y, cv_iter))
    # The cascade and parent information are shared across all subprocesses.
    # Make it a global to avoid the memory overhead of passing a copy to
    # each process
    global cascade
    global cl_parent

    try:
        if sys.argv[6] == "debug":
            debug = True
        else:
            debug = False
    except:
        debug = False

    if sex == 'male':
        sex_id = 0.5
    elif sex == 'female':
        sex_id = -0.5

    log.info("Creating cascade")
    cascade = Cascade(mvid, reimport=False, cv_iter=cv_iter)
    log.info("Done with cascade")

    year_split_lvl = cascade.model_version_meta.fix_year.values[0] - 1
    lt = cascade.loctree
    this_lvl = lt.get_nodelvl_by_id(location_id)
    log.info("Generating cascade loc")
    if location_id == 1:
        cl_parent = Cascade_loc(location_id, 0, 2000, cascade, reimport=False)
    else:
        cl_parent = Cascade_loc(location_id, sex_id, y, cascade,
                                reimport=False)
    num_children = len(lt.get_node_by_id(location_id).children)
    log.info("Done generating cascade loc")

    num_cpus = mp.cpu_count()

    num_workers = min(num_cpus, num_children, 10)
    if not debug:
        pool = mp.Pool(num_workers)

    # Run child locations
    arglist = []
    for child_loc in lt.get_node_by_id(location_id).children:
        if this_lvl >= (year_split_lvl - 1):
            full_timespan = False
        else:
            full_timespan = True
        arglist.append((
            child_loc.id, sex_id, y,
            full_timespan, debug))

    if debug:
        '..... RUNNING IN SINGLE PROCESS DEBUG MODE .....'
        res = map(run_loc, arglist)
    else:
        log.info(
            "Running {} child locations in parallel with {} processes".format(
                len(arglist), num_workers))
        res = pool.map(run_loc, arglist)
        pool.close()
        pool.join()
        log.info("Done running")

    errors = ['%s: %s' % (str(r[0]), r[1]) for r in res if r[1] != 0]

    if len(errors) == 0:
        log.info("No errors found")
    else:
        num_errors = len(errors)
        error_msg = "; ".join(errors)
        log.error(
            "Found {} errors for mvid {} loc {} sex {} year {} cv_iter"
            "{}: {}".format(num_errors, mvid, location_id, sex, y, cv_iter,
                            error_msg))


if __name__ == '__main__':
    main()
