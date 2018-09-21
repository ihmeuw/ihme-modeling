import logging
from copy import copy
import sys
import drill
from drill import Cascade, Cascade_loc
import pandas as pd
import multiprocessing as mp
import gc
import os
from jobmon import job

# Set dUSERt file mask to readable-for all users
os.umask(0o0002)


def run_loc(args):
    gc.collect()
    loc_id, sex_id, year, full_timespan, debug = args
    if debug:
        if full_timespan:
            cl = Cascade_loc(loc_id, sex_id, year, c, timespan=50,
                             parent_loc=cl_parent)
        else:
            cl = Cascade_loc(loc_id, sex_id, year, c, parent_loc=cl_parent)
        cl.run_dismod()
        cl.summarize_posterior()
        cl.draw()
        cl.predict()
        return loc_id, 0
    else:
        try:
            if full_timespan:
                cl = Cascade_loc(loc_id, sex_id, year, c, timespan=50,
                                 parent_loc=cl_parent)
            else:
                cl = Cascade_loc(loc_id, sex_id, year, c, parent_loc=cl_parent)
            cl.run_dismod()
            cl.summarize_posterior()
            cl.draw()
            cl.predict()
            return loc_id, 0
        except Exception as e:
            logging.exception("Failure running location {}".format(loc_id))
            return loc_id, str(e)


if __name__ == "__main__":

    mvid = int(sys.argv[1])
    location_id = int(sys.argv[2])
    sex = sys.argv[3]
    y = int(sys.argv[4])
    cv_iter = int(sys.argv[5])

    try:
        if sys.argv[6]=="debug":
            debug = True
        else:
            debug = False
    except:
        debug = False

    if sex=='male':
        sex_id = 0.5
    elif sex=='female':
        sex_id = -0.5

    c = Cascade(mvid, reimport=False, cv_iter=cv_iter)

    try:
        j = job.Job(os.path.normpath(os.path.join(c.root_dir, '..')))
        j.start()
    except IOError as e:
        logging.exception(e)
    except Exception as e:
        logging.exception(e)

    year_split_lvl = c.model_version_meta.fix_year.values[0]-1
    lt = c.loctree
    this_lvl = lt.get_nodelvl_by_id(location_id)
    if location_id == 1:
        cl_parent = Cascade_loc(location_id, 0, 2000, c, reimport=False)
    else:
        cl_parent = Cascade_loc(location_id, sex_id, y, c, reimport=False)
    num_children = len(lt.get_node_by_id(location_id).children)

    num_cpus = mp.cpu_count()

    if not debug:
        pool = mp.Pool(min(num_cpus, num_children, 10))

    # Run child locations
    arglist = []
    for child_loc in lt.get_node_by_id(location_id).children:
        if this_lvl>=(year_split_lvl-1):
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
        res =  pool.map(run_loc, arglist)
        pool.close()
        pool.join()

    errors = ['%s: %s' % (str(r[0]), r[1]) for r in res if r[1] != 0]

    try:
        if len(errors) == 0:
            j.finish()
        else:
            error_msg = "; ".join(errors)
            j.log_error(error_msg)
            j.failed()
    except NameError as e:
        logging.exception(e)
    except Exception as e:
        logging.exception(e)
