import os
import json

from hierarchies.dbtrees import loctree as lt
from jobmon import sge

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
if os.path.isfile(os.path.join(this_path, "../config.local")):
    settings = json.load(open(os.path.join(this_path, "../config.local")))
else:
    settings = json.load(open(os.path.join(this_path, "../config.dUSERt")))
outdir = settings['cascade_ode_out_dir']


def submit_jobs(run_set, mvid, location_set_version_id):
    loctree = lt(location_set_version_id)
    runfile = "%s/../bin/run_children.py" % this_path
    for sex in ['male', 'female']:
        for y in [1990, 1995, 2000, 2005, 2010, 2016]:

            def dependent_submit(location_id, hold_ids):
                node = loctree.get_node_by_id(location_id)
                num_children = len(node.children)
                if num_children == 0:
                    return 0
                else:
                    if (location_id, sex, y) in run_set:
                        job_name = "casc_%s_%s_%s" % (location_id,
                                                      sex[0],
                                                      str(y)[2:])
                        num_slots = min(8, num_children)
                        jid = sge.qsub(
                            runfile,
                            job_name,
                            holds=hold_ids,
                            slots=num_slots,
                            memory=num_slots*2,
                            parameters=[mvid, location_id, sex, y])
                        jid = [jid]
                    else:
                        jid = []
                    for c in node.children:
                        dependent_submit(c.id, jid)

            dependent_submit(1, [])


def missing_files(mvid, location_set_version_id, cv_iter=0):
    loctree = lt(location_set_version_id)
    missing_files = []
    for loc in [l.id for l in loctree.nodes]:
        for year in [1990, 1995, 2000, 2005, 2010, 2016]:
            for sex in ['male', 'female']:
                if loc == 1:
                    sex = 'both'
                    year = 2000
                if cv_iter == 0:
                    summ_file = (
                        '%s/%s/full/locations/%s/outputs/%s/%s/'
                        'post_pred_draws_summary.csv' % (outdir, mvid, loc,
                                                         sex, year))
                else:
                    summ_file = (
                        '%s/%s/cv%s/locations/%s/outputs/%s/%s/'
                        'post_pred_draws_summary.csv' % (outdir, mvid, cv_iter,
                                                         loc, sex, year))
                if not os.path.isfile(summ_file):
                    missing_files.append((loc, sex, year, cv_iter))
    return missing_files


def reruns(mvid, location_set_version_id, launch=False, cv_iter=0):
    loctree = lt(location_set_version_id)
    mfs = missing_files(mvid, location_set_version_id, cv_iter)
    to_rerun = []
    for mf in mfs:
        loc, sex, year, cv_iter = mf
        parent_loc = loctree.get_node_by_id(loc).parent
        if parent_loc is not None:
            parent_loc_id = parent_loc.id
            to_rerun.append((parent_loc_id, sex, year, cv_iter))
    to_rerun = list(set(to_rerun))

    if launch:
        submit_jobs(to_rerun, mvid, location_set_version_id)
    return to_rerun


def find_logs(mvid):
    from glob import glob
    import os

    logdir = '%s/%s' % (settings['log_dir'], mvid)
    fs = [f for f in glob('%s/*.error' % logdir) if os.stat(f).st_size > 0]
    return fs
