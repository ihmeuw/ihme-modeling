import os

from hierarchies.dbtrees import loctree as lt

from cascade_ode import sge
from cascade_ode.settings import load as load_settings
from cascade_ode.demographics import Demographics

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()
outdir = settings['cascade_ode_out_dir']


def submit_jobs(run_set, mvid, location_set_version_id):
    '''Submits run_children.py over a particular location_set_version_id
    starting at global and working downwards. Adds hold_jids appropriately.

    Args:
        run_set(List[Tuples[int, str, int]]): List of
            (location_id, sex, year_id). If a tuple is missing from run_set,
            that particular run_children.py job won't be launched
        mvid(int): model version id
        location_set_version_id(int): location set version id

    Returns:
        None
    '''
    demo = Demographics()
    loctree = lt(location_set_version_id=location_set_version_id,
                 gbd_round_id=demo.gbd_round_id)
    runfile = "%s/../bin/run_children.py" % this_path
    for sex in ['male', 'female']:
        for y in demo.year_ids:

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
                        jid = sge.qsub_w_retry(
                            runfile,
                            job_name,
                            holds=hold_ids,
                            slots=num_slots,
                            memory=num_slots * 2,
                            parameters=[mvid, location_id, sex, y])
                        jid = [jid]
                    else:
                        jid = []
                    for c in node.children:
                        dependent_submit(c.id, jid)

            dependent_submit(1, [])


def missing_files(mvid, location_set_version_id, cv_iter=0):
    '''Iterates down a location_set_version_id looking for missing
    post_pred_draws_summary.csv file.

    Args:
        mvid(int): model version id
        location_set_version_id(int): location set version id
        cv_iter(int, 0): Cross validation iteration. If not 0, this affects
            the path to the summary csv

    Returns:
        List[Tuple[int, str, int, int]]: List of tuples of
            (location_id, sex, year, cv_iter)
    '''
    demo = Demographics()
    loctree = lt(location_set_version_id=location_set_version_id,
                 gbd_round_id=demo.gbd_round_id)
    missing_files = []
    for loc in [l.id for l in loctree.nodes]:
        for year in demo.year_ids:
            for sex in ['male', 'female']:
                if loc == 1:
                    sex = 'both'
                    year = 2000
                if cv_iter == 0:
                    summ_file = (
                        'FILEPATH'
                        'post_pred_draws_summary.csv' % (outdir, mvid, loc,
                                                         sex, year))
                else:
                    summ_file = (
                        'FILEPATH'
                        'post_pred_draws_summary.csv' % (outdir, mvid, cv_iter,
                                                         loc, sex, year))
                if not os.path.isfile(summ_file):
                    missing_files.append((loc, sex, year, cv_iter))
    return missing_files


def reruns(mvid, location_set_version_id, launch=False, cv_iter=0):
    '''Looks for missing summary files for a model version, to determine
    which demographics for which run_children.py jobs have failed/need to be
    rerun.

    Args:
        mvid(int): model version id
        location_set_version_id(int): location set version id
        launch(bool, False): If True, relaunch jobs for each failed demographic
        cv_iter(int, 0): Cross validation iteration is passed to missing_files
            function to help determine summary csv path

    Returns:
        List[Tuple[int, str, int, int]]: List of tuples of
            (parent_location_id, sex, year, cv_iter)
    '''
    demo = Demographics()
    loctree = lt(location_set_version_id=location_set_version_id,
                 gbd_round_id=demo.gbd_round_id)
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
    '''Return list of paths to all files of a model version ending in 'error'
    that have non-zero size'''
    from glob import glob
    import os

    logdir = '%s/%s' % (settings['log_dir'], mvid)
    fs = [f for f in glob('%s/*.error' % logdir) if os.stat(f).st_size > 0]
    return fs
