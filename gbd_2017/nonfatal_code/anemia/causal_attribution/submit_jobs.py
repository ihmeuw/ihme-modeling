from jobmon import sge
from time import sleep
from glob import glob
import pandas as pd
from db_queries import get_location_metadata

locs = get_location_metadata(location_set_id=35, gbd_round_id=5)
locs = locs[locs['most_detailed']==1]
locs = locs.location_id.unique().tolist()

ages = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235]

outdir = 'FILEPATH'


ndraws = 5
max_iters = 3
sex_id_list = [1, 2]
year_id_list = [1990, 1995, 2000, 2005, 2010, 2017]
consecutive_small_changes = 6
small_change = 1e-6
scale_factor = 1000.
data_dir = FILEPATH
h5_dir = FILEPATH

#malaria pre_processing jobs are submitted via malaria_pre_master.py in the malaria_pre_process subdirectory

def submit_ca():
    runfile = (
        "FILEPATH/"
        "analysis.py")
    ijs = incomplete_jobs()
    for loc in locs:
        any_submitted = False
        for a in ages:
            if "%s/%s_%s.h5" % (outdir, loc, a) in ijs:
                any_submitted = True
                sge.qsub(
                        runfile,
                        'anem_%s' % loc,
                        parameters=[loc, a, ndraws, max_iters, sex_id_list,
                                    year_id_list, consecutive_small_changes,
                                    small_change, scale_factor, data_dir,
                                    h5_dir, outdir],
                        conda_env='anemia_17_v5',
                        slots=7,
                        memory=14,
                        project='proj_anemia',
                        stdout="FILEPATH",
                        stderr="FILEPATH")
        if any_submitted:
            sleep(5)


def submit_upload_prep():
    runfile = (
        "FILEPATH/"
        "uploads/write_standard_files_uc.py")
    for loc in locs:
        sge.qsub(
                runfile,
                'anem_wsf_%s' % loc,
                parameters=[loc],
                conda_env='anemia_17_v5',
                project='proj_anemia',
                slots=4,
                memory=8,
                stdout="FILEPATH",
                stderr="FILEPATH")

#Saving files prepped for upload happens in a different script - use save_results_master.py


def submit_cf():
    runfile = (
        "FILEPATH"
        "calculate_cf.py")
    for y in year_id_list:
        for s in sex_id_list:
            for a in ages:
                sge.qsub(
                    runfile,
                    'cf_%s_%s_%s' % (y, s, a),
                    parameters=[y, s, a],
                    conda_env='anemia_17_v5',
                    project='proj_anemia',
                    jobtype='python',
                    slots=1,
                    memory=2,
                    stdout="FILEPATH",
                    stderr="FILEPATH")



def incomplete_jobs():
    files = glob("%s/*.h5" % outdir)
    rls = locs
    ras = ages
    reqd = ["%s/%s_%s.h5" % (outdir, l, a) for l in rls for a in ras]
    missing = set(reqd) - set(files)
    print("Missing %s files..." % len(missing))
    return missing

