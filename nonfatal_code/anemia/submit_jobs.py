from hierarchies import dbtrees
from jobmon import sge
from time import sleep
from glob import glob
import pandas as pd
import os
from adding_machine.agg_locations import save_custom_results

lt = dbtrees.loctree(None, {LOCATION SET ID})
locs = [l.id for l in lt.leaves()]


ages = [{AGE GROUP IDS}]


outdir = '{FILEPATH}'

ndraws = 5
max_iters = 3
sex_id_list = [{SEX IDS}]
year_id_list = [{YEAR IDS}]
consecutive_small_changes = 6
small_change = 1e-6
scale_factor = 1000.
data_dir = '/{FILEPATH}/priors/'
h5_dir = '{FILEPATH}'



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
                        conda_env='anemia',
                        slots=6,
                        memory=12,
                        project='proj_anemia',
                        stdout="{STD OUTPUT FILEPATH}",
                        stderr="{STD ERROR FILEPATH}")
        if any_submitted:
            sleep(5)

def incomplete_jobs():
    files = glob("%s/*.h5" % outdir)
    rls = locs
    ras = ages
    reqd = ["%s/%s_%s.h5" % (outdir, l, a) for l in rls for a in ras]
    missing = set(reqd) - set(files)
    print("Missing %s files..." % len(missing))
    return missing

def submit_upload_prep():
    runfile = (
        "{FILEPATH}/"
        "uploads/write_standard_files.py")
    for loc in locs:
        sge.qsub(
                runfile,
                'anem_wsf_%s' % loc,
                parameters=[loc],
                conda_env='anemia',
                project='proj_anemia',
                slots=2,
                memory=4)

def submit_uncertainty():
    runfile = (
        "{FILEPATH}/"
        "add_uncertainty.py")
    meids = pd.read_excel("/{FILEPATH}/priors/in_out_meid_map.xlsx", "out_meids")
    meids = meids.filter(like='modelable_entity').values.flatten()
    for meid in meids:
        sge.qsub(
            runfile,
            'anem_un_%s' % meid,
            parameters = [meid, ndraws],
            conda_env='anemia',
            project='proj_anemia',
            jobtype='python',
            slots=40,
            memory=25,
            stdout="{STD OUTPUT FILEPATH}",
            stderr="{STD ERROR FILEPATH}")


def submit_save_results():
    runfile = (
        "/{FILEPATH}/"
        "save_custom.py")


    meids = pd.read_excel("/{FILEPATH}/priors/in_out_meid_map.xlsx", "out_meids")
    meids = meids.filter(like='modelable_entity').values.flatten()
    for meid in meids:

        sge.qsub(
                runfile,
                'anem_sr_%s' % meid,
                parameters=[meid],
                conda_env='anemia',
                project='proj_anemia',
                jobtype='python',
                slots=30,
                memory=20,
                stdout="{STD OUTPUT FILEPATH}",
                stderr="{STD ERROR FILEPATH}")


def submit_cf():
    runfile = (
        "/{FILEPATH}/"
        "calculate_cf.py")
    for y in year_id_list:
        for s in sex_id_list:
            for a in ages:
                sge.qsub(
                    runfile,
                    'cf_%s_%s_%s' % (y, s, a),
                    parameters=[y, s, a],
                    conda_env='anemia',
                    project='proj_anemia',
                    jobtype='python',
                    slots=1,
                    memory=2,
                    stdout="{STD OUTPUT FILEPATH}",
                    stderr="{STD ERROR FILEPATH}")


def submit_rf():
    runfile = (
        "/{FILEPATH}/"
        "uploads/nutrition_iron_exposure.py")
    ijs = rf_incomplete_jobs()
    for loc in locs:
        if "{FILEPATH}/exp_%s.csv" % loc in ijs:
            sge.qsub(
                    runfile,
                    'anem_rf_%s' % loc,
                    parameters=[loc],
                    conda_env='anemia',
                    project='proj_anemia',
                    jobtype='python',
                    slots=6,
                    memory=12,
                    stdout="{STD OUTPUT FILEPATH",
                    stderr="{STD ERROR FILEPATH}")


def rf_incomplete_jobs():
    files = glob("{FILEPATH}/*.csv")
    reqd = ["{FILEPATH}/exp_%s.csv" % l for l in locs]
    missing = set(reqd) - set(files)
    print("Missing %s files..." % len(missing))
    return missing



