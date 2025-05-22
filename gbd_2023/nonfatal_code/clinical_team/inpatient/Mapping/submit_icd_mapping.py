import datetime
import getpass
import glob
import os
import platform
import subprocess

import pandas as pd
from crosscutting_functions import general_purpose
from crosscutting_functions.mapping import clinical_mapping_db

if platform.system() == "Linux":
    root = FILEPATH
else:
    root = FILEPATH

# load our functions
user = getpass.getuser()
repo = FILEPATH.format(user)


def clear_logs():
    root = FILEPATH
    errors = "{}/errors/".format(root)
    outputs = "{}/output/".format(root)

    log_type = [errors, outputs]
    logs_all = []
    for e in log_type:
        var = "e"
        if e.endswith("output/"):
            var = "o"
        file_a = glob.glob(e + "*.{}*".format(var))
        file_b = glob.glob(e + "*.p{}*".format(var))

        logs_all = file_a + file_b + logs_all

    for f in logs_all:
        os.remove(f)


def sbatcher(sources, deaths, run_id, write_log, map_version):
    """
    Send out all the actual mapping jobs

    Make 3 tiers, lower tier is 4s, 8g (under 45 megs)
    mid tier is 6s, 18g (45 to 120 megs)
    high tier is 10s, 40g (over 120 megs)
    """

    clear_logs()

    ppath = FILEPATH{run_id}FILEPATH/icd_mapping.p"
    p = pd.read_pickle(ppath)

    repo = FILEPATH.format(user)
    base = FILEPATH.format(
        run_id
    )

    for source in sources:
        # this is just used to get the size of the file for resource allocation
        data_path = glob.glob("{b}/{s}.H5".format(b=base, s=source))
        assert len(data_path) == 1, "resource allocation will break"
        size = os.path.getsize(data_path[0])

        mem = 8
        if size > 100000000:
            mem = size / 10000000
        # cap at 250G
        mem = int(min(mem, 250))

        # log directory
        log_path = FILEPATH{run_id}FILEPATH/icd_mapping"

        sbatch = (ADDRESS,
            "{r}FILEPATH"
            "{r}FILEPATH/worker_icd_mapping.py {source} {death} {log} {run} {mapv}".format(
                source=source,
                threads=1,
                memory=mem,
                r=repo,
                run=run_id,
                death=deaths,
                log=str(write_log),
                mapv=map_version,
                que=p.que,
                log_path=log_path,
            )
        )

        subprocess.call(sbatch, shell=True)
    return


def mc_reader(source):
    """
    reading hdf files in parallel is kind of finicky so we're not using this at
    the moment
    """
    fpath = "FILEPATH{}.H5".format(source)
    dat = pd.read_hdf(fpath, key="df", format="table")
    return dat


def icd_mapping(
    write_log=False,
    create_en_matrix_data=True,
    save_results=True,
    en_proportions=True,
    extra_name="",
    deaths="non",
    run_id=None,
    map_version=None,
):
    """
    Created on Fri Nov 18 14:49:50 2016
    Modified on Tues Nov 21 2017

    Submit worker jobs to map from ICD codes to ICGs
    Can write a log that holds the following information: All locations in data,
    all sources in data, ICD codes that did not match anything in the maps,
    the number & percent of data that did not match something in the maps.

    If an ICD code doesn't match anything in map, then the ICD code is
    truncated, and another attempt is made to map it.  This continues until the
    ICD code is successfully mapped or can't be truncated any more (3 digits is
    the minimum valid length).

    Parameters:
        write_log (bool): switch that determines if you want to write a log
        create_en_matrix_data: If true, sends out a master script that creates
            the data inputs for the EN matrix. Takes a few hours to run.
        save_results (bool): If true, saves the mapped data
        en_proportions (bool): If true, _and_ id save_results is true, then
            sbatch a script that makes EN proportions.  As it is now,
            save_results must also be True for this to run.  If you don't want
            to wait for icd_mapping to run again, you can sbatch the script and
            tell it what day to use data from. These are aka the inj correction factors
        extra_name (str): suffixes onto the end of the filepath before the date
            Use this to differentiate a run of icd_mapping from our standard process
            ex. we need to include deaths for making CFRs so we can input modified
            data to icd mapping and append the filename to
            "icd_mapping_with_deaths_[date]"
        deaths: (str): Gets passed to the worker script.  If you makes it
            'deaths' then the worker will use deaths only data.  If it is
            _any_ other string then it won't do that.
        run_id: (int): Indicates the run number of clinical data

    @authors: USERNAME
    """

    repo = FILEPATH

    # if we want to send out the master job. Takes a bool arg to delete
    # existing temp data and re-map or not
    if create_en_matrix_data:
        # log directory
        log_path = (
            f"FILEPATH{run_id}FILEPATH"
        )

        sbatch = (
            "ADDRESS",(
                repo=repo,
                run_id=run_id,
                mapv=map_version,
                log_path=log_path,
            )
        )
        print("Launching sbatch for EN Matrix")
        subprocess.call(sbatch, shell=True)

    if en_proportions and not save_results:
        raise AssertionError(
            "save_results must be True if you want "
            "en_proportions to be True. See docstring."
        )

    if deaths == "deaths":
        # glob the files just in deaths
        files = glob.glob(
            "FILEPATH/run_{}"
            "FILEPATH/*.H5".format(run_id)
        )
    else:
        # glob all the files in master data
        files = glob.glob(
            "FILEPATH/run_{}"
            "FILEPATH/*.H5".format(run_id)
        )

    # get all the source names
    sources = []
    for file in files:
        sources.append(os.path.basename(file)[:-3])

    # send out all the worker jobs
    sbatcher(
        sources=sources,
        deaths=deaths,
        run_id=run_id,
        write_log=write_log,
        map_version=map_version,
    )

    # wait then try to read data then wait then read...
    general_purpose.job_holder(job_name="icdm_", sleep_time=40, init_sleep=150)

    print("Reading in mapped sources...")

    # read in data by source
    df = []

    for source in sources:
        print("Appending source {}...".format(source))
        dat = pd.read_hdf(
            r"FILEPATH/run_{}"
            r"FILEPATH/{}.H5".format(run_id, source),
            key="df",
        )
        df.append(dat)
        del dat

    df = pd.concat(df, ignore_index=True)



    if save_results:
        print("Saving mapped data...")

        # map_version could be a string "current". This looks up the integer value.
        map_vers = clinical_mapping_db.test_and_return_map_version(map_version)

        today = datetime.datetime.today().strftime("%Y_%m_%d")

        write_path = (
            "FILEPATH/run_{r}"
            "FILEPATH"
            "FILEPATH.H5".format(
                r=run_id, v=map_vers, extra=extra_name, t=today
            )
        )

        df = general_purpose.downcast_clinical_cols(df, str_to_cats=True)
        general_purpose.write_hosp_file(df, write_path=write_path, backup=False)

        if en_proportions:
            # log directory
            log_path = (
                f"FILEPATH/run_{run_id}"
                "FILEPATH"
            )

            # build sbatch
            sbatch = (
                "ADDRESS",
                "{repo}FILEPATH"
                "{repo}FILEPATH".format(
                    repo=repo,
                    run_id=run_id,
                    log_path=log_path,
                )
            )

            print("Launching sbatch for EN Proportions")
            subprocess.call(sbatch, shell=True)

    return df


def to_bool(s):
    """
    Convert plain language true or false strings into python bools

    Params:
        s: (str) a string that can be naturally converted to a
                 python boolean value
    """
    if s in ["true", "True", "t", "T"]:
        result = True
    elif s in ["false", "False", "f", "F"]:
        result = False
    else:
        assert False, "the input is not acceptable"

    return result
