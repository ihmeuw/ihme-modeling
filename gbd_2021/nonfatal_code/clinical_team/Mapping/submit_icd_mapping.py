import pandas as pd
import platform
import datetime
import re
import getpass
import sys
import subprocess
import glob
import os
import time

from clinical_info.Functions import hosp_prep, data_structure_utils as dsu
from clinical_info.Mapping import clinical_mapping

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# load our functions
user = getpass.getuser()
repo = r"FILEPATH"


def clear_logs():
    root = "FILEPATH"
    errors = "FILEPATH"
    outputs = "FILEPATH"

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


def qsubber(sources, deaths, run_id, write_log, map_version):
    """
    Send out all the actual mapping jobs

    Make 3 tiers, lower tier is 4s, 8g (under 45 megs)
    mid tier is 6s, 18g (45 to 120 megs)
    high tier is 10s, 40g (over 120 megs)
    """

    clear_logs()

    ppath = f"FILEPATH"
    p = pd.read_pickle(ppath)

    repo = r"FILEPATH"
    base = r"FILEPATH"

    for source in sources:
        # this is just used to get the size of the file for resource allocation
        data_path = glob.glob("{b}/{s}.H5".format(b=base, s=source))
        assert len(data_path) == 1, "resource allocation will break"
        size = os.path.getsize(data_path[0])

        mem = 8
        if size > 45000000:
            mem = 18
        if size > 120000000:
            mem = 40

        qsub = "QSUB".format(
            source=source,
            threads=1,
            memory=int(mem),
            r=repo,
            run=run_id,
            death=deaths,
            log=str(write_log),
            mapv=map_version,
            que=p.que,
        )

        subprocess.call(qsub, shell=True)
    return


def mc_reader(source):
    """
    reading hdf files in parallel is kind of finicky so we're not using this at
    the moment
    """
    fpath = "FILEPATH"
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

    repo = r"FILEPATH"

    # if we want to send out the master job. Takes a bool arg to delete
    # existing temp data and re-map or not
    if create_en_matrix_data:
        qsub = "QSUB".format(user=user, repo=repo, run_id=run_id, mapv=map_version)
        # clean qsub
        qsub = " ".join(
            qsub.split()
        )  # split on whitspace and rejoin with one space where the splits occurred
        print("Launching qsub for EN Matrix")
        subprocess.call(qsub, shell=True)

    if en_proportions and not save_results:
        raise AssertionError(
            "save_results must be True if you want "
            "en_proportions to be True. See docstring."
        )

    if deaths == "deaths":
        # glob the files just in deaths
        files = glob.glob("FILEPATH")
    else:
        # glob all the files in master data
        files = glob.glob("FILEPATH")

    # get all the source names
    sources = []
    for file in files:
        sources.append(os.path.basename(file)[:-3])

    # send out all the worker jobs
    qsubber(
        sources=sources,
        deaths=deaths,
        run_id=run_id,
        write_log=write_log,
        map_version=map_version,
    )

    # wait then try to read data then wait then read...
    hosp_prep.job_holder(job_name="icdm_", sleep_time=40, init_sleep=150)

    print("Reading in mapped sources...")

    # read in data by source
    df = []

    for source in sources:
        print("Appending source {}...".format(source))
        dat = pd.read_hdf(r"FILEPATH", key="df",)
        df.append(dat)
        del dat

    df = pd.concat(df, ignore_index=True)

    if save_results:
        print("Saving mapped data...")

        # map_version could be a string "current". This looks up the integer value.
        map_vers = clinical_mapping.test_and_return_map_version(map_version)

        today = datetime.datetime.today().strftime("%Y_%m_%d")

        write_path = "FILEPATH"

        df = dsu.downcast_clinical_cols(df, str_to_cats=True)
        hosp_prep.write_hosp_file(df, write_path=write_path, backup=False)

        if en_proportions:

            # build qsub
            # used 100G according to qpid
            qsub = f"QSUB"

            # clean qsub
            qsub = " ".join(qsub.split())

            print("Launching qsub for EN Proportions")
            subprocess.call(qsub, shell=True)

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
