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

if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"


user = getpass.getuser()
for p in ["FILEPATH", 'Mapping']:
    prep_path = r"FILEPATH".format(user, p)
    sys.path.append(prep_path)
repo = r"FILEPATH".format(user)

import hosp_prep
import clinical_mapping

def clear_logs():
    root = "FILEPATH"
    errors = "FILEPATH".format(root)
    outputs = "FILEPATH".format(root)

    log_type = [errors, outputs]
    logs_all = []
    for e in log_type:
        var = 'e'
        if e.endswith("FILEPATH"):
            var = 'o'
        file_a = glob.glob(e + '*.{}*'.format(var))
        file_b = glob.glob(e + '*.p{}*'.format(var))

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

    repo = r"FILEPATH".format(user)
    base = r"FILEPATH".format(run_id)

    for source in sources:

        data_path = glob.glob("FILEPATH".format(b=base, s=source))

        assert len(data_path) == 1, "resource allocation will break"
        size = os.path.getsize(data_path[0])


        slots = 4
        mem = 8
        if size > 45000000:
            slots = 6
            mem = 18
        if size > 120000000:
            slots = 10
            mem = 40

        qsub = "qsub".format(source=source,
                    threads=int(4), memory=int(mem), r=repo,
                    run=run_id, death=deaths, log=str(write_log), mapv=map_version)

        subprocess.call(qsub, shell=True)
    return


def job_holder():
    """
    don't do anything while the icd_mapping jobs are running, wait until
    they're finished to proceed with the script. Checks qstat for jobs
    starting with "icdm_" and if any are found it waits 40 seconds before
    checking again
    """
    status = "wait"
    while status == "wait":
        print(status)
        p = subprocess.Popen("qstat", stdout=subprocess.PIPE)
        qstat_txt = p.communicate()[0]
        qstat_txt = qstat_txt.decode('utf-8')
        print("waiting...")
        pattern = 'icdm_+'

        found = re.search(pattern, qstat_txt)
        try:
            found.group(0)
            status = "wait"
            time.sleep(40)
        except:
            status = "go"
    print(status)
    print("All jobs left qstat.")
    return

def mc_reader(source):
    fpath = "FILEPATH".format(source)
    dat = pd.read_hdf(fpath, key='df', format='table')
    return dat


def icd_mapping(write_log=False,
                create_en_matrix_data=False,
                save_results=True,
                en_proportions=True,
                extra_name="",
                deaths="non",
                run_id=None,
                map_version=None):
    """
    Submit worker jobs to map from ICD codes to ICGs
    Can write a log that holds the following information: All locations in data,
    all sources in data, ICD codes that did not match anything in the maps,
    the number & percent of data that did not match something in the maps.

    If an ICD code doesn't match anything in MAPMASTER's map, then the ICD code is
    truncated, and another attempt is made to map it.  This continues until the
    ICD code is successfully mapped or can't be truncated any more (3 digits is
    the minimum valid length).

    Parameters:
        write_log (bool): switch that determines if you want to write a log
        create_en_matrix_data: If true, sends out a master script that creates
            the data inputs for the EN matrix. Takes a few hours to run.
        save_results (bool): If true, saves the mapped data
        en_proportions (bool): If true, _and_ id save_results is true, then
            qsubs a script that makes EN proportions.  As it is now,
            save_results must also be True for this to run.  If you don't want
            to wait for icd_mapping to run again, you can qsub the script and
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
    """

    repo = r"FILEPATH".format(user)



    if create_en_matrix_data:
        qsub = """
               qsub""".format(user=user, repo=repo, run_id=run_id)

        qsub = ' '.join(qsub.split())
        print("Launching qsub for EN Matrix")
        subprocess.call(qsub, shell=True)

    if en_proportions and not save_results:
        raise AssertionError("save_results must be True if you want "
                             "en_proportions to be True. See docstring.")


    if deaths == 'deaths':

        files = glob.glob("FILEPATH"\
                        "FILEPATH".format(run_id))
    else:

        files = glob.glob("FILEPATH"
                          "FILEPATH".format(run_id))


    sources = []
    for file in files:
        sources.append(os.path.basename(file)[:-3])


    qsubber(sources=sources, deaths=deaths, run_id=run_id, write_log=write_log, map_version=map_version)


    time.sleep(150)
    job_holder()

    print("Reading in mapped sources...")


    df = []

    for source in sources:
        print("Appending source {}...".format(source))
        dat = pd.read_hdf(r"FILEPATH"
                          r"FILEPATH".\
                        format(run_id, source), key='df')
        df.append(dat)
        del dat

    df = pd.concat(df, ignore_index=True)





    if save_results:
        print("Saving mapped data...")


        map_vers = clinical_mapping.test_and_return_map_version(map_version)

        today = datetime.datetime.today().strftime("%Y_%m_%d")

        write_path= ("FILEPATH"
                     "FILENAME"
                     "FILEPATH".
                     format(r=run_id,v=map_vers, extra=extra_name, t=today))

        hosp_prep.write_hosp_file(df,
                                  write_path=write_path,
                                  backup=False)

        if en_proportions:



            qsub = f"""
            qsub"""


            qsub = ' '.join(qsub.split())

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
    if s in ['true', 'True', 't', 'T']:
        result = True
    elif s in ['false', 'False', 'f', 'F']:
        result = False
    else:
        assert False, "the input is not acceptable"

    return result
