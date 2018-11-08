import pandas as pd
import numpy as np
import platform
import datetime
import re
import warnings
import getpass
import sys
import subprocess
import glob
import os
import time
import multiprocessing

if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

prep_path = r"FILEPATH"
sys.path.append(prep_path)
repo = r"FILEPATH"

import hosp_prep

def qsubber(sources, deaths, write_log):
    """
    Send out all the actual mapping jobs

    Make 3 tiers, lower tier is 4s, 8g (under 45 megs)
    mid tier is 6s, 18g (45 to 120 megs)
    high tier is 10s, 40g (over 120 megs)
    """
    repo = r"FILEPATH"

    for source in sources:
        # this is just used to get the size of the file for resource allocation
        data_path = glob.glob("FILEPATH")
        assert len(data_path) == 1, "resource allocation will break"
        size = os.path.getsize(data_path[0])

        # set slots and mem based on filesize
        slots = 4
        mem = 8
        if size > 45000000:
            slots = 6
            mem = 18
        if size > 120000000:
            slots = 10
            mem = 40

        qsub =  "qsub -P proj_hospital -N icdm_" + source +\
                " -pe multi_slot " +str(slots)+ " -l mem_free=" + str(mem) +\
                "g -o FILEPATH" +  "/output -e FILEPATH" + "/errors " +\
                repo + "/python_shell_env.sh " + repo +\
                "/worker_icd_mapping.py" + " " + source + " " +\
                deaths + " " + str(write_log)

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
        print("waiting...")
        pattern = 'icdm_+'
        
        found = re.search(pattern, qstat_txt)
        try:
            found.group(0)  # if the unc jobs are out this should work
            status = "wait"
            time.sleep(40)  # wait 40 seconds before checking again
        except:
            status = "go"
    print(status)
    return

def mc_reader(source):
    """
    
    """
    fpath = "FILEPATH/{}.H5".format(source)
    dat = pd.read_hdf(fpath, key='df', format='table')
    return dat


def icd_mapping(maps_path=r"FILEPATH",
                level_of_analysis='nonfatal_cause_name',
                write_log=False,
                create_en_matrix_data=False,
                save_results=True, en_proportions=True,
                extra_name="",
                deaths="non"):
    """
    Created on Fri Nov 18 14:49:50 2016
    Modified on Tues Nov 21 2017

    Map spreadsheet to compiled hospital data by ICD code
    Can write a log that holds the following information: All locations in data,
    all sources in data, ICD codes that did not match anything in the maps,
    the number & percent of data that did not match something in the maps.

    If an ICD code doesn't match anything in map, then the ICD code is
    truncated, and another attempt is made to map it.  This continues until the
    ICD code is successfully mapped or can't be truncated any more (3 digits is
    the minimum valid length).

    This won't work if we start getting multiple sources for one location. This
    code drops duplicated rows of the data after merging on the map.  If there
    happend to be a repeated ICD code, age, sex, year, location combo with the
    same number of cases, we would lose that data.  This is because of multi -
    level bundle_ids

    maps_path = r"FILEPATH"
    maps_path = r"FILEPATH"

    Parameters:
        df (DataFrame): dataframe with all the hospital data in it at the
            ICD level.
        maps_path (str): filepath as raw string that points to the latest
            clean map
        level_of_analysis (str): Instructs code to return data mapped and
            collapsed to either 'nonfatal_cause_name' or 'bundle_id'
        write_log (bool): switch that determines if you want to write a log
        create_en_matrix_data: If true, sends out a master script that creates
            the data inputs for the EN matrix. Takes a few hours to run.
        save_results (bool): If true, saves the mapped data
        en_proportions (bool): If true, _and_ id save_results is true, then
            qsubs a script that makes EN proportions.  As it is now,
            save_results must also be True for this to run.  If you don't want
            to wait for icd_mapping to run again, you can qsub the script and
            tell it what day to use data from.
        extra_name (str): suffixes onto the end of the filepath before the date
            Use this to differentiate a run of icd_mapping from our standard process
            ex. we need to include deaths for making CFRs so we can input modified
            data to icd mapping and append the filename to
            "icd_mapping_with_deaths_[date]"
        deaths: (str): Gets passed to the worker script.  If you makes it
            'deaths' then the worker will use deaths only data.  If it is
            _any_ other string then it won't do that.

    """
    repo = r"FILEPATH"

    # if we want to send out the master job. Takes a bool arg to delete
    # existing temp data and re-map or not
    if create_en_matrix_data:
        qsub = """
               qsub -N en_prep_submit -P proj_hospital -pe multi_slot 50 -l mem_free=100g
               -e FILEPATH -o FILEPATH
               {}/python_shell_gbd.sh
               {}/en_matrix_submit.py True
               """.format(repo, repo)
        
        qsub = ' '.join(qsub.split())  # split on whitspace and rejoin with one space where the splits occurred
        print("Launching qsub for EN Matrix")
        subprocess.call(qsub, shell=True)

    if en_proportions and not save_results:
        raise AssertionError("save_results must be True if you want "
            "en_proportions to be True. See docstring.")


    if deaths == 'deaths':
        # glob the files just in deaths
        files = glob.glob("FILEPATH/*.H5")

    else:
        # glob all the files in master data
        files = glob.glob("FILEPATH/*.H5")

    # get all the source names
    sources = []
    for file in files:
        sources.append(os.path.basename(file)[:-3])

    # delete every hdf file in /icd_mapping
    for icd_file in glob.glob("FILEPATH/*.H5"):
        os.remove(icd_file)

    # send out all the worker jobs
    qsubber(sources, deaths, write_log)

    # wait then try to read data then wait then read...
    time.sleep(300)
    job_holder()

    # read in data by source
    df = []
    for source in sources:
        dat = pd.read_hdf("FILEPATH/{}.H5".format(source),
                        key='df', format='table')
        df.append(dat)
        del dat

    df = pd.concat(df, ignore_index=True)

    assert set(sources).symmetric_difference(set(df.source.unique())) == set(),\
        "Some source(s) is missing"

    if save_results:
        print("Saving mapped data...")
        # get the next hospital data write version
        write_vers = pd.read_csv(r"FILEPATH")
        write_vers = int(write_vers['version'].max()) + 1
        map_vers = hosp_prep.get_current_map()

        today = datetime.datetime.today().strftime("%Y_%m_%d")
        hosp_prep.write_hosp_file(df,
                                  write_path="FILEPATH"
                                  r"FILEPATH/icd_mapping_v{}_map{}_{}{}.H5".\
                                  format(write_vers, map_vers, extra_name, today),
                                  backup=False)

        if en_proportions:
            # save a file for the en_proportions to use
            file = "FILEPATH/"\
                r"icd_mapping_for_enprops_{}.H5".format(today)
            df.to_hdf(file, key='df', format='table', complib='blosc', complevel=6)

            # set slots
            slots = 25

            # build qsub
            qsub = """
            qsub -P proj_hospital -N en_proportions -pe multi_slot {} -l mem_free={}g
            -o FILEPATH
             -e FILEPATH
             {}/python_shell_gbd.sh
             {}/prop_code_for_hosp_team.py {}
            """.format(slots, (slots*2),repo, repo, today)

            # clean qsub
            qsub = ' '.join(qsub.split())

            print("Launching qsub for EN Proportions")
            subprocess.call(qsub, shell=True)

    return(df)
