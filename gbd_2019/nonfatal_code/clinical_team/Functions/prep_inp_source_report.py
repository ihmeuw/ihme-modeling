import pandas as pd
import numpy as np
import re
import datetime
import getpass
import glob
import os
import platform
import sys
import time
from db_queries import get_population
import itertools


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

user = getpass.getuser()

repo = r"FILEPATH".format(user)

sys.path.append("FILEPATH".format(repo))
sys.path.append("FILEPATH".format(repo))
sys.path.append("FILEPATH".format(repo))

import submit_icd_mapping
import prep_for_env
import gbd_hosp_prep


def get_raw_cases(df, add_deaths=True):
    """
    Sum the count of hospital admissions aka cases in a dataframe by source
    Write output to a csv in the hospital diagnostics dir

    Parameters:
        df: Pandas DataFrame
            Must have a count col named 'val'
        add_deaths: bool
            Should deaths be added to the sum of cases? If not the data will
            approach live discharges for some sources but other sources don't
            differentiate between deaths and discharges so we have no way of
            extracting just live discharges
    """
    assert "val" in df.columns, "There is no 'val' count variable to sum"

    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
    grpd = df.groupby(['source', 'facility_id', 'outcome_id']).\
        agg({'val': 'sum'}).reset_index()

    for source in grpd.source.unique():
        dat = grpd[grpd.source == source].copy()
        if not add_deaths:
            dat = dat[dat.outcome_id != 'death']
        dat = dat.groupby(['source', 'facility_id']).agg({'val': 'sum'}).\
            reset_index()
        dat.to_csv("FILENAME"
                   r"FILEPATH".format(source, today), index=False)
    return


def get_raw_deaths(df):
    """
    Sum the count of hospital deaths in a dataframe by source
    Write output to a csv in the hospital diagnostics dir

    Parameters:
        df: Pandas DataFrame
            Must have a count col named 'val'
    """
    assert "val" in df.columns, "There is no 'val' count variable to sum"

    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    sources = df.source.unique()


    if df.shape[0] > 0:
        grpd = df.groupby(['source', 'facility_id']).agg({'val': 'sum'}).\
            reset_index()

        for source in sources:
            dat = grpd[grpd.source == source].copy()


            if dat.shape[0] == 0:
                dat = pd.DataFrame({'source': source, 'facility_id': 'hospital',
                                    'val': 0}, index=[0])
            dat.to_csv("FILENAME"
                       r"FILEPATH".format(source, today), index=False)
    return


def get_age_pattern(df):
    """
    Create data that can be used to construct an age pattern with cases on
    the y axis and age on the x axis.
    Write output to a csv in the hospital diagnostics dir

    Parameters:
        df: Pandas DataFrame
    """
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    grpd = df.groupby(['source', 'sex_id', 'age_group_id', 'year_start',
                    'diagnosis_id', 'facility_id', 'outcome_id']).\
                    agg({'val': 'sum'}).reset_index()

    for source in grpd.source.unique():
        dat = grpd[grpd.source == source].copy()
        dat.to_csv("FILENAME"
                       r"FILEPATH".
                       format(source, today), index=False)
    return


def get_bs_age_pattern(df):
    """
    Create data that can be used to construct an age pattern with cases on
    the y axis and age on the x axis for each baby sequela.
    Write output to a csv in the hospital diagnostics dir

    Parameters:
        df: Pandas DataFrame
    """
    assert "nonfatal_cause_name" in df.columns, "Data must have column 'nonfatal_cause_name'"
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]


    df = df[df.diagnosis_id == 1]
    df = df[(df.facility_id == "hospital") | (df.facility_id == "inpatient unknown")]

    sources = df.source.unique()

    grpd = df.groupby(['source', 'sex_id', 'age_group_id', 'year_start',
                    'nonfatal_cause_name', 'outcome_id']).\
                    agg({'val': 'sum'}).reset_index()

    for source in sources:
        dat = grpd[grpd.source == source].copy()


        dat.to_csv("FILENAME"
                       r"FILEPATH".
                       format(source, today), index=False)
    return


def get_hosp_pops(df):
    """
    The Shiny/Rmarkdown tool which this data feeds into was running
    get_population() live but it was taking too long. This function pulls the
    population for a source and writes it to a csv in the hospital diagnostics
    dir

    Parameters:
        df: Pandas DataFrame
    """
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    pop = get_population(
        age_group_id=list(df.age_group_id.unique()),
        location_id=list(df.location_id.unique()),
        year_id=list(df.year_start.unique()),
        sex_id=[1,2])

    for source in df.source.unique():

        dat = df[df.source == source]

        locs = dat.location_id.unique()
        years = dat[dat.location_id.isin(locs)].year_start.unique()
        ages = dat.age_group_id.unique()

        src_pop = pop[pop.location_id.isin(locs) & pop.year_id.isin(years) &\
                  pop.age_group_id.isin(ages)].copy()
        src_pop['source'] = source
        src_pop.to_csv("FILENAME"
                       r"FILEPATH".
                       format(source, today), index=False)
    return


def get_source_locs(df):
    """
    The functions above collapse val across location. We may occasionally need
    a list of the locations present in a datasource to subset out something like
    population or the hospital envelope. This pulls locations by source.
    Write output to a csv in the hospital diagnostics dir

    Parameters:
        df: Pandas DataFrame
    """
    start = time.time()
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]


    dat = df[['source', 'location_id']].copy()
    dat.drop_duplicates(inplace=True)

    for source in dat.source.unique():

        locs = pd.Series(dat[dat.source == source].location_id.unique())

        locs = locs.astype(str)

        locs = locs.str.cat(sep=",")
        dat2 = pd.DataFrame({'source': source, 'location_ids': locs}, index=[0])
        dat2.to_csv("FILENAME"
                       r"FILEPATH".
                       format(source, today), index=False)
    return


def refine_age_pattern(file_date=None, dx_type=[1, 2],
                       outcome_type=['discharge', 'death'], current=True):
    """
    Reads in the most current age pattern file for every source written on that
    date and keeps diff types of diagnoses and outcomes depending on what is
    passed (as a list)

    Parameters:
        file_date: str
        dx_type: list of int
        outcome_type: list of str
        current: bool
    returns a dataframe
    """
    df = metadata_reader('age_pattern', file_date=file_date, current=current)

    df = df[df.diagnosis_id.isin(dx_type)]
    df = df[df.outcome_id.isin(outcome_type)]

    dat = df.groupby(['source', 'sex_id', 'age_start',
                      'year_start']).agg({'val': 'sum'}).\
                      reset_index()
    return(dat)


def metadata_reader(which_dir, file_date=None, current=True):
    """
    Reads in the most current set of files in a directory of metadata files by
    default, with an option to read in older files based on date written
    This requires our files to be stored in an identical format like so
    "FILEPATH" where date is in the format YYYY_MM_DD.  This function
    looks for folders/files inside of

    Parameters:
        which_dir: str
        file_date: str
        current: bool
    Returns a dataframe
    """
    if current:

        file_path = r"FILENAME".format(which_dir)


        assert os.path.isdir(file_path), "'{}' is not an existing filepath".\
            format(file_path)


        files = glob.glob("FILEPATH".format(file_path))


        assert len(files) > 0, "There are no files in {}".format(which_dir)


        dates = [x[-14:-4] for x in files]

        file_date = max(list(set(dates)))

        files = glob.glob("FILEPATH".format(file_path, file_date))

        assert len(files) > 0, "There are no files in {}".format(file_path)
    else:

        assert file_date is not None,\
            "Please provide a date for the files you'd like to read"

        file_path = r"FILENAME".\
            format(which_dir)


        assert os.path.isdir(file_path), "'{}' is not an existing filepath".\
            format(file_path)


        files = glob.glob("FILEPATH".format(file_path, file_date))

        assert len(files) > 0, """There are no matching files in
            {} with the date {}""".format(file_path, file_date)


    df_list = [pd.read_csv(f) for f in files]
    df = pd.concat(df_list)
    return(df)




def get_unique_etis(df):
    """
    read in a dataframe of hospital data and determine if it contains baby
    sequela or bundle id then store the unique values for whichever is present
    along with the number of unique demographic groups a given etiology is
    present in and the sum of all admissions for that source/etiology combo

    Parameters:
        df: Pandas DataFrame
    """
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
    if 'bundle_id' in df.columns:
        cause_type = 'bundle_id'
    elif 'nonfatal_cause_name' in df.columns:
        cause_type = 'nonfatal_cause_name'
    else:
        return("need babies or bundles")

    if cause_type == 'nonfatal_cause_name':
        grpd = df.groupby(['source', 'nonfatal_cause_name']).\
            agg({'val': 'sum', 'nonfatal_cause_name': ['size']}).reset_index()
        grpd.columns = ['source', cause_type, 'eti_cnt_by_demo', 'val']
    if cause_type == 'bundle_id':
        grpd = df.groupby(['source', 'bundle_id']).\
            size().reset_index().rename(columns={0: 'eti_cnt_by_demo'})
    for source in grpd.source.unique():
        dat = grpd[grpd.source == source].copy()




        dat.to_csv("FILENAME"
                         r"FILEPATH".
                         format(cause_type, source, today), index=False)
    return

def get_age_year_combo(df, test=False):
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    if test:
        df = pd.read_csv(r"FILEPATH")
    else:

        df = metadata_reader('age_pattern')



    df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    def expandgrid(*itrs):



        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

    year_list = list(range(1990, 2016))

    age_list = [0,1] + list(range(5,100,5))
    age_list = np.asarray(age_list)

    square = expandgrid(age_list, year_list)
    square = pd.DataFrame(square)
    square.columns = ['age_start', 'year_start']

    for source in df.source.unique():


        df_source = df[df.source == source].copy()
        df_source = df_source[['age_start', 'year_start']].drop_duplicates()


        df_source = df_source[df_source.year_start >= 1990]


        age_year = df_source[['age_start', 'year_start']].drop_duplicates()
        age_year['present'] = 'yes'
        age_year = age_year[age_year.year_start >= 1990]


        exists = square.merge(age_year, how='left', on=['age_start', 'year_start'])
        exists = exists.fillna('no')

        exists.rename(columns={'age_start': 'age', 'year_start':'year'}, inplace=True)

        exists.to_csv("FILENAME"
                      "FILEPATH".format(source, today),
                     index=False)

    if test:
        return exists

def get_baby_age_year_combo(df, test=False):
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    if test:
        df = pd.read_csv(r"FILEPATH")
    else:

        df = metadata_reader('baby_age_pattern')



    df = gbd_hosp_prep.all_group_id_start_end_switcher(df)

    def expandgrid(*itrs):



        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

    year_list = list(range(1990, 2016))

    age_list = [0,1] + list(range(5,100,5))
    age_list = np.asarray(age_list)

    baby_list = df.nonfatal_cause_name.unique()

    square = expandgrid(age_list, year_list, baby_list)
    square = pd.DataFrame(square)
    square.columns = ['age_start', 'year_start', 'nonfatal_cause_name']

    for source in df.source.unique():


        df_source = df[df.source == source].copy()
        df_source = df_source[['age_start', 'year_start', 'nonfatal_cause_name']].drop_duplicates()


        df_source = df_source[df_source.year_start >= 1990]


        age_year = df_source[['age_start', 'year_start', 'nonfatal_cause_name']].drop_duplicates()
        age_year['present'] = 'yes'
        age_year = age_year[age_year.year_start >= 1990]


        exists = square.merge(age_year, how='left', on=['age_start', 'year_start', 'nonfatal_cause_name'])
        exists = exists.fillna('no')

        exists.rename(columns={'age_start': 'age', 'year_start':'year'}, inplace=True)

        exists.to_csv("FILENAME"
                      "FILEPATH".format(source, today),
                     index=False)
    return


def get_master_files(deaths=False, run_id=None):
    """
    pull all the HDF file names from our master data folder
    if you want gbd2019+ data then enter a valid run_id,
    """
    if deaths:
        if run_id:
            mdir = "FILEPATH".format(run_id)
        else:
            mdir = "FILEPATH"
        files = glob.glob(mdir)
    else:
        if run_id:
            mdir = "FILEPATH".format(run_id)
        else:
            mdir = "FILEPATH"
        files = glob.glob(mdir)
    return files


def main():
    """
    Run everything

    Parameters:
        df: Pandas DataFrame
            should be equivalent to master data
    """
    print("Running functions to create diagnostic files")
    start = time.time()

    files = get_master_files(deaths=True)
    for file in files:
        df = pd.read_hdf(file, key='df', format='table')
        get_raw_deaths(df)
    print("Deaths done in {} minutes".format(\
        round((time.time()-start)/60, 2)))

    files = get_master_files()
    for file in files:
        df = pd.read_hdf(file, key='df', format='table')
        assert "cause_code" in df.columns, "ICD codes don't appear to be present"

        print("Processing {}".format(df.source.unique()))

        get_raw_cases(df)
        get_age_pattern(df)
        get_age_year_combo(df, test=False)

        get_hosp_pops(df)
        get_source_locs(df)
        del df
    print("Cases, age pattern, age year combo, pop and locations done in {}".\
        format(round((time.time()-start)/60, 2)))


    df = submit_icd_mapping.icd_mapping(write_log=False, create_en_matrix_data=False,
                     save_results=False, en_proportions=False)
    get_bs_age_pattern(df)


    get_baby_age_year_combo(df, test=False)
    print("Baby age-year combo done in {} minutes".format(\
        round((time.time()-start)/60, 2)))

    get_unique_etis(df)
    print("Unique baby sequelae done in {} minutes".format(\
        round((time.time()-start)/60, 2)))


    df = prep_for_env.expand_bundles(df)
    get_unique_etis(df)
    print("Unique bundles done in {} minutes".format(\
        round((time.time()-start)/60, 2)))

    return

if __name__ == "__main__":
    main()
