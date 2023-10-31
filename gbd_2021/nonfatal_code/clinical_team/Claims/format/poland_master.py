
import pandas as pd
import numpy as np
import getpass
import sys
import itertools
import time
import subprocess
from db_tools.ezfuncs import query
import os
import glob

from clinical_info.Functions import gbd_hosp_prep, hosp_prep
from clinical_info.Mapping import clinical_mapping
from clinical_info.Claims.format.poland_worker import pol_get_sample_size

USER = getpass.getuser()


def pol_get_data(all_data=False, map_version='current'):
    """
    Retrives the incoming data provided by the DSS team

    Returns
        bundle: pandas.DataFrame
        maps:   pandas.DataFrame
        dat:    pandas.DataFrame

    """
    if map_version == 'current':
        map_version = clinical_mapping.get_current_map_version('icg_bundle')

    maps = clinical_mapping.get_clinical_process_data('icg_bundle',
                                                      map_version=map_version)

    # get bid to bundle name map to add on bundle id
    bundles = query(
        "QUERY")

    if all_data:
        root = FILEPATH
        dat = pd.read_csv(
            FILEPATH)
        dat_2018 = pd.read_csv(FILEPATH)
        dat = pd.concat([dat, dat_2018], ignore_index=True, sort=False)
        del dat_2018

        assert dat.shape[0] > 0, "dataframe is empty"
        return dat, bundles, maps
    else:
        return bundles, maps


def pol_get_bundle_measure():
    '''
    Retrive most current bundle measure and then
    switch the measure name to measure id
    '''

    df = clinical_mapping.get_bundle_measure()
    df['measure_id'] = np.where(df['bundle_measure'] == "inc", 6, 5)
    df.drop('bundle_measure', axis=1, inplace=True)
    return df


def make_groups(n_groups, round_id, run_id, step, cause):
    """

    Parameters:
        n_groups (int): The number of groups
        roung_id (int): GBD round ID
        run_id (int): Clinical Informatics run ID
        step (str): GBD decomposition step
        cause (str): Aggergation level. Acceptable parameters are 'bundle'
            or 'icg'

    """

    root = FILEPATH
    df = pd.read_csv(FILEPATH,
                     usecols=['id'],
                     sep='\t')
    df_2018 = pd.read_csv(FILEPATH,
                          usecols=['id'],
                          sep='\t')
    dat = pd.concat([df, df_2018], ignore_index=True, sort=False)
    del df_2018

    path = FILEPATH
    msg = 'Missing df'
    assert df.shape[0] > 0, msg
    ids = df.id.unique()
    x = np.array_split(ids, n_groups)
    master = []
    for e in range(0, n_groups, 1):
        x_t = pd.DataFrame({'id': x[e],
                            'group': e})
        master.append(x_t)
    groups = pd.concat(master, sort=False, ignore_index=True)
    groups.to_csv(path + 'id_group_lookup.csv', index=False)

    pol_qsub(n_groups=n_groups, process_type=2, round_id=round_id,
             run_id=run_id, step=step, cause=cause)


def pol_qsub(n_groups, process_type, round_id, run_id, step, cause):
    """
    Sends out n_groups number of jobs. The number of jobs and the number
    of group dfs must be the same

    Args:
        n_groups (int): individual files to process
        process_type (int) : 0 - standard claims processing
                             1 - perform eda on new data sets
                             2 - make new group files
        round_id (int) : gbd round
        run (int) : run directory
        step (str) : Decomp step ex 'step2'
        cause (str) : Aggergation level. Params are either
                      'bundle' or 'icg'

    """

    # These are for if proces_type is not 2, i.e., groups have
    # already been made, the raw data has been split into
    # groups and contains only good enrollee ids
    df_path = FILEPATH
    df_files = glob.glob(FILEPATH)

    if process_type < 2:
        msg = 'Mismatch between group dfs and number of jobs'
        assert len(df_files) == n_groups, msg

        sge_dir = FILEPATH
        hosp_repo = FILEPATH.format(USER)

    for group_num in range(0, n_groups):

        qsub = f"""QSUB"""
        qsub = " ".join(qsub.split())  # clean up spaces and newline chars
        subprocess.call(qsub, shell=True)
        if group_num == 0:
            print(qsub)
        time.sleep(1)


def pol_fill_missing_square_data(df, round_id, step, run_id):
    df['sex_id'] = pd.to_numeric(df['sex_id'])
    og = df[df['count'] != 0].copy()
    df = df[df['count'] == 0].copy()
    pre = df.shape[0]

    df.drop(['bundle_name', 'age_end'], axis=1, inplace=True)
    df = df.merge(og[['bundle_id', 'bundle_name']].drop_duplicates(),
                  how='left', on='bundle_id')
    df = df.merge(og[['age_start', 'age_end']].drop_duplicates(),
                  how='left', on='age_start')
    assert pre == df.shape[0]
    df = pd.concat([og, df], sort=False, ignore_index=True)

    prev_ids = [548, 547, 546]
    df.loc[(df.bundle_id.isin(prev_ids)) & (df.bid_measure.isnull()),
           'bid_measure'] = 'prev'

    df.loc[(df.bundle_id == 79) & (df.bid_measure.isnull()),
           'bid_measure'] = 'inc'

    # add sample sizes for cases that are 0
    df.loc[df['count'] == 0, 'sample_size'] = np.nan
    dat_bad = df[df['count'] == 0]
    dat_good = df[df['count'] != 0]

    # get_sample_size creates the year_start / end columns
    dat_bad.drop(columns=['year_start', 'year_end',
                          'sample_size'], inplace=True)
    dat_fixed = pol_get_sample_size(df=dat_bad, run_id=run_id,
                                    gbd_round_id=round_id, decomp_step=step)

    # get_sample_size drops the year_id column
    dat_fixed['year_id'] = dat_fixed['year_start']
    dat_fixed.rename({'population': 'sample_size'}, axis=1, inplace=True)
    df = pd.concat([dat_good, dat_fixed], sort=False, ignore_index=True)
    return df


def pol_prep_final_formatting(df, nid_dict, estimate_id):
    # create year_end/start and rename counts to cases
    df.rename(columns={'year_start': 'year_start_id',
                       'count': 'cases'}, inplace=True)
    df['year_end_id'] = df['year_start_id']

    # create the rate estimate column
    df['mean'] = df['cases'] / df['sample_size']

    # merge on nid
    for y in df.year_start_id.unique():
        df.loc[df['year_start_id'] == y, 'nid'] = nid_dict[y]

    # create upper lower with nulls
    df['upper'] = np.nan
    df['lower'] = np.nan

    if estimate_id == 17:
        df['source_type_id'] = 10
    else:
        df['source_type_id'] = 17

    df['estimate_id'] = estimate_id
    df['representative_id'] = 1
    df['diagnosis_id'] = 1

    return df


def square_and_write(df, bundles, maps, facility,
                     round_id, step, run_id, break_if_not_contig):
    nid_dict = {
        2015: 397812,
        2016: 397813,
        2017: 397814,
        2018: 431674
    }

    df = pol_square_it(df)
    df = df.merge(bundles, how='left', on='bundle_id')
    df.rename({'measure_id': 'bid_measure'}, axis=1, inplace=True)
    df = pol_fill_missing_square_data(df, round_id, step, run_id)
    df = clinical_mapping.apply_restrictions(df, age_set='binned',
                                             cause_type='bundle',
                                             break_if_not_contig=break_if_not_contig)

    if facility == 'inp_only':
        estimate_id = 17
    else:
        estimate_id = 21

    df = pol_prep_final_formatting(df, nid_dict, estimate_id)

    df['bundle_id'] = pd.to_numeric(df.bundle_id)

    bid_m = pol_get_bundle_measure()

    df = df.merge(bid_m.drop_duplicates(), how='left', on='bundle_id')
    df.rename({'measure_id': 'bid_measure'}, axis=1, inplace=True)

    df['bid_measure'] = np.where(
        df['bid_measure'] == 5, 'prevalence', 'incidence')

    unneeded_year_cols = ['year_start_id', 'year_end', 'year_end_id']
    for col in unneeded_year_cols:
        if col in df.columns:
            df = df.drop(col, axis=1)

    df['year_end'] = df.year_id
    df['year_start'] = df.year_id

    write_to_file(df, facility, run_id)


def write_to_file(df, facility, run_id):

    df['age_start'] = pd.to_numeric(df['age_start'])
    df['age_end'] = pd.to_numeric(df['age_end'])

    save_filepath = (FILEPATH)

    print(f"Saving {save_filepath}...")
    df.to_csv(save_filepath, index=False)
    print("Saved.")


def pol_make_square(df):
    """
    takes a dataframe and returns the square of every age/sex/bundle id which
    exists in the given dataframe but only the years available for each
    location id
    """

    def expandgrid(*itrs):
        # create a template df with every possible combination of
        #  age/sex/year/location to merge results onto
        # define a function to expand a template with the cartesian product
        product = list(itertools.product(*itrs))
        return({'Var{}'.format(i + 1): [x[i] for x in product] for i in range(len(itrs))})

    agid_map = df[['age_start', 'age_group_id']].drop_duplicates()

    ages = df.age_start.unique()
    sexes = df.sex_id.unique()
    years = df.year_id.unique()
    bundles = df.bundle_id.unique()
    locs = df.location_id.unique()

    dat = pd.DataFrame(expandgrid(ages,
                                  sexes, locs,
                                  years,
                                  bundles))
    dat.columns = ['age_start', 'sex_id', 'location_id', 'year_id',
                   'bundle_id']
    exp_rows = dat.shape[0]

    test = df.merge(dat, how='outer', on=dat.columns.tolist())

    assert dat.shape[0] - \
        df.shape[0] == test['count'].isnull().sum(), "Unexpected rows created"
    test.loc[test['count'].isnull(), 'count'] = 0
    df = test.copy()
    # get age group onto the expanded rows
    assert agid_map.shape[0] == df.age_start.unique(
    ).size, "too many rows in agid map"
    df.drop('age_group_id', axis=1, inplace=True)
    df = df.merge(agid_map, how='left', on='age_start')
    assert df.age_group_id.isnull().sum() == 0,\
        "There should not be null group IDs"
    assert df.shape[0] == exp_rows,\
        "df row count was not equal to expected rows"
    return(df)


def pol_square_it(df):
    # make data square
    df.rename({'cases': 'count'}, axis=1, inplace=True)
    df['year_id'] = df['year_start']

    pre = df['count'].sum()
    cols = ['sex_id', 'bundle_id', 'age_group_id', 'location_id', 'year_id']

    temp = df.groupby(cols).agg({'count': 'sum'}).reset_index()
    df = df.merge(temp, how='left', on=cols, suffixes=['_old', '_new'])
    df.drop(['count_old'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
    df.rename(columns={'count_new': 'count'}, inplace=True)

    df = pol_make_square(df)
    assert pre == df['count'].sum()
    # merge on measure from our map
    pre_shape = df.shape[0]
    pre_shape = df.shape[0]
    bid_m = pol_get_bundle_measure()
    df = df.merge(bid_m.drop_duplicates(), how='left', on='bundle_id')
    assert pre_shape == df.shape[0]
    return df


def agg_files(run_id, n_groups):
    """
    Once the qsub has finished we will need to bring all of the files back
    together. Might need to update this due to different agg types.

    Parameters:
        run_id int: the run id used to store file outputs in
        FILEPATH
    """

    path = FILEPATH

    files_inp = glob.glob(path + FILEPATH)
    files_inp_otp = glob.glob(path + FILEPATH)

    # lists of files expected to exist
    expected_files_inp = [FILEPATH
                          for i in range(n_groups)]
    expected_files_inp_otp = [FILEPATH
                              for i in range(n_groups)]

    if not len(files_inp) == n_groups:
        raise ValueError(
            f"""Expected {n_groups} files but found {len(files_inp)}.
                Missing these files (first 50):
                {list(set(expected_files_inp) - set(files_inp))[:50]}""")
    if not len(files_inp_otp) == n_groups:
        raise ValueError(
            f"""Expected {n_groups} files but found {len(files_inp_otp)}.
                Missing these files (first 50):
                {list(set(expected_files_inp_otp) - set(files_inp_otp))[:50]}""")

    final = []
    for e in files_inp:
        df_temp = pd.read_csv(e)
        final.append(df_temp)
    inp = pd.concat(final, sort=False, ignore_index=True)

    final = []
    for e in files_inp_otp:
        df_temp = pd.read_csv(e)
        final.append(df_temp)
    inp_otp = pd.concat(final, sort=False, ignore_index=True)

    inp_shape_prev = inp.shape
    inp_otp_shape_prev = inp_otp.shape

    cols = inp.columns.tolist()
    cols.remove('cases')
    inp = inp.groupby(cols).agg({'cases': 'sum'}).reset_index()
    inp_otp = inp_otp.groupby(cols).agg({'cases': 'sum'}).reset_index()

    assert inp_shape_prev[0] > inp.shape[0]
    assert inp_otp_shape_prev[0] > inp_otp.shape[0]

    return inp, inp_otp


def main(n_groups, run_id, round_id, step, cause,
         groups=False):
    """
    Parameters:
        n_groups(int): the number of groups we expect to process or create
        run_id(int): the run id used to store file outputs in
           FILEPATH
        round_id(int): GBD round. Used to pull data from shared functions
        step(str): GBD decomp step
        cause(str): Aggergation level. Acceptable parameters are 'bundle'
            or 'icg'
        groups(bool): Remake group files, similar to MS, which are read in by
            the worker script
    """
    df = maps = bundles = None

    if groups:
        print("Making groups...")
        make_groups(n_groups=n_groups, round_id=round_id,
                    run_id=run_id, step=step, cause=cause)
        hosp_prep.job_holder(job_name="gp_", sleep_time=65,
                             init_sleep=60)  # one hour

    else:
        print("Not making groups.")
        bundles, maps = pol_get_data(all_data=False, map_version='current')

    print("Sending out main jobs...")
    pol_qsub(n_groups=n_groups, process_type=0, round_id=round_id, run_id=run_id,
             step=step, cause=cause)
    hosp_prep.job_holder(job_name="gp_", sleep_time=65,
                         init_sleep=7200)  # two hours
    print("All jobs have finished.")

    # bring all of the files together
    print("Reading in files and aggregating...")
    inp, inp_otp = agg_files(run_id, n_groups)

    print("Squaring...")
    square_and_write(df=inp, bundles=bundles, maps=maps,
                     facility='inp_only', round_id=round_id, step=step,
                     run_id=run_id, break_if_not_contig=True)
    square_and_write(df=inp_otp, bundles=bundles, maps=maps,
                     facility='inp_otp', round_id=round_id, step=step,
                     run_id=run_id, break_if_not_contig=True)
    print("Done.")


if __name__ == '__main__':
    run_id = sys.argv[1]
    round_id = sys.argv[2]
    step = sys.argv[3]
    cause = sys.argv[4]
    groups = sys.argv[5]

    run_id = int(run_id)
    round_id = int(round_id)
    step = str(step)
    cause = str(cause)
    groups = bool(groups)

    print(f"""

        All sys.argv: {sys.argv}


        run_id: {run_id}
        round_id: {round_id}
        step: {step}
        cause: {cause}
        groups: {groups}
        """)

    main(n_groups=1000, run_id=run_id, round_id=round_id,
         step=step, cause=cause, groups=groups)
    print("Done.")
