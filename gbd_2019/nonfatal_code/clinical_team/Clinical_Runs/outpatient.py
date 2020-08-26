

import pandas as pd
import numpy as np
from db_tools.ezfuncs import query
import datetime
import os
import platform
import sys
from db_queries import *
import getpass
import subprocess

user = getpass.getuser()
repo_dir = ['Database']
base = r"FILEPATH".format(user)
list(map(sys.path.append, [base + e for e in repo_dir]))


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"


user = getpass.getuser()

repo = r"FILEPATH".format(user)

for module_path in ["FILEPATH", "FILEPATH", "FILEPATH"]:
    sys.path.append(repo + module_path)

import outpatient_funcs as otp
import gbd_hosp_prep as ghp
import clinical_mapping as cm

import hosp_prep


def launch_icpc(run_id, gbd_round_id, decomp_step):
    qsub = """
        qsub""".format(repo, repo, run_id, gbd_round_id, decomp_step)
    qsub = " ".join(qsub.split())
    subprocess.call(qsub, shell=True)
    print("ICPC script has been launched.")


def get_icpc(run_id):

    filepath = ("FILEPATH"
                "FILEPATH".format(run_id))
    icpc = pd.read_csv(filepath)
    return icpc


def run_outpatient(run_id, gbd_round_id, decomp_step, run_icpc):
    """
    SETUP
    Gets the files for our three outpatient sources.  Creates a dataframe
    after reading these files and drops data that cannot be used for the
    outpatient process.

    :param df: None at this moment
    :return df_orig: the raw dataframe from reading outpatient sources
    """


    if run_icpc:
        launch_icpc(run_id, gbd_round_id, decomp_step)

    def file_name(file): return file.split('.')[0]


    sources = ['SWE_PATIENT_REGISTRY_98_12', 'USA_NAMCS', 'USA_NHAMCS_92_10']
    source_dir = "FILEPATH"\
                 "FILENAME".format(run_id)

    file_paths = [os.path.join(source_dir, file) for file in os.listdir(source_dir)
                  if file_name(file) in sources]
    dfs = [pd.read_hdf(file) for file in file_paths]
    df_orig = pd.concat(dfs, ignore_index=True, sort=False)
    del dfs

    assert sorted(df_orig.source.unique()) == sorted(sources),\
        "sources inconsistent after writing to df"


    location = "FILENAME" + str(run_id)

    df = otp.drop_data_for_outpatient(df_orig)

    """
    AGES
    Switches out age_group_id with age_start and age_end.  Then, age_end is
    changed to 125 for ages in the range 85 to 90 for USA_NAMCS
    and USA_NHAMCS to match SWEDEN's oldest age.
    """

    df = ghp.all_group_id_start_end_switcher(df, remove_cols=True)


    df.loc[(df.age_start == 85) & (df.source.isin(['USA_NAMCS',
                                                   'USA_NHAMCS_92_10'])),
           ['age_end']] = 125
    df = ghp.all_group_id_start_end_switcher(df)

    """
    MAPPING ICD TO ICG
    Implements outpatient mapping from ICD to ICG level.
    """



    df = cm.map_to_gbd_cause(df, input_type='cause_code', output_type='icg',
                             write_unmapped=False,
                             truncate_cause_codes=False, extract_pri_dx=False,
                             prod=False, groupby_output=True)

    df.to_hdf(location + "FILEPATH",
              key='df', complib='blosc', complevel=5, mode='w')


    out = df.copy()
    out['estimate_type'] = 'otp-any-unadjusted'
    out = out[['location_id', 'year_start', 'age_group_id', 'sex_id',
               'nid', 'representative_id', 'facility_id', 'estimate_type',
               'diagnosis_id', 'icg_id', 'icg_name', 'val']]
    out = out.rename(columns={'year_start': 'year_id'})
    out[['location_id', 'year_id', 'representative_id',
         'sex_id', 'nid', 'icg_id']] = \
        out[['location_id', 'year_id', 'representative_id',
             'sex_id', 'nid', 'icg_id']].astype(int)

    """
    MAPPING ICG TO BUNDLE
    Implements outpatient mapping from ICG to bundle level.
    """

    df = cm.map_to_gbd_cause(df, input_type='icg', output_type='bundle',
                             write_unmapped=False,
                             truncate_cause_codes=False,
                             extract_pri_dx=False, prod=False,
                             groupby_output=True)
    df = ghp.all_group_id_start_end_switcher(df)


    df.to_hdf(location +
              "FILEPATH",
              key='df', complib='blosc', complevel=5, mode='w')

    """
    INJ FACTORS
    Applies injuries, correction, and restrictions.  Checks to see if 3
    columns are correctly added to the new df.
    """
    df = otp.get_parent_injuries(df)


    df = otp.apply_outpatient_correction(df, run_id)


    (rows_before, cols_before) = df.shape
    col_names_before = df.columns

    df = otp.apply_inj_factor(df, run_id, fillna=True)


    (rows_after, cols_after) = df.shape
    assert cols_after == cols_before + 2
    assert set(df.columns).symmetric_difference(set(col_names_before))\
        == set(['val_inj_corrected', 'factor']),\
        "columns failed to add after applying inj factor"

    df = otp.outpatient_restrictions(df)

    """
    MAKE SQUARE
    The following is because of how our make zeros function works. It finds
    all the ages present, and makes every source have those ages. Because
    different outpatient sources have different ages, after being made
    square every source has every age. That's dumb, but it works in
    inpatient, so here we just fix it post hoc. - previous documentation

    Makes the data square.
    """
    df = ghp.all_group_id_start_end_switcher(df, remove_cols=True)


    swe_ages = list(
        df[df.source == "SWE_PATIENT_REGISTRY_98_12"].age_group_id.unique())
    nhamcs_ages = list(
        df[df.source == "USA_NHAMCS_92_10"].age_group_id.unique())
    namcs_ages = list(df[df.source == "USA_NAMCS"].age_group_id.unique())
    pre_check_val = df.loc[df['val'] > 0,
                           'val'].sort_values().reset_index(drop=True)
    df = hosp_prep.make_zeros(df, etiology='bundle_id',
                              cols_to_square=['val', 'val_corrected',
                                              'val_inj_corrected'], icd_len=5)


    source_age_dict = {
        "SWE_PATIENT_REGISTRY_98_12": swe_ages,
        "USA_NHAMCS_92_10": nhamcs_ages,
        "USA_NAMCS": namcs_ages
    }


    df_list = []
    for source in source_age_dict:
        temp = df[df.source == source].copy()
        temp = temp[temp.age_group_id.isin(source_age_dict[source])].copy()
        df_list.append(temp)
    df = pd.concat(df_list, ignore_index=True, sort=False)


    df = ghp.all_group_id_start_end_switcher(df, remove_cols=False)


    post_check_val = df.loc[df['val'] > 0,
                            'val'].sort_values().reset_index(drop=True)
    assert set(pre_check_val) - set(post_check_val) == set([]), \
        "pre and post check val are different after making square"
    assert (post_check_val == pre_check_val).all(),\
        "pre and post check val are different after making square"
    assert not (post_check_val != pre_check_val).any(),\
        "pre and post check val are different after making square"
    assert np.abs(post_check_val - pre_check_val).sum() == 0.0,\
        "pre and post check val are different after making square"

    """
    FIX NULLS AND DUPES
    Cleans up the data by filling in nulls and removing duplicates.
    """


    df['age_group_unit'] = 1
    df['metric_id'] = 1



    maps = cm.get_bundle_measure(prod=False)
    maps_dups = maps.loc[maps['bundle_id'].duplicated(
        keep=False)].sort_values('bundle_id')
    assert maps_dups.shape[0] == 0, (
        "Map has more than one measure for the same bundle at least once.")
    maps.rename(columns={'bundle_measure': 'measure'}, inplace=True)
    assert "measure" in maps.columns, "Rename to column measure didn't work."
    df = df.merge(maps, how='left', on='bundle_id')



    cause_id_info = query(
        "SQL", conn_def='epi')

    null = df.loc[df.measure.isnull()]
    null = null.merge(cause_id_info, how='left', on='bundle_id')
    null['cause_id'] = null['cause_id'].astype(int)
    null_measure = null['cause_id'].unique()


    inj = get_cause_metadata(cause_set_id=3)
    inj = inj.loc[inj['cause_outline'].str[0:1] == 'C']

    for each_measure in null_measure:
        assert not inj.loc[inj['cause_id'] ==
                           each_measure].empty, "null measures aren't injuries"


    df.loc[df.measure.isnull(), 'measure'] = 'inc'


    df.loc[df.factor.isnull(), 'factor'] = 1


    assert df.isnull().sum().sum() == 0, "cannot be nulls before elmo"

    sum_cols = ['val', 'val_corrected', 'val_inj_corrected']
    sum_dict = dict(list(zip(sum_cols, ['sum'] * 3)))
    df = df.groupby(df.columns.drop(sum_cols).tolist()
                    ).agg(sum_dict).reset_index()


    duplicated_df = df[
        df[['age_start', 'age_end', 'age_group_id', 'year_start', 'year_end',
            'location_id', 'sex_id', 'bundle_id', 'nid']].duplicated(
                keep=False)
    ].copy()
    assert duplicated_df.shape[0] == 0, "duped rows??"

    df = otp.get_sample_size_outpatient(df, gbd_round_id=gbd_round_id,
                                        decomp_step=decomp_step)
    """
    ELMO
    """

    assert (df.loc[df.age_end > 1, 'age_end'].values %
            5 == 0).all(), "ages are not in correct bins"


    test = df[['source', 'nid', 'year_start', 'year_end']].drop_duplicates()
    nid_map = pd.\
        read_excel("FILEPATH")
    test = test.merge(nid_map, how='left', on='nid')
    assert test.isnull().sum().sum() == 0, "there are null nids and years"
    assert test[test.merged_nid.isnull(
    )].shape[0] == 0, "there are null nids and years"

    done = df.copy()

    df['run_id'] = run_id
    df = reshape_long(df)


    df = df[df.cases < df.sample_size]
    assert 6607 not in df.bundle_id.unique(), (
        "6607 is a typo and shouldn't be in the map")



    nulls_ok_columns = ["mean", "lower", "upper"]
    test_df = df.drop(nulls_ok_columns, axis=1).copy()
    assert test_df.notnull().all().all(), "There are nulls:\n{}".format(
        test_df.isnull().sum())


    assert df[df[['age_group_id', 'year_start', 'year_end',
                  'location_id', 'sex_id', 'bundle_id', 'nid', 'estimate_id',
                  'diagnosis_id']].duplicated(keep=False)].shape[0] == 0, (
        "duplicate rows")


    done = otp.outpatient_elmo(done, gbd_round_id)


    nulls_ok_columns = ['mean', 'upper', 'lower', 'seq', 'underlying_nid',
                        'sampling_type', 'recall_type_value',
                        'uncertainty_type', 'uncertainty_type_value',
                        'input_type', 'standard_error',
                        'effective_sample_size', 'design_effect',
                        'response_rate']
    test_df = done.drop(nulls_ok_columns, axis=1).copy()
    assert test_df.notnull().all().all(), "There are nulls:\n{}".format(
        test_df.isnull().sum())


    assert done[done[['age_start', 'age_end', 'age_group_id', 'year_start',
                      'year_end',
                      'location_id', 'sex', 'bundle_id', 'nid']].
                duplicated(keep=False)].shape[0] == 0, "duplicate rows"

    done.drop("age_group_id", inplace=True, axis=1)

    print("run_outpatient() has finished!")

    return done, df


def convert_to_int(df):
    dfcols = df.columns
    int_cols = ['age_group_id', 'age_start', 'age_end', 'location_id',
                'sex_id', 'year_id', 'nid', 'estimate_id', 'icg_id',
                'metric_id',
                'bundle_id', 'diagnosis_id', 'representative_id',
                'year_start', 'year_end']
    int_cols = [c for c in int_cols if c in dfcols]
    for col in int_cols:
        df[col] = df[col].astype(int)
    return df


def reshape_long(df):
    """
    reshape the data long to store in the database using "cases" and
    "sample size"
    """

    df.rename(columns={'year_start': 'year_start',
                       'year_end': 'year_end',
                       'population': 'sample_size'},
              inplace=True)
    df['source_type_id'] = 11
    df['diagnosis_id'] = 3
    for col in ['mean', 'upper', 'lower']:
        df[col] = np.nan

    df.drop(['age_start', 'age_end', 'measure'],
            axis=1, inplace=True)

    print("cols before reshaping are {}".format(df.columns))

    estimate_ids = {'val': 11,
                    'val_corrected': 23,
                    'val_inj_corrected': 24}

    final_db_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                     'year_end', 'representative_id', 'estimate_id',
                     'source_type_id', 'diagnosis_id', 'nid', 'run_id',
                     'bundle_id',
                     'mean', 'lower', 'upper', 'sample_size', 'cases']

    idx = df.filter(regex="^(?!val).*").columns.tolist()
    print("The index to reshape on is {}".format(idx))

    df = df.set_index(idx).stack().reset_index()

    df.rename(columns={'level_{}'.format(len(idx)): 'estimate_name',
                       0: 'cases'}, inplace=True)

    df['estimate_id'] = -1

    for key in list(estimate_ids.keys()):
        df.loc[df['estimate_name'] == key, 'estimate_id'] = estimate_ids[key]
    df.drop('estimate_name', axis=1, inplace=True)
    assert -1 not in df.estimate_id.unique(), (
        "Not all estimate types were mapped sucessfully.")

    df = convert_to_int(df)

    pre = df.columns
    df = df[final_db_cols]
    print("look at this diff {}".format(set(pre) - set(df.columns)))

    assert (df['estimate_id'] > 0).all(), (
        "Some estimate IDs not properly added")

    return df


def save(done, df, run_id):
    """
    Saves a copy of the final data and writes data to final data location.

    :param loc: flag to indicate if writing bundles to 'work' or 'test'
    directories
    """
    done = convert_to_int(done)


    print("starting at {}".format(datetime.datetime.today().strftime("%X")))


    done.to_hdf(r"FILEPATH".format(
        run_id), key='df', complib='blosc', complevel=5, mode='w')
    done.to_csv(r"FILEPATH".format(
        run_id), index=False, encoding='utf-8')

    filepath = "FILEPATH".format(
        run_id)
    print("Saving to {}...".format(filepath))
    df.to_csv(filepath, index=False, encoding='utf-8', na_rep='NULL')

    print("finished at {}\nFinal file located at {}".format(
        datetime.datetime.today().strftime("%X"), filepath))
    return done, df


def inspect(self, done):
    """
    Reads the final data and allows user to make final inspections manually.
    """
    done[done.cases_corrected > done.sample_size].shape[0] / \
        float(done.shape[0]) * 100
    done[done.cases_uncorrected > done.sample_size].shape[0] / \
        float(done.shape[0]) * 100
    done[['year_start', 'location_id']].drop_duplicates().shape
    done[['age_start', 'age_end', 'age_demographer']].drop_duplicates()
    done.nid.unique()
    cirr = done[done.bundle_id == 131].copy()
    cirr = cirr.rename(columns={"cases_corrected": "cases"})
    cirr[cirr.cases > cirr.sample_size].shape








def Outpatient(run_id, gbd_round_id, decomp_step, run_icpc):
    done, df = run_outpatient(
        run_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
        run_icpc=run_icpc)


    icpc = get_icpc(run_id)
    assert set(df.columns) == set(
        icpc.columns), "Columns must be identical before concatenating."
    df = pd.concat([df, icpc], sort=False, ignore_index=True)

    done, df = save(done, df, run_id)

    return df
