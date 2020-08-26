"""
A set of functions to test and validate bundles present in clinical data.

Most of the functions trust clinical.bundle as the definitive source of bundle
information because we use it to refresh. An additional test might be between
the bundle map and the clinical.bundle table

There's a lot of missingness within our data by location/year/nid/etc but
that's out of the scope of this module

The purpose of this module is to check bundles and estimate_ids
"""
import sys
from getpass import getuser
from db_tools.ezfuncs import query
import pandas as pd
import numpy as np

for p in ["FILEPATH", "FILEPATH", "FILEPATH"]:
    prep_path = "FILEPATH".format(getuser(), p)
    sys.path.append(prep_path)
import clinical_mapping


def confirm_bundles_exist(df):
    """
    Pretty simple test to make sure all bundle IDs exist in epi.bundle.bundle
    """
    q = "SQL".format(tuple(df.bundle_id.unique()))
    epi_bundles = query(q, conn_def='epi')
    epi_bundles = set(epi_bundles.bundle_id)


    df_bundles = set(df.bundle_id.unique())

    missing_from_db = df_bundles - epi_bundles
    assert not missing_from_db, "bundles {} are missing from the database!".format(missing_from_db)
    missing_from_data = epi_bundles - df_bundles
    assert not missing_from_data, "bundles {} are missing from the data!! how is that even possible?!".format(missing_from_data)

    return "DF bundles exist in the table"

def get_bundle_estimate_for_upload():
    """This table is proving ever more important"""
    estimate_q = "SQL"
    df = query(estimate_q, conn_def='epi')
    confirm_bundles_exist(df)

    return df

def get_estimate_id():
    """This table is proving ever more important"""
    estimate_q = "SQL"
    df = query(estimate_q, conn_def='epi')

    return df

def test_refresh_bundle_ests(df, pipeline, prod=True):
    """
    Use the clinical.bundle table to identify very specific missingness by
    bundle_id and estimate_id

    Params:
        df : (pd.DataFrame) clinical data in a pandas dataframe
        pipeline : (str) identifies which pipeline to check b/c estimate_ids
                       are intrinsically tied to a pipeline
                       options are ['all', 'inp', 'claims', 'otp']
        prod : (bool) if True the function will break when missing bundle estimates
                      are found. If False it will return the missing data

    """
    if isinstance(pipeline, str):
        pipeline = [pipeline]
    assert not set(['bundle_id', 'estimate_id']) - set(df.columns),\
        "This function needs bundles and estimates"
    acceptable_pipelines = ['all', 'inp', 'claims', 'otp']
    assert not set(pipeline) - set(acceptable_pipelines),\
        "{} is not acceptable. The only acceptable pipeline values are {}".format(pipeline, acceptable_pipelines)


    pipeline_est = {'inp': list(range(1, 10, 1)) + [22],
                    'otp': [11, 23, 24],
                    'claims': [17, 21],
                    'all': list(range(1, 10, 1)) + [11, 17, 21, 22, 23, 24]}
    est_to_keep = []
    for p in pipeline:
        est_to_keep += pipeline_est[p]
    assert len(est_to_keep) > 0, "there are no estimates to keep"

    mdf = df[['bundle_id', 'estimate_id']].drop_duplicates().copy()
    mdf['present_in_data'] = True


    edf = get_bundle_estimate_for_upload()

    edf = edf[edf.estimate_id.isin(est_to_keep)]
    assert len(edf) > 0, "There are no bundles to compare against"

    edf = edf.merge(mdf, how='left', on=['bundle_id', 'estimate_id'])

    missing_bundle_ests = {}
    if edf.present_in_data.isnull().any():
        tmp = edf[edf.present_in_data.isnull()]
        for b in tmp.bundle_id.unique():
            missing_bundle_ests[b] = tmp.loc[tmp.bundle_id == b, 'estimate_id'].unique().tolist()
        if prod:
            assert False, "There are missing bundle estimates {}".format(missing_bundle_ests)
        else:
            return missing_bundle_ests
    else:
        pass

    return "This df seems to have all estimates present"

def hardcode_test_clinical_bundle_est():
    """
    Test the clinical.bundle table to idenfity any incoherent mappings, ie recieving multiple
    types of data from 1 pipeline, receiving inp and otp data, etc.
    """
    pipeline_est = {'inp': list(range(1, 10, 1)) + [22],
                    'otp': [11, 23, 24],
                    'claims': [17, 21]}
    logical_est = {'inpatient_only': [1, 2, 3, 6, 7, 8, 17],
                   'out_and_in': [4, 9, 22, 11, 21, 23, 24]}
    edf = get_bundle_estimate_for_upload()

    failures = {}
    for b in edf.bundle_id.unique():
        tmp = edf[edf.bundle_id == b]
        for an_est in tmp.estimate_id:
            if an_est in logical_est['inpatient_only']:
                if set(tmp.estimate_id) - set(logical_est['inpatient_only']):
                    failures[b] = "Bundle {} contains both inp only and inp+otp estimates".format(b)
            if an_est in logical_est['out_and_in']:
                if set(tmp.estimate_id) - set(logical_est['out_and_in']):
                    failures[b] = "Bundle {} contains both inp only and inp+otp estimates".format(b)
        for pipe, ests in list(pipeline_est.items()):
            if len(ests) - len(set(ests) - set(tmp.estimate_id)) > 1:
                k = "{}_{}".format(b, pipe)
                failures[k] = "More than 1 estimate is present from the same pipeline for bundle {}".format(b)

    assert not failures, failures
    return "The table passed both tests"

def create_est_id_pipeline_map(df):
    """Replace the hardcoded estimate_ids we used before. Uses string matching to
    identify logical type and pipeline"""
    name_split = df.estimate_name.str.split("-", expand=True)
    df = pd.concat([df, name_split], axis=1, sort=False)

    pipeline_est = {'inp': df.loc[df[0] == 'inp' ,'estimate_id'].unique().tolist(),
                    'otp': df.loc[df[0] == 'otp' ,'estimate_id'].unique().tolist(),
                    'claims': df.loc[df[0] == 'claims' ,'estimate_id'].unique().tolist()}

    df['inp_only'] = True
    df.loc[df['estimate_name'].str.contains('otp|cf3|inj'), 'inp_only'] = False
    logical_est = {'inpatient_only': df.loc[df['inp_only'] == True, 'estimate_id'].unique().tolist(),
                   'out_and_in': df.loc[df['inp_only'] == False, 'estimate_id'].unique().tolist()}

    return pipeline_est, logical_est

def test_clinical_bundle_est():
    """
    Test the clinical.bundle table to idenfity any incoherent mappings, ie recieving multiple
    types of data from 1 pipeline, receiving inp and otp data, etc.
    """

    edf = get_bundle_estimate_for_upload()

    est_ids = get_estimate_id()
    pipeline_est, logical_est = create_est_id_pipeline_map(est_ids)

    failures = {}
    warns = {}
    for b in edf.bundle_id.unique():
        tmp = edf[edf.bundle_id == b]
        for an_est in tmp.estimate_id:
            if an_est in logical_est['inpatient_only']:
                if set(tmp.estimate_id) - set(logical_est['inpatient_only']):
                    failures[b] = "Bundle {} contains both inp only and inp+otp estimates".format(b)
            if an_est in logical_est['out_and_in']:
                if set(tmp.estimate_id) - set(logical_est['out_and_in']):
                    failures[b] = "Bundle {} contains both inp only and inp+otp estimates".format(b)


        for pipe, ests in pipeline_est.items():
            if len(ests) - len(set(ests) - set(tmp.estimate_id)) > 1:
                k = "{}_{}".format(b, pipe)
                warns[k] = "More than 1 estimate is present from the same pipeline for bundle {}".format(b)

    assert not failures, failures
    print("These are probably fine if they're caused by ICPC (est 25), but review in case", warns)
    return "The table passed both tests"


def get_map_data(map_version, append_unmapped_bundles):
    """
    For a given map version, return the bundles which are present in it, along with more detailed info
    like which code systems a bundle codes to, and the min length of ICD codes it requires

    Params:
            map_version: (int) use in sql where clause to get specific map version
            append_unmapped_bundles: (bool) should the parent injuries and maternal ratios be appended on?

    Returns:
            df : a pd.dataframe of bundle IDs present in our mapping table
            bundle_icg_detail: a more detailed pd.dataframe containing icg_id and code system. We can
                               use this to determine when bundles aren't expected by code system
                               and the minimum cause_code (just icd9/10) that an icg maps to
    """

    bundle_df = clinical_mapping.get_clinical_process_data('icg_bundle', map_version=map_version)
    cc_icg_df = clinical_mapping.get_clinical_process_data('cause_code_icg', map_version=map_version)

    min_cc_len = cc_icg_df.copy()
    min_cc_len['min_cc_length'] = min_cc_len['cause_code'].apply(len)
    min_cc_len = min_cc_len.query("code_system_id in (1, 2)").groupby('icg_id').agg({'min_cc_length': 'min'}).reset_index()
    pre_cc = cc_icg_df.shape
    cc_icg_df = cc_icg_df.merge(min_cc_len, how='left', on=['icg_id'])
    assert pre_cc[0] == cc_icg_df.shape[0]

    cc_icg_df = cc_icg_df[['code_system_id', 'icg_id', 'min_cc_length', 'map_version']].drop_duplicates()

    df = bundle_df.merge(cc_icg_df, how='left', on=['icg_id', 'map_version'])

    if append_unmapped_bundles:
        unmapped = [264, 269, 270, 272, 275, 276, 362,
                    6113, 6116, 6119, 6122, 6125]
        tmp = pd.DataFrame({'bundle_id': unmapped})
        df = pd.concat([df, tmp], sort=False, ignore_index=True)

    bundle_icg_detail = df.copy()

    df = df[['bundle_id', 'map_version']].drop_duplicates()
    if append_unmapped_bundles:
        assert df.shape[0] == bundle_df.bundle_id.unique().size + len(unmapped)
    else:
        assert df.shape[0] == bundle_df.bundle_id.unique().size

    df['present_in_map'] = True


    confirm_bundles_exist(df)
    confirm_bundles_exist(bundle_icg_detail)

    return df, bundle_icg_detail

def read_clinical_data(filepath):
    """
    Checks the file extension of a filepath and uses the appropriate
    Pandas function to read in data
    """
    if filepath[-3:] == 'csv':
        df = pd.read_csv(filepath)
    elif filepath[-2:] == 'H5':
        df = pd.read_hdf(filepath)
    elif filepath[-3:] == 'dta':
        df = pd.read_stata(filepath)
    else:
        assert False, "don't know how to read this"

    return df

def get_claims_data(filepath, us_locs, remove_bad_sgp_bundle=True):
    """
    Get claims data from a given filepath. The filepath should be
    the same as what will be passed to the uploader script for upload
    """
    df = read_clinical_data(filepath)
    df = df[['bundle_id', 'location_id', 'nid', 'estimate_id']].drop_duplicates()
    df['pipeline'] = 'claims'
    if remove_bad_sgp_bundle:
        df = df[df['bundle_id'] != 2872]

    df['source'] = np.nan
    df.loc[df['location_id'] == 8, 'source'] = 'TWN_CLAIMS'
    df.loc[df['location_id'] == 69, 'source'] = 'SGP_CLAIMS'
    df.loc[df['location_id'].isin(us_locs), 'source'] = 'MARKETSCAN'
    assert df['source'].isnull().sum() == 0

    confirm_bundles_exist(df)
    df['present_in_data'] = True
    return df

def get_inp_data(filepath):
    """Get inpatient data from a given filepath. The filepath should be
    the same as what will be passed to the uploader script for upload
    """
    df = read_clinical_data(filepath)
    df = df[['bundle_id', 'source', 'location_id', 'nid', 'estimate_id']].drop_duplicates()
    df['pipeline'] = 'inp'

    confirm_bundles_exist(df)
    df['present_in_data'] = True
    return df

def get_otp_data(filepath, us_locs):
    """
    Get outpatient data from a given filepath. The filepath should be
    the same as what will be passed to the uploader script for upload
    """
    df = read_clinical_data(filepath)
    df = df[['bundle_id', 'location_id', 'nid', 'estimate_id']].drop_duplicates()
    df['pipeline'] = 'otp'


    df['source'] = np.nan
    df.loc[df['location_id'] == 93, 'source'] = 'SWE_OTP'

    df.loc[df['location_id'].isin(us_locs), 'source'] = 'USA_OTP'
    assert df['source'].isnull().sum() == 0

    confirm_bundles_exist(df)
    df['present_in_data'] = True
    return df

def get_all_data(run_id, map_version, append_unmapped_bundles, keep_only_refresh_data):
    """
    Uses the get functions above to pull in all the data and concats it into 1 df
    """
    map_df, map_detail = get_map_data(map_version, append_unmapped_bundles)

    us_locs = [102, 523, 525, 526, 527, 528, 529, 532, 533, 536,
               537, 538, 539, 540, 541, 543, 544, 545, 546, 547,
               548, 553, 555, 556, 558, 559, 560, 561, 563, 565,
               566, 567, 569, 570, 572, 573, 534, 549, 551, 531,
               568, 562, 554, 564, 530, 542, 550, 571, 557, 524,
               535, 552]

    print("warning we're hardcoding some fileptahs for now!!!")
    inp_path = "FILEPATH"\
               "FILEPATH".format(run_id)
    inp = get_inp_data(inp_path)
    otp_path = "FILEPATH"\
               "FILEPATH".format(run_id)
    otp = get_otp_data(otp_path, us_locs)

    claims_path = "FILEPATH"\
                  "FILEPATH".format(run_id)
    claims = get_claims_data(claims_path, us_locs)

    df = pd.concat([inp, otp, claims], sort=False, ignore_index=True)



    estimate_df = get_bundle_estimate_for_upload()[['bundle_id', 'estimate_id']]

    df = reshape_bundle_data(df, map_df, estimate_df.estimate_id.unique())

    if keep_only_refresh_data:
        df = df.merge(estimate_df, how='inner',
                      on=['bundle_id', 'estimate_id'])


    pre = df.shape
    bnames = query("SQL", conn_def='epi')
    df = df.merge(bnames, how='left', on='bundle_id')
    assert df.shape[0] == pre[0], "Merging on names changed the row count which we don't like"
    return df, map_detail

def reshape_bundle_data(df, map_df, estimate_ids):
    """
    The input has our pseudo-hierarchy as different columns
    we want this split out long
    The logic of the hierarchy breaks down after level 3
    nids are sometimes MORE detailed than just locations
    but usually not
    """

    level_dict = {1: 'all', 2: 'pipeline', 3: 'source'}

    df_list = []
    for lev, lev_name in list(level_dict.items()):
        if lev_name == 'all':
            tmp = df[['bundle_id', 'present_in_data', 'estimate_id']].copy().drop_duplicates()
            tmp['hierarchy_detail'] = 'all'
        else:
            tmp = df[['bundle_id', lev_name, 'present_in_data', 'estimate_id']].copy().drop_duplicates()
            tmp.rename(columns={lev_name: 'hierarchy_detail'}, inplace=True)


        for htype in tmp['hierarchy_detail'].unique():
            tmp2 = tmp[tmp['hierarchy_detail'] == htype].copy()

            for est in estimate_ids:

                tmp3 = tmp2[tmp2['estimate_id'] == est].copy()


                tmp3 = tmp3.merge(map_df, how='outer', on='bundle_id')


                pre = tmp3.shape
                tmp3['hierarchy_level'] = lev
                tmp3['hierarchy_name'] = lev_name
                tmp3.loc[tmp3['hierarchy_detail'].isnull(), 'hierarchy_detail'] = htype
                tmp3.loc[tmp3['estimate_id'].isnull(), 'estimate_id'] = est
                assert pre[0] == tmp3.shape[0]
                df_list.append(tmp3)
                del tmp3
            del tmp2
        del tmp

    res = pd.concat(df_list, sort=False, ignore_index=True)
    res.loc[res['present_in_data'].isnull(), 'present_in_data'] = False
    res.loc[res['present_in_map'].isnull(), 'present_in_map'] = False
    return res

def get_final_estimates_bundle_data(run_id):
    """
    The functions above will prep the data to check for bundles before upload, but it's
    possible that something could be lost during/after upload, so let's pull the actual
    data in the db and check it.
    """
    estimate_df = get_bundle_estimate_for_upload()

    df_list = []
    missing_dict = {}
    for bundle in estimate_df.bundle_id.unique():
        ests = tuple(estimate_df.loc[estimate_df['bundle_id'] == bundle, 'estimate_id'].unique())
        if len(ests) == 1:
            est_w = "estimate_id = {}".format(ests[0])
        else:
            est_w = "estimate_id in {}".format(ests)
        q = """SQL""".format(r=run_id, b=bundle, est=est_w)
        tmp = query(q, conn_def='epi')
        whole_diff = set(tmp.estimate_id.unique()).symmetric_difference(set(ests))
        missing_in_data = set(ests) - set(tmp.estimate_id.unique())
        if missing_in_data:
            missing_dict[bundle] = list(missing_in_data)

        df_list.append(tmp)

    df = pd.concat(df_list, sort=False, ignore_index=True)

    return df, missing_dict


def test_missing_bundles(df, levels, exemption_dict, keep_only_refresh_data):
    """check df of missing data for given levels and identify missing bundles"""

    estimate_df = get_bundle_estimate_for_upload()

    if keep_only_refresh_data:

        pipeline_est = {'inp': list(range(1, 10, 1)) + [22],
                        'otp': [11, 23, 24],
                        'claims': [17, 21]}
        df_list = []
        for key, value in list(pipeline_est.items()):
            mask = "(df['hierarchy_detail'] == '{k}') & (df['estimate_id'].isin({v}))".format(v=value, k=key)
            x = df[eval(mask)].copy()
            print("appending on {} with shape {}".format(key, x.shape))
            df_list.append(x)

        df = pd.concat([df[df['hierarchy_level'] != 2]] + df_list, sort=False, ignore_index=True)

    missing = {}
    for lev in levels:
        tmp = df.query("hierarchy_level == @lev")
        for deet in tmp['hierarchy_detail'].unique():
            mask = "(tmp['present_in_data'] == False) & (tmp['hierarchy_detail'] == deet)"
            missing['level{}_{}'.format(lev, deet)] = tmp.loc[eval(mask), 'bundle_id'].unique().tolist()

    truly_missing = {}
    for key, value in list(missing.items()):
        if key in list(exemption_dict.keys()):

            to_append = list(set(value) - set(exemption_dict[key]))
        else:

            to_append = value
        if to_append:
            truly_missing[key] = to_append

    assert not truly_missing, "These bundles are missing {}".format(truly_missing)

    return "everything looks fine for levels {}".format(levels)
