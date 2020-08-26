import pandas as pd
import numpy as np
import getpass
import sys
import warnings
import glob
from db_queries import get_covariate_estimates, get_cause_metadata
from db_tools.ezfuncs import query

user = getpass.getuser()
dirs = ['Functions', 'Mapping']
[sys.path.append("FILEPATH".format(user, d)) for d in dirs]
import hosp_prep
import bundle_check
import clinical_mapping

decomp_step = sys.argv[1]
run_id = sys.argv[2].replace("\r", "")




convert_sgp_data_to_otp_estimate = True




clinical_decomp_step = 'clinicalstep4refresh3'

types = {"FILEPATH": 17,
         "FILEPATH": 17}

BASE = ("FILEPATH"
        "FILENAME".format(run_id))


def get_asfr(df, decomp_step):
    """
    The Stata script which applied the maternal adjustment is no longer being used
    We must now do this simple adjustment in Python. It's merely the sample size * asfr
    then cases / sample size
    """
    locs = df['location_id'].unique().tolist()
    age_groups = df['age_group_id'].unique().tolist()
    years = df['year_start'].unique().tolist()

    asfr = get_covariate_estimates(covariate_id=13, decomp_step=decomp_step,
                                   sex_id=2,
                                   age_group_id=age_groups,
                                   location_id=locs,
                                   year_id=years)

    asfr = asfr[['location_id', 'year_id', 'age_group_id', 'sex_id', 'mean_value']]
    asfr.rename(columns={'year_id': 'year_start'}, inplace=True)

    return asfr

def merge_asfr(df, asfr):
    """
    merge the asfr covariate df onto the hospital data
    """
    merge_cols = asfr.drop('mean_value', axis=1).columns.tolist()

    pre = df.shape
    df = df.merge(asfr, how='left', on=merge_cols)
    assert pre[0] == df.shape[0], "no row change allowed"
    assert pre[1] + 1 == df.shape[1], "just one column added"

    return df

def apply_asfr(df, maternal_bundles, decomp_step):
    """
    takes all clinical data, splits out maternal data then
    pulls in the get and merge functions above onto just maternal
    data and adjusts the sample size by multiply by asfr and then
    re-calcuating the mean from pre calculated cases (IMPORTANT) / new sample size
    basically you want the pre-adjusted cases to be adjusted by a smaller sample size
    """
    pre = df.shape

    mat_df = df[df.bundle_id.isin(maternal_bundles)].copy()
    df = df[~df['bundle_id'].isin(maternal_bundles)]

    asfr = get_asfr(df=mat_df, decomp_step=decomp_step)
    mat_df = merge_asfr(df=mat_df, asfr=asfr)


    mat_df['cases'] = mat_df['mean'] * mat_df['sample_size']


    mat_df['sample_size'] = mat_df['sample_size'] * mat_df['mean_value']


    mat_df['mean'] = mat_df['cases'] / mat_df['sample_size']

    mat_df.drop(['mean_value', 'cases'], axis=1, inplace=True)

    df = pd.concat([df, mat_df], sort=False, ignore_index=True)

    assert pre[0] == df.shape[0], "rows changed somehow"
    assert pre[1] == df.shape[1], "cols changed"

    return df

def extract_bundle(df):
    """
    The acause column in the Stata outputs is actually acause and bundle_id
    """
    df['bundle_id'] = df.acause.str.split("
    return df

def match_clinical_cols(df):
    """
    Renames columns from claims procesing to upload column names
    """
    rename_dict = {'year': 'year_start', 'age': 'age_start',
                   'sex': 'sex_id', 'NID': 'nid', 'cf_final': 'mean'}
    df.rename(columns=rename_dict, inplace=True)

    df['year_end'] = df['year_start']
    return df

def get_age_groups(df):
    """
    Replaces age_start and age_end with age_group_id
    """
    df['age_end'] = df['age_start'] + 5
    warnings.warn("This is will need to be changed for neonatal groups!")
    df.loc[df['age_start'] == 0, 'age_end'] = 1
    df.loc[df['age_start'] == 1, 'age_end'] = 5
    df.loc[df['age_start'] == 95, 'age_end'] = 125
    df = hosp_prep.group_id_start_end_switcher(df, remove_cols=True)
    return df

def to_int(df):
    """
    Casts certain columns to integer datatype
    """
    for col in ['nid', 'location_id']:
        df[col] = df[col].astype(int)

    return df

def get_maternal_bundles(gbd_round_id):
    """
    Pulls all the bundle_ids associated with maternal causes, based on
    the maternal parent cause_id 366 in the

    Note: This will cause issues if the claims and inpatient prep processes
    diverge for maternal bundles
    """

    causes = get_cause_metadata(cause_set_id=9, gbd_round_id=gbd_round_id)


    condition = (causes.path_to_top_parent.str.contains(",366,")) | (causes.cause_id == 366)

    maternal_causes = causes[condition].copy()


    maternal_cause_list = maternal_causes['cause_id'].unique().tolist()

    maternal_cause_list = ", ".join([str(i) for i in maternal_cause_list])


    bundle_cause = query("SQL".format(maternal_cause_list), conn_def='epi')

    assert len(bundle_cause.cause_id.unique()) ==\
        len(maternal_causes['cause_id'].unique().tolist()),\
        "Lenth mismatch - it's possible data is missing."

    assert bundle_cause.shape[0] > 0, "There are no maternal bundles to adjust"

    maternal_bundles = bundle_cause.bundle_id.unique().tolist()

    assert len(maternal_bundles) > 0, "No maternal bundles in list"

    return maternal_bundles


def get_single_floor(run_id, bundle_id, estimate_id, measure):
    """
    instead of creating a hotfix that pulls in every single combo this function just gets a single one from drive
    Params:
        run_id: (int) identifies a run of clinical data to pull from
        bundle_id: (int) identifies which bundle_id to get
        estimate_id: (int) identifies which estimate_id to use
        measure: (str) 'inc' or 'prev' identifies which claims folder to use
    Returns:
        floor: (float) the lowest non-zero estimate pre-noise reduction
    """
    base = "FILEPATH".format(r=run_id)

    if estimate_id == 21:
        est = 'all'
    elif estimate_id == 17:
        est = 'all_inp'
    else:
        assert False, "The estimate id {} isn't recognised, only 17 and 21 are".format(estimate_id)

    df = pd.read_stata("FILEPATH".format(b=base, m=measure, e=est, bun=bundle_id))
    floor = df.loc[df['cf_raw'] > 0, 'cf_raw'].min()

    assert floor != 0, "floor can't be zero"
    assert floor < 1, "floor is 1? that can't be correct"

    return floor

def claims_read_helper(run_id, claim_source):
    """The claims data is stored in different formats unfortunately. This reads them in by source"""
    path = "FILEPATH".format(run_id)
    if claim_source == 'SGP':
        df = pd.read_csv("FILEPATH".format(path))
    elif claim_source == 'TWN':
        print("This might break because twn was only prepped for 1 run")
        df = pd.read_csv("FILEPATH".format(path))
    elif claim_source == 'POL':

        files = glob.glob("FILEPATH".format(path))
        assert len(files) == 2, "We expect 2 and exactly 2 poland files"
        df = pd.concat([pd.read_csv(f) for f in files], sort=False, ignore_index=True)
    elif claim_source == 'RUS':
        df = pd.read_csv(f"FILEPATH")
    else:
        assert False, "{} is not recognized as a claims source".format(claim_source)

    return df

def aggregate_claims_sources(df, gbd_round_id, clinical_decomp_step, run_id):
    """
    Immediately before writing to drive we should aggregate the other claims sources together with Marketscan
    Params:
        df (pd.DataFrame) with only Marketscan data in it
        gbd_round_id (int) which gbd round we're working with, standardized by central comp
        clinical_decomp_step (str) identifies which step we're writing claims data for. Note this
                                   is different then the central decomp step
        run_id (int) good ol clinical run_id
    Returns:
        df (pd.DataFrame) with Marketscan and other claims sources concatted to it
    """
    warnings.warn("Prepping only SGP for decomp 4, we've removed Poland")
    source_dict = {6: {'clinicalstep4': ['SGP'],
                       'clinicalstep2': ['SGP', 'TWN'],
                       'clincalstep4refresh2' : ['POL', 'RUS'],
                       'clinicalstep4refresh3' : ['POL']}}


    assert gbd_round_id in list(source_dict.keys()), "gbd round not recognized"
    assert clinical_decomp_step in list(source_dict[gbd_round_id].keys()), "clinical step not recognized"

    maternal_bundles = get_maternal_bundles(gbd_round_id)

    for claim_source in source_dict[gbd_round_id][clinical_decomp_step]:
        print (f'Claim Source: {claim_source}')
        tmp = claims_read_helper(run_id, claim_source)
        warnings.warn("The columns {} will be lost".format(set(tmp.columns) - set(df.columns)))
        tmp = tmp[df.columns.tolist()]
        assert not set(tmp.columns).symmetric_difference(set(df.columns)), "No we don't want different columns"

        if tmp[tmp['bundle_id'].isin(maternal_bundles)].shape[0] > 0:
            warnings.warn("we're applying the asfr adjustment for {} maternal data. Bundles are pulled from cause metadata".format(claim_source))
            tmp = apply_asfr(df=tmp, maternal_bundles=maternal_bundles, decomp_step=decomp_step)
        else:

            pass
        df = pd.concat([df, tmp], sort=False, ignore_index=True)
    if clinical_decomp_step == 'clincalstep4refresh2':
        df = df[~df.nid.isin([244370, 244371, 336850, 336849,
                            336848, 336847, 408680, 244369])]
    return df

def final_test_claims(df, clinical_decomp_step):
    """check for expected bundles and square by source"""

    failures = []



    missing_bundle_dict = bundle_check.test_refresh_bundle_ests(df=df, pipeline='claims', prod=False)


    if clinical_decomp_step == 'clinicalstep4':
        if set(missing_bundle_dict.keys()).\
            symmetric_difference(set([181, 195, 196, 198, 213, 766, 6113, 6116, 6119, 6122, 6125])):
            failures.append(missing_bundle_dict)




    if clinical_decomp_step == 'clincalstep4refresh2':
        missing_bun = [181, 195, 196, 198, 213, 825, 6113, 6116, 6119, 6122,
                      6125, 6083, 451, 3020, 327, 176, 3200, 207, 6110, 82]
        if set(missing_bundle_dict.keys()).\
            symmetric_difference(set(missing_bun)):
            failures.append(missing_bundle_dict)



    assert not failures, "Hey these tests failed: {}".format(failures)

    return "Claims data looks like it's ready for upload"

def convert_sgp_bundles(df):
    """See first few lines of script but we're making a very specific bundle based adjust for a modeler
"""
    bundles = [138, 139, 141, 142, 143, 3033]

    bundle_table = query("SQL".format(tuple(bundles)), conn_def='epi')
    assert (bundle_table['estimate_id'] == 21).all(), "Some bundles estimates aren't 21, meaning they'll lose data!"
    warnings.warn("Do NOT pass this function subnational data from Singapore")
    assert (df.loc[(df['bundle_id'].isin(bundles)) & (df['location_id']==69), 'estimate_id'] == 17).all(), "Why were SGP estimates not 17?"
    df.loc[(df['bundle_id'].isin(bundles)) & (df['location_id']==69), 'estimate_id'] = 21
    return df

def claims_main(run_id, gbd_round_id, decomp_step, clinical_decomp_step, convert_sgp_data_to_otp_estimate):
    """
    Formats MARKETSCAN for upload
    Then appends on the formatted claims sources that aren't MS so that our entire set of
    claims data is prepped in a single file for upload
    """

    cols = ['year', 'age', 'sex', 'location_id', 'acause',
            'sample_size', 'NID', 'cf_final', 'variance']

    df_list = []
    for key, value in list(types.items()):
        filepath = "{}{}".format(BASE, key)
        print("Reading {}...".format(filepath))
        back = pd.read_stata(filepath)
        print("Formatting {}...".format(key))

        if 'ALL' in key:
            dx_id = 3
            source_type_id = 17
        elif 'INP' in key:
            dx_id = 1
            source_type_id = 10
        else:
            assert False, "Keys are wrong; Neither 'ALL' nor 'INP' in key"

        if 'incidence' in key:
            measure = 'inc'
        elif 'prevalence' in key:
            measure = 'prev'
        else:
            assert False, "Keys are wrong; Neither 'incidence' nor 'prevalence' in key"

        df = back[cols].copy()
        df = extract_bundle(df)
        df.drop('acause', axis=1, inplace=True)

        df['estimate_id'] = value
        df['diagnosis_id'] = dx_id
        df['source_type_id'] = source_type_id
        df['representative_id'] = 4

        df = match_clinical_cols(df)

        df = get_age_groups(df)


        for b in df['bundle_id'].unique():
            floor = get_single_floor(run_id=run_id, bundle_id=b, estimate_id=value, measure=measure)
            df.loc[(df['bundle_id'] == b) & (df['mean'] < floor), 'mean'] = 0

        warnings.warn("we're applying the asfr adjustment for MARKETSCAN maternal data. Bundles are pulled from cause metadata")
        maternal_bundles = get_maternal_bundles(gbd_round_id)
        if df[df['bundle_id'].isin(maternal_bundles)].shape[0] > 0:
            df = apply_asfr(df=df, maternal_bundles=maternal_bundles, decomp_step=decomp_step)
        else:

            pass

        df = to_int(df)

        df_list.append(df)
    print("Done processing each file. Continuing...")

    df = pd.concat(df_list, ignore_index=True, sort=False)
    df = df.drop("variance", axis=1)

    print("Applying age sex restrictions...")
    df = clinical_mapping.apply_restrictions(df, age_set='age_group_id',
                                             cause_type='bundle',
                                             map_version='current', prod=True)


    df = aggregate_claims_sources(df, gbd_round_id, clinical_decomp_step, run_id)


    df['run_id'] = run_id
    df['upper'] = pd.np.nan
    df['lower'] = pd.np.nan

    pre = df.columns
    ordered_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                    'year_end', 'bundle_id', 'estimate_id', 'diagnosis_id',
                    'source_type_id', 'nid', 'representative_id',
                    'mean', 'lower', 'upper', 'sample_size', 'run_id']
    sym_diff = set(pre).symmetric_difference(ordered_cols)
    assert_msg = """Losing some columns. The symmetric difference is \n{}""".\
        format(sym_diff)
    assert not sym_diff, assert_msg

    if convert_sgp_data_to_otp_estimate:
        df = convert_sgp_bundles(df)

    final_test_claims(df, clinical_decomp_step)


    out_dir = "FILEPATH"\
              "FILENAME".format(run_id)
    print("Saving Claims file...")
    df.to_csv(out_dir + "FILEPATH", index=False, na_rep="NULL")
    print("Saved to {}.".format(out_dir + "FILEPATH"))

    return

def inp_main():
    """
    Formats Inpatient data for upload
    """
    inp_path = "FILEPATH"\
               "FILEPATH".format(run_id)
    print("Reading {}...".format(inp_path))

    df = pd.read_hdf(inp_path)

    print("Formatting {}".format(inp_path))
    cols = df.columns

    if 'diagnosis_id' not in cols:
        print("diagnosis_id wasn't in the columns, adding...")
        df['diagnosis_id'] = 1

    if 'source_type_id' not in cols:

        df['source_type_id'] = 10

    for drop_col in ['estimate_type', 'measure', 'source', 'haqi_cf',
                     'correction_factor']:
        if drop_col in cols:
            df.drop(drop_col, axis=1, inplace=True)


    df['run_id'] = run_id

    pre = df.columns
    final_b_cols = ['age_group_id', 'sex_id', 'location_id', 'year_start',
                    'year_end', 'bundle_id', 'estimate_id', 'diagnosis_id',
                    'source_type_id', 'nid', 'representative_id',
                    'mean', 'lower', 'upper', 'sample_size', 'run_id']
    sym_diff = set(pre).symmetric_difference(final_b_cols)
    assert_msg = """Losing some columns. The symmetric difference is \n{}""".\
        format(sym_diff)
    assert not sym_diff, assert_msg
    df = df[final_b_cols]

    print("Saving inpatient file...")
    out_dir = "FILEPATH"\
              "FILENAME".format(run_id)
    df.to_csv(out_dir + "FILEPATH", index=False, na_rep="NULL")
    print("Saved to {}.".format(out_dir + "FILEPATH"))

    return

if __name__ == '__main__':

    claims_main(run_id=run_id, gbd_round_id=6,
                decomp_step=decomp_step,
                clinical_decomp_step=clinical_decomp_step,
                convert_sgp_data_to_otp_estimate=convert_sgp_data_to_otp_estimate)
    inp_main()
