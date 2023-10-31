
import pandas as pd
import numpy as np
import sys
import warnings
import glob
from db_queries import get_covariate_estimates, get_cause_metadata
from db_tools.ezfuncs import query

from clinical_info.Functions import hosp_prep, bundle_check
from clinical_info.Mapping import clinical_mapping
from clinical_info.Odd_jobs.adhoc_pipeline_fixes import emr_replacement
from clinical_info.Mapping import clinical_mapping as cm
from clinical_info.Database.bundle_relationships import relationship_methods as br


def get_asfr(mat_df, gbd_round_id, decomp_step):
    """
    The Stata script which applied the maternal adjustment is no longer being used
    We must now do this simple adjustment in Python. It's merely the sample size * asfr
    then cases / sample size
    """
    locs = mat_df['location_id'].unique().tolist()
    age_groups = mat_df['age_group_id'].unique().tolist()
    years = mat_df['year_start'].unique().tolist()

    asfr = get_covariate_estimates(covariate_id=13, decomp_step=decomp_step, gbd_round_id=gbd_round_id,
                                   sex_id=2, age_group_id=age_groups, location_id=locs, year_id=years)

    asfr = asfr[['location_id', 'year_id',
                 'age_group_id', 'sex_id', 'mean_value']]
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


def apply_asfr(df, maternal_bundles, decomp_step, gbd_round_id):
    """
    takes all clinical data, splits out maternal data then
    pulls in the get and merge functions above onto just maternal
    data and adjusts the sample size by multiply by asfr and then
    re-calcuating the mean from pre calculated cases
    """
    pre = df.shape

    mat_df = df.loc[df.bundle_id.isin(maternal_bundles), :]
    df = df.loc[~df['bundle_id'].isin(maternal_bundles), :]

    asfr = get_asfr(mat_df=mat_df, decomp_step=decomp_step,
                    gbd_round_id=gbd_round_id)
    mat_df = merge_asfr(df=mat_df, asfr=asfr)

    # create new cases using existing mean and sample
    mat_df['cases'] = mat_df['mean'] * mat_df['sample_size']

    mat_df = mat_df.loc[mat_df.mean_value != 0, :]

    # reduce to live births
    mat_df['sample_size'] = mat_df['sample_size'] * mat_df['mean_value']

    # create new mean using previous cases and new sample size
    mat_df['mean'] = mat_df['cases'] / mat_df['sample_size']

    mat_df.drop(['mean_value', 'cases'], axis=1, inplace=True)

    df = pd.concat([df, mat_df], sort=False, ignore_index=True)

    assert pre[1] == df.shape[1], "cols changed"

    return df


def extract_bundle(df):

    df['bundle_id'] = df.acause.str.split("#", expand=True)[1].astype(int)
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

    df.loc[df['age_start'] == 0, 'age_end'] = 1
    df.loc[df['age_start'] == 1, 'age_end'] = 5
    df.loc[df['age_start'] == 95, 'age_end'] = 125
    df = hosp_prep.group_id_start_end_switcher(df, clinical_age_group_set_id=1, remove_cols=True)
    return df


def to_int(df):
    """
    Casts certain columns to integer datatype
    """
    for col in ['nid', 'location_id']:
        df[col] = df[col].astype(int)

    return df


def get_maternal_bundles(gbd_round_id):

    causes = get_cause_metadata(cause_set_id=9, gbd_round_id=gbd_round_id)
    condition = (causes.path_to_top_parent.str.contains(
        ",366,")) | (causes.cause_id == 366)

    maternal_causes = causes[condition].copy()

    # make list of maternal causes
    maternal_cause_list = maternal_causes['cause_id'].unique().tolist()
    # make it a string separated by commas for SQL query
    maternal_cause_list = ", ".join([str(i) for i in maternal_cause_list])

    # get bundle to cause map
    bundle_cause = query(QUERY.format(
        maternal_cause_list), conn_def='epi')

    assert len(bundle_cause.cause_id.unique()) ==\
        len(maternal_causes['cause_id'].unique().tolist()),\
        "Lenth mismatch - it's possible data is missing."

    assert bundle_cause.shape[0] > 0, "There are no maternal bundles to adjust"

    maternal_bundles = bundle_cause.bundle_id.unique().tolist()

    assert len(maternal_bundles) > 0, "No maternal bundles in list"

    return maternal_bundles


def get_single_floor(run_id, bundle_id, estimate_id, measure):
    """
    Params:
        run_id: (int) identifies a run of clinical data to pull from
        bundle_id: (int) identifies which bundle_id to get
        estimate_id: (int) identifies which estimate_id to use
        measure: (str) 'inc' or 'prev' identifies which claims folder to use
    Returns:
        floor: (float) the lowest non-zero estimate pre-noise reduction
    """
    base = FILEPATH.format(
        r=run_id)

    if estimate_id == 21:
        est = 'all'
    elif estimate_id == 17:
        est = 'all_inp'
    else:
        assert False, "The estimate id {} isn't recognised, only 17 and 21 are".format(
            estimate_id)

    df = pd.read_stata(
        FILEPATH.format(b=base, m=measure, e=est, bun=bundle_id))
    floor = df.loc[df['cf_raw'] > 0, 'cf_raw'].min()


    if (df.cf_raw == 0).all():
        floor = 0
    assert floor < 1, f"floor is 1? that can't be correct. floor = {floor}"

    return floor


def claims_read_helper(run_id, claim_source, gbd_round_id, decomp_step):

    path = FILEPATH.format(
        run_id)
    if claim_source == 'SGP':
        df = pd.read_csv("{}FILEPATH.format(path))
    elif claim_source == 'TWN':

        df = pd.read_csv("{}FILEPATH".format(path))
    elif claim_source == 'POL':
        # do a loop or something over all those bundle files
        files = glob.glob(f"{path}/FILEPATH")
        assert len(files) == 2, "We expect 2 and exactly 2 poland files"
        df = pd.concat([pd.read_csv(f)
                        for f in files], sort=False, ignore_index=True)
        if gbd_round_id == 7 and decomp_step == 'step2':
            pass


    elif claim_source == 'RUS':
        df = pd.read_csv(f"{path}FILEPATH")
    else:
        assert False, "{} is not recognized as a claims source".format(
            claim_source)

    return df


def aggregate_claims_sources(df, gbd_round_id, clinical_decomp_step, run_id, decomp_step, map_version="current"):
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
    source_dict = {6: {'clinicalstep4': ['SGP'],
                       'clinicalstep2': ['SGP', 'TWN'],
                       'clinicalstep4refresh2': ['POL', 'RUS'],
                       'clinicalstep4refresh3': ['POL']},
                   7: {'clinicalstep2': ['POL'],
                       'clinicalstep3': ['POL', 'SGP', 'TWN', 'RUS']}
                   }

    # confirm round and clinical step values
    assert gbd_round_id in list(source_dict.keys()), "gbd round not recognized"
    assert clinical_decomp_step in list(
        source_dict[gbd_round_id].keys()), "clinical step not recognized"

    maternal_bundles = get_maternal_bundles(gbd_round_id)
    # read in the data
    for claim_source in source_dict[gbd_round_id][clinical_decomp_step]:
        if claim_source == 'none':
            print("There will be no additionl claims data appended for this run")
            return df

        print(f'Claim Source: {claim_source}')
        tmp = claims_read_helper(
            run_id, claim_source, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
        warnings.warn("The columns {} will be lost".format(
            set(tmp.columns) - set(df.columns)))

        tmp = tmp[df.columns.tolist()]
        assert not set(tmp.columns).symmetric_difference(set(df.columns)),\
            "No we don't want different columns"

        if tmp[tmp['bundle_id'].isin(maternal_bundles)].shape[0] > 0:
            warnings.warn(
                "we're applying the asfr adjustment for {} maternal data. Bundles are pulled from cause metadata".format(claim_source))

            print("Applying age sex restrictions...")

            tmp = clinical_mapping.apply_restrictions(tmp, age_set='age_group_id',
                                                      cause_type='bundle',
                                                      map_version=map_version, prod=True)

            tmp = apply_asfr(
                df=tmp, maternal_bundles=maternal_bundles, decomp_step=decomp_step, gbd_round_id=gbd_round_id)
        else:
            # there's no maternal data for this one
            pass
        df = pd.concat([df, tmp], sort=False, ignore_index=True)
    if clinical_decomp_step == 'clincalstep4refresh2':
        df = df[~df.nid.isin([244370, 244371, 336850, 336849,
                              336848, 336847, 408680, 244369])]
    return df


def final_test_claims(df, clinical_decomp_step):
    """check for expected bundles and square by source"""

    failures = []

    missing_bundle_dict = bundle_check.test_refresh_bundle_ests(
        df=df, pipeline='claims', prod=False)

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

    bundles = [138, 139, 141, 142, 143, 3033]

    bundle_table = cm.get_active_bundles(bundle_id = bundles, estimate_id=[17,21])
    assert (bundle_table['estimate_id'] == 21).all(
    ), "Some bundles estimates aren't 21, meaning they'll lose data!"

    assert (df.loc[(df['bundle_id'].isin(bundles)) & (df['location_id'] ==
                                                      69), 'estimate_id'] == 17).all(), "Why were SGP estimates not 17?"
    df.loc[(df['bundle_id'].isin(bundles)) & (
        df['location_id'] == 69), 'estimate_id'] = 21
    return df

def clone_bundles(run_id):
    out_dir = FILEPATH".format(run_id)
    df = pd.read_csv(f"{out_dir}/FILEPATH")
    df.to_csv(f"{out_dir}/FILEPATH", index=False, na_rep="NULL")
    bc = br.BundleClone(run_id=run_id)
    df = bc.clone_and_return(df=df)
    df.to_csv(f"{out_dir}/FILEPATH", index=False, na_rep="NULL")


def claims_main(run_id, gbd_round_id, decomp_step, clinical_decomp_step,
                convert_sgp_data_to_otp_estimate, base_dir, types,
                nr_code, map_version):

    # list of cols to keep and order
    if nr_code == 'stata':
        cols = ['year', 'age', 'sex', 'location_id', 'acause',
                'sample_size', 'NID', 'cf_final', 'variance']
        df_list = []
        for key, value in list(types.items()):
            filepath = "{}FILEPATH{}".format(
                base_dir, key)
            print("Reading {}...".format(filepath))
            back = pd.read_stata(filepath)
            print("Formatting {}...".format(key))

            if 'ALL' in key:
                dx_id = 3  # combined inp and otp
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

            for bundle in df['bundle_id'].unique():
                floor = get_single_floor(
                    run_id=run_id, bundle_id=bundle, estimate_id=value, measure=measure)
                df.loc[(df['bundle_id'] == bundle) & (
                    df['mean'] < floor), 'mean'] = 0

            print("Applying age sex restrictions...")
            df = clinical_mapping.apply_restrictions(df, age_set='age_group_id',
                                                     cause_type='bundle',
                                                     map_version=map_version, prod=True)

            df_list.append(df)
        print("Done processing each file. Continuing...")

        df = pd.concat(df_list, ignore_index=True, sort=False)
        df = df.drop("variance", axis=1)

    elif nr_code == 'python':
        cols = ['year_id', 'age_start', 'sex_id', 'location_id',
                'sample_size', 'nid', 'ms_mean_final', 'estimate_id',
                'bundle_id']
        filepath = f"{base_dir}FILEPATH"
        df = pd.read_csv(filepath, usecols=cols)

        # assign col values to match process
        df.rename(columns={'ms_mean_final': 'mean'}, inplace=True)
        df['year_start'], df['year_end'] = df['year_id'], df['year_id']
        df.drop('year_id', axis=1, inplace=True)
        df['representative_id'] = 4
        df['diagnosis_id'], df['source_type_id'] = 0, 0

        # add source type id and dx id
        df.loc[df['estimate_id'] == 21, [
            'diagnosis_id', 'source_type_id']] = [3, 17]
        df.loc[df['estimate_id'] == 17, [
            'diagnosis_id', 'source_type_id']] = [1, 10]
        assert (df['diagnosis_id'] > 0).all()

        # switch to age group id
        df = get_age_groups(df)
    else:
        raise ValueError(f"NR Process type {nr_code} not recognized")

    warnings.warn(
        "we're applying the asfr adjustment for MARKETSCAN maternal data. Bundles are pulled from cause metadata")
    maternal_bundles = get_maternal_bundles(gbd_round_id)
    if df[df['bundle_id'].isin(maternal_bundles)].shape[0] > 0:
        df = apply_asfr(
            df=df, maternal_bundles=maternal_bundles, decomp_step=decomp_step, gbd_round_id=gbd_round_id)
    else:
        # there's no maternal data for this one
        pass

    df = to_int(df)

    if gbd_round_id == 7 and clinical_decomp_step == 'step2':
        warnings.warn("FILTERING MARKETSCAN TO ONLY 2017")
        df = df.loc[df.year_start == 2017, :]

    print("Applying age sex restrictions...")
    df = clinical_mapping.apply_restrictions(df, age_set='age_group_id',
                                             cause_type='bundle',
                                             map_version=map_version, prod=True)

    # add the other claims sources
    df = aggregate_claims_sources(
        df, gbd_round_id, clinical_decomp_step, run_id,
        decomp_step=decomp_step, map_version=map_version)

    # update columns
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

    out_dir = FILEPATH.format(run_id)
    print("Saving Claims file...")
    df.to_csv(out_dir + "FILEPATH", index=False, na_rep="NULL")

    # clone bundles
    print("Cloning bundles...")
    clone_bundles(run_id=run_id)

    print("Saved to {}.".format(out_dir + "FILEPATH"))
    print("Done.")

    return


def inp_main(gbd_round_id, clinical_decomp_step, run_id):
    """
    Formats Inpatient data for upload
    """
    inp_path = FILEPATH.format(
                   run_id)
    print("Reading {}...".format(inp_path))
    u1_path = (FILEPATH)
    u1 = pd.read_csv(u1_path)

    df = pd.read_hdf(inp_path)
    df = pd.concat([df, u1], sort=False, ignore_index=True)

    if not clinical_decomp_step:
        raise ValueError("You must identify the clinical decomp step")

    if (gbd_round_id == 7 and clinical_decomp_step in ('step1')):
        print("Adjusting the year start values for pseudo NIDs in GBD2019==>2020")
        df = adjust_year(df)

    print("Formatting {}".format(inp_path))
    cols = df.columns

    if 'diagnosis_id' not in cols:
        print("diagnosis_id wasn't in the columns, adding...")
        df['diagnosis_id'] = 1

    if 'source_type_id' not in cols:
        # facility inpatient on the shared.source_type table
        df['source_type_id'] = 10

    for drop_col in ['estimate_type', 'measure', 'source', 'haqi_cf',
                     'correction_factor']:
        if drop_col in cols:
            df.drop(drop_col, axis=1, inplace=True)

    # update columns
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
    out_dir = FILEPATH.format(run_id)
    df.to_csv(out_dir + "/FILEPATH", index=False, na_rep="NULL")
    print("Saved to {}.".format(out_dir + "/FILEPATH"))

    return


def replace_year_bin(df, ny_dict):
    """Takes a dataframe and a dictionary and replaces the year start/end bins
    for a set of nids"""

    pre = df.shape
    for nid, years in ny_dict.items():
        df.loc[df['nid'] == nid, ['year_start', 'year_end']] = years
    if df.shape[0] != pre[0] or df.shape[1] != pre[1]:
        raise ValueError("The row or column counts shouldn't change")
    return df


def adjust_year(df):
    print("Adjusting the year start values for pseudo NIDs in GBD2019==>2020")
    ny_dict = {411786: [2015, 2017],
               407536: [2016, 2017],
               411787: [2016, 2017],
               432265: [1993, 1996],
               421046: [2017, 2017]}
    df = replace_year_bin(df, ny_dict)
    return df


if __name__ == '__main__':
    decomp_step = sys.argv[1]
    nr_code = sys.argv[2]
    run_id = sys.argv[3].replace("\r", "")
    map_version = sys.argv[4]

    # need to cast to integer, unless it's the string "current"
    if map_version != "current":
        map_version = int(map_version)

    convert_sgp_data_to_otp_estimate = False

    clinical_decomp_step = 'clinicalstep3'

    TYPES = {'incidence_ALL.dta': 21, 'incidence_INP_ONLY.dta': 17,
             'prevalence_ALL.dta': 21, 'prevalence_INP_ONLY.dta': 17}

    BASE_DIR = (FILEPATH.format(run_id))

    claims_main(run_id=run_id, gbd_round_id=7,
                decomp_step=decomp_step,
                clinical_decomp_step=clinical_decomp_step,
                convert_sgp_data_to_otp_estimate=convert_sgp_data_to_otp_estimate,
                base_dir=BASE_DIR,
                types=TYPES,
                nr_code=nr_code, 
                map_version=map_version)
