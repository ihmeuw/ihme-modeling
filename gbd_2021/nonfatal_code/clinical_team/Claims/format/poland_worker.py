import pandas as pd
import numpy as np
import os
import sys

from db_tools.ezfuncs import query
from db_queries import get_population
import warnings

from clinical_info.Functions import gbd_hosp_prep, hosp_prep
from clinical_info.Mapping import clinical_mapping


def shape_long(df):
    """
    Expand multiple ICD codes stored in a single column to a wide format.
    Then convert from wide format to long format

    Args:
        df (pd.DataFrame): Must contain col ICD10 where multiple ICD codes
            are saved. Each ICD code is deliminated by a ,

    Return:
        df: pd.DataFrame

    """

    # Expand the ICD codes to wide format
    df_dx = df.ICD10.str.split(",", expand=True)
    cols = df_dx.columns
    assert df_dx.shape[0] == df.shape[0], "Check expansion of DX codes"
    print('Done Splitting dx codes')

    new_cols = ['dx_{}'.format(e) for e in cols]
    col_dict = dict(zip(cols, new_cols))
    df_dx.rename(col_dict, axis=1, inplace=True)
    df = pd.concat([df, df_dx], axis=1)

    # No longer need this col since we are now in long format
    df.drop('ICD10', axis=1, inplace=True)

    print('Rename done')
    non_dx = [e for e in df.columns if e not in new_cols]
    df = df.set_index(non_dx).stack().reset_index()
    x = np.nan
    for e in df.columns:
        if str(e).startswith('level_'):
            x = e

    # Retain the primary dx
    df['diagnosis_id'] = [1 if e == 'dx_0' else 2 for e in df[x]]

    df.rename({0: 'cause_code'}, axis=1, inplace=True)
    df.drop(x, axis=1, inplace=True)

    return df


def add_loc_ids(df):
    """
    Converts the voivodeship from unicode to ascii and adds
    the GBD location id
    """
    df.rename({'voivodeship': 'location_name'}, axis=1, inplace=True)
    df['location_name'] = df['location_name'].str.title()

    # There is a mis-spelling
    df.loc[df.location_name == 'Warmińsko-Mazurski',
           'location_name'] = 'Warmińsko-Mazurskie'

    q = """
        QUERY
        """
    locs = query(q)
    locs = locs[['location_name', 'location_ascii_name', 'location_id']]
    m = df.merge(locs, on='location_name', how='left')

    msg = 'Missing location_ids, double check the merge'
    m.location_id.isnull().sum() == 0, msg

    m.drop('location_name', axis=1, inplace=True)
    m.rename({'location_ascii_name': 'location_name'}, axis=1, inplace=True)

    return m


def apply_map(df):
    """
    Apply the current version of the clinical map onto the data

    Parameters:
        df: pandas Dataframe

    Returns:
        Dataframe with bundle mapping
    """
    df = clinical_mapping.map_to_gbd_cause(df, input_type='cause_code',
                                           output_type='bundle',
                                           write_unmapped=False,
                                           truncate_cause_codes=True,
                                           extract_pri_dx=False,
                                           prod=True, map_version='current')

    return df


def prep_cols(df):
    df.rename({'gender': 'sex_id',
               'visit_year': 'year_start',
               'id': 'enrollee_id',
               'place_of_visit': 'is_otp',
               'visit_date': 'adm_date'}, axis=1, inplace=True)

    df['year_end'] = df['year_start']

    df['sex_id'] = np.where(df['sex_id'] == "male", 1, 2)
    df['is_otp'] = np.where(df['is_otp'] == "outpatient", 1, 0)
    assert df.sex_id.isnull().sum() == 0
    return df


def five_yr_age(df, break_if_not_contig):
    """
    Need to agg counts to five year age bins. Agg by
    age /sex/location/bundle

    """
    # create gbd age bans
    df = hosp_prep.age_binning(
        df=df, under1_age_detail=False, terminal_age_in_data=False,
        break_if_not_contig=break_if_not_contig,
        clinical_age_group_set_id=1)

    # add gbd age group ids
    df = gbd_hosp_prep.all_group_id_start_end_switcher(
        df=df, clinical_age_group_set_id=1)

    cols = ['location_id', 'year_start', 'year_end', 'sex_id',
            'age_group_id', 'is_otp', 'bundle_id']

    case_cols = df.columns[df.columns.str.endswith("_cases")].tolist()

    # make dictionary that says sum for all/any columns that end in "_cases"
    case_cols_aggregation_dict = dict(zip(case_cols, ['sum'] * len(case_cols)))

    df = df.groupby(cols).agg(case_cols_aggregation_dict).reset_index()
    df = gbd_hosp_prep.all_group_id_start_end_switcher(
        df=df, clinical_age_group_set_id=1, remove_cols=False)

    return df


def test_visit_year(df):
    """
    Validate that the year in column `visit_year` matches the year
    in `visit_date`
    """

    test = df.copy()

    test['visit_date'] = test.visit_date.astype(str)
    test['visit_year'] = test.visit_year.astype(str)

    test['v_year'], test['v_month'], test['v_day'] =\
        test['visit_date'].str.split('-', 3).str
    rows = test[test.v_year != test.visit_year].shape[0]

    if rows != 0:
        warnings.warn('\nThere are {} rows where the visit year does not match'
                      'the visit date\n'.format(rows))


def test_first_visit_date(df):
    """
    Validate that there is only one unique first_visit_date for each unique
    patient in the data.

    """

    test = df.loc[:, ['id', 'first_visit_date']]
    test = test.drop_duplicates(inplace=False)
    test = test.groupby('id').agg({'first_visit_date': 'count'}).reset_index()

    msg = ("Check to see why there is more than one "
           "unique visit date for patients")
    assert test[test.first_visit_date > 1].shape[0] == 0, msg


def process_inc(df, cause_type):
    final_inc = []

    # durations are added once the data has been mapped
    inc = clinical_mapping.apply_durations(df, cause_type=cause_type,
                                           map_version='current', prod=True,
                                           fill_missing=True)

    inc.sort_values(by=['enrollee_id', 'bundle_id', 'adm_date'], inplace=True)

    # drop all combinations of patient ID and nfc that occur on same day
    inc.drop_duplicates(subset=['enrollee_id', 'adm_date',
                                'bundle_id'], inplace=True)

    # calculcate nfc with 365 day duration exactly like prev
    long_dur = inc[inc['{}_duration'.format(cause_type)] == 365].copy()
    long_dur.drop_duplicates(subset=['enrollee_id', 'bundle_id',
                                     'year_start', 'year_end'],
                             inplace=True)
    final_inc.append(long_dur)

    inc = inc[inc['{}_duration'.format(cause_type)] != 365]

    r = inc.groupby('enrollee_id').size().reset_index()

    l = inc.groupby('enrollee_id')[cause_type + '_id'].nunique().reset_index()
    m = l.merge(r, how='outer', on='enrollee_id')

    id_array = m.loc[m[cause_type + '_id'] == m[0], 'enrollee_id']
    inc_indv = inc[inc.enrollee_id.isin(id_array)].copy()

    del r, l, m
    final_inc.append(inc_indv)
    # remove these IDs from the object that goes to recursive dur
    inc = inc[~inc.enrollee_id.isin(id_array)].copy()

    assert (inc.enrollee_id.value_counts() > 1).all(),\
        "There are enrollee IDs with fewer than 2 value counts"

    inc.sort_values(by=['enrollee_id', cause_type + '_id', 'adm_date'],
                    inplace=True)
    inc = inc.groupby(['enrollee_id', cause_type + '_id'])

    for enrollee_id, cause_df in inc:
        final_inc.append(recursive_duration(cause_df, pd.DataFrame(), 0, 0))

    inc_df = pd.concat(final_inc)

    return inc_df


def recursive_duration(data_sub, return_df, unique_cases, counter):
    """
    A recursive function to estimate individuals from a dataframe of claims
    for a single unique enrolid/bundle id combination.

    The recursive func takes a dataframe with a unique enrolid/bundle combo.
    It orders the claims DF by admission date. It counts the earliest claim
    (row 0) as 1 unique individual and stores it in a "return_df" object. Then
    it drops everything within the duration limit of that claim (including row
    zero, the first "individual") and passes the remaining claims back to the
    function itself to re-run

    Parameters:
        data_sub: Pandas DataFrame
            Contains claims data for a single enrolid/bundle id combination.
            This df loses rows as it is recursively passed through the function
            b/c the duplicated claims are dropped.
        return_df: Pandas DataFrame
            Contains estimates for individuals. This df gains rows as it is
            recursively passed through the function storing each estimate for
            an individual encounter
        unique_cases: int
            Counts the number of loops through itself, perhaps add an assert
            b/c I think unique cases should be exactly equal to the number rows
            in return_df
        counter: int
            Limits the function to 10,000 loops through itself to prevent
            stack overflows
    """
    # data_sub.sort_values(by='adm_date', inplace=True)
    if counter > 10000:
        bundid = int(data_sub.bundle_id.unique())
        enrolid = int(data_sub.enrollee_id.unique())
        write_path = FILEPATH
        return_df.to_csv(FILEPATH.format(
            write_path, bundid, enrolid), index=False)
        data_sub.to_csv(FILEPATH.format(
            write_path, bundid, enrolid), index=False)
        # break the function since this shouldn't be happening
        assert False, "Counter is 10,000"
        return("counter is 10,000")
    if data_sub.shape[0] == 0:
        assert return_df.shape[0] == unique_cases,\
            ("I think the number of unique cases should equal the rows in "
             "return df but it doesn't")
        return(return_df)
    else:
        counter += 1
        unique_cases += 1
        return_df = pd.concat([return_df, data_sub.iloc[0:1, :]])
        return(recursive_duration(data_sub[data_sub.adm_date >= data_sub.adm_limit.iloc[0]],
                                  return_df=return_df,
                                  unique_cases=unique_cases,
                                  counter=counter))


def add_visit_date(df):
    """
    Creates an artifical visit date based on the time lag.
    Defines a month being 30 days
    """

    print("Adding visit date...")

    df.loc[:, "visit_number"] = df.loc[:, "visit_number"].astype(int)

    df.drop_duplicates(subset=['id', 'visit_number'], inplace=True)

    df['time_lag'] = df.time_lag.astype(str)
    df['time_lag'] = df.time_lag.str.replace(',', '.')
    df['time_lag'] = pd.to_numeric(df.time_lag)

    df['first_visit_date'] = pd.to_datetime(df['first_visit_date'])
    df['lag_days'] = (df['time_lag'] * 30).astype("timedelta64[D]")

    df['visit_date'] = pd.NaT
    df.loc[df['visit_number'] == 1,
           'visit_date'] = df.loc[df['visit_number'] == 1, 'first_visit_date']

    for i in range(2, df.visit_number.max() + 1, 1):
        goods = df.loc[df['visit_number'] == i, 'id']
        new_admit = df.loc[(df['visit_number'] == (i - 1)) &
                           (df['id'].isin(goods)),
                           'visit_date'].reset_index(drop=True) +\
            df.loc[df['visit_number'] == i, 'lag_days'].reset_index(drop=True)

        new_admit = pd.Series(
            new_admit.tolist(),
            index=df.loc[df.visit_number == i, 'visit_date'].index)

        df_nrows = df.loc[df['visit_number'] == i, :].shape[0]
        new_admit_nrows = new_admit.shape[0]

        assert df_nrows == new_admit_nrows,\
            f"Shape must match!, df: {df_nrows}, new_admit: {new_admit_nrows}"

        df.loc[df['visit_number'] == i, 'visit_date'] = new_admit

    df = df.drop('lag_days', axis=1)

    print(f"{df[df.visit_date.isnull()].shape[0]} rows will null visit_date")

    df = df.loc[df.visit_date.notnull(), :]

    return df


def write_groups(group_id):
    """
    Subsets the raw data file into n dataframes by unique ids
    """
    path = FILEPATH

    groups = pd.read_csv(path + FILEPATH)
    proccess_g = groups.loc[groups['group'] == group_id, :]

    data_root = FILEPATH

    files = [FILEPATH,
             FILEPATH]

    df_list = []

    gp_ids = proccess_g.id.unique().tolist()
    gp_ids = [int(i) for i in gp_ids]

    if len(gp_ids) == 0:
        raise ValueError("IDs list is empty")

    for file in files:
        found_data = False
        for chunk in pd.read_csv(file, chunksize=100_000, sep='\t'):
            chunk.loc[:, "id"] = chunk.loc[:, "id"].astype(int)
            if chunk.loc[chunk.id.isin(gp_ids), :].shape[0] > 0:
                print("chunk is non-empty")
                found_data = True
            df_list.append(chunk.loc[chunk.id.isin(gp_ids), :])
        if not found_data:
            raise ValueError(f"No data was kept from file {file}")

    final = pd.concat(df_list)

    if final.shape[0] == 0:
        raise ValueError("Final dataframe shouldn't be empty")

    final.to_csv(path + f'dfs/group_{group_id}.csv', index=False)


def pol_get_sample_size(df, run_id, gbd_round_id, decomp_step):
    """
    Need to ensure that poland uses the same population as the rest of the CI
    pipelines
    """

    if "year_id" in df.columns:
        df.rename(columns={'year_id': 'year_start'}, inplace=True)
        df.loc[:, 'year_end'] = df.loc[:, 'year_start']

    pop = get_population(age_group_id=list(df.age_group_id.unique()),
                         location_id=list(df.location_id.unique()),
                         sex_id=[1, 2],
                         year_id=list(df.year_start.unique()),
                         gbd_round_id=gbd_round_id,
                         decomp_step=decomp_step)

    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop.loc[:, 'year_end'] = pop.loc[:, 'year_start']
    pop.drop("run_id", axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end',
                  'age_group_id', 'sex_id']

    # merge on population
    pre_shape = df.shape[0]
    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"
    assert df.population.notnull().all(),\
        "population is missing for some rows. look at this df! \n {}".\
        format(df.loc[df.population.isnull(), demography].drop_duplicates())

    assert "population" in df.columns, "population was not attached"

    return df


def process_claims(df, group_id, round_id, step, run_id, cause_type,
                   break_if_not_contig,
                   agg_types=['inp_pri', 'inp_any', 'otp_any',
                              'inp_otp_any_adjusted_otp_only']):
    """
    Main function of the script. Process the Poland claims for prev / inc
    cases only. This will need to be updated in GBD 2020 to format data
    for correction factor usage.

    This function more or less mirrors the steps and sequence in
    marketscan_estimate_indv.py.


    Parameters:
        temp (pandas DataFrame): group dataframe
        group_id (int): group number
        round_id (int): GBD round id
        step (str): GBD decomp step
    """
    df = df[df.age != -1]

    df = df[df['time_lag'] != 'nan']

    df = add_visit_date(df)

    print('OG size {}'.format(df.shape[0]))
    df = shape_long(df)

    # Make sure there is one unique inital visit for each
    # unique enrollee
    test_first_visit_date(df)

    print('Reshape: {}'.format(df.shape[0]))
    df = add_loc_ids(df)

    print('Applying map and age sex restrictions')
    df['code_system_id'] = 2
    df = apply_map(df)
    df = prep_cols(df)
    df = clinical_mapping.apply_restrictions(df,
                                             age_set='indv',
                                             cause_type='bundle',
                                             break_if_not_contig=break_if_not_contig)

    # create the template DF that all the aggregation results will be merged
    cols = ['age', 'sex_id', 'location_id', 'year_start', 'year_end',
            cause_type + '_id']
    template_df = pd.DataFrame(index=[0], columns=cols)
    counter = 1

    for agg_type in agg_types:
        # drop DX depending on inp/otp/primary/any
        if agg_type == 'inp_pri':
            # drop all non inpatient primary data
            dat_indv = df[(df.diagnosis_id == 1) & (df.is_otp == 0)].copy()
            claim_chk = dat_indv.shape[0]

        if agg_type == 'inp_any' or agg_type == 'inp_any_adjusted':
            # drop all non inpatient data
            dat_indv = df[df.is_otp == 0].copy()

        if agg_type == 'inp_otp_pri':
            # drop all non inpatient/outpatient primary data
            dat_indv = df[df.diagnosis_id == 1].copy()

        if agg_type == 'inp_otp_any' or agg_type == 'inp_otp_any_adjusted' or agg_type == 'inp_otp_any_adjusted_otp_only':
            # keep everything
            dat_indv = df.copy()

        if agg_type == 'otp_pri':
            # drop all non outpatient primary data
            dat_indv = df[(df.diagnosis_id == 1) & (df.is_otp == 1)].copy()

        if agg_type == 'otp_any':
            # drop all non outpatient data
            dat_indv = df[df.is_otp == 1].copy()

        # make a copy for claims cases
        dat_claims = dat_indv.copy()
        # if the subset dataframe is empty move on to next set
        if dat_indv.shape[0] == 0:
            print("{} seems to have no cases".format(agg_type))
            continue

        prev = dat_indv[dat_indv['{}_measure'.format(
            cause_type)] == 'prev'].copy()

        if agg_type == 'inp_otp_any_adjusted' or agg_type == 'inp_any_adjusted' or agg_type == 'inp_otp_any_adjusted_otp_only':

            print("Adjusting the numerator of the third Correction Factor")
            prev['adm_date'] = pd.to_datetime(prev['adm_date'])

            prev = prev.drop_duplicates(subset=[
                                        'enrollee_id', 'adm_date', 'bundle_id'],
                                        keep='first')

            # set dummy var to sum
            prev['rows'] = 1
            # sum the row counts to get # of unique enrollee id/bundle occurences
            prev['keep'] = prev.groupby(['enrollee_id', 'bundle_id', 'year_start',
                                         'year_end'])['rows'].transform('sum')
            if agg_type == 'inp_otp_any_adjusted' or agg_type == 'inp_any_adjusted':
                # drop the rows that only have 1 occurence of bid and eid
                prev = prev[prev['keep'] > 1]

            if agg_type == 'inp_otp_any_adjusted_otp_only':
                # read in file of which bundles to process differently
                unadj_causes = pd.read_csv(
                    FILEPATH)
                unadj_causes = unadj_causes.loc[
                    unadj_causes['adj_ms_prev_otp'] == 0,
                    cause_type + '_id']
                # new direction to keep rows with only 1 claim if they're inpatient
                prev = prev[(prev['keep'] > 1) |
                            (prev['is_otp'] == 0) |
                            prev[cause_type + '_id'].isin(unadj_causes)]

            # drop the cols we used to calc this
            prev.drop(['rows', 'keep'], axis=1, inplace=True)

        prev.drop_duplicates(subset=['enrollee_id', 'bundle_id',
                                     'year_start', 'year_end'], inplace=True)

        inc = dat_indv.loc[dat_indv['{}_measure'.format(cause_type)] == 'inc',
                           :]

        ########################
        # CREATE DURATION LIMITS
        ########################
        inc_df = process_inc(inc, cause_type)

        # bring the data back together
        dat_indv = pd.concat([inc_df, prev], sort=False)
        dat_indv.drop(labels=['adm_limit', '{}_duration'.format(
            cause_type)], axis=1, inplace=True)

        indv_loss = dat_indv.isnull().sum().max()
        claims_loss = dat_claims.isnull().sum().max()

        print(f"the most null claims from any columns is {claims_loss} nulls")
        indv_sum = dat_indv.shape[0] - indv_loss
        claims_sum = dat_claims.shape[0] - claims_loss

        # now create cases
        col_name_a = agg_type + "_claims_cases"
        dat_claims[col_name_a] = 1

        col_name_i = agg_type + "_indv_cases"
        dat_indv[col_name_i] = 1

        # groupby and collapse summing cases
        groups = ['location_id', 'year_start', 'year_end',
                  'age', 'sex_id', 'bundle_id', 'is_otp']

        if dat_indv.shape[0] > 2000:
            assert (dat_indv[groups].isnull().sum() <
                    dat_indv.shape[0] * .2).all()
            assert (dat_claims[groups].isnull().sum() <
                    dat_claims.shape[0] * .2).all()
        if agg_type == 'inp_pri':
            print("These cols will be used in the groupby {}".format(groups))
            print(dat_claims.isnull().sum())
        dat_claims = dat_claims.groupby(groups).agg({col_name_a:
                                                     'sum'}).reset_index()
        dat_indv = dat_indv.groupby(groups).agg({col_name_i:
                                                 'sum'}).reset_index()

        if counter == 1:
            template_df = dat_claims
            counter += 1
        else:
            # merge onto our template df created above
            template_df = template_df.merge(dat_claims, how='outer',
                                            on=['age', 'sex_id', 'location_id',
                                                'year_start', 'year_end',
                                                'is_otp', cause_type + '_id'])
        template_df = template_df.merge(dat_indv, how='outer',
                                        on=['age', 'sex_id', 'location_id',
                                            'year_start', 'year_end',
                                            'is_otp', cause_type + '_id'])

        # check sum of cases to ensure we're not losing beyond what's expected
        print(agg_type)
        assert template_df[col_name_a].sum() == claims_sum,\
            ("Some claims cases lost. claims sum is {} type is "
             "{} data col sum is {}".format(claims_sum, col_name_a,
                                            template_df[col_name_a].sum()))

        if agg_type != 'inp_otp_any_adjusted' and agg_type != 'inp_any_adjusted' and agg_type != 'inp_otp_any_adjusted_otp_only':
            assert template_df[col_name_i].sum() == indv_sum,\
                ("Some individual cases lost. claims sum "
                 "{} {} sum {}".format(claims_sum, col_name_i,
                                       template_df[col_name_i].sum()))

        #######################################################################
        # END AGG TYPE FOR LOOP
        #######################################################################

    case_cols = dat_indv.columns[dat_indv.columns.str.endswith(
        "_cases")].tolist()
    col_sums = dat_indv[case_cols].sum()
    dat_indv.dropna(axis=0, how='all', subset=case_cols,
                    inplace=True)
    assert (col_sums == dat_indv[case_cols].sum()).all()

    # another check
    if claim_chk != template_df.inp_pri_claims_cases.sum():
        print(f"something is off. should be {claim_chk} inp pri claims but "
              f"we have {template_df.inp_pri_claims_cases.sum()}")
        template_df.to_csv(
            FILEPATH index=False
        )

    case_cols = template_df.columns[template_df.columns.str.endswith(
        "_cases")].tolist()
    groupby_cols = [col for col in template_df.columns if col != "is_otp"]
    groupby_cols = list(set(groupby_cols) - set(case_cols))

    case_cols_aggregation_dict = dict(zip(case_cols, ['sum'] * len(case_cols)))

    template_df = template_df.groupby(groupby_cols).agg(
        case_cols_aggregation_dict).reset_index()

    base_dir = (FILEPATH)
    assert os.path.isdir(base_dir), "This directory needs to be made."
    if not os.path.exists(f"{base_dir}/FILEPATH"):
        os.makedirs(f"{base_dir}/FILEPATH")
    filepath = f"{base_dir}/FILEPATH"
    filepath = filepath.replace("\r", "")

    template_df.to_csv(filepath, index=False)

    df = five_yr_age(dat_indv, break_if_not_contig=break_if_not_contig)

    df = pol_get_sample_size(df=df, run_id=run_id,
                             gbd_round_id=round_id, decomp_step=step)

    df.rename({'population': 'sample_size'}, axis=1, inplace=True)

    # rename cases column
    case_cols = df.columns[df.columns.str.endswith("_cases")].tolist()
    assert len(case_cols) == 1, ("There is not exactly one cases column. "
                                 f"Instead there are these: {case_cols}")
    df = df.rename(columns={case_cols[0]: "cases"})

    # Create inp and inp_otp dfs
    inp = df.loc[df.is_otp == 0, :]
    inp.drop('is_otp', axis=1, inplace=True)
    cols = ['location_id', 'year_start', 'year_end',
            'sex_id', 'age_group_id', 'bundle_id', 'age_start',
            'age_end', 'sample_size']
    df.drop('is_otp', axis=1, inplace=True)

    inp_otp = df.groupby(cols).agg({"cases": "sum"}).reset_index()

    base_path = FILEPATH
    path_inp = FILEPATH
    path_inp_otp = FILEPATH

    # make sure columns at the end are the same
    final_columns = ['location_id', 'year_start', 'year_end', 'sex_id',
                     'age_group_id', 'bundle_id', 'age_start', 'age_end',
                     'sample_size', 'cases']
    inp = inp.loc[:, final_columns]
    inp_otp = inp_otp.loc[:, final_columns]

    inp.to_csv(path_inp + FILEPATH.format(group_id), index=False)
    inp_otp.to_csv(path_inp_otp + FILEPATH.format(group_id), index=False)
    print('Files saved')

    return None


if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    group_id = sys.argv[1]
    group_id = int(group_id)

    process_type = sys.argv[2]
    process_type = int(process_type)

    round_id = sys.argv[3]
    round_id = int(round_id)

    run_id = sys.argv[4]
    run_id = int(run_id)

    step = sys.argv[5]

    cause_type = sys.argv[6]

    if process_type < 2:
        path = FILEPATH
        df = pd.read_csv(path + FILEPATH.format(group_id))

    if process_type == 0:
        process_claims(df=df, group_id=group_id, round_id=round_id,
                       step=step, run_id=run_id, cause_type=cause_type,
                       break_if_not_contig=False)
    if process_type == 1:
        raise ValueError("No longer supported")
    if process_type == 2:
        write_groups(group_id=group_id)
