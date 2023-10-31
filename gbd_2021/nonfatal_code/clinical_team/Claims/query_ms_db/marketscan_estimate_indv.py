
import time
import pandas as pd
import numpy as np
import sys
import itertools
import datetime
import re
import os
import getpass
import warnings

from clinical_info.Functions.hosp_prep import sanitize_diagnoses, stack_merger, natural_sort, write_hosp_file
from clinical_info.Functions.live_births import live_births
from clinical_info.Mapping import clinical_mapping

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
            Contains claims data for a single enrolid/bundle id combination. This df
            loses rows as it is recursively passed through the function b/c the duplicated
            claims are dropped.
        return_df: Pandas DataFrame
            Contains estimates for individuals. This df gains rows as it is recursively passed
            through the function storing each estimate for an individual encounter
        unique_cases: int
            Counts the number of loops through itself, perhaps add an assert b/c
            I think unique cases should be exactly equal to the number rows in return_df
        counter: int
            Limits the function to 10,000 loops through itself to prevent stack overflows
    """
    if counter > 10000:
        bundid = int(data_sub.bundle_id.unique())
        enrolid = int(data_sub.enrollee_id.unique())
        write_path = FILEPATH
        return_df.to_csv("{}final_{}_{}.csv".format(write_path, bundid, enrolid), index=False)
        data_sub.to_csv("{}sub_df_{}_{}.csv".format(write_path, bundid, enrolid), index=False)
        assert False, "Counter is 10,000"
        return("counter is 10,000")
    if data_sub.shape[0] == 0:
        assert return_df.shape[0] == unique_cases,\
            "I think the number of unique cases should equal the rows in return df but it doesn't"
        return(return_df)
    else:
        counter += 1
        unique_cases += 1
        return_df = pd.concat([return_df, data_sub.iloc[0:1, :]])
        return( recursive_duration(data_sub[data_sub.adm_date >= data_sub.adm_limit.iloc[0]],
                return_df=return_df,
                unique_cases=unique_cases,
                counter=counter))

def report_if_merge_fail(df, check_col, id_cols):
    """Report a merge failure if there is one"""
    merge_fail_text = """
        Could not find {check_col} for these values of {id_cols}:

        {values}
    """
    if df[check_col].isnull().values.any():
        # 'missing' can be df or series
        missing = df.loc[df[check_col].isnull(), id_cols]
        raise AssertionError(
            merge_fail_text.format(check_col=check_col,
                                   id_cols=id_cols, values=missing)
        )
    else:
        pass


def format_ms_cols(df):
    df['year'] = pd.to_numeric(df['year'])
    # format MS db data to flow with our process
    df.rename(columns={'year': 'year_start'}, inplace=True)
    df['year_end'] = df['year_start']

    df.rename(columns={'enrolid': 'enrollee_id', 'sex': 'sex_id',
                       'date': 'adm_date', 'stdplac': 'facility_id'}, inplace=True)

    df['sex_id'] = pd.to_numeric(df['sex_id'])
    df['egeoloc'] = pd.to_numeric(df['egeoloc'])
    df['facility_id'] = pd.to_numeric(df['facility_id'])

    # rename all the DX columns
    # Find all columns with dx_ at the start
    diagnosis_feats = df.columns[df.columns.str.contains('^DX|^dx')]

    new_dx_feats = ["dx_" + str(num) for num in np.arange(1, len(diagnosis_feats) + 1, 1)]

    name_dict = dict(list(zip(diagnosis_feats, new_dx_feats)))

    df.rename(columns=name_dict, inplace=True)

    return df


def reshape_long(df):
    """
    Reshaping the claims data long in preparation for mapping. Much more
    efficient to merge on the map once then doing 15 merges
    """
    
    # list of columns to use for index
    idx = ['enrollee_id', 'adm_date', 'age', 'egeoloc', 'sex_id',
           'is_otp', 'year_start', 'year_end', 'facility_id', 'code_system_id']

    # if the dataframe is too large, split it up into 100 pieces and reshape long
    # one at a time then concat back together
    if df.shape[0] > 1000000:
        split_df = np.array_split(df, 100)
        del df
        df = []
        for each_df in split_df:
            each_df = each_df.set_index(idx).stack().reset_index()
            # drop blank diagnoses
            each_df = each_df[each_df[0] != ""]
            # rename cols
            each_df = each_df.rename(columns={'level_{}'.format(len(idx)): 'diagnosis_id',
                                              0: 'cause_code'})
            # replace diagnosis_id with codes, 1 for primary, 2 for secondary
            # and beyond
            each_df['diagnosis_id'] = np.where(each_df['diagnosis_id'] ==
                                        'dx_1', 1, 2)
            # append to list of dataframes
            df.append(each_df)

        df = pd.concat(df)
        df.reset_index(inplace=True, drop=True)

    # otherwise do the entire reshape at once
    else:
        df = df.set_index(idx).stack().reset_index()

        df = df[df[0] != ""]

        df = df.rename(columns={'level_{}'.format(len(idx)): 'diagnosis_id', 0: 'cause_code'})

        # replace diagnosis_id with codes, 1 for primary, 2 for secondary
        # and beyond
        df['diagnosis_id'] = np.where(df['diagnosis_id'] ==
                                              'dx1', 1, 2)
    return df


def swap_loc_ids_egeoloc(df, run_id):
    """
    Marketscan uses egeoloc in the dB. Merge on location id when needed
    or egeoloc when needed
    """
    run_id = int(run_id)
    loc_map = pd.read_csv(FILEPATH.format(run_id))
    loc_map = loc_map[['egeoloc', 'location_id']]
    assert loc_map.shape[0] == loc_map.drop_duplicates().shape[0]

    if 'egeoloc' in df.columns:
        merge_on = 'egeoloc'
    elif 'location_id' in df.columns:
        merge_on = 'location_id'

    pre_shape = df.shape[0]    
    df = df.merge(loc_map, how='left', on=merge_on)
    assert pre_shape == df.shape[0]

    return df


def final_reshape(df):
    """
    reshape claims data long before writing to drive
    we will store clinical data long in the new dB
    """
    idx = [x for x in df.columns if 'cases' not in x]
    df = df.set_index(idx).stack().reset_index()
    df.rename(columns={'level_{}'.format(len(idx)): 'estimate_type', 0: 'val'}, inplace=True)
    return df


def process_marketscan(df, group, clinical_age_group_set_id, run_id='test',
                       cause_type='bundle', prod=True,
                       agg_types=['inp_pri', 'inp_any', 'otp_any', 'inp_otp_any_adjusted_otp_only'],
                       break_if_not_contig=False):
    """
    This function takes the mostly unprepped outputs from querying the marketscan dB
    and creates tabulations of the data in the different ways below
    
    # Aggregate the data multiple ways--
    # INP PRIMARY ADMISSIONS -- INPATIENT PRIMARY INDIVIDUALS
    # INP ANY ADMISSIONS -- INP ANY INDIVIDUALS
    # INP+OTP ANY ADMISSIONS -- INP+OTP ANY INDIVIDUALS
    # OTP ANY ADMISSIONS - INDIVIDUALS

    # these four don't make sense b/c outpatient data doesn't have a primary dx
    # INP+OTP PRIMARY ADMISSIONS -- INP+OTP PRIMARY INDIVIDUALS
    # OTP PRIMARY ADMISSIONS - AND INDIVIDUALS


    Params:
        df: (pd.DataFrame)
            pandas dataframe of mostly unprepped MS data from the dB
        group: (str?)
            Identifies which group of unique enrolids to use
        cause_type: (str)
            Identifies if data is being processed at the bundle or icg level
        agg_types: (list)
            which type of claims aggregations should be performed.
            Note: the otp aggregations take much more time.
    """

    if cause_type == 'icg':
        raise ValueError("ICG is not currently supported as an aggregation type")
    pm_start = time.time()
    # If the dataframe is empty end the program
    if df.shape[0] == 0:
        return None

    df = format_ms_cols(df)

    # perform live birth swap
    # swap live birth codes
    placeholders = ['nan', 'NAN', 'NaN', 'NONE', 'NULL', '']
    for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
        df.loc[df[col].isin(placeholders), col] = np.nan

    df = live_births.swap_live_births(df,
                                    user=getpass.getuser(),
                                    drop_if_primary_still_live=False)

    df.loc[(df.facility_id.isnull()) & (df.is_otp == 0), 'facility_id'] = 99999

    print("First formatting has finished")

    df = reshape_long(df)

    print("Reshape from wide to long has finished")

    # remove non-alphanumeric characters
    df['cause_code'] = df['cause_code'].str.replace("\W", "")
    # make sure all letters are capitalized
    df['cause_code'] = df['cause_code'].str.upper()

    # get the bundle level map
    bm = clinical_mapping.get_clinical_process_data('icg_bundle', map_version='current')

    # merge bundle id onto data
    df = clinical_mapping.map_to_gbd_cause(df, input_type='cause_code', output_type=cause_type,
                                           write_unmapped=False,  # This uses J drive so leave it False on Fair.
                                           truncate_cause_codes=True,
                                           extract_pri_dx=False,
                                           prod=prod, map_version='current')

    # apply the age/sex restrictions
    df = clinical_mapping.apply_restrictions(df,
                                             clinical_age_group_set_id=clinical_age_group_set_id,
                                             age_set='indv',
                                             cause_type=cause_type,
                                             prod=prod, 
                                             break_if_not_contig=break_if_not_contig)

    # if the dataframe becomes empty after mapping end the program
    if df.shape[0] == 0:
        exit

    # merge on location IDs                          
    df = swap_loc_ids_egeoloc(df, run_id)

    # create the template DF that all the aggregation results will be merged onto
    cols = ['age', 'sex_id', 'location_id', 'year_start', 'year_end','facility_id', cause_type + '_id']
    template_df = pd.DataFrame(index=[0], columns=cols)
    counter = 1

    print("Finished expanding the template df variable. it's {} shape".format(template_df.shape))
    print("Beginning to calculate aggregated counts")

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

        # we also want to go from claims data to estimates for individuals
        prev = dat_indv[dat_indv['{}_measure'.format(cause_type)] == 'prev'].copy()

        if agg_type == 'inp_otp_any_adjusted' or agg_type == 'inp_any_adjusted' or agg_type == 'inp_otp_any_adjusted_otp_only':

            print("Adjusting the numerator of the third Correction Factor")
            prev['adm_date'] = pd.to_datetime(prev['adm_date'])

            prev = prev.drop_duplicates(subset=['enrollee_id', 'adm_date', cause_type + '_id'], keep='first')  # FLAG this might break

            dot_duplicated_rows = prev.duplicated(subset=['enrollee_id', 'adm_date', cause_type + '_id'], keep=False).sum()
            if dot_duplicated_rows != 0:
                warnings.warn("The df has {} duplicates of enrolid, bid, admis date".format(dot_duplicated_rows))
                # let's write this as well
                dupes = prev.copy()
                dupes = dupes[dupes.duplicated(subset=['enrollee_id', 'adm_date', cause_type + '_id'], keep=False)]
                dupes.to_csv(FILEPATH.\
                             format(group))
                del dupes

            prev['rows'] = 1
            # sum the row counts to get # of unique enrollee id/bundle id occurences
            prev['keep'] = prev.groupby(['enrollee_id', cause_type + '_id', 'year_start', 'year_end'])['rows'].transform('sum')

            if agg_type == 'inp_otp_any_adjusted' or agg_type == 'inp_any_adjusted':
                # drop the rows that only have 1 occurence of bid and eid
                prev = prev[prev['keep'] > 1]

            if agg_type == 'inp_otp_any_adjusted_otp_only':
                # read in file of which bundles to process differently
                unadj_causes = pd.read_csv(FILEPATH.format(cause_type))
                unadj_causes = unadj_causes.loc[unadj_causes['adj_ms_prev_otp'] == 0, cause_type + '_id']

                # validate that all these exemptions are in the current map
                cause_diff = set(unadj_causes) - set(bm[f'{cause_type}_id'].unique())
                assert not cause_diff,\
                    f"There are unexpected {cause_type}s present {cause_diff}"

                prev = prev[(prev['keep'] > 1) |\
                       (prev['is_otp'] == 0) |\
                       prev[cause_type + '_id'].isin(unadj_causes)]

            # drop the cols we used to calc this
            prev.drop(['rows', 'keep'], axis=1, inplace=True)

        prev.drop_duplicates(subset=['enrollee_id', cause_type + '_id', 'year_start', 'year_end'], inplace=True)
        prev['facility_id'] = 1111

        inc = dat_indv[dat_indv['{}_measure'.format(cause_type)] == 'inc'].copy()

        inc = inc[(inc.facility_id != 1) & (inc.facility_id != 98) & (inc.facility_id != 81)]
        facilities = inc.facility_id.unique()
        assert 1 not in facilities
        assert 98 not in facilities
        assert 81 not in facilities,\
            "Pharmacy and Lab facility types were not dropped for incidence data"

        ########################
        # CREATE DURATION LIMITS
        ########################
        final_inc = []
        if inc.shape[0] > 0:


            inc = clinical_mapping.apply_durations(df=inc, cause_type=cause_type,
                                                   map_version='current', prod=prod,
                                                   fill_missing=True)

            # compare pd concat to appending a list
            start = time.time()

            inc.sort_values(by=['enrollee_id', cause_type + '_id', 'adm_date'], inplace=True)

            inc.drop_duplicates(subset=['enrollee_id', 'adm_date',
                                        cause_type + '_id'], inplace=True)

            long_dur = inc[inc['{}_duration'.format(cause_type)] == 365].copy()
            long_dur.drop_duplicates(subset=['enrollee_id', cause_type + '_id', 'year_start', 'year_end'],
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

            print("{} done in {} min".format(agg_type, (time.time()-start)/60))

        # bring the data back together
        if len(final_inc) > 0:
            inc_df = pd.concat(final_inc)
            dat_indv = pd.concat([inc_df, prev], sort=False)
            dat_indv.drop(labels=['adm_limit', '{}_duration'.format(cause_type)], axis=1, inplace=True)
        else:
            dat_indv = prev.copy()

        dat_indv.facility_id.fillna(9191, inplace=True)
        dat_claims.facility_id.fillna(9191, inplace=True)

        indv_loss = dat_indv.isnull().sum().max()
        claims_loss = dat_claims.isnull().sum().max()
        print("the most null claims from any columns {}".format(claims_loss))
        indv_sum = dat_indv.shape[0] - indv_loss
        claims_sum = dat_claims.shape[0] - claims_loss

        # now create cases
        col_name_a = agg_type + "_claims_cases"
        dat_claims[col_name_a] = 1

        col_name_i = agg_type + "_indv_cases"
        dat_indv[col_name_i] = 1

        # groupby and collapse summing cases
        groups = ['location_id', 'year_start', 'year_end',
                    'age', 'sex_id', cause_type + '_id', 'facility_id']

        if dat_indv.shape[0] > 2000:
            assert (dat_indv[groups].isnull().sum() < dat_indv.shape[0] * .2).all()
            assert (dat_claims[groups].isnull().sum() < dat_claims.shape[0] * .2).all()
        if agg_type == 'inp_pri':
            print("These cols will be used in the groupby {}".format(groups))
            print(dat_claims.isnull().sum())
        dat_claims = dat_claims.groupby(groups).agg({col_name_a: 'sum'}).reset_index()
        dat_indv = dat_indv.groupby(groups).agg({col_name_i: 'sum'}).reset_index()

        if counter == 1:
            template_df = dat_claims

            counter += 1
        else:
            # merge onto our template df created above
            template_df = template_df.merge(dat_claims, how='outer',
                                            on=['age', 'sex_id', 'location_id',
                                                'year_start', 'year_end', cause_type + '_id',
                                                'facility_id'])

        template_df = template_df.merge(dat_indv, how='outer',
                                        on=['age', 'sex_id', 'location_id',
                                            'year_start', 'year_end', cause_type + '_id',
                                            'facility_id'])

        print(agg_type)
        assert template_df[col_name_a].sum() == claims_sum, "Some claims cases lost. claims sum is {} type is {} data col sum is {}".format(claims_sum, col_name_a, template_df[col_name_a].sum())
        # the assert below breaks because of how we've adjusted individual cases
        if agg_type != 'inp_otp_any_adjusted' and agg_type != 'inp_any_adjusted' and agg_type != 'inp_otp_any_adjusted_otp_only':
            assert template_df[col_name_i].sum() == indv_sum, "Some individual cases lost. claims sum {} {} sum {}".format(claims_sum, col_name_i, template_df[col_name_i].sum())


        #######################################################################
        # END AGG TYPE FOR LOOP
        #######################################################################

    # remove rows where every value is NA
    case_cols = template_df.columns[template_df.columns.str.endswith("_cases")]
    col_sums = template_df[case_cols].sum()
    template_df.dropna(axis=0, how='all', subset=case_cols,
                       inplace=True)
    assert (col_sums == template_df[case_cols].sum()).all()

    if claim_chk != template_df.inp_pri_claims_cases.sum():
        print("something is off. should be {} inp pri claims but we have {}".\
            format(claim_chk, template_df.inp_pri_claims_cases.sum()))
        template_df.to_csv(FILEPATH.format(cause_type, group), index=False)
    
    out_dir = FILEPATH.format(run_id)
    out_dir = out_dir.replace("\r", "")

    write_date = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]

    filepath = FILEPATH
    
    # reshape long then write
    write_hosp_file(final_reshape(template_df.copy()),
                    filepath, backup=True)

    # write data for the stata scripts but only at the bundle level
    if cause_type == 'bundle':
        prevs = df.loc[df['{}_measure'.format(cause_type)] == 'prev', cause_type + '_id'].unique()
        incs = df.loc[df['{}_measure'.format(cause_type)] == 'inc', cause_type + '_id'].unique()

        template_df = swap_loc_ids_egeoloc(template_df, run_id)

        template_df.rename(columns={'sex_id': 'sex'}, inplace=True)

        if 'inp_otp_any_adjusted_otp_only_indv_cases' in template_df.columns:
            # take only the cases col we want, rename it to cases
            ms_all = template_df[['sex', 'egeoloc', 'age', cause_type + '_id',
                                  'inp_otp_any_adjusted_otp_only_indv_cases', 'year_start']].copy()
            ms_all.rename(columns={'inp_otp_any_adjusted_otp_only_indv_cases': 'cases',
                                   'year_start': 'year',
                                   'age': 'age_start'}, inplace=True)

            ms_all = ms_all[ms_all.cases.notnull()]

            ms_all['age_end'] = ms_all['age_start']
            # write prev and inc

            prev_path = FILEPATH.format(run_id, cause_type, group)
            prev_path = prev_path.replace("\r", "")
            ms_all[ms_all[cause_type + '_id'].isin(prevs)].to_stata(prev_path, write_index=False)


            inc_path = FILEPATH.format(run_id, cause_type, group)
            inc_path = inc_path.replace("\r", "")

            ms_all[ms_all[cause_type + '_id'].isin(incs)].to_stata(inc_path, write_index=False)

        if 'inp_any_indv_cases' in template_df.columns:
            ms_inp_any = template_df[['sex', 'egeoloc', 'age', cause_type + '_id',
                                  'inp_any_indv_cases', 'year_start']].copy()
            ms_inp_any.rename(columns={'inp_any_indv_cases': 'cases',
                                       'year_start': 'year',
                                       'age': 'age_start'}, inplace=True)
            # drop rows which don't have any cases for this agg type
            ms_inp_any = ms_inp_any[ms_inp_any.cases.notnull()]

            ms_inp_any['age_end'] = ms_inp_any['age_start']


            prev_path = FILEPATH.format(run_id, cause_type, group)
            prev_path = prev_path.replace("\r", "")
            ms_inp_any[ms_inp_any[cause_type + '_id'].isin(prevs)].to_stata(prev_path, write_index=False)


            inc_path = FILEPATH.format(run_id, cause_type, group)
            inc_path = inc_path.replace("\r", "")
            ms_inp_any[ms_inp_any[cause_type + '_id'].isin(incs)].to_stata(inc_path, write_index=False)

    print("The process_marketscan function ran in {} minutes".format(round((time.time()-pm_start)/60, 2)))
    return
