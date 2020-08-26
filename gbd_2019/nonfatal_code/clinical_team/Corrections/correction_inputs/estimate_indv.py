"""
Estimate individuals in the non-Marketscan correction factor inputs

as of 3/21/2019 we have 3 non-MS sources that the clinical team preps
PHL, USA HCUP SID and NZL NMDS.

This is a helper script that each CF input script will call to get
from admissions to individual cases. Since this is based on the marketscan
claims process we'll import some modules from there
"""

import itertools
import time
import pandas as pd

from clinical_info.Functions import hosp_prep
from clinical_info.Mapping import clinical_mapping
from clinical_info.Claims.query_ms_db import marketscan_estimate_indv as mei

def clean_bad_ids(df):
    """
    If a patient ID includes multiple ages or sexes we don't want to use it
    """
    print("df shape with missing pat IDs is {}".format(df.shape))
    # drop missing patient IDs
    df = df[df.patient_id != "."]
    df = df[df.age.notnull()]
    if df.shape[0] == 0:
        print("This source is missing unique patient ids")
        # return a blank dataframe b/c we can't use this source
        # these are currently only certain HCUP states
        # but will check it to make sure all jobs finished
        return pd.DataFrame()

    print("df shape is", df.shape)
    # drop duplicates by ID and sex
    x = df[['patient_id', 'sex_id']].drop_duplicates()
    # keep only IDs that are duplicated i.e. that have multiple sex IDs for them
    x = x[x.duplicated(subset=['patient_id'])]
    # drop enrollee IDs with 2 sexes associated
    df = df[~df.patient_id.isin(x.patient_id)]
    print("there were {} bad patient IDs due to multiple sexes".format(x.shape[0]))

    dfG = df.copy()
    dfG['age_min'] = dfG['age']
    dfG['age_max'] = dfG['age']
    # create age min and max by enrollee ID groups
    dfG = dfG.groupby(['patient_id']).agg({'age_min': 'min', 'age_max': 'max'}).reset_index()
    min_age_df = dfG[['patient_id', 'age_min']].copy()
    # find the difference beween min and max
    dfG['age_diff'] = dfG['age_max'] - dfG['age_min']
    # drop where difference is greater than 1 year
    dfG = dfG[dfG.age_diff > 1]
    print("there were {} bad patient IDs due to large age differences".format(dfG.shape[0]))

    df = df[~df.patient_id.isin(dfG.patient_id)]
    del x

    # the difference in patient ages is causing problems with the counts
    # ie someone is 49 on their first visit and 50 on the second, but they're
    # admitted twice in a year for the same bundle id
    # so take only the patient's youngest age on record
    df = df.merge(min_age_df, how='left', on='patient_id')
    df['age_diff'] = df['age'] - df['age_min']
    print("age diffs are", df['age_diff'].value_counts(dropna=False))
    assert df['age_diff'].max() <= 1
    df.drop(['age', 'age_diff'], axis=1, inplace=True)
    df.rename(columns={'age_min': 'age'}, inplace=True)
    del dfG

    # remove null ages and sexes values
    df = df[df['age'].notnull() & df['sex_id'].notnull()]

    # remove sexes that need to be age split
    df = df[df.sex_id != 9]

    print(df.patient_id.unique().size)
    return df

def expandgrid(*itrs):
    # create a template df with every possible combination of
    #  age/sex/year/location to merge results onto
    # define a function to expand a template with the cartesian product
    product = list(itertools.product(*itrs))
    return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

def loop_over_agg_types(df, cause_type, map_version, prod=True):
    # now loop over every possible way to sum up cases
    agg_types = ['inp_pri', 'inp_any']

    # counter to determine to merge or append
    loop_counter = 1

    for agg_type in agg_types:
        print("beginning {} individual calculations".format(agg_type))
        # drop DX depending on inp/otp/primary/any
        if agg_type == 'inp_pri':
            # drop all non inpatient primary data
            dat_indv = df[(df.diagnosis_id == 1)].copy()
            dat_claims = df[(df.diagnosis_id == 1)].copy()
        if agg_type == 'inp_any':
            # source is inp only so keep everything
            dat_indv = df.copy()
            dat_claims = df.copy()

        # if the subset dataframe is empty move on to next set
        if dat_indv.shape[0] == 0:
            continue

        # we also want to go from claims data to estimates for individuals
        prev = dat_indv[dat_indv['{}_measure'.format(cause_type)] == 'prev'].copy()
        # drop all the duplicates for prev causes, equivalent to a 365 day duration
        prev.drop_duplicates(subset=['patient_id', cause_type + '_id'], inplace=True)

        inc = dat_indv[dat_indv['{}_measure'.format(cause_type)] == 'inc'].copy()

        # create incidence estimates
        final_inc = []
        if inc.shape[0] > 0:
            inc = clinical_mapping.apply_durations(inc, map_version=map_version, cause_type=cause_type, prod=prod, fill_missing=False)

            # compare pd concat to appending a list
            start = time.time()

            ##### ATTEMPTS TO SPEED THINGS UP#######
            # drop all combinations of patient ID and nfc that occur on same day
            inc.drop_duplicates(subset=['patient_id', 'adm_date',
                                        cause_type + '_id'], inplace=True)

            # if an enrollee ID is only present once in this dataframe then thats
            # it, theres just one individual. No need to pass these rows to our
            # recursive function
            r = inc.groupby('patient_id').size().reset_index()
            l = inc.groupby('patient_id')[cause_type + '_id'].nunique().reset_index()
            m = l.merge(r, how='outer', on='patient_id')
            id_array = m.loc[m[cause_type + '_id'] == m[0], 'patient_id']
            inc_indv = inc[inc.patient_id.isin(id_array)].copy()
            
            del r, l, m

            final_inc.append(inc_indv)
            # remove these IDs from the object that goes to recursive dur
            inc = inc[~inc.patient_id.isin(id_array)].copy()
            assert (inc.patient_id.value_counts() > 1).all(),\
                "There are enrollee IDs with fewer than 2 value counts"

            inc.sort_values(by=['patient_id', cause_type + '_id', 'adm_date'], inplace=True)

            inc = inc.groupby(['patient_id', cause_type + '_id'])
            
            counter = 0
            for patient_id, cause_df in inc:
                counter += 1
                if counter % 15000 == 0:
                    print("{} Still going...".format(round((time.time() - start)/60, 2)))
                final_inc.append(mei.recursive_duration(cause_df, pd.DataFrame(), 0, 0))
            print("{} done in {} min".format(agg_type, (time.time()-start)/60))

        # bring the data back together
        if len(final_inc) > 0:
            inc_df = pd.concat(final_inc)
            dat_indv = pd.concat([inc_df, prev], sort=False)
            # actually I think these are just called icg durations b/c they're defined at the icg level and then expanded
            # somewhat artificially to bundle
            drop_cols = [col for col in ['adm_limit', '{}_duration'.format(cause_type)] if col in dat_indv.columns]
            dat_indv.drop(labels=drop_cols, axis=1, inplace=True)
        else:
            dat_indv = prev.copy()

        # null rows are lost in the groupby so these max cols are used
        # to make sure we're not losing any extra data beyond these nulls
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

        print("Review the null values, ideally there would be zero in all columns",
              dat_claims.isnull().sum())
        print("Review the null values, ideally there would be zero in all columns",
              dat_indv.isnull().sum())
        # groupby and collapse summing cases
        groups = ['location_id', 'year_start', 'year_end',
                    'age', 'sex_id', cause_type + '_id']
        # add these assertions because pandas groupby is very aggressive with
        # dropping NAs. set a 20% data loss threshold
        if dat_indv.shape[0] > 2000:
            assert (dat_indv[groups].isnull().sum() < dat_indv.shape[0] * .2).all()
            assert (dat_claims[groups].isnull().sum() < dat_claims.shape[0] * .2).all()

        # perform the groupby
        dat_claims = dat_claims.groupby(groups).agg({col_name_a: 'sum'}).reset_index()
        dat_indv = dat_indv.groupby(groups).agg({col_name_i: 'sum'}).reset_index()

        if loop_counter == 1:
            print("Creating template df from the dat_claims object")
            template_df = dat_claims
            loop_counter += 1  # set to 2, won't overwrite on next loop
        else:
            print("Merging dat claims object onto template df b/c it already exists")
            # merge onto our template df created above
            template_df = template_df.merge(dat_claims, how='outer', on = ['age', 'sex_id',
                                            'location_id', 'year_start',
                                            'year_end', cause_type + '_id'])

        template_df = template_df.merge(dat_indv, how='outer', on = ['age', 'sex_id',
                                        'location_id', 'year_start',
                                        'year_end', cause_type + '_id'])

        # check sum of cases to ensure we're not losing beyond what's expected
        assert template_df[col_name_a].sum() == claims_sum,\
            "Some cases lost. claims sum is {} type is {} data col sum is {}".\
            format(claims_sum, col_name_a, template_df[col_name_a].sum())
        assert template_df[col_name_i].sum() == indv_sum,\
            "Some cases lost. individual sum {} {} sum {}".\
            format(indv_sum, col_name_i, template_df[col_name_i].sum())

    # remove rows where every value is NA
    case_cols = template_df.columns[template_df.columns.str.endswith("_cases")]
    col_sums = template_df[case_cols].sum()
    template_df.dropna(axis=0, how='all', subset=case_cols,
                       inplace=True)
    assert (col_sums == template_df[case_cols].sum()).all()

    return template_df

def main(df, map_version='current', cause_type='icg'):
    """
    run all the functions defined in this script to go from inpatient admissions
    to inpatient individuals
    """
    df = clean_bad_ids(df)

    if df.shape[0] == 0:
        pass
    else:
        df = clinical_mapping.map_to_gbd_cause(df, input_type='cause_code',
                                               output_type=cause_type,
                                               write_unmapped=False,
                                               truncate_cause_codes=True,
                                               extract_pri_dx=True,
                                               prod=True, map_version=map_version,
                                               groupby_output=False)

        df = loop_over_agg_types(df, map_version=map_version, cause_type=cause_type)

    return df
