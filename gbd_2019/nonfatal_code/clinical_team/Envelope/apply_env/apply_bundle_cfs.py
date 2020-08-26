"""
Apply final corrections to 5 year bundle data and write to drive

I think this will be a function that the create_bundle_draw worker script pulls in

1) Append parent bundles onto the map using the flat file that inj provided our team
2) Apply the mr-brt correction factors to estimate_id 1 and 6 at the draw level
        for maternal data and full_coverage data only mean is used
3) calculate mean/upper/lower values of those draws
4) Write the full draw products to drive for review
5) identify the correct value to use for a given row's sample size
6) Merge measure values back onto the data using the clinical_mapping db
7) set the uncertainty upper/lower values to null when sample size is present.
        I think this is mostly housekeeping/cleaning the data a little
8) apply the injury correction factors
9) append on the HAQi corrections and divide ALL estimates by their scalar 
10) each job writes final data to drive (~252 files in total)

"""
import os
import sys
import warnings
import pandas as pd
import numpy as np
from collections import namedtuple
from getpass import getuser

from db_queries import get_population, get_covariate_estimates, get_cause_metadata

from clinical_info.Functions import hosp_prep
from clinical_info.Mapping import clinical_mapping


def append_parent_bundle(df, parent_data_path):
    def create_injury_hierarchy(parent_data_path):
        # Prep data
        data = pd.read_csv(parent_data_path)
        data = data.sort_values('e_code')
        data = data.rename(columns={"Level1-Bundle ID": "bundle_id"})
        data.child = data.child.notnull()
        data.parent = data.parent.notnull()

        # Create a hierarchy
        # - Since e codes are sorted we know that parent bundles are always
        # - directly above their children bundles.
        Bundle = namedtuple("Bundle", ["id", "name"])
        hierarchy = {}
        parent_bundle = None
        for index, row in data.iterrows():
            bundle = Bundle(row.bundle_id, row.e_code)
            child, parent = row.child, row.parent
            if parent:
                parent_bundle = bundle
                hierarchy[parent_bundle] = []
            elif child:
                assert parent_bundle, "Structure of parent child injury cause file broken" # We have found a child bundle that isn't directly below a parent
                hierarchy[parent_bundle].append(bundle)
            else:
                parent_bundle = None

        return hierarchy


    dtypes = dict(list(zip(df.columns, df.dtypes)))
    parent_dfs = []
    # All columns that aren't draws, mean or bundle_id or denoms
    groupby_columns = ['location_id', 'year_start', 'year_end','age_group_id',
                       'sex_id', 'nid', 'representative_id', 'estimate_id', 
                       'years', 'source', 'use_draws']

    hierarchy = create_injury_hierarchy(parent_data_path)
    for parent, children in list(hierarchy.items()):
        child_bundles = [child.id for child in children]
        child_df = df.loc[df.bundle_id.isin(child_bundles)]
        
        for names, group_df in child_df.groupby(groupby_columns):
            # 4/26/2019 sum the sample size cols
            columns_to_sum = ['population', 'sample_size', 'maternal_sample_size']
            if (group_df['use_draws'] == 1).all():
                columns_to_sum += child_df.filter(regex='^draw', axis=1).columns.tolist()
            elif (group_df['use_draws'] == 0).all():
                columns_to_sum += ["mean"]
            else:
                assert False, "Child injuries for parent bundle (id: {}) contains a mix of draws and mean".format(parent.id)
            estimates = group_df[columns_to_sum].sum()
            
            # Build a new row for this specific demographic/parent bundle and add it
            # to our parent dataframe
            row = pd.Series(names, index=groupby_columns)
            row = row.append(estimates)
            row.at['bundle_id'] = parent.id
            parent_dfs.append(row)

    parent_df = pd.concat(parent_dfs, sort=False, axis=1).transpose()
    result = df.append(parent_df, sort=False)
    result = result.astype(dtypes)
    return result

    # replace(np.nan, None seems to have generated some strange values for parent bundle means)
    # return (result
    #         .replace(np.nan, None)
    #         .astype(dtypes)
    #        )

def apply_CFs(estimate_df, correction_df):
    # ASSUME - DF and CF are both for the same age_group_id and sex_id
    # ASSUME - Draws are not ordered in either DF or CF
    final_dfs = []

    mean0_df = estimate_df.copy()  # to concat along with corrected data

    # just in case
    assert estimate_df['age_group_id'].unique().size == 1 and estimate_df['sex_id'].unique().size == 1,\
        "There are multiple ages or sexes which will mean the merge won't work correctly"
    correction_df = correction_df.drop(columns=['age_start', 'sex_id', 'age_group_id'])

    # The number of draws could differ between estimates and corrections, so use draws they have in common
    draw_columns = list(
        set(correction_df.filter(regex="^draw").columns) &
        set(estimate_df.filter(regex="^draw").columns)
    )

    group_cols = ["bundle_id", "use_draws"]
    for names, group_df in estimate_df.groupby(group_cols):
        bundle_id, use_draws = names
        
        # Set up our correction factors and get relavent info
        CF  = correction_df.loc[correction_df.bundle_id == bundle_id, draw_columns].values
        n_estimates = len(group_df)
        n_CFs = len(CF)
        
        # The algorithm is the same whether we use 1000 draws or a single mean. Take our estimates and
        # repeat them for each correction factor. For each correction factor tile them
        # for each estimate. Then multiply them.
        # In the case of draws, we are multiplying two vectors element-wise to produce a new vector
        # In the case of a mean, we are multiplying a vector of means with a vector of averaged correction factors
        # Q: What is the difference between repeat and tile? 
        # A:    tile(<a,b,c>, 2) = <a, a, b, b, c, c>
        #    repeate(<a,b,c>, 2) = <a, b, c, a, b, c> Notice that this gives us all combinations. 
        if use_draws:
            DRAWS = group_df[draw_columns].values
            corrected = np.repeat(DRAWS, n_CFs, axis=0) * np.tile(CF, (n_estimates, 1))
            
            estimate_cols = draw_columns
        else:
            MEAN = group_df["mean"].values
            n_draws = CF.shape[1]
            CF_MEAN = CF.sum(axis=1) / n_draws
            corrected = np.repeat(MEAN, n_CFs, axis=0) * np.tile(CF_MEAN, n_estimates)
            
            estimate_cols = ["mean"]
            
        # Construct the new DF - 
        # We are still missing all the old demographic info for each estimate. 
        # We can get this by merging both data frames. This will take care of the repetition issues.
        # We can merge on the old index, we just need to repeat (not tile) it, then set it as the 
        # index of the applied CFs. 
        index = np.repeat(group_df.index.values, n_CFs)
        cf_applied_estimates_df = pd.DataFrame(corrected, columns=estimate_cols, index=index)
        
        final = pd.merge(cf_applied_estimates_df, group_df.drop(columns=estimate_cols), left_index=True, right_index=True)
        cf_types = correction_df.loc[correction_df.bundle_id == bundle_id, 'cf_type'].values    
        final["cf_type"] = np.tile(cf_types, n_estimates)

        # fix the incorrect estimate_ids
        # a db conn would be best, but these estimate_ids should never change
        cf_dict = {'cf1_1': 2, 'cf2_1': 3, 'cf3_1': 4,
                   'cf1_6': 7, 'cf2_6': 8, 'cf3_6': 9}
        final['estimate_id'] = final['cf_type'] + "_" + final['estimate_id'].astype(str)
        final['estimate_id'].replace(cf_dict, inplace=True)
        final['estimate_id'] = pd.to_numeric(final['estimate_id'], errors='raise', downcast='integer')
        final_e = final['estimate_id'].unique()
        assert 1 not in final_e and 6 not in final_e

        final_dfs.append(final)
    
    return pd.concat([mean0_df] + final_dfs, sort=False).reset_index(drop=True)

def add_UI(df):
    """
    Per convo on 4/30/2019, we're going to use the mean/upper/lower of the product of
    mr-brt draws * env draws. We'll also store the median of the draws, but only for our team's use
    """
    df['use_draws'] = df['use_draws'].astype(bool)

    if 'upper' not in df.columns and 'lower' not in df.columns:
        df['upper'] = np.nan
        df['lower'] = np.nan

    draw_columns = df.filter(regex="^draw").columns.tolist()
    draws_df = df.loc[df['use_draws'], draw_columns].values

    df.loc[df['use_draws'], 'mean'] = df.loc[df['use_draws'], draw_columns].mean(axis=1)

    df['median_CI_team_only'] = np.nan
    df.loc[df['use_draws'], 'median_CI_team_only'] = df.loc[df['use_draws'], draw_columns].median(axis=1)

    df.loc[df['use_draws'], 'lower'] = np.percentile(draws_df, 2.5, axis=1)
    df.loc[df['use_draws'], 'upper'] = np.percentile(draws_df, 97.5, axis=1)

    return df

def write_draws(df, run_id, age_group_id, sex_id, year, drop_draws_after_write=True):
    """
    Write the file with draws of all estimate types (where available) to the run
    """
    write_path = FILEPATH.format(\
                                                                run=run_id,
                                                                age=age_group_id,
                                                                sex=sex_id,
                                                                year=year)
    hosp_prep.write_hosp_file(df, write_path, backup=False)

    if drop_draws_after_write:
        draw_cols = df.filter(regex='^draw_').columns.tolist()
        df.drop(draw_cols, axis=1, inplace=True)

    return df


def merge_measure(df, drop_mult_measures=True):
    cm = clinical_mapping.get_clinical_process_data("cause_code_icg", prod=True)
    cm = cm[['icg_id', 'icg_measure']].drop_duplicates()

    bun = clinical_mapping.get_clinical_process_data('icg_bundle', prod=True)
    measures = bun.merge(cm, how='left', on='icg_id')
    measures = measures[['bundle_id', 'icg_measure']].drop_duplicates()
    if drop_mult_measures:
        doubles = measures.bundle_id[measures.bundle_id.duplicated(keep=False)].unique()
        warnings.warn("We will be dropping distinct measures for bundle ids {}".format(doubles))
        measures.drop_duplicates(subset=['bundle_id'], inplace=True)
    measures.rename(columns={'icg_measure': 'measure'}, inplace=True)

    pre = df.shape[0]
    df = df.merge(measures, how='left', on='bundle_id')
    assert pre == df.shape[0], "Row counts have changed, not good"

    parent_inj = [264, 269, 270, 272, 275, 276]
    df.loc[df.bundle_id.isin(parent_inj), 'measure'] = 'inc'
    assert df.measure.isnull().sum() == 0, "There shouldn't be any null measures"
    
    df['measure'] = df['measure'].astype(str)
    return df

def align_uncertainty(df):
    """
    We're using different forms of uncertainty, depending on estimate type and
    data source so set the values we'd expect and test them
    """
    # A bunch of uncertainty checks
    # When mean_raw is zero, then sample_size should not be null.
    assert df.loc[df['mean'] == 0, "sample_size"].notnull().all(),\
        "Imputed zeros are missing sample_size in some rows."

    # Make upper and lower Null when sample size is not null
    est_types = [2, 3, 4]

    df.loc[(df['sample_size'].notnull()) &\
           (df['estimate_id'].isin(est_types)),
           ['upper', 'lower']] = np.nan

    return df

def get_5_year_haqi_cf(gbd_round_id, decomp_step, min_treat=0.1, max_treat=0.75):
    """
    A function to get the health access quality covariates data which we'll
    use to divide our mean_raw values by to adjust our estimates up

    Parameters:
        min_treat: float
            minimum access. Sets a floor for the CF. If 0.1 then the lowest possible CF will be 0.1,
            in practice this is a 10x increase in the estimate
        max_treat: float or int
            maximum acess. Sets a cap for the CF. If 75 then any loc/year with a covariate above 75
            will have a CF of 1 and the data will be unchanged
    """
    # get a dataframe of haqi covariate estimates
    df = get_covariate_estimates(covariate_id=1099, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    df.rename(columns={'year_id': 'year_start'}, inplace=True)

    if df.mean_value.mean() > 1 and max_treat < 1:
        warn_msg = """Increasing max_treat variable 100X. Mean of the HAQi column is larger 
        than 1. We assume this means the range is from 0 to 100. Summary stats for the 
        mean_value column in the haqi covar are \n {}""".format(df.mean_value.describe())
        warnings.warn(warn_msg)
        max_treat = max_treat * 100

    # set the max value
    df.loc[df.mean_value > max_treat, 'mean_value'] = max_treat

    # get min df present in the data
    # Note, should this just be 0.1?
    min_df = df.mean_value.min()

    # make the correction
    df['haqi_cf'] = \
        min_treat + (1 - min_treat) * ((df['mean_value'] - min_df) / (max_treat - min_df))

    # drop the years outside of hosp_data so year binner doesn't break
    df['year_end'] = df['year_start']
    warnings.warn("Currently dropping HAQi values before 1988 and after 2017")
    df = df[(df.year_start > 1987) & (df.year_start < 2018)].copy()
    df = hosp_prep.year_binner(df)

    # Take the average of each 5 year band
    df = df.groupby(['location_id', 'year_start', 'year_end']).agg({'haqi_cf': 'mean'}).reset_index()

    assert df.haqi_cf.max() <= 1, "The largest haqi CF is too big"
    assert df.haqi_cf.min() >= min_treat, "The smallest haqi CF is too small"

    return df

def apply_haqi_corrections(df, gbd_round_id, decomp_step):
    """
    merge the haqi correction (averaged over 5 years) onto the hospital data
    """
    haqi = get_5_year_haqi_cf(gbd_round_id, decomp_step)

    pre = df.shape
    df = df.merge(haqi, how='left', on=['location_id', 'year_start', 'year_end'])
    assert pre[0] == df.shape[0],\
        "DF row data is different. That's not acceptable. Pre shape {}. Post shape {}".\
        format(pre, df.shape)
    assert df.haqi_cf.isnull().sum() == 0,\
        "There are rows with a null haqi value. \n {}".format(\
            df[df.haqi_cf.isnull()])

    df = apply_haqi(df)

    return df

def clean_after_write(df):
    """
    data structure and type is mostly there, make some little changes
    """
    int_cols = ['nid', 'bundle_id']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast='integer', errors='raise')
        
    # lose cf_type and years (years seems potentiall useful though)
    df.drop(['cf_type', 'years'], axis=1, inplace=True)
    
    return df

def identify_correct_sample_size(df, full_coverage_sources):
    """
    We have 3 distinct types of sample sizes currently
    population: gbd population for UTLA data
    maternal_sample_size: ifd * asfr * gbd population for maternal causes
    sample_size: inpatient admissions for rows where mean == 0 NOTE: this is
        calculated from our cause fractions- so these values are only available
        currently for sources which use the envelope (ie not UTLA)
    
    THE ORDER IS VERY IMPORTANT
    UTLA pop is used, except for maternal bundles then live births are used
    inp admits are used for mean0==0, except for maternal bundles
    If the order is changed then sample_size values will change
    """
    df.rename(columns={'sample_size': 'inp_admits'}, inplace=True)
    
    # gen new ss column
    df['sample_size'] = np.nan
    
    # use gbd pop for full coverage
    df.loc[df['source'].isin(full_coverage_sources), 'sample_size'] =\
        df.loc[df['source'].isin(full_coverage_sources), 'population']

    # use inp admits for mean == 0
    df.loc[df['mean'] == 0, 'sample_size'] =\
        df.loc[df['mean'] == 0, 'inp_admits']

    # use maternal for maternal estimate
    df.loc[df['estimate_id'].isin([6, 7, 8, 9]), 'sample_size'] =\
        df.loc[df['estimate_id'].isin([6, 7, 8, 9]), 'maternal_sample_size']

    # drop original sample cols
    df.drop(['population', 'maternal_sample_size', 'inp_admits'], axis=1, inplace=True)

    return df

def apply_haqi(df):
    # divide mean0 by haqi
    assert 'haqi_cf' in df.columns, 'need the haqi correction'
    for col in ['mean', 'upper', 'lower']:
        df[col] = df[col] / df['haqi_cf']
    if 'median_CI_team_only' in df.columns:
        df['median_CI_team_only'] = df['median_CI_team_only'] / df['haqi_cf']

    return df

def apply_bundle_cfs_main(df, age_group_id, sex_id, year, run_id, gbd_round_id, decomp_step, full_coverage_sources):
    """
    Put everything above together into 1 function that will be called into the other bundle uncertainty script
    """
    CF_path = os.path.expanduser(FILEPATH.\
                                 format(r=run_id, age=age_group_id, sex=sex_id))
    cf_df = pd.read_csv(CF_path)

    # run data through the pipe
    # df = estimate_df.copy()
    df = append_parent_bundle(df)

    df['use_draws'] = df['use_draws'].astype(bool)
    df = apply_CFs(df, cf_df)

    df = add_UI(df)

    # can't sum UI vals, gotta sum draws
    draw_cols = pd.Series(df.filter(regex='^draw_').columns.tolist())
    for col in draw_cols.sample(7):
        hosp_prep.check_parent_injuries(df[df['use_draws']], col_to_sum=col, verbose=True)

    df = write_draws(df, run_id=run_id, age_group_id=age_group_id, sex_id=sex_id, year=year)

    df = clean_after_write(df)

    # unify sample size
    df = identify_correct_sample_size(df, full_coverage_sources)

    # merge measures on using icg measures
    df = merge_measure(df)

    # set nulls
    df = align_uncertainty(df)

    # apply the 5 year inj corrections
    df = hosp_prep.apply_inj_corrections(df, run_id)

    # apply e code proportion cutoff, removing rows under cutoff
    df = hosp_prep.remove_injuries_under_cutoff(df, run_id)

    # append and apply the haqi correction
    df = apply_haqi_corrections(df, gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    # final write to
    write_path = FILEPATH.format(\
                                                                run=run_id,
                                                                age=age_group_id,
                                                                sex=sex_id,
                                                                year=year)
    df.to_csv(write_path, index=False)
