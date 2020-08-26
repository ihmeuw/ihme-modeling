"""
Mr Brt outputs are kind of ugly
I think he needs a buddy

This script aggregates about 1,000-3,000 individual files from mr-brt models to currently
retain the draws which are written by age and sex within the run_id paradigm

"""
import pandas as pd
import numpy as np
from scipy.special import expit
import warnings
import sys
import multiprocessing
import functools

from clinical_info.Functions import hosp_prep
from clinical_info.Mapping import clinical_mapping

def ernie(ids, cause_type, run_id, use_draw_mean, retain_draws, file_suffix=""):
    """
    Takes a bundle id, reads in mr brt draws, corrects col names
    fixes age_start values, exp to normal space
    Params:
        ids (list) either bundle_ids or icg_ids
        cause_type (str) either 'bundle' or 'icg'
        run_id (int or str) which run we doin
        use_draw_mean (bool) mr brt produces point estimates that differ
                             from the mean of the draws, if True calc draw mean
                             else use point estimates
        retain_draws (bool) this basically identifies the type of output you want
                            False outputs a single file with just mean/upper/lower
                            True outputs the full draws split by age and sex
    """
    restrict = clinical_mapping.create_bundle_restrictions()
    counter = 0
    df_list = []
    for cf in ['cf1', 'cf2', 'cf3']:
        for an_id in ids:
            counter += 1
            dat = read_brt(an_id, run_id, cf, cause_type)
            if dat.shape[0] == 0:
                continue
            if 'X_age_start' not in dat.columns:
                warnings.warn("theres no age start values?!?!")
                continue

            if cf == 'cf1':
                space = 'logit'
            else:
                space = 'log'

            dat = exp_brt(dat, space, cap=1e7)  # set cap at 10 million

            dat = clean_brt(dat, restrict)
            
            if 'sex_id' not in dat.columns:
                dat['sex_id'] = get_sex_id(an_id, run_id, cf, cause_type)

            dat = get_summary_stats(dat, use_draw_mean=use_draw_mean, cause_type=cause_type, retain_draws=retain_draws)
            df_list.append(dat)
            print("processing {} {} {}. This is {}/{}".format(cause_type, an_id, cf, counter, len(ids)*3))

    res = pd.concat(df_list, ignore_index=True, sort=False)

    if cause_type == 'bundle':
        # apply the parent/child CF mapping for certain MNCH causes.
        pre_bundles = set(res.bundle_id.unique())
        cong_bundles = [610, 437, 438, 638, 606, 624]
        cong_cf_dict = {610: 608,  # creates mapping from children: parent bundles
                        437: 3029, 438: 3029, 638: 3029,
                        606: 602,
                        624: 620}
        res = res[~res.bundle_id.isin(cong_bundles)]  # remove original child CFs
        fix_list = []
        for b in cong_cf_dict.keys():
            if b in pre_bundles:
                tmp = res[res.bundle_id == cong_cf_dict[b]].copy()
                tmp['bundle_id'] = b  # overwrite parent id with child
                fix_list.append(tmp)
            else:
                continue
        res = pd.concat([res] + fix_list, sort=False, ignore_index=True)
        diffs = pre_bundles.symmetric_difference(set(res.bundle_id.unique()))
        assert not diffs, "These bundles don't match {}".format(diffs)


    base = "FILEPATH".format(r=run_id)
    if retain_draws:
        # get age group_id
        res = prep_age_groups(res)
        for age in res.age_group_id.unique():
            for sex in [1, 2]:
                write_path = "{b}/{c}/by_age_sex/{a}_{s}{f}.csv".format(b=base, c=cause_type, a=age, s=sex, f=file_suffix)
                tmp = res.query("age_group_id == @age & sex_id == @sex")
                tmp.to_csv(write_path, index=False)

    else:
        write_path = "{b}/{c}/{c}_id_CFs_no_draws{f}.csv".format(b=base, c=cause_type, f=file_suffix)
        res.to_csv(write_path, index=False)
    return res


def prep_age_groups(df):
    df['age_end'] = df['age_start'] + 5
    df.loc[df['age_start'] == 1, 'age_end'] = 5
    df.loc[df['age_start'] == 0, 'age_end'] = 1
    df.loc[df['age_start'] == 95, 'age_end'] = 125

    df = hosp_prep.group_id_start_end_switcher(df.copy(), remove_cols=False)
    df.drop(['age_end'], axis=1, inplace=True)
    return df

def get_sex_id(an_id, run_id, cf, cause_type):
    """
    Sometimes the model draws files don't have sex ID. I think this is due to sex
    restrictions. But we still need that data in our process. Pull it from the input data files
    """
    assert cf in ['cf1', 'cf2', 'cf3'], 'bad cf name'

    base = "FILEPATH/run_{r}".format(r=run_id)
    readpath = base + "FILEPATH/input_data.csv".\
                        format(r=run_id, cf=cf, i=an_id, c=cause_type)
    df = pd.read_csv(readpath)
    assert df.sex_id.unique().size == 1
    sex_id = df.sex_id.iloc[0]
    return sex_id

def read_brt(an_id, run_id, cf, cause_type):
    """
    reads in mrbrt files from a clinical run
    """
    assert cf in ['cf1', 'cf2', 'cf3'], 'bad cf name'

    base = "FILEPATH/run_{r}".format(r=run_id)
    readpath = base + "FILEPATH/model_draws.csv".\
                        format(r=run_id, cf=cf, i=an_id, c=cause_type)
    summarypath = base + "FILEPATH".\
                        format(r=run_id, cf=cf, i=an_id, c=cause_type)
    try:
        add_age_col = False
        df = pd.read_csv(readpath)
        if 'X_age_start' not in df.columns:
            warnings.warn("There is no age column present. This only happens for neonatal bundles")
            df['X_age_start'] = 0.5
            add_age_col = True
        # cols related to some prep nick was doing but we don't need
        drops = [d for d in ['limit', 'index', 'age_group_id', 'age_end', 'sex'] if d in df.columns]
        if drops:
            df.drop(drops, axis=1, inplace=True)

        # drop neonatal groups for now
        df = df[(df['X_age_start'] == 0.5) | (df['X_age_start'] > .999)]
        # drop duplicates per NickR they're probably created in a merge he's doing
        df.drop_duplicates(inplace=True)

        pre = df.shape
        merge_cols = [col for col in ['X_sex_id', 'X_age_start'] if col in df.columns]
        sdf = pd.read_csv(summarypath)
        if add_age_col:
            sdf['X_age_start'] = 0.5
        sdf = sdf[merge_cols + ['Y_mean']]
        sdf.drop_duplicates(inplace=True)

        df = df.merge(sdf[merge_cols + ['Y_mean']], how='left', on=merge_cols)
        assert pre[0] == df.shape[0] and pre[1] == df.shape[1] - 1, "row and column counts changed"

        df.rename(columns={'Y_mean': 'point_estimate'}, inplace=True)
        # we gotta have bundle_id or icg_id on our data!
        df['{c}_id'.format(c=cause_type)] = an_id
        df['cf_type'] = cf
        df['problematic'] = False
        ages = df['X_age_start'].unique().size
        if ages != 21:
            warnings.warn("\n\n\n bundle {} only has {} ages!!!\n\n\n".format(an_id, ages))

    except Exception as e:
        warnings.warn("{} ID {} for {} isn't there!!!! {}".format(cause_type, an_id, cf, e))
        df = pd.DataFrame()
    return df

def get_summary_stats(df, use_draw_mean, cause_type, retain_draws, sample_draws=True):
    """
    Get the mean/upper/lower from mr brt draws
    The draws are sorted so that's wack
    """
    draws = df.filter(regex='draw_').columns.tolist()

    if sample_draws:
        if 'Unnamed: 0' in df.columns:
            print("removing the unnamed index col from df")
            df.drop('Unnamed: 0', axis=1, inplace=True)
        pre = df.shape[1]
        draws = pd.Series(draws).sample(len(draws), replace=False).tolist()
        df = df[['age_start', 'sex_id', '{i}_id'.format(i=cause_type), 'cf_type', 'problematic', 'point_estimate'] + draws].copy()
        assert pre == df.shape[1], "draw counts changed"

    if retain_draws:
        df.drop(['point_estimate', 'problematic'], inplace=True, axis=1)
    else:
        if use_draw_mean:
            df['mean'] = df[draws].mean(axis=1)
        else:
            df.rename(columns={'point_estimate': 'mean'}, inplace=True)

        # calculate quantiles for every row, then transpose and col bind
        quant = df[draws].quantile([0.025, 0.975], axis=1).transpose()
        # rename columns
        quant.columns = ['lower', 'upper']
        # col bind quant to env
        df = pd.concat([df, quant], axis=1)

        df.loc[df['mean'] < df['lower'], 'problematic'] = True
        df.loc[df['upper'] < df['mean'], 'problematic'] = True
        df.drop(draws, axis=1, inplace=True)

    return df


def fix_ages(df, restrict):
    """
    mr brt uses age mid points, correct these back to age_start
    """
    # good age starts to check results
    good_ages = hosp_prep.get_hospital_age_groups()
    
    if 'bundle_id' in df.columns:
        # get the age restrictions we expect
        tmp_res = restrict.query("bundle_id in ({})".format(df.bundle_id.unique()))[['yld_age_start', 'yld_age_end']]
        assert tmp_res.shape[0] == 1, "wrong number of restriction rows"
        min_age, max_age = tmp_res.iloc[0]
        good_ages = good_ages.query("age_start >= @min_age & age_start <= @max_age")
        
        # fix the single age neonatal bundles
        bundles = [502, 80, 82, 458, 500]
        if df.bundle_id.iloc[0] in bundles:
            warnings.warn("We're duplicating the only model value for bundles {}".format(bundles))
            df_list = [df]
            # duplicates the neonatal models for each age group
            ages = [3] + list(np.arange(7.5, 99, 5))
            for an_age in ages:
                tmp = df.copy()
                tmp['age_start'] = an_age
                df_list.append(tmp)
            df = pd.concat(df_list, sort=False, ignore_index=True)
            weird_max_age = max_age + 2.5
            df = df.query("age_start >= @min_age & age_start <= @weird_max_age").copy().drop_duplicates()

    # midpoint for 0-1 group
    df.loc[df['age_start'] == 0.5, 'age_start'] = 0
    # fix 1-4
    df.loc[df['age_start'] == 3, 'age_start'] = 1
    # fix terminal age group
    term_age = df['age_start'].max()
    if term_age in [110, 97.5]:
        df.loc[df['age_start'] == term_age, 'age_start'] = 95

    # drop neonatal groups for now
    df = df[(df['age_start'] == 0) | (df['age_start'] > .999)]

    df.loc[(df['age_start'] > 3) & (df['age_start'] < 95), 'age_start'] = df.loc[(df['age_start'] > 3) & (df['age_start'] < 95), 'age_start'] - 2.5

    diffs = set(df['age_start']) - set(good_ages['age_start'])
    assert not diffs, "bad ages present"

    return df


def clean_brt(df, restrict):
    """
    fix age start values and clean col names
    """
    name_dict = {'X_sex_id': 'sex_id', 'X_age_start': 'age_start'}

    df.rename(columns=name_dict, inplace=True)
    df.drop(['X_intercept', 'Z_intercept'], axis=1, inplace=True)

    df = fix_ages(df, restrict)

    return df

def exp_brt(df, space, cap=1e7):
    """
    exponentiate or expit the mr-brt draws because they're created in log or
    logit space and we need them to be normal when we apply them
    """
    drawcols = df.filter(regex='draw_').columns.tolist() + ['point_estimate']
    assert len(drawcols) == 1001, "wrong number of draws!"
    for draw in drawcols:
        if space == 'log':
            df[draw] = np.exp(df[draw])
        elif space == 'logit':
            df[draw] = expit(df[draw])
        else:
            assert False, "The space arg needs to be log or logit"
        # When a model fails it produces random draws between 0 and infinity. Some of these
        # values are so large they cause problems in our process.
        # Apply an arbitrary cap to the draws
        df.loc[df[draw] > cap, draw] = cap

    return df

if __name__ == '__main__':
    run_id = sys.argv[1]

    bundles = clinical_mapping.get_clinical_process_data('icg_bundle', map_version='current')
    icgs = clinical_mapping.get_clinical_process_data('cause_code_icg', map_version='current')

    # prep bundle level CF draws
    df = ernie(ids=bundles.bundle_id.unique().tolist(),
               cause_type='bundle',
               run_id=run_id,
               use_draw_mean=False,
               retain_draws=True,
               file_suffix='')

    assert False, "I don't think we can do this for ICGs currently, needs work"
    # prep icg level CFs with draws
    df = ernie(ids=icgs.icg_id.unique().tolist(),
          cause_type='icg',
          run_id=run_id,
          use_draw_mean=True, # this doesn't matter?
          retain_draws=True)

    
#
