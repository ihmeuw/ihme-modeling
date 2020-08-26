"""
apply and test the modeled CFs to hospital data, specifically Maternal and UTLA

"""
import getpass
import sys
import glob
import os
import functools
import warnings
import pandas as pd
from db_queries import get_location_metadata

user = getpass.getuser()
repo = "FILEPATH".format(user)
sys.path.append(repo + "Functions")
import hosp_prep

def apply_corrections(df, run_id, cf_model_type):
    """
    Applies the marketscan correction factors to the hospital data at the
    icg level.  The corrections are merged on by 'age_start', 'sex_id',
    'icg_id' and 'location_id'.  Reads in the corrections from 3 static csv.

    Parameters:
        df: Pandas DataFrame
            Must be aggregated and collapsed to the icg level
        run_id: (int or str)
            Identifies which clinical run we're using, ie 1, 2, 'test'
    """

    assert "icg_id" in df.columns, "'icg_id' must exist."

    start_columns = df.columns


    if cf_model_type == 'rmodels':
        corr_files = glob.glob("FILEPATH"\
                               "FILEPATH".format(run_id))
        id_cols = ['age_start', 'sex_id', 'cf_location_id', 'icg_id', 'icg_name']
    elif cf_model_type == 'mr-brt':

        corr_files = glob.glob("FILEPATH".format(run_id))
        id_cols = ['age_start', 'sex_id', 'icg_id', 'icg_name']
    else:
        assert False, "{} is not a recognized correction factor type".format(cf_model_type)

    idx = -4

    assert corr_files, "There are no correction factor files"

    corr_list = []
    cf_names = []
    for f in corr_files:

        draw_name = os.path.basename(f)[:idx]

        draw_name = draw_name[5:]
        cf_names.append(draw_name)

        dat = pd.read_csv(f)


        dat.rename(columns={'mean_' + draw_name: draw_name}, inplace=True)
        if "Unnamed: 0" in dat.columns:
            dat.drop("Unnamed: 0", 1, inplace = True)
        pre_rows = dat.shape[0]

        assert dat.shape[0] == pre_rows, "The number of rows changed"


        if draw_name == 'prevalence' and cf_model_type == 'rmodels':
            locs = get_location_metadata(location_set_id = 35)
            locs = pd.concat([locs, locs.path_to_top_parent.str.split(",", expand=True)], axis=1)
            locs = locs[locs[3].notnull()]
            locs['cf_location_id'] = locs[3].astype(int)
            locs = locs[['cf_location_id', 'super_region_id']].drop_duplicates()

            dat.rename(columns={'cf_location_id': 'super_region_id'}, inplace=True)
            dat = dat.merge(locs, how='left', on='super_region_id')
            dat.drop('super_region_id', axis=1, inplace=True)

        corr_list.append(dat)

        del dat


    correction_factors = functools.reduce(lambda x, y:\
        pd.merge(x, y,
                 on=id_cols, how='outer'), corr_list)

    if 'sex' in correction_factors.columns:

        correction_factors.rename(columns={'sex': 'sex_id'}, inplace=True)


    df = hosp_prep.group_id_start_end_switcher(df)


    id_cols = [f + "_id" if f == "sex" else f for f in id_cols]

    pre_shape = df.shape[0]


    locs = get_location_metadata(location_set_id=35)[['location_id', 'path_to_top_parent']]
    locs = pd.concat([locs, locs.path_to_top_parent.str.split(",", expand=True)], axis=1)
    locs = locs[locs[3].notnull()]
    locs['cf_location_id'] = locs[3].astype(int)
    locs = locs[['cf_location_id', 'location_id']]
    df = df.merge(locs, how='left', on='location_id')



    df = df.merge(correction_factors, how='left', on=id_cols)

    assert pre_shape == df.shape[0], ("You unexpectedly added rows while "
        "merging on the correction factors. Don't do that!")


    for col in ['super_region_id', 'model_prediction', 'cf_location_id']:
        if col in df.columns:
            df.drop(col, axis=1, inplace=True)





    for level in cf_names:
        df["mean_" + level] = df["mean_raw"] * df[level]


    df = hosp_prep.group_id_start_end_switcher(df)




    df.drop(cf_names, axis=1, inplace=True)

    assert set(start_columns).issubset(set(df.columns)), """
        Some columns that were present at the start are missing now"""

    return df


def test_corrections(df):

    print(df.mean_incidence.isnull().sum())



    for ctype in ["incidence", "prevalence", "indvcf"]:

        assert (df.loc[df['lower_' + ctype].notnull(), 'lower_' + ctype] <=
                df.loc[df["lower_" + ctype].notnull(), 'mean_' + ctype]).all(),\
            "lower_{} should be less than mean_{}".format(ctype, ctype)

        assert (df.loc[df["upper_" + ctype].notnull(), 'mean_' + ctype] <=
                df.loc[df["upper_" + ctype].notnull(), 'upper_' + ctype]).all(),\
            "mean_{} should be less than upper_{}".format(ctype, ctype)
    return


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

    df = get_covariate_estimates(covariate_id=1099, gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    df.rename(columns={'year_id': 'year_start'}, inplace=True)





    if df.mean_value.mean() > 1 and max_treat < 1:
        warn_msg = """Increasing max_treat variable 100X. Mean of the HAQi column is larger
        than 1. We assume this means the range is from 0 to 100. Summary stats for the
        mean_value column in the haqi covar are \n {}""".format(df.mean_value.describe())
        warnings.warn(warn_msg)
        max_treat = max_treat * 100



    df.loc[df.mean_value > max_treat, 'mean_value'] = max_treat



    min_df = df.mean_value.min()


    df['haqi_cf'] = \
        min_treat + (1 - min_treat) * ((df['mean_value'] - min_df) / (max_treat - min_df))


    df['year_end'] = df['year_start']
    df = df[df.year_start > 1987].copy()
    df = hosp_prep.year_binner(df)


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





    return df
