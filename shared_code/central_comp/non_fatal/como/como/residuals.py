import pandas as pd
import numpy as np
import os

from transmogrifier.super_gopher import SuperGopher
from db_tools import ezfuncs

this_path = os.path.abspath(os.path.dirname(__file__))
rkey = pd.read_excel('{}/config/residual_key.xlsx'.format(this_path))


def compute_global_ratios(year_id, drawcols):
    eng = ezfuncs.get_engine(conn_def="cod")
    ccv = pd.read_sql("""
        SELECT output_version_id FROM cod.output_version
        WHERE code_version=4 AND is_best=1""", eng).squeeze()
    sg = SuperGopher({
        'file_pattern': '{measure_id}_{location_id}.h5',
        'h5_tablename': 'draws'},
        'filepath/codcorrect/{ccv}/draws'.format(ccv=ccv))
    ylls = sg.content(location_id=1, year_id=year_id, sex_id=[1, 2],
                      measure_id=4)

    ratios = []
    for resid_cid, yldmap in rkey.groupby('input_cause_id'):
        # get the ylls
        these_ylls = ylls[ylls.cause_id == resid_cid]
        ratio_ylls = ylls[ylls.cause_id.isin(yldmap.ratio_cause_id.unique())]

        # aggregate the inputs to the appropriate level
        group_cols = ['age_group_id', 'year_id']
        these_ylls = these_ylls.groupby(group_cols)
        these_ylls = these_ylls[drawcols].sum().mean(axis=1)
        ratio_ylls = ratio_ylls.groupby(group_cols)
        ratio_ylls = ratio_ylls[drawcols].sum().mean(axis=1)

        # compute the ratio
        ratio = these_ylls / ratio_ylls
        ratio = ratio.reset_index()
        ratio = ratio.replace(np.inf, 0)
        ratio = ratio.replace(np.NaN, 0)

        ratio["cause_id"] = resid_cid
        ratios.append(ratio)

    df = pd.concat(ratios)
    df_male = df.copy()
    df_male["sex_id"] = 1
    df_female = df.copy()
    df_female["sex_id"] = 2

    return df_male.append(df_female)


def calc(location_id, ratio_df, output_type, drawcols, seq_ylds,
         cause_ylds):
    assert output_type in ["cause_id", "sequela_id"], (
        "output_type must be cause_id or sequela_id")

    resids = []
    for resid_cid, yldmap in rkey.groupby('input_cause_id'):

        # get the ylds
        if yldmap.ratio_level.unique().squeeze() == 'cause':
            ylds = cause_ylds[cause_ylds.cause_id.isin(
                yldmap.ratio_cause_id.unique())]
        else:
            ylds = seq_ylds[seq_ylds.sequela_id.isin(
                yldmap.ratio_sequela_id.unique())]

        # aggregate the inputs to the appropriate level
        group_cols = ['age_group_id', 'year_id', 'sex_id']
        ylds = ylds.groupby(group_cols)
        ylds = ylds.sum()

        # grab the ratio we want
        ratio = ratio_df[ratio_df.cause_id == resid_cid]
        ratio = ratio.set_index(group_cols)
        ratio = ratio.reindex(ylds.index, fill_value=0)
        ratio = ratio[[col for col in ratio if col != "cause_id"]]
        ratio = pd.DataFrame(
            data=pd.np.tile(ratio.values, (1, len(drawcols))),
            index=ratio.index,
            columns=drawcols)

        # apply the ratio
        ylds.ix[:, drawcols] = (ylds[drawcols].values * ratio.values)
        ylds = ylds.reset_index()

        # prep for export
        ylds["location_id"] = location_id
        ylds['measure_id'] = 3
        ylds = ylds[
            ['measure_id', 'location_id', 'year_id', 'age_group_id',
             'sex_id'] + drawcols]
        if output_type == 'cause_id':
            ylds['cause_id'] = yldmap.output_cause_id.unique().squeeze()
            resids.append(ylds)
        else:
            ylds['sequela_id'] = yldmap.output_sequela_id.unique().squeeze()
            resids.append(ylds)

    return pd.concat(resids)
