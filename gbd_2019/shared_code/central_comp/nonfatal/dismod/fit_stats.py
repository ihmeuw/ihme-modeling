import logging
import numpy as np
import os
import pandas as pd
from multiprocessing import Pool

from hierarchies.dbtrees import loctree as lt

from cascade_ode.importer import get_model_version
from cascade_ode.settings import load as load_settings
from cascade_ode.demographics import Demographics

# Set default file mask to readable-for all users
os.umask(0o0002)

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()


def read_data_pred_file(fdef):
    """ A wrapper for reading individual data pred files into
    DataFrames, returning an empty data frame where files are not
    found. """

    log = logging.getLogger(__name__)

    mvid, cv, l, s, y = fdef
    outdir = settings['cascade_ode_out_dir']

    f = ('{od}/{mv}/{cv}/locations/{l}/outputs/{s}/{y}/'
         'post_data_pred.csv'.format(od=outdir, cv=cv, mv=mvid, l=l, s=s, y=y))
    fadj = ('{od}/{mv}/{cv}/locations/{l}/outputs/{s}/{y}/'
            'post_data_pred.csv'.format(od=outdir, cv=cv, mv=mvid, l=l, s=s,
                                        y=y))
    try:
        df = pd.read_csv(f)
        dfadj = pd.read_csv(fadj)
        df = df.drop(['adjust_median', 'adjust_lower', 'adjust_upper'], axis=1)
        dfadj = dfadj[['adjust_median', 'adjust_lower', 'adjust_upper']]
        df = df.join(dfadj)
        df = df[df.a_data_id.notnull()]
        df['cv_id'] = cv
    except FileNotFoundError:
        log.warning('Missing pred file: {}'.format(f))
        df = pd.DataFrame()
    return df


def compile_dp_files(mvid):
    """ Compile all data prediction files for the given model version
    and return them as one DataFrame """

    demo = Demographics(mvid)
    mvm = get_model_version(mvid)

    # Identify root directories
    cv_run = mvm.cross_validate_id.values[0] == 1
    rootdirs = ['full']
    if cv_run:
        rootdirs.extend(['cv%s' % cvn for cvn in range(1, 11)])

    # Identify location set
    lsvid = mvm['location_set_version_id'].values[0]
    loctree = lt(location_set_version_id=lsvid,
                 gbd_round_id=demo.gbd_round_id)
    leaf_ids = [l.id for l in loctree.nodes]

    # Construct filepaths to read
    fdefs = []
    for cv in rootdirs:
        for l in leaf_ids:
            for s in ['male', 'female']:
                for y in demo.year_ids:
                    fdef = (mvid, cv, l, s, y)
                    fdefs.append(fdef)

    pool = Pool(40)
    df = pool.map(read_data_pred_file, fdefs)
    pool.close()
    pool.join()

    df = pd.concat(df)

    return df


def calc_rmses(df):
    thisdf = df[((df.adjust_median > 0) & (df.pred_median > 0))]
    rmses = thisdf.groupby(
        ['integrand', 'cv_id', 'hold_out']).apply(
            lambda x: np.sqrt(
                np.mean((np.log(x['adjust_median']
                                ) - np.log(x['pred_median']))**2)))
    rmses = rmses.reset_index()
    rmses.rename(columns={0: 'rmse'}, inplace=True)
    return rmses


def calc_mean_errors(df):
    thisdf = df[((df.adjust_median > 0) & (df.pred_median > 0))]
    means = thisdf.groupby(
        ['integrand', 'cv_id', 'hold_out']).apply(
            lambda x: np.mean((np.log(x['adjust_median']
                                      ) - np.log(x['pred_median']))**2))
    means = means.reset_index()
    means.rename(columns={0: 'mean_error'}, inplace=True)
    return means


def calc_coverage(df):
    unadj_bool = (df.adjust_lower == df.adjust_upper)
    df.loc[unadj_bool, 'adjust_lower'] = (
        df.loc[unadj_bool, 'meas_value'] - 1.96 *
        df.loc[unadj_bool, 'meas_stdev'])
    df.loc[unadj_bool, 'adjust_upper'] = (
        df.loc[unadj_bool, 'meas_value'] + 1.96 *
        df.loc[unadj_bool, 'meas_stdev'])
    adj_var = ((df.adjust_upper - df.adjust_lower) / (2 * 1.96))**2
    pred_var = ((df.pred_upper - df.pred_lower) / (2 * 1.96))**2
    cov_lower = df.pred_median - 1.96 * np.sqrt(adj_var + pred_var)
    cov_upper = df.pred_median + 1.96 * np.sqrt(adj_var + pred_var)
    df['covered'] = (
        (df.adjust_median > cov_lower) &
        (df.adjust_median < cov_upper))
    covered_counts = df.groupby(
        ['integrand', 'covered', 'cv_id', 'hold_out'])[
        'pred_median'].count()
    covered_counts = covered_counts.reset_index()
    covered_counts['pct'] = covered_counts.groupby(
        ['integrand', 'cv_id', 'hold_out'])['pred_median'].apply(
        lambda x: x / x.sum())
    covered_counts.rename(
        columns={'pred_median': 'adj_data_count'}, inplace=True)
    return covered_counts


# mvid = 33639
def write_fit_stats(mvid, outdir, joutdir):
    df = compile_dp_files(mvid)
    rmses = calc_rmses(df)
    ms = calc_mean_errors(df)

    # If we can't compute rmses and mean errors because of no non-zero
    # rows, exit early
    if ms.empty:
        return

    fs_df = rmses.merge(ms, on=['integrand', 'cv_id', 'hold_out'])
    cov_df = calc_coverage(df)

    try:
        os.makedirs(joutdir)
    except:
        pass
    try:
        os.chmod(joutdir, 0o775)
    except:
        pass
    fs_df.to_csv("%s/error_stats.csv" % joutdir, index=False)
    cov_df.to_csv("%s/coverage_stats.csv" % joutdir, index=False)

    measure_map = pd.read_csv("%s/measure_map.csv" % outdir)
    fs_df = fs_df.merge(measure_map, left_on='integrand', right_on='measure',
                        how='left')
    cov_df = cov_df.merge(measure_map, left_on='integrand', right_on='measure',
                          how='left')

    if len(fs_df.cv_id.unique()) > 1:
        fs_df = fs_df[fs_df.cv_id != 'full']
    fs_df = fs_df.groupby(['measure_id', 'hold_out'])[
        'rmse'].mean().reset_index()
    fs_df.loc[fs_df.hold_out == 0, 'fit_stat_id'] = 1
    fs_df.loc[fs_df.hold_out == 1, 'fit_stat_id'] = 2
    fs_df.rename(columns={'rmse': 'fit_stat_value'}, inplace=True)

    cov_df = cov_df[cov_df.covered]
    if len(cov_df.cv_id.unique()) > 1:
        cov_df = cov_df[cov_df.cv_id != 'full']
    cov_df = cov_df.groupby(['measure_id', 'hold_out'])[
        'pct'].mean().reset_index()
    cov_df.loc[cov_df.hold_out == 0, 'fit_stat_id'] = 3
    cov_df.loc[cov_df.hold_out == 1, 'fit_stat_id'] = 4
    cov_df.rename(columns={'pct': 'fit_stat_value'}, inplace=True)

    mv_fs = fs_df.append(cov_df)
    mv_fs['fit_stat_value'] = mv_fs.fit_stat_value.replace({np.inf: -9999})
    mv_fs[['measure_id', 'fit_stat_id', 'fit_stat_value']].to_csv(
        '%s/model_version_fit_stat.csv' % outdir, index=False)
    return mv_fs
