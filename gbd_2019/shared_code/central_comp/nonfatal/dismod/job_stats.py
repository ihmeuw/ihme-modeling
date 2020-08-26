from pathlib import Path
import requests
import pandas as pd
from db_tools.ezfuncs import query
from datetime import datetime as dt
from datetime import timedelta

from cascade_ode.sge_utils import max_run_time_on_queue, get_sec

"""
Script for producing a historical record of max runtime/gb per job/ME.
"""

root_dir = Path('PATH')
node_file = root_dir / Path('node_stats.h5')
varn_global_file = root_dir / Path('varnish_and_global_stats.h5')
ME_factors_file = root_dir / Path('safety_factors.csv')


def gen_data():
    start_date = dt(2018, 8, 1)
    end_date = dt(2019, 7, 5)

    results = []
    job_cols = [
        'job_name', 'ram_gb', 'runtime_min', 'cluster'
    ]

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(days=n)

    clusters = ['prod', 'fair']

    for cluster in clusters:
        qpid_api = f'API'
        for date in daterange(start_date, end_date):
            jobs = requests.get(qpid_api, params={
                'limit': 50000,
                'project': ['proj_dismod', 'proj_tb'],
                'ran_after': date,
                'finished_before': date + timedelta(days=1),
                'job_prefix': 'dm_'
            }).json()
            print(f"{cluster}, {date}, {len(jobs)}")
            if jobs:
                df = pd.DataFrame(jobs, columns=job_cols)
                df.to_csv(f"PATH",
                          index=False)
                results.append(df)

    df = pd.concat(results)
    df['varnish'] = df.job_name.str.contains('varnish')
    df['driver'] = df.job_name.str.contains('driver')
    df['G0'] = df.job_name.str.contains('G0')
    df['node'] = ~(df.varnish | df.driver | df.G0)
    df.to_csv("PATH", index=False)


def read_data():
    return pd.read_csv("PATH")


def format_df(df):
    df = df[df.job_name.str.startswith('dm_')]
    df = df[~df.job_name.str.contains('drew')]
    df = df[~df.job_name.str.contains('redo')]
    df = df[~df.job_name.str.contains('P')]
    df['node'] = ~(df.varnish | df.driver | df.G0)
    splits = df.job_name.str.split('_', expand=True)
    df['model_version_id'] = splits.get(1).astype(int)
    df.loc[df.node, 'location_id'] = splits.loc[df.node].get(2).astype(int)
    df.loc[df.node, 'sex'] = splits.loc[df.node].get(3)
    df.loc[df.node, 'year'] = splits.loc[df.node].get(4)
    df.loc[df.node, 'crossval'] = splits.loc[df.node].get(5)
    return df


def add_metadata(df):
    mes = query("""
                select model_version_id, mv.modelable_entity_id,
                modelable_entity_name
                from epi.model_version mv join
                epi.modelable_entity using (modelable_entity_id)
                where
                model_version_id in :mvs""",
                parameters={"mvs": df.model_version_id.unique().tolist()},
                conn_def="epi")
    df = df.merge(mes, how='inner')
    return df


def compute_stats_node(df):
    """
    get max runtime for each ME for each job, and max ram
    """
    df = df[df.node]
    grp = df.groupby(
        ['modelable_entity_id', 'location_id', 'sex', 'year']).agg(
            {'runtime_min': 'max', 'ram_gb': 'max'})
    return grp


def compute_stats_varnish_global(df):
    """
    get max runtime for each ME for each job, and max ram
    """
    grp_varn = df[df.varnish].groupby(
        ['modelable_entity_id']).agg(
            {'runtime_min': 'max', 'ram_gb': 'max'}).assign(varnish=True)
    grp_global = df[df.G0].groupby(
        ['modelable_entity_id']).agg(
            {'runtime_min': 'max', 'ram_gb': 'max'}).assign(G0=True)
    grp_global = grp_global.rename({'G0': 'global'}, axis=1)

    return pd.concat([grp_varn, grp_global]).fillna(False)


def prune_job_stats(df):
    """
    Lets make sure we stay within satisfiable bounds. A job's runtime cannot
    exceed long.q max runtime (ie this was a job from old cluster)
    """
    runtime_limit = get_sec(max_run_time_on_queue('long.q'))/60
    df.loc[df.runtime_min > runtime_limit, 'runtime_min'] = runtime_limit
    return df


def save_for_production(df_w_meta, node_stats, vg_stats):
    vg_stats = vg_stats.query('ram_gb > 0 and runtime_min > 0')
    node_stats = node_stats.query('ram_gb > 0 and runtime_min > 0')
    df_w_meta.to_hdf(root_dir / Path('raw_new.h5'), format='fixed', key='data')
    node_stats.to_hdf(str(node_file).replace('.h5', '_new.h5'), format='fixed',
                      key='data')
    vg_stats.to_hdf(str(varn_global_file).replace('.h5', '_new.h5'),
                    format='fixed', key='data')


def main():
    df = format_df(read_data())
    meta = add_metadata(df)
    node_stats = compute_stats_node(meta)
    vg_stats = compute_stats_varnish_global(meta)
    vg_stats = prune_job_stats(vg_stats)
    save_for_production(meta, node_stats, vg_stats)
