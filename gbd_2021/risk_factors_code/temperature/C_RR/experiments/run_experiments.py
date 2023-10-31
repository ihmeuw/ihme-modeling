# -*- coding: utf-8 -*-
"""
    Main script running the experiments.

    Here is the outcome list:
    * lri
    * resp_copd
    * cvd_ihd
    * cvd_stroke
    * diabetes
    * inj_homicide
    * inj_suicide
    * ckd
    * inj_drowning
    * neonatal
"""
import numpy as np
import pandas as pd
import importlib
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import os
import sys
import pickle
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
# sys.path.append("../src")
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/temperature"))

import utils
import process
import viz
import actions
import score

DATA_PATH = {
    # 'old_data':'PATHWAY'\
    #            'FILENAME.csv',
    # 'new_data':'/PATHWAY/'\
    #            'FILENAME.csv'
    # 'PATHWAY'\
    #              'FILENAME.csv'
    'test_score':'/PATHWAY/'\
                 'FILENAME.csv'
}


def run_temp_model(outcome, path_to_data, path_to_result_folder, n_samples=1000):
    if not os.path.exists(path_to_result_folder):
        os.makedirs(path_to_result_folder)

    # # get temp stuff
    # df = pd.read_csv(path_to_data)
    # annual_temps = []
    # daily_temps = []
    # for annual_temp in np.arange(df.meanTempDegree.min(), df.meanTempDegree.max() + 1, 1):
    #     at_dt_temps = np.arange(df.loc[df.meanTempDegree == annual_temp, 'dailyTempCat'].min(),
    #                             df.loc[df.meanTempDegree == annual_temp, 'dailyTempCat'].max() + 0.1, 0.1)
    #     annual_temps += [np.repeat(annual_temp, at_dt_temps.size)]
    #     daily_temps += [at_dt_temps]
    # annual_temps = np.hstack(annual_temps)
    # daily_temps = np.hstack(daily_temps)
    # del df

    # load data
    # -------------------------------------------------------------------------
    tdata = process.load_data(path_to_data, outcome)
    tdata = actions.mtslice.adjust_mean(tdata)
    with open(path_to_result_folder + "/" + outcome + "_tdata.pkl", 'wb') as fwrite:
        pickle.dump(tdata, fwrite, -1)
    tdata_agg = actions.mtslice.aggregate_mtslice(tdata)
    tdata_agg = actions.mtslice.adjust_agg_std(tdata_agg)
    with open(path_to_result_folder + "/" + outcome + "_tdata_agg.pkl", 'wb') as fwrite:
        pickle.dump(tdata_agg, fwrite, -1)

    # fit the mean surface
    # -------------------------------------------------------------------------
    linear_no_mono = ('inj' in outcome)
    surface_result = actions.surface.fit_surface(tdata_agg,
                                                 linear_no_mono=linear_no_mono)
    with open(path_to_result_folder + "/" + outcome + "_surface_result.pkl", 'wb') as fwrite:
        pickle.dump(surface_result, fwrite, -1)

    # fit the study structure in the residual
    # -------------------------------------------------------------------------
    trend_result, tdata_residual = actions.mtslice.fit_trend(tdata,
                                                             surface_result,
                                                             inlier_pct=0.95)
    with open(path_to_result_folder + "/" + outcome + "_trend_result.pkl", 'wb') as fwrite:
        pickle.dump(trend_result, fwrite, -1)
    with open(path_to_result_folder + "/" + outcome + "_tdata_residual.pkl", 'wb') as fwrite:
        pickle.dump(tdata_residual, fwrite, -1)

    # predict surface with UI
    # -----------------------------------------------------------------------------
    annual_temps, daily_temps = utils.create_grid_points_alt(np.unique(tdata_agg.mean_temp), 0.1, tdata)
    curve_samples = process.sample_surface(
        mt=annual_temps, dt=daily_temps, num_samples=n_samples,
        surface_result=surface_result, trend_result=trend_result,
        include_re=True
    )
    curve_samples_df = pd.DataFrame(
        np.vstack([annual_temps, daily_temps, curve_samples]).T,
        columns=['annual_temperature', 'daily_temperature'] + [f'draw_{i}' for i in range(n_samples)]
    )
    curve_samples_df.to_csv(
        path_to_result_folder + "/" + outcome + "_curve_samples.csv",
        index=False
    )

    evidence_score = score.scorelator(curve_samples_df, trend_result, 
        tdata, outcome, path_to_result_folder)
    evidence_score.to_csv(
        path_to_result_folder + "/" + outcome + "_score.csv",
        index=False
    )

    del curve_samples, curve_samples_df

    # plot the result
    # -------------------------------------------------------------------------
    # 3D surface and the level plot
    actions.surface.plot_surface(tdata_agg, surface_result)
    plt.savefig(path_to_result_folder + "/" + outcome + "_surface.pdf",
                bbox_inches="tight")
    # plot uncertainty for each mean temp (can be subset of this)
    plt.figure(figsize=(8, 6))
    for mt in trend_result.mean_temp:
        fig, ax = plt.subplots(1, 1, figsize=(8, 5))
        viz.plot_slice_uncertainty(
                mt,
                tdata,
                surface_result,
                trend_result,
                ylim=[-1.0, 1.0], ax=ax)
        ax.set_xlabel("daily temperature")
        ax.set_title(outcome + " at mean temperature %i" %mt)
        fig.savefig(path_to_result_folder + "/" + outcome + "_slice_%i.pdf" % mt,
                    bbox_inches="tight")
        plt.close(fig)


def submit_all_outcomes():
    df = pd.read_csv(list(DATA_PATH.values())[0])
    outcomes = [i.replace('lnRr_', '') for i in list(df) if i.startswith('lnRr_') and not i.endswith(('_upper', '_lower'))]
    qsub_str = 'qsub -N {job_name} -P proj_mscm -q long.q -l m_mem_free=3G '\
        '-l fthread=6 -o omp_num_threads=6 -b y '\
        '-l archive=TRUE -e /FILEPATH'\
        '/PATHWAY'\
        '/FILENAME.py '\
        '{outcome}'

    for outcome in outcomes:
        job_id = os.popen(
            qsub_str.format(job_name=f'temp_{outcome}', outcome=outcome)
        ).read()
        print(job_id)


if __name__ == '__main__':
    for data_label, data_path in DATA_PATH.items():
        run_temp_model(outcome=sys.argv[1],
                       path_to_data=data_path,
                       path_to_result_folder=f'/PATHWAY/{data_label}/{sys.argv[1]}',
                       n_samples=10000)
