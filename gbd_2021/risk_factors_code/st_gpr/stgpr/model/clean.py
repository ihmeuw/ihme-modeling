
import os
import sys
import shutil
import pandas as pd

from stgpr.st_gpr import helpers as hlp
from stgpr.model import paths
from stgpr.model.config import *

######################## Functions #######################################################


def save_rake_summaries(run_id, output_path, holdout_num=0, param_set=0):

    # will need to merge on whichever level col designates national locations
    var = 'level_{}'.format(NATIONAL_LEVEL)

    # separate country locations and subnational locations
    locs = hlp.model_load(run_id, 'location_hierarchy',
                          output_path=output_path)
    subnat_locations = locs.loc[locs.level > NATIONAL_LEVEL, var].unique()

    # get gpr means and observations from locations needing raking
    df = hlp.model_load(run_id, 'gpr', param_set=param_set,
                        holdout=holdout_num, output_path=output_path)
    df = df.merge(locs[[SPACEVAR, var]], on=SPACEVAR, how='left')
    df = df.loc[~df[var].isin(subnat_locations)]

    inpath = '{}/rake_means_temp_{}/'.format(output_path, holdout_num)
    raked = pd.concat([pd.read_csv('{}/{}'.format(inpath, f)) for f in os.listdir(inpath)],
                      sort=True)

    df = pd.concat([df, raked], sort=True)
    df.drop(columns='level_3', inplace=True)
    df = df[IDS + ['gpr_mean', 'gpr_lower', 'gpr_upper']]

    hlp.model_save(df, run_id, 'raked',
                   holdout=holdout_num, param_set=param_set, output_path=output_path)


def clean_up(output_path, run_type, holdout_num):

    st_temps = ['st_temp_{}'.format(holdout_num)]
    gpr_temps = ['gpr_temp_{}'.format(holdout_num)]

    rake_temps = ['rake_temp_{}'.format(holdout_num)]
    if holdout_num == 0:
        rake_mean_temps = ['rake_means_temp_{}'.format(holdout_num)]
    else:
        rake_mean_temps = []

    full = st_temps + gpr_temps + rake_temps + rake_mean_temps

    for x in full:
        shutil.rmtree('{}/{}'.format(output_path, x))

    # last get rid of the two temp CSVs for stage1, if another holdout
    # has not already deleted
    stupid_files = ['{}/prepped.csv'.format(output_path)]

    for i in stupid_files:
        try:
            os.remove(i)
        except OSError:
            pass

    print('all clean')


if __name__ == '__main__':
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    run_type = sys.argv[3]
    holdout_num = int(sys.argv[4])
    draws = int(sys.argv[5])

    # make sure rmse jobs completed before erasing stuff
    msg = 'No fit stats! Not gonna clean til that is sorted.'
    assert os.path.exists('{}/fit_stats.csv'.format(output_path)), msg

    # find best param_set if run with holdouts - else it's automatically zero
    if run_type in ['in_sample_selection', 'oos_selection']:
        fit = pd.read_csv('{}/fit_stats.csv'.format(output_path))
        param_set = fit.loc[fit.best == 1, 'parameter_set'].unique()[0]
    else:
        param_set = 0

    if holdout_num == 0:
        print('Saving raked GP summaries.')
        save_rake_summaries(run_id, output_path, param_set=param_set)

    # clean out temp folders
    clean_up(output_path, run_type, holdout_num)

    # move draws_temp_0/{param_set} files so draws in a consistent location
    if (draws > 0) & (holdout_num == 0):
        inpath = '{}/draws_temp_0/{}'.format(output_path, param_set)
        outpath = '{}/draws_temp_0'.format(output_path)
        hlp.move_all_files(startpoint=inpath, endpoint=outpath)
        os.rmdir(inpath)

    fin = pd.DataFrame()
    fin.to_csv('{}/model_complete.csv'.format(output_path))
    print('Model complete. You win!')
