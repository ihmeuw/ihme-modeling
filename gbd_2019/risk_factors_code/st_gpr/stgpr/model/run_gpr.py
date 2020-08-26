import os
import sys
import time

import numpy as np
import pandas as pd

from orm_stgpr.lib.util import transform as transform_helpers

from stgpr.model import paths
from stgpr.model.config import *
from stgpr.st_gpr import gpr as gp, helpers as hlp

np.seterr(invalid='ignore')

class RunGPR:
    def __init__(self,
                 run_id,
                 output_path,
                 location_group,
                 spacevar="location_id",
                 timevar="year_id",
                 agevar="age_group_id",
                 sexvar="sex_id",
                 mean_var='st',
                 amp_var='st_amp',
                 datavar='data',
                 variance_var='variance',
                 draws=0,
                 nparallel=100,
                 holdout_num=1,
                 n_params=1,
                 param_set=0):

        self.run_id = run_id
        self.loc_group = location_group
        self.n_params = n_params
        self.param_set = param_set
        self.draws = draws
        self.holdout_num = holdout_num
        self.nparallel = nparallel

        self.spacevar = spacevar
        self.timevar = timevar
        self.agevar = agevar
        self.sexvar = sexvar
        self.mean_var = mean_var
        self.amp_var = amp_var
        self.datavar = datavar
        self.variance_var = variance_var

        self.output_path = output_path

        self.ids = [self.spacevar, self.timevar, self.agevar, self.sexvar]

    def enumerate_parameters(self):

        parameters = hlp.model_load(
            self.run_id, 'parameters', param_set=None, output_path=self.output_path)
        self.data_transform = parameters.data_transform.iat[0]
        self.location_set_id = int(parameters.location_set_id.iat[0])
        self.gbd_round_id = int(parameters.gbd_round_id.iat[0])
        self.decomp_step = parameters.decomp_step.iat[0]

        # determine parallel_group
        self.prediction_location_ids = \
            hlp.get_parallelization_location_group(
                self.location_set_id,
                self.nparallel,
                self.loc_group,
                self.gbd_round_id,
                self.decomp_step)

    def pull_data(self):

        self.amplitude = hlp.model_load(self.run_id,
                                        'amp_nsv', self.holdout_num, self.param_set, output_path=self.output_path)

        # pull data and drop specified ko data
        adj_data = hlp.model_load(self.run_id,
                                  'adj_data', self.holdout_num, self.param_set, output_path=self.output_path)
        adj_data = hlp.drop_ko_values(adj_data,
                                      self.holdout_num, datavar=self.datavar)
        self.adj_data = adj_data

        # pull spacetime
        self.spacetime = hlp.model_load(self.run_id,
                                        'st', self.holdout_num, self.param_set, output_path=self.output_path)

        # pull locs
        locs = hlp.model_load(
            self.run_id, 'location_hierarchy', output_path=self.output_path)
        lvlcols = ['level_{}'.format(i) for i
                   in range(0, max(locs.level.unique() + 1))]
        self.locs = locs[[self.spacevar, 'level'] + lvlcols]

    def prep_gpr(self):

        # subset to locations of interest before merge
        if self.nparallel > 0:
            dt = self.adj_data.loc[
                self.adj_data[self.spacevar].isin(
                    self.prediction_location_ids)]
            dt = dt.loc[dt[self.datavar].notnull()]
            st = self.spacetime.loc[
                self.spacetime[self.spacevar].isin(
                    self.prediction_location_ids)]

        in_gpr = pd.merge(st, self.amplitude,
                          on=[self.spacevar, self.sexvar])

        data_cols = [self.datavar, self.variance_var]
        in_gpr = pd.merge(in_gpr,
                          dt[self.ids + data_cols], on=self.ids, how='outer')
        self.in_gpr = in_gpr.sort_values(by=self.ids)

    def run_gpr(self):

        groups = self.in_gpr.groupby(
            by=[self.spacevar, self.sexvar, self.agevar])
        self.gpr = groups.apply(
            lambda y: gp.fit_gpr(y, obs_variable=self.datavar,
                                 obs_var_variable=self.variance_var,
                                 mean_variable=self.mean_var,
                                 amp=y[self.amp_var].values[0] * 1.4826,
                                 scale=y['scale'].values[0],
                                 draws=self.draws))

    def clean_gpr(self):

        # define gpr columns based on draws or no draws
        if self.draws == 0:
            gpr_cols = ['gpr_mean', 'gpr_lower', 'gpr_upper']
        else:
            gpr_cols = ['draw_{}'.format(i) for i in range(0, self.draws)]

        # subset to needed columns for cleaning
        cols = self.ids + gpr_cols
        self.clean_gpr = self.gpr[cols].drop_duplicates(
        ).reset_index(drop=True)

        """ make a copy of clean results with results in
		level (non-transformed) space for GPR output"""
        level_space_gpr = self.clean_gpr.copy()
        for var in gpr_cols:
            level_space_gpr[var] = transform_helpers.transform_data(
                level_space_gpr[var], self.data_transform, reverse=True
            )

        self.level_space_gpr = level_space_gpr

        # last, if draws,  make a GPR summaries dataframe out of level-space GPR draws
        if self.draws > 0:
            summary = self.level_space_gpr.copy()

            summary['gpr_mean'] = summary[gpr_cols].mean(axis=1)
            summary['gpr_lower'] = summary[gpr_cols].quantile(.025, axis=1)
            summary['gpr_upper'] = summary[gpr_cols].quantile(.975, axis=1)

            # transform back to level space
            sumcols = ['gpr_mean', 'gpr_lower', 'gpr_upper']
            self.summary_gpr = summary[self.ids + sumcols]

    def validate_gpr(self):
        """ Make sure that there are no missings in gpr_mean,
        gpr_lower or gpr_upper cols before allowing save. If
        draws, do the same for each draw"""

        if self.draws == 0:
            df = self.clean_gpr.copy()
        else:
            df = self.summary_gpr.copy()

        mi = []
        cols = ['gpr_mean', 'gpr_lower', 'gpr_upper']
        for col in cols:
            mi_col = df[df[col].isnull()]
            mi.append(mi_col)
        mi = pd.concat(mi).drop_duplicates()


    def save_gpr_outputs(self):

        rake_locs = self.locs.loc[self.locs['level']
                                  > NATIONAL_LEVEL, SPACEVAR]
        rake_locs = rake_locs.unique().astype(int).tolist()

        national_id = 'level_{}'.format(NATIONAL_LEVEL)
        nat_rake_locs = self.locs.loc[
            (self.locs['level'] > NATIONAL_LEVEL), national_id]
        nat_rake_locs = nat_rake_locs.unique().astype(int).tolist()
        self.rake_locs = nat_rake_locs + rake_locs

        if self.nparallel > 0:

            temp_root = '{}/gpr_temp_{}/{}'.format(self.output_path,
                                                   self.holdout_num, self.param_set)
            rake_root = '{}/rake_temp_{}/{}'.format(self.output_path,
                                                    self.holdout_num, self.param_set)
            draw_root = '{}/draws_temp_{}/{}'.format(self.output_path,
                                                     self.holdout_num, self.param_set)

            if not os.path.isdir(rake_root):
                os.system('mkdir -m 777 -p {}'.format(rake_root))

            if not os.path.isdir(temp_root):
                os.system('mkdir -m 777 -p {}'.format(temp_root))

            if self.draws == 0:

                outlocs = self.locs.loc[
                    self.locs[SPACEVAR].isin(self.prediction_location_ids), SPACEVAR]
                outlocs = outlocs.tolist()
                print(('saving unraked GPR summaries for location_ids {} '
                       'to gpr_temp_{}').format(outlocs, self.holdout_num))
                temp_file = '{}/{}.csv'.format(temp_root, self.loc_group)
                self.level_space_gpr.to_csv(temp_file, index=False)

                for loc in self.clean_gpr[SPACEVAR].unique().astype(int):

                    if loc in self.rake_locs:
                        print(('saving location_id {} to rake_temp_{} '
                               'for raking prep').format(loc, self.holdout_num))
                        tmp = self.clean_gpr.loc[self.clean_gpr[self.spacevar] == loc]
                        rake_file = '{}/{}.csv'.format(rake_root, loc)
                        tmp.to_csv(rake_file, index=False)

            else:

                print('saving unraked GPR summaries for location_ids {} '
                      'to gpr_temp_{}'.format(self.prediction_location_ids, self.holdout_num))
                temp_file = '{}/{}.csv'.format(temp_root, self.loc_group)
                self.summary_gpr.to_csv(temp_file, index=False)

                """ now save draws into rake_temp or draws_temp,
				based on whether done or need raking"""
                if not os.path.isdir(draw_root):
                    os.system('mkdir -m 777 -p {}'.format(draw_root))

                # Save draws for this holdout to use in coverage calculation.
                for loc in self.clean_gpr[SPACEVAR].unique().astype(int):
                    tmp = self.level_space_gpr.loc[self.clean_gpr[self.spacevar] == loc]
                    draw_file = '{}/{}.csv'.format(draw_root, loc)
                    tmp.to_csv(draw_file, index=False)

                if self.holdout_num == 0:

                    for loc in self.clean_gpr[SPACEVAR].unique().astype(int):

                        """ save rake locations draws in modeling space,
                        non-rake-locations in level space"""
                        if loc in self.rake_locs:
                            print(('saving draws of location_id {} to rake_temp_{} for '
                                   'raking prep').format(loc, holdout_num))
                            tmp = self.clean_gpr.loc[self.clean_gpr[self.spacevar] == loc]
                            rake_file = '{}/{}.csv'.format(rake_root, loc)
                            tmp.to_csv(rake_file, index=False)
                        else:
                            print(('saving final draws of location_id {} '
                                   'to draws_temp_{}').format(loc, holdout_num))
                            tmp = self.level_space_gpr.loc[self.clean_gpr[self.spacevar] == loc]
                            draw_file = '{}/{}.csv'.format(draw_root, loc)
                            tmp.to_csv(draw_file, index=False)

        else:

            hlp.model_save(self.level_space_gpr, self.run_id, 'gpr',
                           holdout=self.holdout_num, param_set=self.param_set,
                           output_path=self.output_path)


if __name__ == '__main__':
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    holdout_num = int(sys.argv[3])
    draws = int(sys.argv[4])
    param_sets = sys.argv[5]
    nparallel = int(sys.argv[6])
    if nparallel > 0:
        location_group = int(sys.argv[7])

    for i in ['run_id', 'output_path', 'holdout_num', 'draws',
              'param_sets', 'nparallel', 'location_group']:
        print('{} : {}'.format(i, eval(i)))

    start = time.time()

    param_groups = hlp.separate_string_to_list(param_sets, typ=int)

    for x in param_groups:

        r = RunGPR(run_id=run_id,
                   output_path=output_path,
                   draws=draws,
                   holdout_num=holdout_num,
                   nparallel=nparallel,
                   n_params=len(param_groups),
                   location_group=location_group,
                   param_set=x)

        r.pull_data()
        r.enumerate_parameters()
        r.prep_gpr()
        print('Running GPR for location_ids {}'.format(r.prediction_location_ids))
        r.run_gpr()
        r.clean_gpr()
        r.validate_gpr()
        r.save_gpr_outputs()

    end = time.time()
    print('Total time to completion: {sec} seconds'.format(sec=end - start))
