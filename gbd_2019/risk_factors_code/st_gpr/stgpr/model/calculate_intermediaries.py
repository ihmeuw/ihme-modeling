
import os
import sys
import time

import numpy as np
import pandas as pd
from sklearn import linear_model

from orm_stgpr.lib.constants import columns
from orm_stgpr.lib.util import (
    helpers as orm_helpers,
    transform as transform_helpers
)

from stgpr.common.constants import (
    amplitude as amp_constants,
    location as loc_constants
)
from stgpr.model import paths
from stgpr.st_gpr import helpers as hlp


np.seterr(invalid='ignore')

class Intermediaries:
    def __init__(self,
                run_id,
                output_path,
                spacevar=columns.LOCATION_ID,
                timevar=columns.YEAR_ID,
                agevar=columns.AGE_GROUP_ID,
                sexvar=columns.SEX_ID,
                datavar=columns.DATA,
                variance_var=columns.VARIANCE,
                stage1='stage1',
                stage2='st',
                amp_cutoff=10,
                nsv_threshold=10,
                draws=0,
                nparallel=100,
                holdout_num=0,
                param_set=0):

        # paths
        self.run_id = run_id
        self.output_path = output_path
        self.st_temp_root = f'{output_path}/st_temp_{holdout_num}/{param_set}'
        self.param_set = param_set

        # variable names
        self.spacevar = spacevar
        self.timevar = timevar
        self.agevar = agevar
        self.sexvar = sexvar
        self.datavar = datavar
        self.variance_var = variance_var
        self.stage1 = stage1
        self.stage2 = stage2

        # parameters for amp and NSV
        self.amp_cutoff = amp_cutoff
        self.nsv_threshold = nsv_threshold
        self.prod_amp_threshold = nsv_threshold

        # basic run arguments
        self.draws = draws
        self.holdout_num = holdout_num
        self.nparallel = nparallel

        # helpful keys
        self.ids = [self.spacevar, self.timevar, self.agevar, self.sexvar]

    def enumerate_parameters(self):
        """Initialize all model parameters"""

        # pull parameters
        parameters = hlp.model_load(self.run_id, 'parameters', param_set=None, output_path=self.output_path)

        # st necessities
        self.lsid = int(parameters['location_set_id'].iat[0])
        self.data_transform = parameters['data_transform'].iat[0]

        # amplitude necessities
        self.nsv_on = int(parameters['add_nsv'].iat[0])
        self.amp_factor = float(parameters['gpr_amp_factor'].iat[0])
        self.amp_method = parameters['gpr_amp_method'].iat[0]

        if pd.isna(parameters['gpr_amp_cutoff'].iat[0]):
            lvl = f'level_{loc_constants.NATIONAL_LEVEL}'
            cy = self.country_years[[lvl, 'max_country_years']].drop_duplicates([lvl])
            cy = cy['max_country_years'].tolist()
            self.amp_cutoff = float(np.percentile(
                cy, amp_constants.DEFAULT_CUTOFF_PERCENTILE
            ))
        else:
            self.amp_cutoff = float(parameters['gpr_amp_cutoff'].iat[0])

        years = parameters['prediction_year_ids'].iat[0]
        ages = parameters['prediction_age_group_ids'].iat[0]
        sexes = parameters['prediction_sex_ids'].iat[0]
        self.prediction_year_ids = hlp.separate_string_to_list(str(years), int)
        self.prediction_age_group_ids = hlp.separate_string_to_list(str(ages), int)
        self.prediction_sex_ids = hlp.separate_string_to_list(str(sexes), int)

        self.n_square = (len(self.prediction_sex_ids) *
            len(self.prediction_age_group_ids) *
            len(self.prediction_year_ids) *
            len(self.locs.loc[self.locs.level >= 3, columns.LOCATION_ID]))

    def pull_data(self):

        self.locs = hlp.model_load(self.run_id, 'location_hierarchy', output_path=self.output_path)
        self.data = hlp.model_load(self.run_id, 'prepped', holdout=self.holdout_num, output_path=self.output_path)
        self.stage1_df = hlp.model_load(self.run_id, 'stage1',
            holdout=self.holdout_num, output_path=self.output_path)

    def generate_st_file_list(self):

        self.st_file_list = ['{dir}/{n}.csv'.format(dir=self.st_temp_root, n=z)
                                for z in list(range(0, self.nparallel))]

    def prep_indata(self):

        spacetime = pd.concat([pd.read_csv(f) for f in self.st_file_list])

        self.spacetime = spacetime.drop_duplicates()

        df = pd.merge(self.data, self.stage1_df, on=self.ids)
        df = pd.merge(df, self.spacetime, on=self.ids)

        # drop ko data specified
        df = hlp.drop_ko_values(df, self.holdout_num, datavar=self.datavar)

        self.df = df.copy()

    def squareness_checks(self):
        if self.spacetime.shape[0] != self.n_square:
            raise ValueError('Spacetime estimates are not square')

        if self.stage1_df.shape[0] != self.n_square:
            raise ValueError('Stage 1 estimates are not square')

    def check_st_files_exist(self):

        dt = {'file_name': self.st_file_list, 'file_status': np.nan}
        check = pd.DataFrame(data=dt)
        for fname in self.st_file_list:
            fstatus = os.path.isfile(fname)
            check.loc[check.file_name == fname, 'file_status'] = fstatus

        if all(check.file_status):
            print("All spacetime jobs successfully completed")
        else:
            fail_list = check.loc[check.file_status is False, 'file_name'].tolist()
            msg = ('You are missing the following files: {failed}'
                .format(failed=', '.join(fail_list)))
            raise RuntimeError(msg)

    def create_nsv_amp_dataframes(self):

        # merge location-specific country-years for amplitude calculation
        cy = self.country_years[[f'level_{loc_constants.NATIONAL_LEVEL}',
                                    self.sexvar, 'max_country_years']].drop_duplicates()
        temp = self.df.merge(cy, on=[f'level_{loc_constants.NATIONAL_LEVEL}', self.sexvar],
            how='left').copy()
        temp['amp_resid'] = temp[self.stage1] - temp[self.stage2]

        # calculate NSV residuals of spacetime and
        temp['st_level'] = transform_helpers.transform_data(
            temp[self.stage2], self.data_transform, reverse=True
        )
        temp['data_level'] = transform_helpers.transform_data(
            temp[self.datavar], self.data_transform, reverse=True
        )
        temp['variance_level'] = transform_helpers.transform_variance(
            temp['data_level'],
            temp[self.variance_var],
            self.data_transform,
            reverse=True
        )
        temp['nsv_resid'] = (temp['data_level'] - temp['st_level'])

        # output amp dataframe
        lvlcols = [x for x in temp.columns if 'level' in x]
        self.amp_df = temp[[self.spacevar, self.sexvar] + lvlcols +
            ['max_country_years', 'amp_resid']]

        # NSV dataframe
        cols = ([self.spacevar, self.sexvar, 'nsv_resid', 'variance_level'] +
            self.level_cols)
        self.nsv_df = temp[cols].dropna(subset=['nsv_resid'])

        msg = 'NAs or Infs in variance BEFORE calculating NSV.'
        assert np.isfinite(self.nsv_df.variance_level.astype(float)).all(), msg

    def count_max_country_years(self):
        """For locations where the max number of country-years
        is below the NSV threshold, will need to find the nearest
        location hierarchy level where the max number of country-years
        exceeds the threshold. Will calculate the same thing but for
        location levels (regions, subnational levels, etc"""

        # setup
        self.n_levels = int(max(self.df.level))
        self.level_cols = ['level_{i}'.format(i=i) for i in range(self.n_levels + 1)]

        # pull data frame
        base = self.df.copy()[self.level_cols + [self.spacevar, self.timevar,
            self.agevar, self.sexvar, self.datavar, 'level']]
        full = base[self.level_cols + [self.spacevar, self.sexvar,
            'level']].drop_duplicates()

        # loop over levels to get max cy for each level
        for i in list(range(0, self.n_levels + 1)):

            # get level col name
            lvl = 'level_{}'.format(i)
            cy_col = '{}_cy'.format(lvl)

            # start parsing out country-years
            cy = base.copy()
            cy = cy.query('level <= {}'.format(max(3, i)))
            cy = cy[cy.data.notnull()].drop_duplicates(subset=[lvl, self.timevar,
                self.agevar, self.sexvar])
            cy = cy.groupby([lvl, self.agevar,
                self.sexvar])[self.datavar].count().reset_index()
            cy = cy.groupby([lvl, self.sexvar]).agg('max').reset_index()[[lvl,
                self.sexvar, self.datavar]].drop_duplicates()
            cy.rename(columns={self.datavar: cy_col}, inplace=True)
            full = full.merge(cy, on=[lvl, self.sexvar], how='left')
            full.loc[(full[lvl].notnull()) & (full[cy_col].isnull()),
                cy_col] = 0.

            msg = 'Missing country-year counts for some locations! Not good'
            all_locs = self.locs.loc[self.locs.level >= 3,
                self.spacevar].unique()
            assert len(full[self.spacevar].unique()) == len(all_locs), msg

        full['max_country_years'] = full[f'level_{loc_constants.NATIONAL_LEVEL}_cy']
        self.country_years = full.copy()

    def count_datapoints(self):
        """Count number of datapoints at each location level to determine nsv unit"""

        dt = self.df[[self.spacevar, self.datavar, self.sexvar] + self.level_cols]

        out = dt[[self.spacevar, self.sexvar] + self.level_cols]
        for lvl in self.level_cols:
            count = dt.loc[dt[lvl].notnull(), [lvl, self.datavar,
                self.sexvar]].groupby([lvl, self.sexvar])
            count = count.agg('count').reset_index()
            count.rename(columns={'data': 'n_datapoints_{}'.format(lvl)}, inplace=True)
            out = out.merge(count, on=[lvl, self.sexvar], how='left')

        # reformat
        dpcols = ['n_datapoints_{}'.format(x) for x in self.level_cols]
        out = out.melt(id_vars=[self.spacevar, self.sexvar], value_vars=dpcols)
        out['level'] = out['variable'].str.split('n_datapoints_level_',
            expand=True)[1].astype(int)
        self.datapoint_counts = out[[self.spacevar, self.sexvar,
            'level', 'value']].drop_duplicates()

    def assign_nsv_levels(self):
        """Non-sampling variance is calculated at the location-level where the number of
        country-years of data exceeds the  for each location/sex """

        df = self.datapoint_counts[self.datapoint_counts.value >= self.nsv_threshold]
        df = df.groupby([self.spacevar,
            self.sexvar]).agg({'level': max}).reset_index()
        df.rename(columns={'level': 'nsv_unit'}, inplace=True)

        # merge on level columns for eventual merge with nsv
        df = df.merge(self.locs[[self.spacevar] + self.level_cols])
        df = df.melt(id_vars=[self.spacevar, self.sexvar, 'nsv_unit'],
            value_vars=self.level_cols, var_name=['level'], value_name='level_id')

        # keep only level_id associated with nsv unit
        df['level'] = df['level'].str.split('level_', expand=True)[1].astype(int)
        df = df[df.nsv_unit == df.level]

        self.nsv_unit = df[[self.spacevar, self.sexvar, 'nsv_unit', 'level_id']]

    def calculate_nsv(self):
        """Calculate NSV for each location level and sex.
        Then merge onto the established level where NSV criteria
        is met for each country."""

        nsv_in = self.nsv_df[[self.sexvar] + self.level_cols].drop_duplicates()
        for lvl in self.level_cols:
            tmp = self.nsv_df.groupby([self.sexvar, lvl])
            tmp = tmp.apply(lambda x: hlp.nsv_formula(x, 'nsv_resid', 'variance_level'))
            if not tmp.empty:
                tmp = tmp.to_frame(name='nsv_{}'.format(lvl)).reset_index()
                nsv_in = nsv_in.merge(tmp, on=[lvl, self.sexvar], how='left')
            else:
                nsv_in['nsv_{}'.format(lvl)] = np.nan

        # melt twice for formatting
        nsvcols = ['nsv_{}'.format(x) for x in self.level_cols]
        nsv = nsv_in.melt(id_vars=[self.sexvar] + self.level_cols, value_vars=nsvcols,
                        var_name=['nsv_unit'], value_name='nsv')
        nsv = nsv.melt(id_vars=[self.sexvar, 'nsv_unit', 'nsv'],
            value_vars=self.level_cols, var_name=['level'], value_name='level_id')

        # prep for merge
        nsv['nsv_unit'] = nsv['nsv_unit'].str.split('nsv_level_',
            expand=True)[1].astype(int)
        nsv['level'] = nsv['level'].str.split('level_', expand=True)[1].astype(int)

        nsv = nsv[nsv.nsv_unit == nsv.level]
        nsv = nsv.drop_duplicates(subset=[self.sexvar, 'nsv_unit', 'level_id'])

        out = pd.merge(self.nsv_unit, nsv,
            on=['level_id', self.sexvar, 'nsv_unit'],
            how='left')
        self.nsv = out[[self.spacevar, self.sexvar, 'nsv_unit', 'nsv']].dropna()

        msg = 'Missing country-year counts for some locations! Not good'
        all_locs = self.locs.loc[self.locs.level >= 3,
            self.spacevar].unique()
        assert len(self.nsv[self.spacevar].unique()) == len(all_locs), msg

    def add_nsv_to_variance(self):

        if self.nsv_on:

            print('adding NSV to datapoint variance')

            # convert back to level space
            tmp = self.data.copy()
            # merge in NSV
            tmp = pd.merge(tmp, self.nsv[[self.spacevar, self.sexvar, 'nsv']],
                            on=[self.spacevar, self.sexvar])
            tmp['variance'] = tmp['original_variance'] + tmp['nsv']

            # now re-transform data and variance
            tmp['data'] = transform_helpers.transform_data(
                tmp['data'], self.data_transform, reverse=True
            )
            tmp['variance'] = transform_helpers.transform_variance(
                tmp['data'], tmp['variance'], self.data_transform
            )
            tmp['data'] = transform_helpers.transform_data(
                tmp['data'], self.data_transform
            )

            msg = ('There are infinite or NA variances after adding NSV! ')
            finite = np.isfinite(tmp.loc[tmp.data.notnull(), 'variance'].astype(float).unique()).all()
            assert finite, msg

            self.adj_data = tmp.copy()

        else:
            self.adj_data = self.data.copy()

    def calculate_prod_amplitude(self):

        # first get amp_unit for each location/sex
        cy = self.country_years.copy()
        max_cy = cy.level_0_cy.max()
        threshold = min(self.prod_amp_threshold, max_cy)

        for z in list(range(0, self.n_levels + 1)):

            lvl = 'level_{}'.format(z)
            lvl_count = '{}_cy'.format(lvl)

            cy.loc[cy[lvl_count] >= threshold, 'amp_unit'] = z

        amp_unit_df = cy[[self.spacevar, self.sexvar,
            'amp_unit']].drop_duplicates()

        amp_in = self.amp_df.copy()
        amp_in = amp_in.merge(amp_unit_df, on=[self.spacevar, self.sexvar],
            how='left')

        for lvl in self.level_cols:
            tmp = amp_in.groupby([self.sexvar, lvl])['amp_resid']
            tmp = tmp.apply(lambda x: hlp.mad(x)).reset_index()
            tmp = tmp.rename(columns={'amp_resid': 'amp_{}'.format(lvl)})
            amp_in = amp_in.merge(tmp, on=[lvl, self.sexvar], how='left')

        amp_out = amp_in.drop_duplicates([self.spacevar, self.sexvar])
        amp_final = pd.DataFrame()
        for i in list(range(0, self.n_levels + 1)):
            amp_tmp = amp_out[amp_out['amp_unit'] == i]
            amp_tmp['st_amp'] = amp_tmp['amp_level_{}'.format(i)]
            amp_final = pd.concat([amp_final, amp_tmp])

        amplitude = amp_final[[self.spacevar, self.sexvar,
            'st_amp']].drop_duplicates()
        amplitude['st_amp'] = self.amp_factor * amplitude['st_amp']
        self.amplitude = amplitude
        msg = ('Some locations disappeared while alculating prod amplitude')
        all_locs = self.locs.loc[self.locs.level >= 3,
            self.spacevar].unique()
        assert hlp.equal_sets(self.amplitude[self.spacevar], all_locs), msg

    def calculate_amplitude(self):
        """Calculate global amplitude using residuals
        from all locations with >= self.amp_cutoff
        country-years of data. For the countries with
        fewer country-years, run a linear regression on
        amplitude  """

        amp_methods = ['global_above_cutoff', 'broken_stick', 'prod']
        msg = ('amp_cutoff must be one of {}, but your '
            'amp_method is <{}>'.format(amp_methods, self.amp_method))
        assert self.amp_method in amp_methods, msg

        # calculate global_above_cutoff amplitude
        if self.amp_method in ['global_above_cutoff', 'broken_stick']:
            df = self.amp_df.copy()
            df = self.calculate_mad(df=df,
                grouping_level=3,
                cutoff=int(self.amp_cutoff))
            nationals_only = df.drop_duplicates(subset=['level_{}'.format(3),
                self.sexvar])
            col = 'level_{}_mad_above_{}'.format(3, int(self.amp_cutoff))
            global_means = nationals_only.groupby(self.sexvar)[col].mean()
            global_means = global_means.rename(
                'final_global_above_cutoff_amplitude').reset_index()
            df = df.merge(global_means, on=self.sexvar)

            # next calculate country-level amplitude for all locations
            if self.amp_cutoff == 0:
                pass
            else:
                df = self.calculate_mad(df=df, grouping_level=3, cutoff=0)

            """onto broken stick - root at origin by subtracting
            off global amplitude and amp_threshold"""
            df['adj_max_country_years'] = self.amp_cutoff - df['max_country_years']

            country_mad = 'level_{}_mad_above_0'.format(3)
            df['adj_amp'] = df[country_mad] - df['final_global_above_cutoff_amplitude']

            """regress adjusted amplitude values on adjusted
            country-years (no intercept) by sex ONLY include
            values where max_country_years < amp_threshold"""
            final = df[df.max_country_years >= self.amp_cutoff].copy()
            for sex in self.prediction_sex_ids:
                tmp = df[(df.max_country_years < self.amp_cutoff) &
                    (df[self.sexvar] == sex)].copy()
                if (not tmp.empty) & (len(tmp.adj_amp.unique()) > 1):
                    inmod = tmp.drop_duplicates(subset=['level_{}'.format(3),
                        self.sexvar]).copy()
                    mod = linear_model.LinearRegression(fit_intercept=False)
                    mod.fit(X=inmod.adj_max_country_years.values.reshape(-1, 1),
                            y=inmod.adj_amp.values.reshape(-1, 1))
                    # Note: .item() is called on a numpy array here, not a
                    # pandas dataframe
                    slope = mod.coef_.item()
                    tmp['broken_stick_amp'] = (slope * tmp['adj_max_country_years'] +
                                                tmp['final_global_above_cutoff_amplitude'])
                    final = pd.concat([tmp, final], axis=0, sort=True)
                else:
                    final['broken_stick_amp'] = np.nan
                    if self.amp_method == 'broken_stick':
                        self.amp_method = 'global_above_cutoff'

            # only use broken_stick amp if greater than global_above_cutoff amp
            final['final_broken_stick_amplitude'] = \
                final[['final_global_above_cutoff_amplitude',
                    'broken_stick_amp']].max(axis=1)

            # set output to whatever method the modeler specified
            final.rename(columns={'final_{}_amplitude'.format(self.amp_method): 'st_amp'},
                inplace=True)
            amplitude = final[[self.spacevar, self.sexvar,
                'st_amp']].drop_duplicates()
            amplitude['st_amp'] = self.amp_factor * amplitude['st_amp']
            self.amplitude = amplitude
        else:
            self.calculate_prod_amplitude()

    def calculate_mad(self, df, grouping_level, cutoff=0):

        tmp = df.copy()
        lvl = 'level_{}'.format(grouping_level)
        out = tmp[tmp.max_country_years >=
            cutoff].groupby([lvl, self.sexvar])['amp_resid']
        out = out.apply(lambda x: hlp.mad(x)).reset_index()
        out.rename(columns={'amp_resid': '{}_mad_above_{}'.format(lvl, cutoff)},
            inplace=True)
        df = df.merge(out, on=[lvl, self.sexvar], how='left')

        return(df)

    def prep_gpr(self):

        in_gpr = pd.merge(self.spacetime, self.amplitude, on=[self.sexvar])

        data_cols = [self.datavar, 'variance']
        in_gpr = pd.merge(in_gpr, self.adj_data[self.ids + data_cols],
            on=self.ids,
            how='outer')
        self.in_gpr = in_gpr.sort_values(by=self.ids)

    def run_gpr(self):

        groups = self.in_gpr.groupby(by=[self.spacevar, self.sexvar, self.agevar])
        self.gpr = groups.apply(lambda y: gp.fit_gpr(y,
            obs_variable=self.datavar,
            obs_var_variable='variance',
            mean_variable=self.stage2,
            amp=y['st_amp'].values[0] * 1.4826,
            scale=y['scale'].values[0],
            draws=self.draws))

    def save_outputs(self):
        print('saving spacetime estimates, amplitude, and non-sampling variance')
        st_path = hlp.model_path(self.run_id, 'st',
            holdout=self.holdout_num,
            param_set=self.param_set,
            output_path=self.output_path)

        if os.path.exists(st_path):
            hlp.model_save(self.spacetime, self.run_id, 'st',
                            holdout=self.holdout_num, param_set=self.param_set,
                            output_path=self.output_path)
        else:
            hlp.model_save(self.spacetime, self.run_id, 'st',
                            holdout=self.holdout_num, param_set=self.param_set, mode='w',
                            output_path=self.output_path)

        hlp.model_save(self.adj_data, self.run_id, 'adj_data',
                        holdout=self.holdout_num, param_set=self.param_set,
                        output_path=self.output_path)
        hlp.model_save(self.amplitude, self.run_id, 'amp_nsv',
                        holdout=self.holdout_num, param_set=self.param_set,
                        output_path=self.output_path)

def main():
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    holdout_num = int(sys.argv[3])
    draws = int(sys.argv[4])
    nparallel = int(sys.argv[5])
    param_sets = sys.argv[6]

    for i in ['run_id', 'output_path', 'holdout_num',
        'draws', 'nparallel', 'param_sets']:
        print('{} : {}'.format(i, eval(i)))

    start = time.time()

    param_groups = hlp.separate_string_to_list(str(param_sets), typ=int)

    for p in param_groups:
        x = Intermediaries(run_id=run_id,
                    output_path=output_path,
                    draws=draws,
                    holdout_num=holdout_num,
                    nparallel=nparallel,
                    param_set=p)

        print('Checking that all spacetime jobs completed successfully')
        x.pull_data()
        x.generate_st_file_list()
        x.check_st_files_exist()

        print('prepping data')
        x.prep_indata()

        print('calculating data density and getting parameters')
        x.count_max_country_years()
        x.count_datapoints()
        x.enumerate_parameters()
        x.squareness_checks()

        print('prepping for nsv and amp calculation')
        x.create_nsv_amp_dataframes()

        print('calculating amplitude')
        x.calculate_amplitude()

        print('calculating NSV')
        x.assign_nsv_levels()
        x.calculate_nsv()
        x.add_nsv_to_variance()

        x.save_outputs()

        end = time.time()
        print('Total time to completion: {sec} seconds'.format(sec=end - start))

if __name__ == '__main__':
    main()
