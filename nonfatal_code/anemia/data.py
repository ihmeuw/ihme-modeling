""" Data

This class sets up the data/priors for CT sampling.
The data consist of:
A) The names of the subtypes of anemia (rows of the table)
B) The proportion of anemia that is due to each subtype (right margin)
C) The names of the sequelae of anemia (columns of the table)
D) The prevalence of each sequelae (lower margin)
The priors consist of:
0) Basic model priors: mu's, taus etc that just need a value
1) 0: subtype leans mild, 1: subtype leans moderate, 2: subtype leans severe
2) shifts: the estimated decriment to hemoglibn levels (in g/Dl)
3) variance ranks: ranks of how much each subtype should vary in severity
"""

# Setup
# -----
import pandas as pd
import numpy as np
import scipy.stats as sp
from transmogrifier import gopher
from transmogrifier.draw_ops import get_draws
np.seterr('warn')


draw_cols = ['draw_%s' % d for d in range(1000)]


class Data(object):

    def __init__(self, location_id, year_id, sex_id, age_group_id, draw_num, data_dir, h5_dir):

        # Identifiers
        # -----------
        self.location_id = location_id
        self.year_id = year_id
        self.sex_id = sex_id
        self.age_group_id = age_group_id
        self.draw_num = draw_num

        # Numeric and Language Codes for Sex
        self.ndraws = 1000

        # Files
        self.data_dir = data_dir

        self.h5_dir = h5_dir

        self.env_mes = {"hgb": {MODELABLE ENTITY ID}, "prev_anemic": {MODELABLE ENTITY ID}, "prev_severe": {MODELABLE ENTITY ID}, "prev_moderate": {MODELABLE ENTITY ID}, "prev_mild": {MODELABLE ENTITY ID}}
        self.hgb_file = self.h5_dir +'hgb_d.h5'

        # TODO: Change this filepath
        self.subtypes_file = self.data_dir+'in_out_meid_map.xlsx'

        self.prior_largest_file = self.data_dir+'prior_largest.csv'
        self.prior_variation_file = self.data_dir+'prior_variation.csv'
        self.prior_hb_shifts_file = self.data_dir+'prior_hb_shifts.csv'
        self.hb_levels_file = self.data_dir+'severity_definitions.csv'
        self.hb_shifts_file = self.data_dir+'prior_hemoglobin_shifts.csv'
        self.residuals_file = self.data_dir+'residual_proportions.csv'

    def identify_subtypes(self):
        self.subin = pd.read_excel(self.subtypes_file, 'in_meids')
        self.subout = pd.read_excel(self.subtypes_file, 'out_meids')
        input_meids = []
        for i, row in self.subin.iterrows():
            meid = row['modelable_entity_id']
            if row['use_incidence'] == 1:
                measure_id = {MEASURE ID}
            else:
                measure_id = {MEASURE ID}
            if meid != 'RESIDUAL':
                input_meids.append((int(meid), measure_id))
        return input_meids

    def get_subtype_draws(self):
        meid_measures = self.identify_subtypes()
        df = []
        for mm in meid_measures:
            meid, msid = mm
            has_draws = False
            try:
                thisdf = gopher.draws(
                    {'modelable_entity_ids': [meid]},
                    location_ids=self.location_id,
                    year_ids=self.year_id,
                    sex_ids=self.sex_id,
                    age_group_ids=self.age_group_id,
                    measure_ids=msid,
                    source='dismod',
                    gbd_round_id={GBD ROUND ID})
                if len(thisdf) > 0:
                    has_draws = True
                    print 'Retrieved measure_id %s for meid %s' % (msid, meid)
            except:
                try:
                    thisdf = gopher.draws(
                        {'modelable_entity_ids': [meid]},
                        location_ids=self.location_id,
                        year_ids=self.year_id,
                        sex_ids=self.sex_id,
                        age_group_ids=self.age_group_id,
                        measure_ids=msid,
                        source='dismod',
                        status='latest')
                    if len(thisdf) > 0:
                        has_draws = True
                        print 'Retrieved measure_id %s for meid %s' % (
                                msid, meid)
                except:
                    pass
            if has_draws:
                df.append(thisdf)
            else:
                print 'meid %s draws not found. filling with zeros.' % meid
                dummy_draws = {
                    'modelable_entity_id': meid,
                    'model_version_id': 0,
                    'location_id': self.location_id,
                    'year_id': self.year_id,
                    'age_group_id': self.age_group_id,
                    'sex_id': self.sex_id}
                dummy_draws.update({'draw_%s' % d: 0
                                   for d in range(1000)})
                df.append(pd.DataFrame([dummy_draws]))
        df = pd.concat(df)
        reqd_cols = ['modelable_entity_id']
        reqd_cols.extend(draw_cols)
        self.model_version_map = df[[
            'modelable_entity_id', 'model_version_id']]
        self.prevalence = df[reqd_cols].merge(
            self.subin, on='modelable_entity_id', how='left')
        return self.prevalence

    def adjust_hemog(self):
        sp = self.prevalence
        sp.ix[sp.attribution_group == 'hemog_g6pd_hemi', draw_cols] = (
                sp.ix[sp.attribution_group == 'hemog_g6pd_hemi', draw_cols] *
                0.005)
        self.prevalence = sp
        return self.prevalence

    def format_to_column_orientation(self):
        self.prevalence = self.prevalence.transpose()
        assert self.prevalence.shape == (1000, 30), (
            'Prevalence draws are the wrong shape. Should be 1000 x 30')
        return self.prevalence

    def collapse_to_attribution_groups(self):
        """some subtypes are supposed to be added together,
        as indicated by multiple subtypes having the same attribution_group"""
        self.prevalence = self.prevalence[
            ['attribution_group']+draw_cols].groupby('attribution_group').sum()
        return self.prevalence

    def get_matrix_row_labels(self):
        self.subtypes = list(self.subin.attribution_group.unique())
        self.subtypes.sort()
        self.rows = self.subtypes

    def read_hb_shifts(self):
        # Hb shifts
        # ---------
        self.hb_shifts = pd.read_csv(self.prior_hb_shifts_file)
        self.hb_shifts['sex_id'] = self.hb_shifts.sex.replace({
            'M': 1, 'F': 2})
        self.hb_shifts = self.hb_shifts[
                self.hb_shifts['sex_id'] == self.sex_id]
        self.hb_shifts = self.hb_shifts[['subtype', 'mean_hb_shift']]
        self.hb_shifts = self.hb_shifts.sort_values('subtype')
        self.hb_shifts = self.hb_shifts.set_index('subtype').squeeze()

    def compute_shifts(self):
        """ Get the 'columns' (aka the severity margin, the lower/upper margin,
        the mild/moderate/severe anemia values) of the matrix """
        idcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        estimated_hgb_levels = pd.read_hdf(
            self.hgb_file,
            where="year_id=={y} & sex_id=={s} & age_group_id=={a}".format(
                y=self.year_id, s=self.sex_id, a=self.age_group_id))[
                    idcols + ['hgb_%s' % self.draw_num]]
        self.pop_normal_hgb = sp.scoreatpercentile(
                estimated_hgb_levels['hgb_%s' % self.draw_num], 95)
        #total anemia prevalence
        anem = get_draws('modelable_entity_id',
                        {MODELABLE ENTITY ID},
                        'dismod',
                        location_ids=self.location_id,
                        year_ids=self.year_id,
                        sex_ids=self.sex_id,
                        age_group_ids=self.age_group_id,
                        gbd_round_id={GBD ROUND ID})
        anem["anem_mean"] = anem[['draw_%s' % d for d in range(1000)]].mean(axis=1)
        anem = anem.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
        renames = {'draw_%s' % d: 'prev_anemic_%s' % d for d in range(1000)}
        anem.rename(columns=renames, inplace=True)
        prev_anemic = anem[idcols+['prev_anemic_%s' % self.draw_num]]
        #mild anemia prevalence
        mild = get_draws('modelable_entity_id',
                         {MODELABLE ENTITY ID},
                         'dismod',
                         location_ids=self.location_id,
                         year_ids=self.year_id,
                         sex_ids=self.sex_id,
                         age_group_ids=self.age_group_id,
                         gbd_round_id={GBD ROUND ID})
        mild["mild_mean"] = mild[['draw_%s' % d for d in range(1000)]].mean(axis=1)
        mild = mild.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
        renames = {'draw_%s' % d: 'prev_mild_%s' % d for d in range(1000)}
        mild.rename(columns=renames, inplace=True)
        prev_mild = mild[idcols+['prev_mild_%s' % self.draw_num]]
        #moderate anemia prevalence
        mod = get_draws('modelable_entity_id',
                         {MODELABLE ENTITY ID},
                         'dismod',
                         location_ids=self.location_id,
                         year_ids=self.year_id,
                         sex_ids=self.sex_id,
                         age_group_ids=self.age_group_id,
                         gbd_round_id={GBD ROUND ID})
        mod["moderate_mean"] = mod[['draw_%s' % d for d in range(1000)]].mean(axis=1)
        mod = mod.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
        renames = {'draw_%s' % d: 'prev_moderate_%s' % d for d in range(1000)}
        mod.rename(columns=renames, inplace=True)
        prev_moderate = mod[idcols+['prev_moderate_%s' % self.draw_num]]
       #severe anemia prevalence
        severe = get_draws('modelable_entity_id',
                         {MODELABLE ENTITY ID},
                         'dismod',
                         location_ids=self.location_id,
                         year_ids=self.year_id,
                         sex_ids=self.sex_id,
                         age_group_ids=self.age_group_id,
                         gbd_round_id={GBD ROUND ID})
        severe["severe_mean"] = severe[['draw_%s' % d for d in range(1000)]].mean(axis=1)
        severe = severe.drop(['measure_id', 'modelable_entity_id', 'model_version_id'], axis=1)
        renames = {'draw_%s' % d: 'prev_severe_%s' % d for d in range(1000)}
        severe.rename(columns=renames, inplace=True)
        prev_severe = severe[idcols+['prev_severe_%s' % self.draw_num]]

        # Merge data columns
        self.col_sums = estimated_hgb_levels.merge(prev_anemic, on=idcols)
        self.col_sums = self.col_sums.merge(prev_mild, on=idcols)
        self.col_sums = self.col_sums.merge(prev_moderate, on=idcols)
        self.col_sums = self.col_sums.merge(prev_severe, on=idcols)

        # Rename draw columns
        self.col_sums.rename(columns={
            'hgb_%s' % self.draw_num: 'mean_hgb',
            'prev_anemic_%s' % self.draw_num: 'prev_anemic',
            'prev_mild_%s' % self.draw_num: 'prev_mild',
            'prev_moderate_%s' % self.draw_num: 'prev_moderate',
            'prev_severe_%s' % self.draw_num: 'prev_severe'}, inplace=True)

        # Set normal hemoglobin based on individual level hgb
        self.mean_hgb = self.col_sums.mean_hgb
        self.normal_hb = np.max([self.mean_hgb.values[0], self.pop_normal_hgb])
        self.hb_shift = (self.normal_hb - self.mean_hgb.values[0])

        if self.hb_shift == 0:
            self.hb_shift = 1
        self.total_anemia = np.squeeze(self.col_sums['prev_anemic'])

        # keep only the column sums, as a pd.Series
        self.col_sums = self.col_sums[[
            'prev_mild', 'prev_moderate', 'prev_severe']]
        self.col_sums = self.col_sums.squeeze()
        self.cols = self.col_sums.index

        # make sure they sum to one
        self.col_sums /= np.sum(self.col_sums)
        return self.col_sums

    def compute_directly_attributable(self):
        """compute the row margins - the proportion of anemia due to each
        subtype as prevalence * hb_shift / mean_hb"""
        df = []
        for st in self.subtypes:
            if st in self.prevalence.columns:
                curr_hb_fraction = self.hb_shifts[st] / self.hb_shift
                curr_row_sum = pd.DataFrame({
                    st: self.prevalence[st] * curr_hb_fraction})
                df.append(curr_row_sum)
        df = pd.concat(df, axis=1)
        self.row_sums = df
        return self.row_sums

    def cap_direct_allocate_resids(self):
        # allocate remaining anemia to residuals using fixed proportions
        self.residuals = pd.read_csv(self.residuals_file)
        if self.age_group_id < {AGE GROUP ID}:
            self.residuals = self.residuals[['acause', 'resid_prop_u15']]
        elif self.age_group_id >= {AGE GROUP ID}:
            self.residuals = self.residuals[['acause', 'resid_prop_o60']]
        elif self.sex_id == {SEX ID}:
            self.residuals = self.residuals[['acause', 'resid_prop_m_15_59']]
        else:
            self.residuals = self.residuals[['acause', 'resid_prop_f_15_59']]
        self.residuals.rename(columns={
            'resid_prop_u15': 'resid_prop',
            'resid_prop_o60': 'resid_prop',
            'resid_prop_m_15_59': 'resid_prop',
            'resid_prop_f_15_59': 'resid_prop'}, inplace=True)

        # Zero out the ntd_other proportion if schisto and hookworm are zero
        if (self.prevalence.ntd_nema_hook.max() == 0 and
                self.prevalence.ntd_schisto.max() == 0):
            self.residuals.ix[
                    self.residuals.acause == 'ntd_other', 'resid_prop'] = 0
            self.residuals['resid_prop'] = (
                    self.residuals['resid_prop'] /
                    self.residuals['resid_prop'].sum())

        # Cap the directly attributed causes as 90% of the total
        big_draws = self.row_sums.sum(axis=1) > 0.9
        big_draw_sums = self.row_sums[big_draws].sum(axis=1)
        self.row_sums.loc[big_draws] = self.row_sums[big_draws].divide(
                big_draw_sums, axis=0) * 0.9
        self.remaining_anemia = 1 - self.row_sums.sum(axis=1)

        # Assign remainder to residual categories
        resid_row_sums = []
        for rc in self.residuals['acause']:
            curr_resid_prop = self.residuals.ix[
                    self.residuals['acause'] == rc, 'resid_prop']
            curr_resid_prop = curr_resid_prop.squeeze()
            resid_row_sums.append(pd.DataFrame(
                {rc: self.remaining_anemia * curr_resid_prop}))
        self.row_sums = pd.concat([self.row_sums]+resid_row_sums, axis=1)

        # make sure all draws sum to one across subtypes
        self.row_sums = self.row_sums.div(self.row_sums.sum(axis=1), axis=0)

        # sort columns alphabetically
        self.row_sums = self.row_sums[sorted(self.row_sums.columns)]

    def make_one_mean_draw(self):
        """Set draw_0 equal to the mean proportions, for the purposes of
        running without uncertainty"""
        self.row_sums.loc['draw_0', :] = self.row_sums.mean()

    def prior_0_likelihood_sds(self):
        # standard deviation of the normal likelihood function for row sums
        self.log_row_sum_sd = 0.05

        # standard deviation (by column) of the normal likelihood function
        # for col sums
        self.log_col_sum_sd = np.array([0.01, 0.01, 0.001])

    def prior_1_subtype_severity(self):
        self.prior_largest = pd.read_csv(self.prior_largest_file)
        self.prior_largest = pd.Series(
                self.prior_largest.expected_largest.values,
                index=self.prior_largest.subtype.values)
        self.prior_largest = self.prior_largest.sort_index()

    def prior_2_order_of_variation(self):
        """Expert prior on order of variation in Hb shifts"""
        self.prior_variation = pd.read_csv(self.prior_variation_file)
        self.prior_variation = pd.Series(
                self.prior_variation.expected_variation_rank.values,
                index=self.prior_variation.subtype.values)
        self.prior_variation = self.prior_variation.sort_index()

    def prior_3_hb_severity_defs(self):
        self.hb_levels = pd.read_csv(self.hb_levels_file)
        self.hb_levels['sex_id'] = self.hb_levels.sex.replace({
            'M': {SEX ID}, 'F': {SEX ID}})
        if self.age_group_id >= {AGE GROUP ID}:
            self.hb_levels = self.hb_levels[(self.hb_levels['age'] == 'adult')]
        elif self.age_group_id < {AGE GROUP ID}:
            self.hb_levels = self.hb_levels[(self.hb_levels['age'] == 'child')]
        self.hb_levels = self.hb_levels[
                self.hb_levels['sex_id'] == self.sex_id]
        self.hb_levels['mild'] = (
                self.hb_levels['mild_low']+self.hb_levels['mild_high'])/2
        self.hb_levels['moderate'] = (
                self.hb_levels['moderate_low'] +
                self.hb_levels['moderate_high'])/2
        self.hb_levels['severe'] = (
                self.hb_levels['severe_low']+self.hb_levels['severe_high'])/2
        self.hb_levels = self.hb_levels[(['mild', 'moderate', 'severe'])]
        self.hb_levels = np.array(self.hb_levels.values).T

    def main(self):
        self.get_subtype_draws()
        # self.apply_age_restrictions()
        self.adjust_hemog()
        self.collapse_to_attribution_groups()
        self.format_to_column_orientation()
        self.get_matrix_row_labels()
        self.read_hb_shifts()
        self.compute_shifts()
        self.compute_directly_attributable()
        self.cap_direct_allocate_resids()
        self.make_one_mean_draw()

        # get priors
        self.prior_0_likelihood_sds()
        self.prior_1_subtype_severity()
        self.prior_2_order_of_variation()
        self.prior_3_hb_severity_defs()
