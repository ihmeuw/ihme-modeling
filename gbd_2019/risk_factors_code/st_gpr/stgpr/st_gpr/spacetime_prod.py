import os

import numpy as np
import pandas as pd
from scipy import spatial, stats

thispath = os.path.dirname(__file__)


def invlogit(x):
    return np.exp(x)/(np.exp(x)+1)


def logit(x):
    return np.log(x/(1-x))


def invlogit_var(mu, var):
    return (var*(np.exp(mu)/(np.exp(mu)+1)**2)**2)


def logit_var(mu, var):
    return (var/(mu*(1-mu))**2)


def mad(x):
    return stats.nanmedian(np.abs(x-stats.nanmedian(x)))


class Smoother:

    def __init__(
            self,
            dataset,
            locations,
            timevar='year_id',
            agevar='age_group_id',
            spacevar='location_id',
            datavar='observed_data',
            modelvar='stage1_prediction',
            pred_age_group_ids=range(2, 22),
            pred_year_ids=range(1980, 2018)):

        # Setup template for year/age predictions
        self.p_years = pred_year_ids
        self.p_ages = pred_age_group_ids
        self.age_map = {value: idx for idx, value in enumerate(
            pred_age_group_ids)}
        time_series = pd.DataFrame({'joinkey': 1, timevar: self.p_years})
        age_series = pd.DataFrame(
            {'joinkey': 1, agevar: pred_age_group_ids})
        self.results = age_series.merge(time_series, on='joinkey')
        self.results.drop('joinkey', axis=1, inplace=True)

        # No default age and time weights, must be set explicitly
        self.a_weights = None
        self.t_weights = None

        # Assume an input data set with default variable names
        self.timevar = timevar
        self.agevar = agevar
        self.spacevar = spacevar
        self.datavar = datavar
        self.modelvar = modelvar
        self.locations = locations

        # Bind the data and stage 1 models, reogranizing for smoothing
        self.dataset = dataset.copy()
        self.data = self.dataset.copy()
        self.data = self.data[self.data[datavar].notnull()]
        self.data = self.data[self.data[agevar].isin(self.p_ages)]
        self.data = self.data[self.data[timevar].isin(self.p_years)]

        # Ensure that stage 1 predictions are square
        self.stage1 = dataset.ix[
            dataset[modelvar].notnull(),
            [spacevar, timevar, agevar, modelvar]].drop_duplicates()
        self.stage1 = self.stage1[self.stage1[agevar].isin(self.p_ages)]
        self.stage1 = self.stage1[self.stage1[timevar].isin(self.p_years)]
        assert ~self.stage1[[
            spacevar, timevar, agevar]].duplicated().any(), """
            Stage 1 predictions must exist and be unique for every location,
            year, age, and sex group"""
        assert len(self.stage1.groupby(
            [spacevar])[modelvar].count().unique()) == 1, """
            Stage 1 predictions must exist and be unique for every location,
            year, age, and sex group"""

        # Ensure that stage 1 predictions contain all years/ages to be
        # predicted
        assert len(set(self.p_years)-set(self.stage1[timevar])) == 0, """
            Stage 1 predictions must exist for every year in the prediction
            time interval"""
        assert len(set(self.p_ages)-set(self.stage1[agevar])) == 0, """
            Stage 1 predictions must exist for every year in the prediction
            time interval"""

        self.data['resid'] = self.data[self.datavar]-self.data[self.modelvar]
        self.stage1 = self.stage1.sort_values(
            [self.spacevar, self.agevar, self.timevar])

        # Set default smoothing parameters
        self.lambdaa = 1.0
        self.omega = 2
        self.zeta = 0.9
        self.sn_weight = 0.1
        #self.zeta_no_data = None

    def time_weights(self):
        """ Generate time weights """
        p_years = np.atleast_2d(self.p_years)
        o_years = np.atleast_2d(self.data[self.timevar].values)

        # Pre-compute weights for each time-distance
        t_weights_lookup = {}
        for i in self.p_years:
            t_weights_lookup[i] = {}
            for j in self.p_years:
                a_dist = abs(float(i-j))
                max_a_dist = max(
                    abs(i-self.p_years[0]),
                    abs(i-self.p_years[-1]))
                t_weights_lookup[i][j] = (
                    1-(a_dist/(max_a_dist+1))**self.lambdaa)**3

        t_weights = spatial.distance.cdist(
            p_years.T, o_years.T,
            lambda u, v: t_weights_lookup[u[0]][v[0]])
        self.t_weights = t_weights

        return t_weights

    def exp_time_weights(self, wts_only=False):
        """ Generate  exponential decay time weights """
        time_map = self.p_years - np.min(self.p_years)
        p_years = np.atleast_2d(self.p_years)
        o_years = np.atleast_2d(self.data[self.timevar].values)

        t_weights_lookup = {}
        # run computation of exponential decay
        for i in time_map:
            t_weights_lookup[i] = 1/np.exp(self.lambdaa*i)

        # lookup t_weights
        t_weights = spatial.distance.cdist(
            p_years.T, o_years.T,
            lambda u, v: t_weights_lookup[abs(u[0] - v[0])])

        # only make t_weights class object for full location set of timeweights
        self.t_weights = t_weights

        if wts_only:
            return t_weights_lookup

    def age_weights(self):
        """ Generate age weights """
        p_ages = np.atleast_2d(self.p_ages)
        o_ages = np.atleast_2d(self.data[self.agevar].values)

        # Pre-compute weights for each age-distance
        a_weights_lookup = {}
        for i in range(len(self.age_map)+1):
            a_weights_lookup[i] = 1 / np.exp(self.omega*i)
            a_weights_lookup[-i] = a_weights_lookup[i]

        a_weights = spatial.distance.cdist(
            p_ages.T, o_ages.T,
            lambda u, v: a_weights_lookup[u[0]-v[0]])
        self.a_weights = a_weights

        return a_weights

    def gbd_spacemap(self, locs):
        """ This will generate a spatial-relatedness matrix based on the
        GBD region/super-regions. Arbitrary spatial-relatedness matrices
        will be accepted by the space-weighting function, but they must
        be of this form. """
        gbd_regions = pd.read_csv(thispath+"/regions.csv")

        locs = np.atleast_1d(locs)
        p_locs = pd.DataFrame(locs, columns=['iso3'])
        o_locs = self.data[[self.spacevar]]

        """
        Make sure the data doesn't get misaligned, since the matrix
        lookups are not order invariant """
        p_locs = pd.merge(
            p_locs, gbd_regions, left_on=self.spacevar, right_on='iso3')
        o_locs['data_order'] = o_locs.reset_index().index
        o_locs = pd.merge(
            o_locs, gbd_regions, left_on=self.spacevar,
            right_on='iso3').sort_values('data_order').reset_index()

        in_country = (np.asmatrix(p_locs['location_id']).T == np.asmatrix(
            o_locs['location_id'])).astype(int)
        in_region = (np.asmatrix(p_locs['gbd_analytical_region_id']).T == (
            np.asmatrix(o_locs['gbd_analytical_region_id'])).astype(int))
        in_sr = (np.asmatrix(p_locs['gbd_analytical_superregion_id']).T == (
            np.asmatrix(o_locs['gbd_analytical_superregion_id'])).astype(int))

        spacemap = in_country + in_region + in_sr

        return np.array(spacemap)

    def calc_spatial_distances(self, o_locs, level):

        o_locs = np.atleast_1d(o_locs).ravel()

        # pull locations and pare down to necessities
        lflat = self.locations
        lflat = lflat.filter(regex='location_id|level')
        lflat = lflat.rename(columns={'location_id': 'leaf_node'})

        leaf_lvls = []
        for lvl in range(3, level+1):
            leaf_lvls.append(lvl)
        leaf_lvls.reverse()

        o_locdf = pd.DataFrame({'leaf_node': o_locs})
        o_locdf = o_locdf.merge(lflat, on='leaf_node')
        d_df = []
        for lvl in leaf_lvls:
            leaf_df = lflat[lflat['level_%s' % lvl].notnull()]
            #lflat = lflat[lflat['level_%s' % lvl].isnull()]
            d0_locs = (
                np.atleast_2d(leaf_df['level_%s' % lvl].values).T ==
                np.atleast_2d(o_locdf['level_%s' % lvl].values)).astype(int)
            d_locs = d0_locs
            d1_locs = (
                np.atleast_2d(leaf_df['level_%s' % (lvl-1)].values).T ==
                np.atleast_2d(o_locdf['level_%s' % (lvl-1)].values)).astype(int)
            d_locs = d_locs + d1_locs
            d2_locs = (
                np.atleast_2d(leaf_df['level_%s' % (lvl-2)].values).T ==
                np.atleast_2d(o_locdf['level_%s' % (lvl-2)].values)).astype(int)
            d_locs = d_locs + d2_locs
            if lvl > 3:
                d3_locs = (
                    np.atleast_2d(leaf_df['level_%s' % (lvl-3)].values).T ==
                    np.atleast_2d(o_locdf['level_%s' % (lvl-3)].values)).astype(int)
                d_locs = d_locs + d3_locs
            if lvl > 4:
                d4_locs = (
                    np.atleast_2d(leaf_df['level_%s' % (lvl-4)].values).T ==
                    np.atleast_2d(o_locdf['level_%s' % (lvl-4)].values)).astype(int)
                d_locs = d_locs + d4_locs
            if lvl > 5:
                d5_locs = (
                    np.atleast_2d(leaf_df['level_%s' % (lvl-5)].values).T ==
                    np.atleast_2d(o_locdf['level_%s' % (lvl-5)].values)).astype(int)
                d_locs = d_locs + d5_locs
            d_df.append(pd.DataFrame(
                d_locs.T, columns=leaf_df['level_%s' % lvl].values,
                index=o_locs))

        d_df = pd.concat(d_df, axis=1)
        d_df = d_df.fillna(0)

        return d_df

    def space_weights(self, loc, level):
        """ Generate space weights """
        o_locs = self.data[[self.spacevar]]
        spacemap = self.calc_spatial_distances(o_locs, level)

        spacemap = spacemap.loc[:, [loc]].values.T

        # Getting rid of duplicate rows 
        spacemap = spacemap[0, :]

        if (self.t_weights is not None and self.a_weights is not None):
            """ Align time weights with a prediction space that is
            sorted by age group, then year """
            t_weights = np.tile(self.t_weights.T, len(self.p_ages)).T

            """ Align age weights with a prediction space that is
            sorted by age group, then year """
            a_weights = np.repeat(self.a_weights, len(self.p_years), axis=0)

            weights = t_weights * a_weights

        elif self.t_weights is not None:
            t_weights = np.tile(self.t_weights.T, len(self.p_ages)).T
            weights = t_weights

        elif self.a_weights is not None:
            a_weights = np.repeat(
                self.a_weights, len(self.p_years), axis=0)
            weights = a_weights

        else:
            return None

        sp_weights_temp = [self.zeta] + [self.zeta *
                                         (1-self.zeta)**n for n in range(1, level+1)]
        sp_weights_temp[-1] /= self.zeta

        sp_weights_temp.reverse()

        # Check if that level of depth has data, if not replace weight to 0
        for depth in range(0, level+1):
            if (depth not in np.unique(spacemap)):
                sp_weights_temp[depth] = 0

        # Rescale weights
        #weight_rescale_denominator = np.sum(sp_weights_temp)
        #sp_weights_temp = sp_weights_temp / weight_rescale_denominator

        # Use list of weights to populate weighting dictionary
        sp_weights = dict(enumerate(sp_weights_temp))

        normalized_weights = np.zeros(weights.shape)
        sgms = {}
        for spatial_group in np.unique(spacemap):
            sp_grp_mask = (spacemap == spatial_group).astype(int)
            sgms[spatial_group] = sp_grp_mask
            if (np.sum(weights*sp_grp_mask) > 0):
                mskd_wts = (sp_weights[spatial_group]*(weights*sp_grp_mask)).T
                wt_sum = np.sum(weights*sp_grp_mask, axis=1)
                normalized_weights = normalized_weights+(mskd_wts/wt_sum).T

        # Normalize the weights
        normalized_weights = normalized_weights / \
            np.atleast_2d(normalized_weights.sum(axis=1)).T

        # Log output
        nws = {}
        sp_weight_output_temp = []
        for spatial_group in sgms.keys():
            nws[spatial_group] = (normalized_weights *
                                  sgms[spatial_group]).sum(axis=1)
        for depth in range(0, level+1):
            if (depth not in np.unique(sgms.keys())):
                nws[depth] = 0
            sp_weight_output_temp.append(
                np.unique(np.round(nws[depth], decimals=4))[0])

        sp_weight_output = dict(enumerate(sp_weight_output_temp))

        self.sp_weight_output = sp_weight_output
        self.final_weights = normalized_weights
        return normalized_weights

    def smooth(self, level, locs=None):
        """ Add the weighted-average of the residuals back into the
        original predictions """
        if locs is None:
            locs = pd.unique(self.stage1[self.spacevar])
        else:
            locs = np.atleast_1d(locs)

        for loc in locs:

            """ Generate space weights... Could modify this to count
            whether there is any data for this country
            [[ len(data[data['iso3'] == iso]) == 0  ]] and change zeta
            accordingly. i.e. implement 'zeta_no_data' """
            self.space_weights(loc, level=level)

            prior = self.stage1[
                self.stage1[self.spacevar] == loc][self.modelvar]

            wtd_resids = np.sum(
                np.array(self.data['resid'])*self.final_weights, axis=1)
            smooth = np.array(prior)+wtd_resids
            self.results[loc] = smooth

        return smooth

    def set_params(self, params, values):
        """ Method to set ST parameters """
        for i, param in enumerate(params):
            self[param] = values[i]

    def long_result(self):
        """ Convert results to long format for more convenient appends """
        melted = pd.melt(
            self.results, id_vars=[self.agevar, self.timevar],
            var_name=self.spacevar, value_name='st')
        melted[self.spacevar] = melted[self.spacevar].astype('int')
        melted[self.agevar] = melted[self.agevar].astype('int')
        melted[self.timevar] = melted[self.timevar].astype('int')
        return melted
