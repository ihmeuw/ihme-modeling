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
        self.age_map = {value: idx for value, idx in enumerate(
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

        self.stage1 = dataset.loc[
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
        self.no_data = 5

    def exp_time_weights(self, loc=None, wts_only=False):
        """ Generate  exponential decay time weights """
        time_map = self.p_years - np.min(self.p_years)
        p_years = np.atleast_2d(self.p_years)

        if loc == None:
            o_years = np.atleast_2d(self.data[self.timevar].values)
        else:
            o_years = np.atleast_2d(
                (self.data[self.data[self.spacevar] == loc][self.timevar]).drop_duplicates())

        t_weights_lookup = {}
        # run computation of exponential decay
        for i in time_map:
            t_weights_lookup[i] = 1/np.exp(self.lambdaa*i)

           # lookup t_weights^M
        t_weights = spatial.distance.cdist(
            p_years.T, o_years.T,
            lambda u, v: t_weights_lookup[abs(u[0] - v[0])])

        # only make t_weights class object for full location set of timeweights
        if(loc == None):
            self.t_weights = t_weights

        if wts_only:
            return t_weights_lookup

    def time_weights(self, loc=None):
        """ Generate time weights """
        p_years = np.atleast_2d(self.p_years)

        if loc == None:
            o_years = np.atleast_2d(self.data[self.timevar].values)
        else:
            o_years = np.atleast_2d(
                (self.data[self.data[self.spacevar] == loc][self.timevar]).drop_duplicates())

        print(self.lambdaa)

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

        # only make t_weights class object for full location set of timeweights
        self.t_weights = t_weights
        return t_weights

    def age_weights(self, wts_only=False):
        """ Generate age weights """
        p_ages = np.atleast_2d(self.p_ages)
        o_ages = np.atleast_2d(self.data[self.agevar].values)

        # Pre-compute weights for each age-distance
        a_weights_lookup = np.empty([len(self.age_map), len(self.age_map)])
        for i in range(len(self.age_map)):
            for j in range(len(self.age_map)):
                dist = abs(self.age_map[i] - self.age_map[j])
                a_weights_lookup[i][j] = 1 / np.exp(self.omega*dist)

        a_weights = spatial.distance.cdist(
            p_ages.T, o_ages.T,
            lambda u, v: a_weights_lookup[u[0]][v[0]])

        if(wts_only):
            a_weights = np.array(a_weights_lookup.values())
            return(a_weights)
        else:
            self.a_weights = np.atleast_2d(a_weights)

    def time_specific_zeta(self, loc, zeta_no_data, function="exp"):
        """ Generate a time-specific data by getting location-specific time-series
        and summing for each year, then normalizing to the max weight observed
        in that time series"""

        # first, get observed years of ts for this location
        o_years = sorted(self.data[
            self.data[self.spacevar] == loc][self.timevar].drop_duplicates())

        # deal with locations with no data (apparently you can check for emptiness with "not")
        if not o_years:
            ts_zeta = np.repeat(self.zeta, len(self.p_years))
            self.ts_zeta = ts_zeta
            return(ts_zeta)

        # generate timeweights for this location only
        if function == "exp":
            t_weights = self.exp_time_weights(loc=loc)
        if function == "cubic":
            t_weights = self.time_weights(loc=loc)

        # sum timewts by year to get the total weight given data for that loc/year
        fullwts = np.sum(t_weights, axis=1)

        # normalize to highest observed weight for that location/age ts
        # below .5, zeta actually gives more weight to super-region that region, so make sure min zeta is .5
        ts_zeta = np.round((fullwts / np.max(fullwts))*self.zeta, decimals=3)
        ts_zeta[ts_zeta < .5] = zeta_no_data

        self.ts_zeta = ts_zeta

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

    def space_time_weights(self, loc, level):
        """ Generate space weights """
        # get observed locations

        o_locs = self.data[[self.spacevar]]

        # calculate spacemap for given location_id based on GBD spatial heirarchy
        spacemap = self.calc_spatial_distances(o_locs, level)
        spacemap = spacemap[loc].values.T

        # Getting rid of duplicate rows
        if spacemap.ndim > 1:
            spacemap = spacemap[0, :]
        spacemap = np.atleast_2d(spacemap)

        # PREP LOCATION-LEVEL SCALAR
        # create weights based on the number of levels, with wt = 1 for location_id level
        dpwt = np.atleast_2d(1 * [self.zeta**(n)
                                  for n in reversed(range(0, level+1))])

        # grab time weights
        st_weight = self.t_weights

        # Use list of weights to populate weighting dictionary
        dpwt = dpwt.T
        level_wt = dict(enumerate(dpwt))

        """Normalize to 1"""
        spacetime_weights = np.zeros(shape=st_weight.shape)
        denom = np.zeros(shape=(1, len(self.p_years)))
        sgms = {}
        for spatial_group in np.unique(spacemap):
            space_mask = (spacemap == spatial_group).astype(int)
            sgms[spatial_group] = space_mask
            st_weights_temp = level_wt[spatial_group]*space_mask*st_weight
            denom_temp = np.atleast_2d(
                level_wt[spatial_group]*(space_mask*st_weight).sum(axis=1)).T
            # add this levels' results to total spacetime_weights and denom
            spacetime_weights = spacetime_weights + st_weights_temp
            denom = denom + denom_temp

        # Normalize the weights
        normalized_weights = spacetime_weights / \
            np.atleast_2d(spacetime_weights.sum(axis=1)).T

        nws = {}
        sp_weight_output_temp = []
        for spatial_group in sgms.keys():
            # get the weights for each location level
            nws[spatial_group] = (normalized_weights *
                                  sgms[spatial_group]).sum(axis=1)
        for depth in range(0, level+1):
            if (depth not in np.unique(sgms.keys())):
                nws[depth] = np.repeat(0, len(self.p_years))
            sp_weight_output_temp.append(
                np.round(nws[depth].sum(), decimals=4))
        sp_weight_output = sp_weight_output_temp/np.sum(sp_weight_output_temp)

        self.wts_by_year = nws  # shows proportion of the weight going to each level by year
        # proportion of weight going to each location-level across all years
        self.sp_weight_output = sp_weight_output
        self.st_weights = normalized_weights  # output weights after normalizing by
        return normalized_weights

    def combine_all_weights(self, loc):

        # Check for age weights
        if(self.a_weights is not None):
            """ Align time weights with a prediction space that is
            sorted by age group, then year """
            st_weights = np.tile(self.st_weights.T, len(self.p_ages)).T

            """ Align age weights with a prediction space that is
			sorted by age group, then year """
            a_weights = np.repeat(self.a_weights, len(self.p_years), axis=0)

            weights = st_weights * a_weights
        else:
            weights = self.st_weights

        self.final_weights = weights/np.atleast_2d(weights.sum(axis=1)).T

    def smooth(self, level, locs=None):
        """ Add the weighted-average of the residuals back into the
        original predictions """

        if locs is None:
            locs = pd.unique(self.stage1[self.spacevar])
        else:
            locs = np.atleast_1d(locs)

        for loc in locs:

            # get each weigh sequentially
            self.space_time_weights(loc=loc, level=level)

            # combine weights
            self.combine_all_weights(loc=loc)

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
