# In-progress dev code to run ST GPR at the age-specific level -- scrapped for now due to time constraints
import numpy as np
from scipy import stats
from scipy import spatial
import sp_dists
import pandas as pd
import os
from hierarchies import dbtrees

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
            self, dataset, location_set_version_id, timevar='year_id',
            agevar='age_group_id', spacevar='location_id',
            datavar='observed_data', modelvar='stage1_prediction',
            snvar=None, pred_age_group_ids=range(2, 22),
            pred_start_year=1980, pred_end_year=2015, age_mad = False):

        # Setup template for year/age predictions
        self.p_years = range(pred_start_year, pred_end_year+1)
        self.p_ages = pred_age_group_ids
        self.age_map = {value: idx for idx, value in enumerate(
            pred_age_group_ids)}

        time_series = pd.DataFrame({'joinkey': 1, timevar: self.p_years})
        age_series = pd.DataFrame(
                {'joinkey': 1, agevar: pred_age_group_ids})
        self.results = time_series.merge(age_series, on='joinkey')
        self.results.drop('joinkey', axis=1, inplace=True)

        # No default age and time weights, must be set explicitly
        self.a_weights = None
        self.t_weights = None

        # Assume an input data set with default variable names
        self.age_mad = age_mad
        self.timevar = timevar
        self.agevar = agevar
        self.spacevar = spacevar
        self.datavar = datavar
        self.modelvar = modelvar
        self.snvar = snvar
        self.lvid = location_set_version_id

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
        self.stage1 = self.stage1.sort(
                [self.spacevar, self.agevar, self.timevar])

        # Set default smoothing parameters
        self.lambdaa = 1.0
        self.omega = 2
        self.zeta = 0.9
        self.sn_weight = 0.2

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
                right_on='iso3').sort('data_order').reset_index()

        in_country = (np.asmatrix(p_locs['location_id']).T == np.asmatrix(
            o_locs['location_id'])).astype(int)
        in_region = (np.asmatrix(p_locs['gbd_analytical_region_id']).T == (
            np.asmatrix(o_locs['gbd_analytical_region_id'])).astype(int))
        in_sr = (np.asmatrix(p_locs['gbd_analytical_superregion_id']).T == (
            np.asmatrix(o_locs['gbd_analytical_superregion_id'])).astype(int))

        spacemap = in_country + in_region + in_sr

        return np.array(spacemap)

    def space_weights(self, loc):
        """ Generate space weights """
        o_locs = self.data[[self.spacevar]]
        spacemap = sp_dists.calc_spatial_distances(42, o_locs)
        spacemap = spacemap.loc[:, [loc]].values.T

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

        sp_weights = {3: self.zeta,
                      2: self.zeta*(1-self.zeta),
                      1: (1-self.zeta)**2,
                      0: 0}

        # Downweight subnational data points if requested
        if self.snvar is not None:
            nat_map = [
                    0 if val == 1 else 1 for val in self.data[self.snvar]]
            sp_grp_mask = (spacemap == 3).astype(int)
            all_sum = np.sum(weights*sp_grp_mask)
            subnat_sum = np.sum(
                    weights*sp_grp_mask*[self.data[self.snvar]])

            # If there is no national data, treat subnational as national
            if subnat_sum > 0 and subnat_sum != all_sum:
                # Isolate the subnational weights
                numerator = all_sum*(self.sn_weight)
                scaled_sn_weight = (1-self.sn_weight)*subnat_sum
                masked_weights = (
                        weights*sp_grp_mask*[self.data[self.snvar]])
                sn_weights = numerator/(scaled_sn_weight*masked_weights)

                """ Zero out the subnationals in the original weight
                file, and add them back in """
                weights = weights*([nat_map]) + sn_weights

        """
        Normalize to 1. NOTE A POSSIBLE BUG... IF SUBNATIONAL IS ON AND
        THERE IS NO NATIONAL DATA IN THE SURROUNDING REGION, THIS WILL
        BREAK... CONSIDER POSSIBLE FIXES """
        normalized_weights = np.zeros(weights.shape)
        for spatial_group in np.unique(spacemap):
            sp_grp_mask = (spacemap == spatial_group).astype(int)
            if (np.sum(weights*sp_grp_mask) > 0):
                mskd_wts = (sp_weights[spatial_group]*(weights*sp_grp_mask)).T
                wt_sum = np.sum(weights*sp_grp_mask, axis=1)
                normalized_weights = normalized_weights+(mskd_wts/wt_sum).T

        self.final_weights = normalized_weights
        return normalized_weights

    def smooth(self, locs=None):
        """ Add the weighted-average of the residuals back into the
        original predictions """
        if locs is None:
            locs = pd.unique(self.stage1[self.spacevar])
        else:
            locs = np.atleast_1d(locs)

        for loc in locs:
            print('iso: '+str(loc))

            """ Generate space weights... Could modify this to count
            whether there is any data for this country
            [[ len(data[data['iso3'] == iso]) == 0  ]] and change zeta
            accordingly. i.e. implement 'zeta_no_data' """
            self.space_weights(loc)

            """ Smooth away... might want to take care of the residuals
            in the function itself at some point """
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
                var_name=self.spacevar, value_name='st_prediction')
        melted[self.spacevar] = melted[self.spacevar].astype('int')
        melted[self.agevar] = melted[self.agevar].astype('int')
        melted[self.timevar] = melted[self.timevar].astype('int')
        return melted

    def calculate_mad(self):
        """ Merge the ST results back onto the input dataset and calculate
        MAD estimates at every location level """
        flatmap = dbtrees.loctree(self.lvid).flatten()
        melted = self.long_result()
        merged = pd.merge(
                self.dataset, melted,
                on=[self.agevar, self.timevar, self.spacevar], how='right')
        merged = pd.merge(
                merged, flatmap, left_on=self.spacevar, right_on='leaf_node',
                how='left')

        # Calculate residuals
        merged['st_resid'] = merged[self.datavar] - merged['st_prediction']

        if self.age_mad==True:
            print "We are running the age and location-specific MAD here"
			# Calculate MAD estimates at various geographical levels
            for lvlcol in merged.filter(like='level').columns:
				if merged[lvlcol].notnull().any():
					mad_lvl = merged.groupby([lvlcol,self.agevar]).agg(
							{'st_resid': mad}).reset_index().rename(
									columns={'st_resid': 'mad_%s_%s' % [lvlcol,self.agevar]})
					merged = pd.merge(merged, mad_lvl, on=[lvlcol,self.agevar], how="left")
        else:
            print "We are running the location-only MAD"
			# Calculate MAD estimates at various geographical levels
            for lvlcol in merged.filter(like='level').columns:
				if merged[lvlcol].notnull().any():
					mad_lvl = merged.groupby([lvlcol]).agg(
							{'st_resid': mad}).reset_index().rename(
									columns={'st_resid': 'mad_%s' % [lvlcol]})
					merged = pd.merge(merged, mad_lvl, on=[lvlcol], how="left")
        return merged
