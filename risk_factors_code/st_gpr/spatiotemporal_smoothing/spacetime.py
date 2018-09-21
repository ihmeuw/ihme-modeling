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
			self,
			dataset,
			location_set_version_id,
			timevar='year_id',
			agevar='age_group_id',
			spacevar='location_id',
			datavar='observed_data',
			modelvar='stage1_prediction',
			snvar=None,
			pred_age_group_ids=range(2, 22),
			pred_start_year=1980,
			pred_end_year=2016):

		# Setup template for year/age predictions
		self.p_years = range(pred_start_year, pred_end_year+1)
		self.p_ages = pred_age_group_ids
		self.age_map = {value: idx for idx, value in enumerate(
			pred_age_group_ids)}
		time_series = pd.DataFrame({'joinkey': 1, timevar: self.p_years})
		age_series = pd.DataFrame(
				{'joinkey': 1, agevar: pred_age_group_ids})
		self.results = age_series.merge(time_series, on='joinkey')
		self.results.drop('joinkey', axis=1, inplace=True)

		# No dUSERt age and time weights, must be set explicitly
		self.a_weights = None
		self.t_weights = None

		# Assume an input data set with dUSERt variable names
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

		# Set dUSERt smoothing parameters
		self.lambdaa = 1.0
		self.omega = 2
		self.zeta = 0.9
		self.sn_weight = 0.1
		self.zeta_no_data = None

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

	def space_weights(self, loc, level, zeta_threshold):
		""" Generate space weights """
		o_locs = self.data[[self.spacevar]]
		spacemap = sp_dists.calc_spatial_distances(self.lvid, o_locs, level)

		spacemap = spacemap.loc[:, [loc]].values.T

		spacemap = spacemap[0,:]

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

		# Does not have most granular data, eg. no country data
		if zeta_threshold == 0 and self.zeta_no_data is not None:
			sp_weights_temp = [self.zeta_no_data] + [self.zeta_no_data * (1-self.zeta_no_data)**n for n in range(1, level+1)]
			sp_weights_temp[-1] /= self.zeta_no_data
		# Most granular data exists
		if zeta_threshold == 1:
			sp_weights_temp = [self.zeta] + [self.zeta * (1-self.zeta)**n for n in range(1, level+1)]
			sp_weights_temp[-1] /= self.zeta
		
		sp_weights_temp.reverse()

		# Check if that level of depth has data, if not replace weight to 0
		for depth in range(0,level+1):
			if (depth not in np.unique(spacemap)):
				sp_weights_temp[depth]=0

		# Use list of weights to populate weighting dictionary
		sp_weights = dict(enumerate(sp_weights_temp))

		# Downweight subnational data points if requested
		if self.snvar is not None:
			nat_map = [0.2 if val == 1 else 1 for val in self.data[self.snvar]]
		else:
			nat_map = 1

		normalized_weights = np.zeros(weights.shape)
		sgms = {}
		for spatial_group in np.unique(spacemap):
			sp_grp_mask = (spacemap == spatial_group).astype(int)
			sgms[spatial_group] = sp_grp_mask
			if (np.sum(weights*sp_grp_mask) > 0):
				mskd_wts = (sp_weights[spatial_group]*(weights*sp_grp_mask*nat_map)).T
				wt_sum = np.sum(weights*sp_grp_mask, axis=1)
				normalized_weights = normalized_weights+(mskd_wts/wt_sum).T

		# Normalize the weights
		normalized_weights = normalized_weights / np.atleast_2d(normalized_weights.sum(axis=1)).T

		# Log output
		nws = {}
		sp_weight_output_temp = []
		for spatial_group in sgms.keys():
			nws[spatial_group] = (normalized_weights*sgms[spatial_group]).sum(axis=1) 
		for depth in range(0,level+1):
			if (depth not in np.unique(sgms.keys())):
				nws[depth]=0
			sp_weight_output_temp.append(np.unique(np.round(nws[depth], decimals = 4))[0])

		sp_weight_output = dict(enumerate(sp_weight_output_temp))

		self.sp_weight_output = sp_weight_output			  		
		self.final_weights = normalized_weights
		return normalized_weights

	def smooth(self, zeta_threshold, level, locs=None):
		""" Add the weighted-average of the residuals back into the
		original predictions """
		if locs is None:
			locs = pd.unique(self.stage1[self.spacevar])
		else:
			locs = np.atleast_1d(locs)

		for loc in locs:
			print('iso: '+str(loc))

			self.space_weights(loc, level=level, zeta_threshold=zeta_threshold)

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