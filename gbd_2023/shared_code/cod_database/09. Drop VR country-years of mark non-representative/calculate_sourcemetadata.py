import sys
import os
import pandas as pd
import numpy as np
import getpass

sys.path.append("FILEPATH".format(getpass.getuser()))
from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.claude_io import get_claude_data, makedirs_safely
from cod_prep.claude.configurator import Configurator

from cod_prep.downloaders.locations import get_current_location_hierarchy, add_location_metadata, get_country_level_location_id
from cod_prep.downloaders.population import get_env
from cod_prep.downloaders.ages import get_cod_ages, get_mortality_based_age_weights

from cod_prep.utils import cod_timestamp, report_if_merge_fail, report_duplicates

CONF = Configurator('standard')

def assign_star_thresholds(x):
	if x >= 0.85:
		return 5
	elif x >= 0.65:
		return 4
	elif x >=0.35:
		return 3
	elif x >= 0.1:
		return 2
	elif x > 0:
		return 1
	else:
		return 0

class SourceMetadata(CodProcess):
	def __init__(self, env_run=None):

		if env_run == None:
			self.env_run = CONF.get_id('env_run')
		else:
			self.env_run = env_run

		self.env_meta = get_env(
			env_run_id=self.env_run, force_rerun=False, block_rerun=True
		)
		self.loc_meta = get_current_location_hierarchy(
			location_set_id=CONF.get_id('location_set'), location_set_version_id=CONF.get_id('location_set_version'),
			force_rerun=False, block_rerun=True
		)
		self.age_meta = get_cod_ages(force_rerun=False, block_rerun=True)
		self.ddm_path = "FILEPATH"

		self.source_cols = ["source", "nid", "data_type_id"]
		self.geo_cols = ["location_id", "year_id"]
		self.meta_cols = ["nationally_representative", "detail_level_id"]
		self.completeness_cols = ["location_id", "source", "year_id", "nid"]
		self.year_end = CONF.get_id('year_end')
		self.full_time_series = "full_time_series"
		self.cod_ages = self.age_meta.age_group_id.unique().tolist()

		self.gbg_threshold = 0.5
		self.comp_threshold = 0.5
		self.mccd_comp_threshold = 0.2

		self.comp_out_dir = "FILEPATH"
		self.smp_out_dir = "FILEPATH"
		self.timestamp = cod_timestamp()[0:10]

		self.completeness_df = pd.DataFrame()


	###################################
	# GENERATION OF COMPLETENESS ESTIMATES
	###################################

	def create_national_aggregates(self, df):
		eng = df.loc[df.source.isin(["England_UTLA_ICD9", "England_UTLA_ICD10"])]
		eng = eng.groupby(['year_id', 'source', 'nid'], as_index=False).deaths.sum()
		eng['location_id'] = 4749

		detail_ind = self.loc_meta.loc[
			(self.loc_meta.iso3 == 'IND') & (self.loc_meta.level == 5)
		].location_id.unique().tolist()
		ind = df.loc[df.location_id.isin(detail_ind)]

		ind = ind.merge(
			self.loc_meta[['location_id', 'parent_id']], how='left', on='location_id'
		)
		ind['location_id'] = ind['parent_id']
		ind = ind.groupby(['location_id', 'year_id', 'source', 'nid'], as_index=False).deaths.sum()

		natl = df.loc[df.location_id != df.country_location_id]
		natl.loc[natl.source.str.startswith("India_MCCD"), 'source'] = 'India_MCCD'
		natl = natl.loc[~(
			(natl.country_location_id == 6) & (natl.source.isin(['ICD10', 'ICD9_detail', 'ICD9_BTL']))
		)]
		natl = natl.loc[~((natl.source == 'China_DSP_prov_ICD10') & (natl.year_id.isin(range(2004, 2018))))]
		natl.loc[(natl.country_location_id == 63) & (natl.year_id >= 2015), 'source'] = "UKR_MoH_ICD10_tab"
		natl.loc[natl.country_location_id == 95, 'source'] = 'GBR_aggregated_sources'

		nid_dict = pd.read_csv(CONF.get_resource('nid_replacements')).set_index("match_location_id")["NID"].to_dict()
		natl['new_nid'] = df['country_location_id'].map(nid_dict)
		report_if_merge_fail(natl, 'new_nid', 'country_location_id')
		natl['nid'] = natl['new_nid']

		natl = natl.groupby(
			['country_location_id', 'year_id', 'source', 'nid'], as_index=False
		).deaths.sum()
		natl['location_id'] = natl['country_location_id']

		df = pd.concat([df, natl, eng, ind])
		df = df.drop('country_location_id', axis=1)
		assert df.notnull().values.all()
		report_duplicates(df, ['location_id', 'year_id', 'source'])
		return df

	def calculate_env_completeness(self, df):
		env = self.env_meta.query("age_group_id == 22 & sex_id == 3")
		report_duplicates(env, ['location_id', 'year_id'])

		df = df.merge(env[['location_id', 'year_id', 'mean_env']], on=['location_id', 'year_id'], how='left')
		df['comp'] = df['deaths'] / df['mean_env']
		assert df.comp.notnull().all()
		df['denominator'] = 'envelope'

		df = df.rename(columns={"deaths": "vr_deaths", "mean_env": "env_deaths"})
		return df

	def incorporate_ddm_comp(self, df):
		ddm = pd.read_csv(self.ddm_path)
		ddm = ddm.loc[
			(ddm["sex_id"] == 3) & (ddm["age_group_id"] == 199) & (ddm["source"] == "DSP") &
			(ddm["estimate_stage_name"] == "final completeness") & (ddm["ihme_loc_id"].str.startswith("CHN"))
		]
		ddm.loc[ddm["location_id"] == 44533, "location_id"] = 6
		for yr in range(2020, self.year_end + 1):
			temp = ddm.loc[ddm.year_id == 2019]
			temp['year_id'] = yr
			ddm = pd.concat([ddm, temp])

		ddm = ddm.rename(columns={'mean': 'comp'})
		ddm = ddm[['location_id', 'year_id', 'comp']]
		ddm = ddm.assign(denominator='sample', source='ddm')
		report_duplicates(ddm, ['year_id', 'location_id'])
		df = pd.concat([df, ddm])
		return df

	def generate_completeness(self):
		df = get_claude_data(
			'sourcemetadata', data_type_id=[9, 10], filter_locations=True, is_active=True,
			year_id=range(1980, self.year_end + 1), lazy=True
		)
		df = df.groupby(self.completeness_cols).deaths.sum().reset_index()
		df = df.compute()

		country_locs = get_country_level_location_id(df.location_id.unique().tolist(), self.loc_meta)
		df = df.merge(country_locs, on='location_id', how='left')
		report_if_merge_fail(df, 'country_location_id', 'location_id')

		df = df.loc[df.source != 'Other_Maternal']
		df = df.loc[~((df["year_id"] == 1980) & (df["country_location_id"] == 95))]

		df = self.create_national_aggregates(df)

		df = self.calculate_env_completeness(df)
		df = self.incorporate_ddm_comp(df)

		self.completeness_df = df[[
			'location_id', 'year_id', 'source', 'nid', 'comp', 'denominator', 'vr_deaths', 'env_deaths'
		]]

	###################################
	# GENERATION OF SMP OUTPUTS
	###################################

	def pull_data_for_smp(self, dtypes):
		df = get_claude_data(
			'sourcemetadata', is_active=True, filter_locations=True, data_type_id=dtypes,
			year_id=range(1980, self.year_end + 1), lazy=True
		)

		df = df.loc[~((df.year_id == 1980) & (df.location_id.isin([434, 433])))]
		df = df.loc[~((df.source == 'Other_Maternal') & (df.data_type_id == 9))]

		group_cols = self.geo_cols + self.source_cols + self.meta_cols + ['age_group_id', 'sex_id']
		df = df.groupby(group_cols).deaths.sum().reset_index()
		df = df.compute()
		return df

	def create_vr_national_aggregates(self, df):
		group_cols = self.meta_cols + self.source_cols + ['country_location_id', 'year_id', 'age_group_id', 'sex_id']

		country_locs = get_country_level_location_id(df.location_id.unique().tolist(), self.loc_meta)
		df = df.merge(country_locs, on='location_id', how='left')
		report_if_merge_fail(df, 'country_location_id', 'location_id')

		eng = df.loc[df.source.isin(['England_UTLA_ICD9', 'England_UTLA_ICD10'])]
		eng = eng.groupby(group_cols, as_index=False).deaths.sum()
		eng['country_location_id'] = 4749

		detail_ind = self.loc_meta.loc[
			(self.loc_meta.iso3 == 'IND') & (self.loc_meta.level == 5)
		].location_id.unique().tolist()
		ind = df.loc[df.location_id.isin(detail_ind)]

		ind = ind.merge(
			self.loc_meta[['location_id', 'parent_id']], how='left', on='location_id'
		)
		ind['country_location_id'] = ind['parent_id']
		ind = ind.groupby(group_cols, as_index=False).deaths.sum()

		natl = df.loc[df.location_id != df.country_location_id]
		natl = natl.loc[~((natl.country_location_id == 6) & (natl.source.isin(['ICD9_BTL', 'ICD9_detail', 'ICD10'])))]
		natl.loc[natl.source.str.startswith("India_MCCD"), 'source'] = 'India_MCCD'
		natl.loc[(natl.country_location_id == 63) & (natl.year_id >= 2015), 'source'] = "UKR_MoH_ICD10_tab"
		natl.loc[natl.country_location_id == 95, 'source'] = 'GBR_aggregated_sources'

		nid_dict = pd.read_csv(CONF.get_resource('nid_replacements')).set_index("match_location_id")["NID"].to_dict()
		natl['new_nid'] = df['country_location_id'].map(nid_dict)
		report_if_merge_fail(natl, 'new_nid', 'country_location_id')
		natl['nid'] = natl['new_nid']

		natl = natl.groupby(group_cols, as_index=False).deaths.sum()
		aggregates = pd.concat([eng, ind, natl])
		aggregates = aggregates.rename(columns={'country_location_id': 'location_id'})
		df = df.drop('country_location_id', axis=1)
		assert set(df.columns) == set(aggregates.columns)
		df = pd.concat([df, aggregates])

		report_duplicates(df, ['data_type_id', 'location_id', 'year_id', 'detail_level_id', 'age_group_id', 'sex_id'])
		return df

	def create_nonvr_national_aggregates(self, df):
		group_cols = self.meta_cols + self.source_cols + ['country_location_id', 'year_id', 'age_group_id', 'sex_id']

		country_locs = get_country_level_location_id(df.location_id.unique().tolist(), self.loc_meta)
		df = df.merge(country_locs, on='location_id', how='left')
		report_if_merge_fail(df, 'country_location_id', 'location_id')

		natl = df.loc[(df.location_id != df.country_location_id)]
		natl = natl.groupby(group_cols, as_index=False).deaths.sum()
		natl = natl.rename(columns={"country_location_id": "location_id"})
		df = df.drop('country_location_id', axis=1)
		assert set(df.columns) == set(natl.columns)
		df = pd.concat([df, natl])

		report_duplicates(df, ['location_id', 'year_id', 'nid', 'source', 'detail_level_id', 'age_group_id', 'sex_id'])
		return df

	def create_child_deaths_indicator(self, df):
		under5ages = self.age_meta.query("age_group_years_end <= 5")["age_group_id"].unique().tolist()

		df["under5deaths"] = 1 * df["age_group_id"].isin(under5ages) * df["deaths"]

		df = df.groupby(self.source_cols, as_index=False)["under5deaths", "deaths"].sum()

		df["child5"] = 1 * ((df["under5deaths"] / df["deaths"]) >= 0.95)

		df = df.drop(["under5deaths", "deaths"], axis=1)
		return df

	def calculate_env_coverage(self, df):
		dem_cols = self.geo_cols + ['age_group_id', 'sex_id']

		env = self.env_meta.loc[
			(self.env_meta.sex_id.isin([1, 2])) & (self.env_meta.age_group_id.isin(self.cod_ages))
		][dem_cols + ['mean_env']]
		env['total_env'] = env.groupby(self.geo_cols).mean_env.transform(sum)

		df_cov = df[self.source_cols + dem_cols].drop_duplicates()
		df_cov = df_cov.merge(env, on=dem_cols, how='left')
		report_if_merge_fail(df_cov, 'mean_env', dem_cols)

		df_cov['env_covered'] = df_cov.groupby(self.source_cols + self.geo_cols).mean_env.transform(sum)
		assert not ((df_cov['env_covered'] - df_cov['total_env']) > 0.0001).any()

		df_cov['pct_env_coverage'] = df_cov.env_covered / df_cov.total_env
		df_cov.loc[df_cov.data_type_id.isin([9, 10]), 'pct_env_coverage'] = 1

		df_cov = df_cov[self.source_cols + self.geo_cols + ['pct_env_coverage']].drop_duplicates()
		report_duplicates(df_cov, self.source_cols + self.geo_cols)

		df = df.merge(df_cov, on = self.source_cols+self.geo_cols, how='left')
		report_if_merge_fail(df, 'pct_env_coverage', self.source_cols + self.geo_cols)
		return df

	def calculate_pct_garbage(self, df):
		df['g12_deaths'] = 0
		df['agr_deaths'] = 0
		df.loc[df.detail_level_id.isin([11, 12]), 'g12_deaths'] = df['deaths']
		df.loc[df.detail_level_id.isin([21, 22]), 'agr_deaths'] = df['deaths']

		df = df.groupby(
			self.source_cols + self.geo_cols + ['age_group_id'], as_index=False
		)['g12_deaths', 'agr_deaths', 'deaths'].sum()

		age_wgt_df = get_mortality_based_age_weights(env_run_id=self.env_run)
		age_wgt_dict = (
			age_wgt_df.drop_duplicates(subset=['age_group_id', 'age_group_weight_value'])
			.set_index('age_group_id')['age_group_weight_value']
			.to_dict()
		)

		df['wgt'] = df.age_group_id.map(age_wgt_dict)
		report_if_merge_fail(df, 'wgt', 'age_group_id')
		df['pctgarbage'] = df.g12_deaths / df.deaths * df.wgt
		df['pctagr'] = df.agr_deaths / df.deaths * df.wgt

		df.loc[(df.deaths == 0) & (df.pctgarbage.isnull()), 'pctgarbage'] = 0
		df.loc[(df.deaths == 0) & (df.pctagr.isnull()), 'pctagr'] = 0
		assert df.notnull().values.all()
		df = df.groupby(self.source_cols + self.geo_cols, as_index=False)['pctgarbage', 'pctagr'].sum()
		return df

	def add_completeness_estimates(self, df):
		comp = add_location_metadata(self.completeness_df, 'ihme_loc_id')

		mainland = (comp.ihme_loc_id.str.startswith("CHN")) & ~(comp.location_id.isin([354, 361]))
		sample_denom = (comp.denominator == "sample")
		comp = comp.loc[(mainland & sample_denom) | (~mainland & ~sample_denom)]
		report_duplicates(comp, ['location_id', 'year_id'])

		comp.loc[comp.comp > 1, 'comp'] = 1

		comp = comp[['location_id', 'year_id', 'comp']]
		df = df.merge(comp, on=['location_id', 'year_id'], how='left')
		df.loc[df.data_type_id.isin([8, 12]), 'comp'] = 1
		assert df.comp.notnull().all()

		return df

	def apply_vr_drop_buffers(self, df):
		incoming_cols = df.columns.tolist()
		old_drops = pd.read_csv(CONF.get_resource("vr_indicators"))
		old_drops = old_drops[["location_id", "year_id", "data_type_id", "drop_status"]]
		df = df.merge(
			old_drops, on=["location_id", "year_id", "data_type_id"],
			how="left", suffixes=("", "_old"),
		)
		sub_rep = df["nationally_representative"] == 0
		mccd = df["source"].str.startswith("India_MCCD")

		df["comp_buffer"] = self.comp_threshold - 0.05
		df["garbage_buffer"] = self.gbg_threshold + 0.05
		df.loc[mccd, "comp_buffer"] = self.mccd_comp_threshold - 0.02
		df.loc[sub_rep, "comp_buffer"] = 0

		newly_dropped = df.drop_status > df.drop_status_old
		df.loc[
			newly_dropped & (df.comp > df.comp_buffer) & (df.pctgarbage < df.garbage_buffer),
			"drop_status",
		] = 0
		df = df[incoming_cols]
		assert df.notnull().values.all()
		return df

	def prep_source_location_year_indicators(self, df, df_child):
		out_cols = [
			'location_id', 'year_id', 'time_window', 'nid', 'data_type_id', 'child5', 'nationally_representative',
			'pct_env_coverage', 'pctgarbage', 'pctagr', 'comp', 'va_adjustment', 'subnat_va_adjustment',
			'pct_well_certified', 'time_window_pct_wc'
		]

		df = df.merge(df_child, on=self.source_cols, how='left')

		int_cols = df.select_dtypes(include=['number'])
		str_cols = list(set(df.columns) - set(int_cols))
		for col in int_cols:
			df[col] = df[col].fillna(-1)
		for col in str_cols:
			df[col] = df[col].fillna("")

		df["va_adjustment"] = 1
		df["subnat_va_adjustment"] = 1

		df.loc[df["data_type_id"] == 8, "va_adjustment"] = 0.64
		df.loc[
			df["data_type_id"].isin([8, 12]), "subnat_va_adjustment"
		] = 1 - 0.9 * (df["nationally_representative"] == 0)

		df = df.drop_duplicates(subset=out_cols)

		self.source_location_year_indicators = df[out_cols]

	def prep_vr_indicators(self, df):
		keep_cols = [
			'location_id', 'year_id', 'data_type_id', 'nid', 'source',
			'nationally_representative', 'pctgarbage', 'pctagr', 'comp'
		]
		vr = df.loc[df.data_type_id.isin([9, 10])][keep_cols].drop_duplicates()
		report_duplicates(vr, ['location_id', 'year_id', 'data_type_id'])
		vr = add_location_metadata(vr, 'ihme_loc_id')

		vr['pct_well_cert'] = vr.comp * (1 - vr.pctgarbage - vr.pctagr)

		subn_rep = (vr['nationally_representative'] == 0)
		nigeria = (vr['source'] == 'Nigeria_VR')
		mccd = (vr['source'].str.startswith('India_MCCD'))
		too_much_garbage = (vr['pctgarbage'] > self.gbg_threshold)
		too_low_comp = (vr['comp'] < self.comp_threshold)
		valid_mccd_comp = (vr["comp"] > self.mccd_comp_threshold)

		vr['drop_status'] = 0
		vr.loc[too_much_garbage | too_low_comp, 'drop_status'] = 1
		vr.loc[mccd & valid_mccd_comp & ~too_much_garbage, 'drop_status'] = 0
		vr.loc[subn_rep & ~too_much_garbage, 'drop_status'] = 0
		vr.loc[nigeria, 'drop_status'] = 1
		exception_nids = [16516, 16517, 375235]
		exception_nids += [464324, 464325, 464326, 464327]
		vr.loc[vr.nid.isin(exception_nids), 'drop_status'] = 0
		vr.loc[vr.source == 'UAE_Abu_Dhabi', 'drop_status'] = 0

		vr['comp_under_70'] = 0
		vr.loc[vr.comp < 0.7, 'comp_under_70'] = 1

		self.vr_indicators = self.apply_vr_drop_buffers(vr)

	def calculate_percent_well_certified(self, df):
		df["va_adjustment"] = 1
		df["subnat_va_adjustment"] = 1

		df.loc[df["data_type_id"] == 8, "va_adjustment"] = 0.64
		df.loc[
			df["data_type_id"].isin([8, 12]), "subnat_va_adjustment"
		] = 1 - 0.9 * (df["nationally_representative"] == 0)

		base_equation = df["comp"] * (1 - df["pctgarbage"] - df["pctagr"])

		va_adj = df["va_adjustment"] * df["subnat_va_adjustment"] * df["pct_env_coverage"]
		assert np.allclose(
		    va_adj[df["data_type_id"].isin([9, 10])], 1
		)

		df["pct_well_certified"] = base_equation * va_adj
		df = df.drop(['va_adjustment', 'subnat_va_adjustment'], axis=1)
		return df

	def assign_time_group(self, year):
		if year >= 2015:
			year_start = 2010
		else:
			year_start = 5 * (int(year) // 5)
		if year_start == 2010:
			year_end = self.year_end
		else:
			year_end = year_start + 4
		return "{}_{}".format(year_start, year_end)

	def create_pwc_by_time_window(self, df):
		df = self.calculate_percent_well_certified(df)

		square = self.loc_meta.query(
			"level == 3 | is_estimate == 1 | location_id == 4749"
		)[['location_id']]
		square['key'] = 1
		years = pd.DataFrame(list(range(1980, self.year_end + 1)), columns=['year_id'])
		years['key'] = 1
		square = square.merge(years, on='key', how='left')
		square = square.drop('key', axis=1)
		df = square.merge(df, on=['location_id', 'year_id'], how='left')
		df['pct_well_certified'] = df['pct_well_certified'].fillna(0)

		years = df.year_id.unique().tolist()
		year_group_dict = dict()
		for year in years:
			year_group_dict[year] = self.assign_time_group(year)
		df['time_window'] = df.year_id.map(year_group_dict)

		df['time_window_pct_wc'] = df.groupby(
			['location_id', 'time_window']
		).pct_well_certified.transform(np.max)

		assert df.time_window_pct_wc.notnull().all()
		assert not (df.time_window_pct_wc > 1).any()
		return df

	def apply_star_buffer(self, df):
		incoming_cols = df.columns.tolist()
		old_stars = pd.read_csv(CONF.get_resource("stars_by_iso3"))
		old_stars = old_stars[["location_id", "time_window", "stars"]]
		old_stars.loc[old_stars.time_window.str.startswith('2010_'), 'time_window'] = "2010_" + str(self.year_end)
		assert set(old_stars.time_window) == set(df.time_window)
		df = df.merge(
			old_stars, on=["location_id", "time_window"], how="left", suffixes=("", "_old")
		)

		buffer_dict = {5: 0.82, 4: 0.62, 3: 0.32, 2: 0.07, 1: 0}
		df["buffer_pwc"] = df.stars_old.map(buffer_dict)

		decreased_stars = df.stars_old > df.stars
		greater_than_zero = df.stars > 0
		df.loc[
			decreased_stars & greater_than_zero & (df.time_window_pct_wc >= df.buffer_pwc),
			"stars",
		] = df["stars_old"]
		df = df[incoming_cols]
		assert df.notnull().values.all()
		return df

	def prep_stars(self, df):
		df = df[['location_id', 'time_window', 'time_window_pct_wc']].drop_duplicates()
		report_duplicates(df, ['location_id', 'time_window'])
		assert len(set(df.groupby("location_id")["time_window"].count())) == 1

		full_time_df = df.groupby('location_id', as_index=False).time_window_pct_wc.mean()
		full_time_df['time_window'] = self.full_time_series
		df = pd.concat([df, full_time_df])

		df['stars'] = df.time_window_pct_wc.apply(lambda x: assign_star_thresholds(x))
		df = self.apply_star_buffer(df)

		assert df.notnull().values.all()
		self.stars_df = add_location_metadata(df, ['ihme_loc_id', 'level', 'location_name', 'most_detailed'])
		self.stars_df = self.stars_df.rename(columns={'level': 'location_level'})

		self.stars_hierarchy = self.stars_df.loc[
			(self.stars_df.time_window == self.full_time_series) & (self.stars_df.most_detailed == 1)
		]
		self.stars_hierarchy['parent_id'] = 0
		self.stars_hierarchy.loc[self.stars_hierarchy.stars >= 4, 'parent_id'] = 1
		self.stars_hierarchy = self.stars_hierarchy[["ihme_loc_id", "location_id", "parent_id", "time_window", "stars"]]

	def prep_locations_four_star_plus(self):
		india = self.stars_df["ihme_loc_id"].str.startswith("IND")
		stars_over_4 = self.stars_df["stars"] >= 4
		full_time = self.stars_df["time_window"] == self.full_time_series

		self.four_plus = self.stars_df.loc[~india & stars_over_4 & full_time][[
			'location_id', 'ihme_loc_id', 'time_window', 'stars'
		]]

	def generate_smp_outputs(self):
		if len(self.completeness_df) == 0:
			self.completeness_df = pd.read_csv(CONF.get_resource('completeness'))

		vr = self.pull_data_for_smp(dtypes=[9, 10])
		nonvr = self.pull_data_for_smp(dtypes=[8, 12])

		vr = self.create_vr_national_aggregates(vr)
		nonvr = self.create_nonvr_national_aggregates(nonvr)
		df = pd.concat([vr, nonvr])

		df_child = self.create_child_deaths_indicator(df)

		df = self.calculate_env_coverage(df)

		df_gar = self.calculate_pct_garbage(df)
		df = df.merge(df_gar, on = self.source_cols+self.geo_cols, how='left')
		report_if_merge_fail(df, 'pctgarbage', self.source_cols + self.geo_cols)

		df = self.add_completeness_estimates(df)

		self.prep_vr_indicators(df)

		df = self.create_pwc_by_time_window(df)

		self.prep_source_location_year_indicators(df, df_child)

		self.prep_stars(df)

		self.prep_locations_four_star_plus()


	###################################
	# MAKE VETTING COMPARISONS
	###################################

	def make_comparisons(self):

		vr_ind_cols = ['location_id', 'year_id', 'nid', 'source', 'pctgarbage', 'comp', 'drop_status']
		old = pd.read_csv(CONF.get_resource('vr_indicators'))[vr_ind_cols]

		self.drops_compare = old.merge(
			self.vr_indicators[vr_ind_cols], on=['location_id', 'year_id', 'nid', 'source'],
			how='outer', suffixes=('_old', '_new'), indicator=True
		)
		self.drops_compare['change'] = '0'
		self.drops_compare.loc[self.drops_compare._merge == 'left_only', 'change'] = 'lost_data'
		self.drops_compare.loc[self.drops_compare._merge == 'right_only', 'change'] = 'new_data'
		self.drops_compare.loc[
			(self.drops_compare._merge == 'both') & (self.drops_compare.drop_status_new < self.drops_compare.drop_status_old),
			'change'
		] = '-1'
		self.drops_compare.loc[
			(self.drops_compare._merge == 'both') & (self.drops_compare.drop_status_new > self.drops_compare.drop_status_old),
			'change'
		] = '1'

		star_cols = ['location_id', 'ihme_loc_id', 'time_window', 'time_window_pct_wc', 'stars']
		old = pd.read_csv(CONF.get_resource('stars_by_iso3'))[star_cols]

		self.stars_compare = old.merge(
			self.stars_df[star_cols], on=['location_id', 'ihme_loc_id', 'time_window'],
			how='outer', suffixes=('_old', '_new'), indicator=True
		)
		self.stars_compare = self.stars_compare.loc[self.stars_compare.ihme_loc_id.str.len() == 3]
		self.stars_compare['change'] = '0'
		self.stars_compare.loc[self.stars_compare._merge == 'left_only', 'change'] = 'lost_data'
		self.stars_compare.loc[self.stars_compare._merge == 'right_only', 'change'] = 'new_data'
		self.stars_compare.loc[
			(self.stars_compare._merge == 'both') & (self.stars_compare.stars_new < self.stars_compare.stars_old),
			'change'
		] = '-1'
		self.stars_compare.loc[
			(self.stars_compare._merge == 'both') & (self.stars_compare.stars_new > self.stars_compare.stars_old),
			'change'
		] = '1'

	###################################
	# WRITING OUTPUTS
	###################################

	def write_completeness(self):
		self.completeness_df.to_csv(
			"FILEPATH"
		)
		self.completeness_df.to_csv(
			"FILEPATH"
		)

	def write_source_location_year_indicators(self):
		makedirs_safely(self.smp_out_dir + '/' + self.timestamp)

		self.source_location_year_indicators.to_csv("FILEPATH")
		self.source_location_year_indicators.to_csv("FILEPATH")

	def write_vr_indicators(self):
		makedirs_safely(self.smp_out_dir + '/' + self.timestamp)

		self.vr_indicators.to_csv("FILEPATH")
		self.vr_indicators.to_csv("FILEPATH")

	def write_stars(self):
		makedirs_safely(self.smp_out_dir + '/' + self.timestamp)

		self.stars_df.to_csv("FILEPATH")
		self.stars_df.to_csv("FILEPATH")

		self.stars_hierarchy.to_csv("FILEPATH")
		self.stars_hierarchy.to_csv("FILEPATH")

		self.four_plus.to_csv("FILEPATH")
		self.four_plus.to_csv("FILEPATH")

	def write_comparisons(self):
		makedirs_safely(self.smp_out_dir + '/' + self.timestamp)

		self.drops_compare.to_csv("FILEPATH")
		self.stars_compare.to_csv("FILEPATH")

if __name__ == "__main__":
	rerun_comp = int(sys.argv[1])
	rerun_smp_outputs = int(sys.argv[2])

	smp = SourceMetadata()
	if rerun_comp:
		smp.generate_completeness()
		smp.write_completeness()

	if rerun_smp_outputs:
		smp.generate_smp_outputs()
		smp.write_source_location_year_indicators()
		smp.write_vr_indicators()
		smp.write_stars()

		smp.make_comparisons()
		smp.write_comparisons()