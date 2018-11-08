import pandas as pd
import sys
from getpass import getuser
from os import rename, listdir

if getuser() == 'USER':
    SDG_REPO = 'FILEPATH'
sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

INDICATOR_ID_COLS = ['location_id', 'year_id', 'measure_id', 'metric_id', 'age_group_id', 'sex_id']
FORECAST_ID_COLS = ['location_id', 'year_id', 'age_group_id', 'sex_id']

ncds = [158, 161, 164, 167]
ntds = [1433, 104, 107, 110, 113, 116, 119, 122, 125, 128, 131, 134, 137, 140, 143, 146, 149, 152]


def aggregate_components(indicator_id, version, assert_0_1 = False):
	'''pulls pre-prepped component_id files and aggregates to the indicator level'''

	if indicator_id == 1020: # NCDs
		path = '{dd}codcorrect/{v}'.format(dd=dw.INPUT_DATA_DIR, v=version)
		component_ids = ncds
	elif indicator_id == 1004: # NTDs
		path = '{dd}como_prev/{v}'.format(dd=dw.INPUT_DATA_DIR, v=version)
		component_ids = ntds
	else:
		raise ValueError("bad indicator type: {}".format(indicator_id))

	dfs = []
	for component_id in component_ids: # pull dfs and append to list
		print("pulling {c}".format(c=component_id))
		df = pd.read_feather('{p}/{id}.feather'.format(p=path, id=component_id))
		dfs.append(df)

	df = pd.concat(dfs, ignore_index=True)
	df = df.groupby(INDICATOR_ID_COLS)[dw.DRAW_COLS].sum() # group by and sum
	
	if assert_0_1: # for NCDs only
		assert df.applymap(lambda x: x>0 and x<1).values.all(), \
		'sum produced rates outside of realistic bounds'

	df = df.reset_index()
	
	print("outputting feather".format(c=indicator_id))
	df.to_feather('{p}/{id}.feather'.format(p=path, id=indicator_id))


def aggregate_hrh_unscaled(version = dw.COV_VERS):
	'''aggregates unscaled health worker density covariates (past and future)
	and saves them to their respective hrh folders'''
	past_path = dw.INPUT_DATA_DIR + 'covariate' + '/' + version
	future_path = dw.FORECAST_DATA_DIR + 'covariate' + '/' + version
	
	past_dfs = []
	future_dfs = []
	for component_id in [1457, 1460, 1463]:
		print("pulling {c}".format(c=component_id))
		df_past = pd.read_feather(past_path + '/' + str(component_id) + '.feather')
		df_past = df_past[INDICATOR_ID_COLS + dw.DRAW_COLS]
		df_future = pd.read_feather(future_path + '/' + str(component_id) + '.feather')
		past_dfs.append(df_past)
		future_dfs.append(df_future)
	
	past_agg = pd.concat(past_dfs, ignore_index=True)
	future_agg = pd.concat(future_dfs, ignore_index=True)
	future_agg = future_agg.loc[future_agg['scenario'] == 0,:].drop('scenario', axis=1)

	print("aggregating")
	past_agg = past_agg.groupby(INDICATOR_ID_COLS)[dw.DRAW_COLS].sum() # group by and sum
	future_agg = future_agg.groupby(FORECAST_ID_COLS)[dw.DRAW_COLS].sum()

	past_agg.reset_index(inplace=True)
	future_agg.reset_index(inplace=True)
	
	print("outputting hrh unscaled feathers")
	past_agg.to_feather(dw.INPUT_DATA_DIR + 'hrh' + '/' + version + '/' + '1096_unscaled.feather')
	future_agg.to_feather(dw.FORECAST_DATA_DIR + 'hrh' + '/' + version + '/' + '1096_unscaled.feather')
	print("done")


def output_hrh_components_full_time_series(cov_version = dw.COV_VERS, sdg_version = dw.SDG_VERS):

	past_path = dw.INPUT_DATA_DIR + 'covariate' + '/' + cov_version
	future_path = dw.FORECAST_DATA_DIR + 'covariate' + '/' + cov_version
	
	dfs = []
	for component_id in [1457, 1460, 1463]:
		print("pulling {c}".format(c=component_id))

		df_past = pd.read_feather(past_path + '/' + str(component_id) + '.feather')
		df_past = df_past[['indicator_component_id', 'location_id', 'year_id'] + dw.DRAW_COLS]

		df_future = pd.read_feather(future_path + '/' + str(component_id) + '.feather')
		df_future = df_future.loc[df_future.scenario == 0,:]
		df_future['indicator_component_id'] = component_id
		df_future = df_future[['indicator_component_id', 'location_id', 'year_id'] + dw.DRAW_COLS]

		dfs.extend([df_past, df_future])

	print "concatenating"
	df = pd.concat(dfs, ignore_index=True)

	# adding level column
	locsdf = qry.get_sdg_reporting_locations().loc[:, ['location_id', 'level']]
	df = df.merge(locsdf, on = 'location_id')

	print "outputting feather"
	df['indicator_id'] = 1096
	df = df[['indicator_id', 'indicator_component_id', 'location_id', 'year_id', 'level'] + dw.DRAW_COLS]
	df.columns = df.columns.astype(str)
	df.to_feather(dw.INDICATOR_DATA_DIR + 'gbd2017/hrh_unscaled_components_v{v}.feather'.format(v=sdg_version))
	print("done")


def rename_components(indicator_type, version):
	'''The prep_draws_parallel.py script saves files by indicator_component_id.
	We want everything to be saved at the indicator level, so we change the names
	of all 1-component indicator files to indicator_id.feather'''

	path = '{dd}/{i}/{v}/'.format(dd=dw.INPUT_DATA_DIR, i=indicator_type, v=version)
	comp_ind_map = pd.read_csv('FILEPATH')
	comp_ind_map = comp_ind_map.loc[comp_ind_map['indicator_analytical_group'] == indicator_type,:]

	if indicator_type == 'codcorrect':
		comp_ind_map = comp_ind_map.loc[comp_ind_map.indicator_id != 1020]
	if indicator_type == 'dismod':
		comp_ind_map = comp_ind_map.loc[comp_ind_map.indicator_id != 1064]
	if indicator_type == "covariate":
		comp_ind_map = comp_ind_map.loc[~comp_ind_map.indicator_id.isin([1096, 1037, 1099, 1097])]

	old_names = comp_ind_map.indicator_component_id.astype(str).tolist()
	old_names = [fname + '.feather' for fname in old_names]
	new_names = comp_ind_map.indicator_id.astype(str).tolist()
	new_names = [fname + '.feather' for fname in new_names]

	print("renaming {} components".format(indicator_type))
	for i in range(0, len(old_names)):
		rename(path + old_names[i], path + new_names[i])


if __name__ == '__main__':
	aggregate_components(indicator_id = 1020, version = dw.CC_VERS, assert_0_1 = True)
	aggregate_components(indicator_id = 1004, version = dw.COMO_VERS, assert_0_1 = False)
	rename_components('como_inc', version = dw.COMO_VERS)
	rename_components('dismod', version = dw.DISMOD_VERS)
	rename_components('codcorrect', version = dw.CC_VERS)
	rename_components('demographics', version = dw.DEMO_VERS)
	rename_components('risk_burden', version = dw.BURDENATOR_VERS)
	rename_components('risk_exposure', version = dw.RISK_EXPOSURE_VERS)
	rename_components('covariate', version = dw.COV_VERS)
	rename_components('mmr', version = dw.MMR_VERS)
	rename_components('sev', version = dw.SEV_VERS)
	aggregate_hrh_unscaled()
	output_hrh_components_full_time_series()