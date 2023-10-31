import sys
import os
import pandas as pd
import numpy as np
import itertools
import getpass
from multiprocessing import Pool
from functools import partial

sys.path.append(FILEPATH)
from FILEPATH import Configurator
from FILEPATH import makedirs_safely
from FILEPATH import get_current_location_hierarchy
from FILEPATH import get_current_cause_hierarchy, get_all_related_causes
from FILEPATH import get_cod_ages
from FILEPATH import print_log_message, report_if_merge_fail, report_duplicates
CONF = Configurator('standard')

COD_CORRECT_RUN_ID = 297
OUT_DIR = FILEPATH
CAUSES_DIR = FILEPATH

META_DICT = {
	'_inj': (FILEPATH, 'rr_'),
	'maternal_indirect': (FILEPATH, 'rr_'),
	'lri': (FILEPATH, 'ratio_draw_'),
	'whooping': (FILEPATH, 'ratio_draw_'),
	'measles': (FILEPATH, "draw_")
}

def make_out_directories(cause_meta):
	if CAUSES == 'VPD':
		acauses = ['lri', 'whooping', 'measles']
		cause_ids = cause_meta.loc[cause_meta.acause.isin(acauses)].cause_id.unique().tolist()
	elif CAUSES == 'COD':
		injuries = cause_meta.loc[
			(cause_meta.acause.str.startswith('inj_')) & (cause_meta.most_detailed == 1)
		].cause_id.unique().tolist()
		maternal = cause_meta.loc[cause_meta.acause == 'maternal_indirect'].cause_id.unique().tolist()
		cause_ids = maternal
	for cause in cause_ids:
		path_name = "{}/{}".format(OUT_DIR, cause)
		if not os.path.exists(path_name):
			makedirs_safely(path_name)

def assign_scaling_factors(df, scalar_df, loc_meta, loc_id, rel_risk_cols):
	if CAUSES == 'COD':
		level = loc_meta.loc[loc_meta.location_id == loc_id].level.iloc[0]
		natl_loc = int(loc_meta.loc[loc_meta.location_id == loc_id].path_to_top_parent.iloc[0].split(',')[3])
		region_loc = int(loc_meta.loc[loc_meta.location_id == loc_id].path_to_top_parent.iloc[0].split(',')[2])
		merge_cols = ['scaling_location', 'scaling_cause']

		if level > 3:
			scaling_loc = natl_loc
		else:
			scaling_loc = loc_id
		if not (scaling_loc in scalar_df.scaling_location.unique().tolist()):
			scaling_loc = region_loc
	elif CAUSES == 'VPD':
		scaling_loc = loc_id
		merge_cols = ['scaling_location', 'scaling_cause', 'age_group_id', 'sex_id']

	df['scaling_location'] = scaling_loc
	incoming_size = df.shape[0]
	df = df.merge(scalar_df, on=merge_cols, how='left')
	assert df[rel_risk_cols].notnull().values.all()
	assert df.shape[0] == incoming_size, 'Duplicate observations in the CC data.'
	return df

def get_scalar_df():
    rel_risk_cols = []
    for i in range(0, 1000):
        rel_risk_cols.append('rr_' + str(i))

    if CAUSES == 'COD':
        demo_cols = ['location_id', 'cause']
        df = pd.concat([
            pd.read_csv(META_DICT['_inj'][0]).query("cause == '_inj'"),
            pd.read_csv(META_DICT['maternal_indirect'][0]).query("cause == 'maternal_indirect'")
        ], ignore_index=False)
        df = df[demo_cols + rel_risk_cols].rename(columns={'cause': 'scaling_cause', 'location_id': 'scaling_location'})
    elif CAUSES == 'VPD':
        demo_cols = ['location_id', 'cause', 'age_group_id', 'sex_id']
        lri = pd.read_csv(META_DICT['lri'][0]).assign(cause='lri')
        whooping = pd.read_csv(META_DICT['whooping'][0]).assign(cause='whooping')
        measles = pd.read_csv(META_DICT['measles'][0]).assign(cause='measles')
        lri_rename_dict = {}
        measles_rename_dict = {}
        whooping_rename_dict = {}
        for i in range(0, 1000):
            lri_rename_dict.update({META_DICT['lri'][1] + str(i): 'rr_' + str(i)})
            whooping_rename_dict.update({META_DICT['whooping'][1] + str(i): 'rr_' + str(i)})
            measles_rename_dict.update({META_DICT['measles'][1] + str(i): 'rr_' + str(i)})
        lri = lri.rename(columns=lri_rename_dict)
        whooping = whooping.rename(columns=whooping_rename_dict)
        measles = measles.rename(columns=measles_rename_dict)
        df = pd.concat([lri, whooping, measles], ignore_index=True)
        df = df[demo_cols + rel_risk_cols].rename(columns={'cause': 'scaling_cause', 'location_id': 'scaling_location'})
    return df

def apply_covid_scalar(df):
	for i in range(0, 1000):
		df['draw_' + str(i)] = df['draw_' + str(i)] * (df['rr_' + str(i)] - 1)
	return df

def validate_scaling(df, draw_cols, rel_risk_cols):
	df['mean_rel_risk'] = df[rel_risk_cols].mean(axis=1)
	df['cause_total'] = df.groupby(['location_id', 'year_id', 'cause_id']).deaths.transform(sum)
	df['desired_scaling'] = df.cause_total * (df.mean_rel_risk - 1)
	df['effective_scaling'] = df[draw_cols].mean(axis=1)
	df['total_effective_scaling'] =  df.groupby(['location_id', 'year_id', 'cause_id']).effective_scaling.transform(sum)
	df = df[['location_id', 'year_id', 'cause_id', 'desired_scaling', 'total_effective_scaling']].drop_duplicates()
	df['pct_diff'] = (df.desired_scaling - df.total_effective_scaling) / df.desired_scaling
	try:
		assert df['pct_diff'].apply(lambda x: np.allclose(x, 0, atol=2*1e-2)).all()
	except:
		loc = df.location_id.unique().tolist()[0]
		fail_val = abs(df['pct_diff']).max()
		print_log_message("Failed validation for location {} at {} pct diff...".format(loc, fail_val))

def make_square(df, demo_cols, draw_cols, age_meta, loc_id):
	data_dict = {
		'location_id': [loc_id],
		'year_id': list(range(1980, 2023)),
		'age_group_id': age_meta.age_group_id.unique().tolist(),
		'sex_id': [1, 2]
	}
	rows = itertools.product(*list(data_dict.values()))
	square_df = pd.DataFrame.from_records(rows, columns=data_dict.keys())
	for col in draw_cols:
		square_df[col] = 0

	df = df.append(square_df, ignore_index=True)
	df = df.drop_duplicates(subset=demo_cols, keep='first')
	return df


def scale_one_loc(loc_id, cause_meta=None, loc_meta=None, age_meta=None, scalar_df=None):
	cod_ages = age_meta.age_group_id.unique().tolist()
	demo_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
	draw_cols = []
	rel_risk_cols = []
	for i in range(0, 1000):
		draw_cols.append('draw_' + str(i))
		rel_risk_cols.append('rr_' + str(i))

	if CAUSES == 'COD':
		injuries = cause_meta.loc[
			(cause_meta.acause.str.startswith('inj_')) & (cause_meta.most_detailed == 1)
		].cause_id.unique().tolist()
		maternal = cause_meta.loc[cause_meta.acause == 'maternal_indirect'].cause_id.unique().tolist()
		affected_causes = maternal
	elif CAUSES == 'VPD':
		lri = cause_meta.loc[cause_meta.acause == 'lri'].cause_id.unique().tolist()
		whooping = cause_meta.loc[cause_meta.acause == 'whooping'].cause_id.unique().tolist()
		measles = cause_meta.loc[cause_meta.acause == 'measles'].cause_id.unique().tolist()
		affected_causes = lri + whooping + measles
	df = pd.concat([
		pd.read_hdf(FILEPATH),
		pd.read_hdf(FILEPATH)
		],ignore_index=True
	)
	df = df.loc[df.cause_id.isin(affected_causes)]

	if CAUSES == 'COD':
		df.loc[df.cause_id.isin(injuries), 'scaling_cause'] = '_inj'
		df.loc[df.cause_id.isin(maternal), 'scaling_cause'] = 'maternal_indirect'
	elif CAUSES == 'VPD':
		df.loc[df.cause_id.isin(lri), 'scaling_cause'] = 'lri'
		df.loc[df.cause_id.isin(whooping), 'scaling_cause'] = 'whooping'
		df.loc[df.cause_id.isin(measles), 'scaling_cause'] = 'measles'

	df = assign_scaling_factors(df, scalar_df, loc_meta, loc_id, rel_risk_cols)

	df['deaths'] = df[draw_cols].mean(axis=1)
	df = df[demo_cols + ['cause_id', 'deaths'] + draw_cols + rel_risk_cols]
	df = apply_covid_scalar(df)
	diag_df = df[demo_cols + ['cause_id', 'deaths'] + draw_cols]

	if CAUSES == 'COD':
		validate_scaling(df, draw_cols, rel_risk_cols)
	df = df[demo_cols + ['cause_id'] + draw_cols]
	report_duplicates(df, demo_cols+['cause_id'])

	for cause in df.cause_id.unique().tolist():
		out_df = df.loc[df.cause_id == cause][demo_cols + draw_cols]
		out_df = make_square(out_df, demo_cols, draw_cols, age_meta, loc_id)
		out_df.to_csv(FILEPATH)

	diag_df['scaling'] = diag_df[draw_cols].mean(axis=1)
	diag_df = diag_df.groupby(['location_id', 'cause_id', 'age_group_id', 'sex_id'], as_index=False)['deaths', 'scaling'].sum()
	return diag_df


def main():
	print_log_message("Getting the inputs...")
	loc_meta = get_current_location_hierarchy(location_set_id=35,
		location_set_version_id=CONF.get_id('location_set_version'), force_rerun=False, block_rerun=True)
	locs = loc_meta.loc[loc_meta.most_detailed == 1].location_id.unique().tolist()

	cause_meta = pd.read_csv(CAUSES_DIR)
	make_out_directories(cause_meta)
	age_meta = get_cod_ages()

	scalar_df = get_scalar_df()

	print_log_message("Getting and scaling the cc results...")
	pool = Pool(5)
	_scale_one_loc = partial(scale_one_loc, cause_meta=cause_meta, loc_meta=loc_meta, age_meta=age_meta, scalar_df=scalar_df)
	dfs = pool.map(_scale_one_loc, locs)
	pool.close()
	pool.join()

	print_log_message('Done! Writing out diag df...')
	diag_df = pd.concat(dfs, ignore_index=True)
	if CAUSES == 'COD':
		diag_df.to_csv(FILEPATH)
	elif CAUSES == 'VPD':
		diag_df.to_csv(FILEPATH)

	print_log_message('Done!')

CAUSES = 'VPD'
if __name__ == "__main__":
	assert CAUSES in ['VPD', 'COD'], 'please select VPD or COD to run for'
	main()