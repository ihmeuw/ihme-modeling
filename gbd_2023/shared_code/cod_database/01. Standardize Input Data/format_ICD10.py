
import sys
import os
from datetime import datetime
import pandas as pd
import numpy as np

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../..'))
sys.path.append(repo_dir)
from cod_prep.claude.formatting import finalize_formatting, update_nid_metadata_status
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders import (
	get_cause_map, get_current_location_hierarchy, add_code_metadata
)
from cod_prep.utils import (
	report_if_merge_fail, get_adult_age_codebook, get_infant_age_codebook, report_duplicates
)
from cod_prep.claude.configurator import Configurator

CONF = Configurator('standard')

PROJECT_ID = CONF.get_id('project')
WRITE = False
IS_ACTIVE = True
IS_MORT_ACTIVE = True

IN_DIR = "FILEPATH"

ID_COLS = [
    'nid', 'parent_nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
]
INT_COLS = [
    'nid', 'parent_nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id'
]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

SYSTEM_SOURCE = 'ICD10'


def pool_years_and_ages(df):
	"""
	Function for pooling of specific ages/years, note from stata code below

    DP 7/6/2011
    Various GBD researchers have identified a few country-years with a sample size that is so small that the time trend/age trend is very noisy
    Similar to the STP and CPV fix in BTL, we should pool the ages or years and then age split the pooled ages to get a better trend
        Bahrain 2001, Kuwait 1996, Kuwait 1998, Kuwait 2001, Montenegro 2000, Qatar 1995 - pool ages
	"""
	zero_cols = [('Deaths' + str(x)) for x in list(range(2, 26))] + ['IM_Deaths1', 'IM_Deaths2', 'IM_Deaths3', 'IM_Deaths4']
	age_pool_logic = ((df.Country == 3020) & (df.Year == 2001)) | ((df.Country == 3190) & (df.Year == 1996)) | \
		((df.Country == 3190) & (df.Year == 1998)) | ((df.Country == 3190) & (df.Year == 2001)) | \
		((df.Country == 4207) & (df.Year == 2000)) | ((df.Country == 3320) & (df.Year == 1995))
	df.loc[age_pool_logic, 'Deaths26'] = df['Deaths1']
	df.loc[age_pool_logic, zero_cols] = 0
	df.loc[age_pool_logic, 'Frmat'] = 9
	df.loc[age_pool_logic, 'IM_Frmat'] = 8

	df.loc[(df.Country == 2270) & (df.Year.isin([1997, 1998, 1999, 2000, 2001])), 'Year'] = 1999
	df.loc[(df.Country == 1310) & (df.Year.isin([2003, 2004, 2005, 2006, 2007])), 'Year'] = 2005

	return df

def read_data(release_date, parts):
	dfs = []
	for part in parts:
		try:
			df = pd.read_csv("{in_dir}/FILENAME" \
									"{date}.CSV".format(in_dir=IN_DIR, part=part, date=release_date))
		except:
			df = pd.read_csv("{in_dir}/FILENAME" \
									"{date}.TXT".format(in_dir=IN_DIR, part=part, date=release_date))
		dfs.append(df)
	df = pd.concat(dfs, ignore_index=True)

	df['List'] = df['List'].astype(str)
	df = df.loc[df.List.isin(['103', '104', '10M', 'ICD10'])]

	df = pool_years_and_ages(df)

	return df

def get_country_map(release_date):
	"""
	This reads in the country map supplied by the WHO alongside the actual data.
	We use it to identify which loc/year are new to this release and if any loc/years
	are replacement data.
	"""
	try:
		df = pd.read_excel("{in_dir}/FILENAME" \
										"{date}.XLSX".format(in_dir=IN_DIR, date=release_date)
									)
	except:
		df = pd.read_excel("{in_dir}/FILENAME" \
										"{date}.xlsx".format(in_dir=IN_DIR, date=release_date)
									)
	header_index = df.index[df.iloc[:,0] == 'Country'].tolist()[0]
	df.columns = df.iloc[header_index,:].tolist()
	df.drop(range(0, header_index+1), axis=0, inplace=True)
	df.rename(columns={'Update': 'Updates'}, inplace=True)

	df = df.loc[df.Country.notnull()]
	df['Country'] = df['Country'].astype(int)
	df['Year'] = df['Year'].astype(int)
	df['List'] = df['List'].astype(str)
	return df

def drop_locations(df, filter_to_new):

	"""
		Drop locations from the WHO data that we do not model in gbd
		Mayotte, Reunion, Rodrigues, Aruba, French Guiana, Guadeloupe, Martinique,
		Montserrat, Turks and Caicos, Anguilla as of 06/10/2019
		- Update to include Cayman Islands, Netherlands Antilles, British Virgin Islands,
		Serbia and Montenegro Former, Cabo Verde, St Pierre and Miquelon
	"""
	unmodeled_countries = [1303, 1360, 1365, 2025, 2210, 2240, 2300, 2320, 2445, 2005,
		2110, 2330, 2085, 4350, 1060, 2410
	]
	df = df.loc[~(df.Country.isin(unmodeled_countries))]

	"""
		Drop countries from the WHO data that we model, but instead use a better source
		- United States, US Virgin Islands, Puerto Rico, UK, England/Wales, ZAF, Japan, Sweden,
		Brazil, New Zealand, Saudi Arabia, Mexico, Iran, Singapore as of 06/10/2019
		- Updated to include Italy, Poland, Colombia, and Philippines 01/29/2020
		- Undrop Singapore 04/07/2021
		- Updated for general cleanup of drops, added Norway, Iraq 04/24/22
		- Undrop Sweden, no longer modeled subnationally. And Iran, will split subnational 2/12/24
	"""
	dropped_countries = [2450, 2455, 2380, 4308, 4310, 1430, 3160, 2070, 5150, 3340,
		2310, 4180, 4230, 2130, 3300, 4220
	]
	df = df.loc[~(df.Country.isin(dropped_countries))]

	"""
	Dropping some countries starting with a specific year, where we have since
	started receiving collaborator data.
	"""
	df = df.loc[~((df.Country == 4160) & (df.Year > 2016))]
	df = df.loc[~((df.Country == 2180) & (df.Year >= 2017))]
	df = df.loc[~((df.Country == 2250) & (df.Year >= 2010))]
	df = df.loc[~((df.Country == 2140) & (df.Year >= 2015))]
	df = df.loc[~((df.Country == 3350) & (df.Year >= 2017))]
	df = df.loc[~((df.Country == 2120) & (df.Year.isin([2019, 2020])))]
	df = df.loc[~((df.Country == 3260) & (df.Year.isin([2018, 2019, 2020])))]


	if not filter_to_new:
		df = df.loc[~(df.Country.isin([4070, 3170, 3260, 4207, 1520]))]

	df = df.loc[~((df.Country == 4038) & (df.Year == 2015))]

	df = df.loc[df.List != 'UE1']

	if datetime.now() > datetime(2024, 6, 1, 12, 0, 0, 0):
		raise AssertionError('Double check if Dominica 2018-2020 has been fixed.')
	else:
		df = df.loc[~((df.Country == 2160) & (df.Year.isin([2018, 2019, 2020])))]

	df = df.loc[~((df.Country == 2360) & (df.Year.between(2004, 2017)))]

	df = df.loc[~((df.Country == 3365) & (df.Year.isin([2009, 2010])))]
	df = df.loc[~((df.Country == 4184) & (df.Year == 2017))]
	df = df.loc[~((df.Country == 3283) & (df.Year.isin([2017, 2018])))]

	df = df.loc[~((df.Country == 4085) & (df.Year == 2020))]
	df = df.loc[~((df.Country == 4280) & (df.Year.between(2018, 2020)))]
	df = df.loc[~((df.Country == 4020) & (df.Year.between(2017, 2018)))]
	df = df.loc[~((df.Country == 4084) & (df.Year == 2020))]
	df = df.loc[~((df.Country == 3210) & (df.Year == 2020) & (df.SubDiv == 'B10'))]

	return df

def subset_location_years(df, country_map, filter_to_new=True):
	country_map = country_map[['Country', 'Year', 'List', 'Updates']]
	df = df.merge(country_map, on=['Country', 'Year', 'List'], how='left', indicator=True)
	if filter_to_new:
		df = df.loc[df.Updates.isin(['new', 'revised'])]
	else:
		df = df.loc[df._merge == 'both']
	df.drop(['Updates', '_merge'], axis=1, inplace=True)

	df = drop_locations(df, filter_to_new)

	return df

def get_gbd_locations(df, country_map, loc_meta):
	country_map = country_map[['Country', 'name']].drop_duplicates()
	assert len(country_map) == len(set(country_map.Country))

	df = df.merge(country_map, on='Country', how='left')
	report_if_merge_fail(df, 'name', 'Country')
	df.rename(columns={'name':'location_name'}, inplace=True)

	df['location_name'] = df['location_name'].replace(
							{'Saint Vincent and Grenadines': 'Saint Vincent and the Grenadines',
							'Hong Kong SAR': 'Hong Kong Special Administrative Region of China',
							'United Kingdom, Northern Ireland': 'Northern Ireland',
							'United Kingdom, Scotland': 'Scotland',
							'Occupied Palestinian Territory': 'Palestine',
							'TFYR Macedonia': 'North Macedonia',
							'Bolivia': 'Bolivia (Plurinational State of)',
							'Venezuela': 'Venezuela (Bolivarian Republic of)',
							'Czech Republic': 'Czechia',
							'Libyan Arab Jamahiriya': 'Libya',
							'Turkey': 'Türkiye'
							}
						)

	loc_meta = loc_meta.loc[loc_meta.location_id != 533]
	df = df.merge(loc_meta[['location_name', 'location_id']], on='location_name',
					how='left'
				)
	report_if_merge_fail(df, 'location_id', 'location_name')
	df.drop(['Country', 'location_name'], axis=1, inplace=True)
	return df

def adjust_WHO_ages(df):
	"""
	Very aware this is the messiest part of the script, and it's not pretty in
	Stata either. It's just a bunch of manual remaps of the raw WHO ages so they
	line up with their given frmats.
	"""

	df.rename(columns={'IM_Deaths1':'Deaths91', 'IM_Deaths2':'Deaths92',
						'IM_Deaths3': 'Deaths93', 'IM_Deaths4': 'Deaths94'},
						inplace=True
			)

	df['Deaths91_94'] = df[['Deaths91', 'Deaths92', 'Deaths93', 'Deaths94']].sum(axis=1)
	df['Deaths91_94_tot'] = df.groupby(['location_id', 'Year'],
								).Deaths91_94.transform(sum)
	df['Deaths2_tot'] = df.groupby(['location_id', 'Year'],
								).Deaths2.transform(sum)
	df.loc[(df.Deaths91_94_tot == 0) &
			(df.Deaths2 != 0),
			'IM_Frmat'
		] = 8
	df.loc[(df.Deaths91_94_tot) > 0, 'Deaths2'] = 0

	df[['Deaths4', 'Deaths5', 'Deaths6']] = df[['Deaths4', 'Deaths5', 'Deaths6']].fillna(0)
	df['Deaths4_6'] = df[['Deaths4', 'Deaths5', 'Deaths6']].sum(axis=1)
	df['Deaths4_6_tot'] = df.groupby(['location_id', 'Year'],
							).Deaths4_6.transform(sum)
	df['Deaths-3'] = 0
	df.loc[(df.Deaths3 > 0) & (df.Deaths4_6_tot == 0),
		'Deaths-3'
	] = df['Deaths3']
	df.loc[(df.Deaths3 > 0) & (df.Deaths4_6_tot == 0),
		'Deaths3'
	] = 0

	df['Deaths-3_tot'] = df.groupby(['location_id', 'Year'],
							)['Deaths-3'].transform(sum)
	df.loc[(df['Frmat'] == 1) & (df['Deaths4_6_tot'] == 0) & (df['Deaths-3_tot'] > 0), 
		'Frmat'] = 2

	df['Deaths91'] = df[['Deaths91', 'Deaths92']].sum(axis=1)
	df['Deaths92'] = 0
	df.loc[df.IM_Frmat == 1, 'IM_Frmat'] = 2

	df.loc[(df.Deaths91 > 0) &
			(df.IM_Frmat == 8),
			'Deaths2'
		] = 0
	df.loc[(df.Deaths2 > 0) &
			(df.IM_Frmat == 8),
			'Deaths91'
		] = df['Deaths2']

	df['deaths23sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							).Deaths23.transform(sum)
	df['deaths24sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							).Deaths24.transform(sum)
	df['deaths25sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							).Deaths25.transform(sum)

	df.loc[(df.Frmat.isin([1, 2])) &
		((df.deaths24sum > 0) | (df.deaths25sum > 0)),
		'Frmat'
	] = 0

	df.loc[(df.Frmat == 0) & (df.deaths25sum == 0) & (df.location_id != 28) &
		((df.deaths23sum > 0) | (df.deaths24sum > 0)),
		'flag'
	] = 1
	df.loc[df.flag == 1, 'Deaths23'] = df[['Deaths23', 'Deaths24', 'Deaths25']].sum(axis=1)
	df.loc[df.flag == 1, ['Deaths24', 'Deaths25']] = 0
	df.loc[df.flag == 1, 'Frmat'] = 1

	df.drop(['deaths23sum', 'deaths24sum', 'deaths25sum', 'flag'],
		axis=1, inplace=True
	)

	unknowns = df.loc[df.Deaths26 > 0]
	non_unknowns = ["Deaths" + str(x) for x in range(3, 26)] + \
					['Deaths91', 'Deaths92', 'Deaths93', 'Deaths94']
	unknowns[non_unknowns] = 0
	df['Deaths26'] = 0
	df = df.append(unknowns, ignore_index=True)
	df.loc[df.Deaths26 > 0, 'Frmat'] = 9
	df.loc[df.Deaths26 > 0, 'IM_Frmat'] = 8

	death_cols = ["Deaths" + str(x) for x in range(3, 27)] + \
					["Deaths91", "Deaths92", "Deaths93", "Deaths94"]
	df['Deaths1'] = df[death_cols].sum(axis=1)

	assert (df[['Deaths92']].sum(axis=1) == 0).all()
	df.drop(['Deaths92'], axis=1, inplace=True)

	return df

def fix_rows_with_zero_deaths(df):
	"""
	Typically we will keep all zeros, but due to the special handling of WHO data,
	several rows with zero deaths are created with nonsense who_age/frmat combinations.
	We want to drop these, but keep any real zeros in the data. We need to do this to make
	merging age group ids successful later on. May need to continue adding exceptions to
	this with each new WHO release.
	"""
	df = df.loc[~((df.Frmat == 9) & (df.IM_Frmat == 8) & (df.who_age != 26) & (df.deaths == 0))]

	df = df.loc[~((df.IM_Frmat == 8) & (df.who_age.isin([93, 94])) & (df.deaths == 0))]

	df = df.loc[~((df.who_age == 26) & (df.Frmat != 9) & (df.deaths == 0))]

	df = df.loc[~((df.who_age.isin([24, 25])) & (df.Frmat.isin([1, 2])) & (df.deaths == 0))]

	df = df.loc[~((df.who_age.isin([22, 23, 24, 25])) & (df.Frmat == 4) & (df.deaths == 0))]

	df = df.loc[~((df.who_age.isin([22, 23, 24, 25])) & (df.Frmat == 3) & (df.deaths == 0))]

	df = df.loc[~((df.who_age == -3) & (df.Frmat.isin([0, 1, 3])) & (df.deaths == 0))]

	df = df.loc[~((df.who_age.isin([3, 4, 5, 6])) & (df.Frmat.isin([2, 4, 9])) &
		(df.deaths == 0))]
	return df


def melt_df(df):
	var_cols = ['Cause', 'Frmat', 'IM_Frmat', 'Sex', 'Year', 'location_id']
	death_cols = ["Deaths" + str(x) for x in (list(range(1, 27)) + [-3])] + \
					["Deaths91", "Deaths92", "Deaths93", "Deaths94"]
	for col in ['Deaths92']:
		death_cols.remove(col)
	df = df[var_cols+death_cols]

	df = df.melt(id_vars=var_cols, value_vars=death_cols,
					var_name='who_age', value_name='deaths'
				)
	df['deaths'] = df['deaths'].fillna(0)
	df = df.loc[df.deaths > 0]
	return df

def get_age_group_ids(df):
	df['who_age'] = df['who_age'].str.replace('Deaths', '')
	df['who_age'] = df['who_age'].astype(int)

	df = df.loc[~(df.who_age.isin([1, 2]))]
	start_len = len(df)

	adult_cb = get_adult_age_codebook()
	infant_cb = get_infant_age_codebook()
	adult_cb.rename(columns={"frmat":"Frmat", 'cod_age':'who_age'}, inplace=True)
	infant_cb.rename(columns={'im_frmat':'IM_Frmat', 'cod_age':'who_age'}, inplace=True)
	adult_cb = adult_cb[['Frmat', 'who_age', 'age_group_id']]
	infant_cb = infant_cb[['IM_Frmat', 'who_age', 'age_group_id']]

	adult_cb.loc[(adult_cb['Frmat'] == 2) & (adult_cb['who_age'] == 22),
		'age_group_id'] = 30
	adult_cb = adult_cb.append({'Frmat': 2 , 'who_age': 23, 'age_group_id': 160},
		ignore_index=True)

	adult_ages = list(range(3, 27)) + [-3]
	infant_df = df.loc[df.who_age.isin([91, 92, 93, 94])]
	adult_df = df.loc[df.who_age.isin(adult_ages)]

	infant_df = infant_df.merge(infant_cb, on = ['IM_Frmat', 'who_age'], how='left')
	adult_df = adult_df.merge(adult_cb, on=['Frmat', 'who_age'], how='left')

	adult_df.loc[(adult_df.Frmat == 9) & (adult_df.who_age == 26), 'age_group_id'] = 283

	adult_df.loc[adult_df.who_age.isin([-3]), 'age_group_id'] = 5
	adult_df.loc[adult_df.who_age.isin([3]), 'age_group_id'] = 238
	adult_df.loc[adult_df.who_age.isin([4, 5, 6]), 'age_group_id'] = 34

	df = pd.concat([infant_df, adult_df], ignore_index=True)
	assert len(df) == start_len, "You added/dropped rows in age mapping"
	df = fix_rows_with_zero_deaths(df)

	report_if_merge_fail(df, 'age_group_id', ['Frmat', 'IM_Frmat', 'who_age'])
	assert df.age_group_id.notnull().all()
	return df

def apply_cause_adjustments(df):
	df['Cause'] = df['Cause'].astype(str)
	df = df.loc[df.Cause != 'AAA']

	df['Cause'] = df['Cause'].str.replace(".", "")
	df['Cause'] = df['Cause'].str.replace(",", "")
	df['Cause'] = df['Cause'].str.replace(" ", "")

	df.loc[(df.Cause.str.len() >= 4) &
		(df.Cause.str[-1] == 'X'),
		'Cause'
	] = df['Cause'].str[:-1]
	
	remap_codes = ["HEMO", "PARO", "SEPS", "ENFE", "INSU", "PARO", "TRAU"]
	df.loc[df.Cause.isin(remap_codes), 'Cause'] = 'R99'

	assert len(df.loc[df.Cause.str.startswith(('S', 'T'))]) == 0, "There are" \
				"S and T codes in the data, consult with NAME about how to handle."

	irq_inj_mech_causes = ['W32', 'W33', 'W34', 'W40', 'W42', 'W43', 'W49']
	df.loc[(df.location_id == 143) &
		(df.Cause.isin(irq_inj_mech_causes)),
		'Cause'
	] = 'Y36'

	bol_causes = ["Y349", "W849", "X598", "X599", "X596", "X590", "W789", "Y209"]
	df.loc[(df.location_id == 121) &
		(df.Cause.isin(bol_causes)),
		'Cause'
	] = 'Y34'
	return df


def map_code_id(df, cause_map):
	df = apply_cause_adjustments(df)

	df.rename(columns={'Cause':'value'}, inplace=True)
	cause_map = cause_map[['value', 'code_id']]
	cause_map['value'] = cause_map['value'].str.replace(".", "")
	df = df.merge(cause_map, on='value', how='left')

	if len(df.loc[df.code_id.isnull()]) > 0:
		df.loc[df.code_id.isnull(), 'value'] = df['value'].str[:3]
		df = df.merge(cause_map, on='value', how='left')
		df.loc[df.code_id_x.isnull(), 'code_id_x'] = df['code_id_y']
		df.drop('code_id_y', axis=1, inplace=True)
		df.rename(columns={'code_id_x':'code_id'}, inplace=True)
	report_if_merge_fail(df, 'code_id', 'value')
	return df

def map_nids(df, release_date):
	nid_map_path = "{repo_dir}FILEPATH" \
						"{date}.csv".format(repo_dir=repo_dir, date=release_date)
	assert os.path.exists(nid_map_path), "You need to create an nid map for this WHO release" \
		"and save it at: FILEPAT"

	nid_map = pd.read_csv(nid_map_path)
	nid_map = nid_map[['parent_nid', 'nid', 'location_id', 'year_id']]
	report_duplicates(nid_map, ['location_id', 'year_id'])

	df = df.merge(nid_map, on=['year_id', 'location_id'], how='right', indicator=True)
	df = df.loc[df._merge == 'both']
	report_if_merge_fail(df, 'nid', ['year_id', 'location_id'])
	df = df.loc[df.parent_nid.notnull()]
	return df

def cleanup(df):
	df.rename(columns={'Sex':'sex_id', 'Year':'year_id'}, inplace=True)
	df.loc[df.sex_id.isnull(), 'sex_id'] = 9
	assert df.sex_id.isin([1, 2, 9]).all()

	df['representative_id'] = 1
	df['code_system_id'] = 1
	df['site'] = ""
	df['data_type_id'] = 9
	return df

def split_iran(df):
	"""Split national Iran into subnats using old VR."""
	start_deaths = df.deaths.sum()
	irn = df.loc[df.location_id == 142]
	df = df.loc[df.location_id != 142]

	vr = get_claude_data(
		'disaggregation', iso3='IRN', is_active=True, data_type_id=9, year_id=[2014, 2015, 2016]
	)
	vr = vr.loc[vr.deaths > 0]

	vr = vr.groupby(['location_id', 'age_group_id', 'sex_id', 'cause_id'], as_index=False).deaths.sum()
	vr_fback = vr.groupby(['location_id', 'age_group_id', 'sex_id'], as_index=False).deaths.sum()
	vr_fback2 = vr.groupby('location_id', as_index=False).deaths.sum()

	vr['natl_total'] = vr.groupby(['age_group_id', 'sex_id', 'cause_id']).deaths.transform(sum)
	vr['frac'] = vr.deaths / vr.natl_total
	vr = vr[['location_id', 'age_group_id', 'sex_id', 'cause_id', 'frac']]
	
	vr_fback['natl_total'] = vr_fback.groupby(['age_group_id', 'sex_id']).deaths.transform(sum)
	vr_fback['frac'] = vr_fback.deaths / vr_fback.natl_total
	vr_fback = vr_fback[['location_id', 'age_group_id', 'sex_id', 'frac']]

	vr_fback2['natl_total'] = vr_fback2.deaths.sum()
	vr_fback2['frac'] = vr_fback2.deaths / vr_fback2.natl_total
	vr_fback2['merge_key'] = 1
	vr_fback2 = vr_fback2[['merge_key', 'location_id', 'frac']]

	irn = add_code_metadata(irn, 'cause_id', code_system_id=1)
	irn = irn.drop('location_id', axis=1)
	irn = irn.merge(vr, on=['age_group_id', 'sex_id', 'cause_id'], how='left')

	irn1 = irn.loc[irn.location_id.isnull()]
	irn = irn.loc[irn.location_id.notnull()]
	irn1 = irn1.drop(['location_id', 'frac'], axis=1)
	irn1 = irn1.merge(vr_fback, on=['age_group_id', 'sex_id'], how='left')

	irn2 = irn1.loc[irn1.location_id.isnull()]
	irn1 = irn1.loc[irn1.location_id.notnull()]
	irn2 = irn2.drop(['location_id', 'frac'], axis=1)
	irn2['merge_key'] = 1
	irn2 = irn2.merge(vr_fback2, on='merge_key', how='left')
	irn2 = irn2.drop('merge_key', axis=1)

	irn = pd.concat([irn, irn1, irn2])
	irn['deaths'] = irn.deaths * irn.frac
	irn = irn.drop(['frac', 'cause_id'], axis=1)
	df = pd.concat([df, irn])

	assert np.allclose(df.deaths.sum(), start_deaths), "Bad Iran splitting..."

	return df

def apply_special_adjustments(df):
	df.loc[(df.location_id == 155) & (df.year_id < 2010), 'representative_id'] = 0
	df.loc[df.location_id == 149, 'representative_id'] = 0

	df.loc[
		(df.location_id == 106) &
		(df.year_id == 2003) & 
		(df.age_group_id.isin([19, 20])),
		'age_group_id'
	] = 26

	df.loc[
		(df['nid'].isin([521280, 521307, 521213])) &
		(df['age_group_id'].isin([34, 238])), 
		'age_group_id'
	] = 5

	df = split_iran(df)

	return df


def format_source(release_date):
	PARTS = list(range(1, 6))

	df = read_data(release_date, PARTS)
	country_map = get_country_map(release_date)

	df = subset_location_years(df, country_map, filter_to_new=True)

	loc_meta = get_current_location_hierarchy(location_set_id=CONF.get_id('location_set'),
							location_set_version_id=CONF.get_id('location_set_version'),
							force_rerun=False, block_rerun=True
					)

	df = get_gbd_locations(df, country_map, loc_meta)

	df = adjust_WHO_ages(df)

	df = melt_df(df)

	df = get_age_group_ids(df)

	cause_map = get_cause_map(1, force_rerun=False, block_rerun=True)
	df = map_code_id(df, cause_map)

	df = cleanup(df)

	df = map_nids(df, release_date)

	df = apply_special_adjustments(df)

	df = df[FINAL_FORMATTED_COLS]
	for col in INT_COLS:
		df[col] = df[col].astype(int)
	assert df.notnull().values.all()
	df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

	locals_present = finalize_formatting(df, SYSTEM_SOURCE, PROJECT_ID, write=WRITE, check_ages=False)
	nid_meta_df = locals_present['nid_meta_df']

	if WRITE:
	    nid_extracts = nid_meta_df[
	        ['nid', 'extract_type_id']
	    ].drop_duplicates().to_records(index=False)
	    for nid, extract_type_id in nid_extracts:
	        nid = int(nid)
	        extract_type_id = int(extract_type_id)
	        update_nid_metadata_status(PROJECT_ID, nid, extract_type_id, is_active=IS_ACTIVE,
	                                   is_mort_active=IS_MORT_ACTIVE)

if __name__ == "__main__":
	release_date = str(sys.argv[1])
	format_source(release_date)