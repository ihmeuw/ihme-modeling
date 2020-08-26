import sys
import os
import pandas as pd
import numpy as np

from cod_prep.claude.formatting import finalize_formatting, update_nid_metadata_status
from cod_prep.downloaders import (
	get_cause_map, get_current_location_hierarchy
)
from cod_prep.utils import (
	report_if_merge_fail, get_adult_age_codebook, get_infant_age_codebook
)
from cod_prep.claude.configurator import Configurator

CONF = Configurator('standard')

# finalize formatting / nid_metadata globals
WRITE = False
IS_ACTIVE = True
IS_MORT_ACTIVE = True

# base location of the incoming data
IN_DIR = "FILEPATH"

# columns for our output dataframe
ID_COLS = [
    'nid', 'parent_nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
	]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

SYSTEM_SOURCE = 'ICD10'

#############################################
#############################################

def read_data(release_date):
	# read the incoming data for the respective release date
	# WHO releases typically come in two parts, so append them together
	try:
		part1 = pd.read_csv("FILEPATH")
		part2 = pd.read_csv("FILEPATH")
	except:
		part1 = pd.read_csv("FILEPATH")
		part2 = pd.read_csv("FILEPATH")
							
	df = pd.concat([part1, part2], ignore_index=True)

	# keep only ICD10 detail data
	df['List'] = df['List'].astype(str)
	df = df.loc[df.List.isin(['103', '104', '10M', 'ICD10'])]

	return df

def get_country_map(release_date):
	df = pd.read_excel("FILEPATH")
	# there's a header in the map with varying length between releases
	# maybe there is a better way to get rid of it than this?
	header_index = df.index[df.iloc[:,0] == 'Country'].tolist()[0]
	df.columns = df.iloc[header_index,:].tolist()
	df.drop(range(0, header_index+1), axis=0, inplace=True)

	# some dtype adjustments so later merges are successful
	df = df.loc[df.Country.notnull()]
	df['Country'] = df['Country'].astype(int)
	df['Year'] = df['Year'].astype(int)
	df['List'] = df['List'].astype(str)
	return df

def drop_locations(df):
	# drop locations from the WHO data that we do not model in gbd
	# Mayotte, Reunion, Rodrigues, Aruba, French Guiana, Guadeloupe, Martinique,
	# Montserrat, Turks and Caicos, Anguilla as of 06/10/2019
	unmodeled_countries = [1303, 1360, 1365, 2025, 2210, 2240, 2300, 2320, 2445, 2005]
	df = df.loc[~(df.Country.isin(unmodeled_countries))]

	# drop countries from the WHO data that we model, but instead use a better source
	# United States, US Virgin Islands, Puerto Rico, UK, England/Wales, ZAF, Japan, Sweden,
	# Brazil, New Zealand, Saudi Arabia, Mexico, Iran, Singapore as of 06/10/2019
	dropped_countries = [2450, 2455, 2380, 4308, 4310, 1430, 3160, 4290, 2070, 5150, 3340,
							2310, 3130, 3350
						]
	df = df.loc[~(df.Country.isin(dropped_countries))]

	# dropping Croation 2015 data
	df = df.loc[~((df.Country == 4038) & (df.Year == 2015))]

	# drop special Portugal ICD10 (should be covered when reading in the data, but just incase)
	df = df.loc[df.List != 'UE1']

	# GBD 2019 Specific drops for data that overlap with other sources
	# Iceland 2017
	df = df.loc[~((df.Country == 4160) & (df.Year == 2017))]
	# Norway 2016
	df = df.loc[~((df.Country == 4220) & (df.Year == 2016))]
	# Iraq 2015/2016
	df = df.loc[~((df.Country == 3140) & (df.Year.isin([2015, 2016])))]
	# Poland 2016
	df = df.loc[~((df.Country == 4230) & (df.Year == 2016))]

	return df

def subset_location_years(df, country_map):
	country_map = country_map[['Country', 'Year', 'List', 'Updates']]
	# merge the new location/year map onto the data and subset accordingly
	df = df.merge(country_map, on=['Country', 'Year', 'List'], how='left')
	df = df.loc[df.Updates.isin(['new', 'revised'])]
	df.drop('Updates', axis=1, inplace=True)

	# dropping location/years we don't want - adapted from Stata code
	df = drop_locations(df)

	return df

def get_gbd_locations(df, country_map, loc_meta):
	# subset country map to just WHO codes and names and make sure no duplicates
	country_map = country_map[['Country', 'name']].drop_duplicates()
	assert len(country_map) == len(set(country_map.Country))

	# merge country names onto the data
	df = df.merge(country_map, on='Country', how='left')
	report_if_merge_fail(df, 'name', 'Country')
	df.rename(columns={'name':'location_name'}, inplace=True)

	# need to adjust some of the WHO location names to match GBD locations
	# will probably need to update as we get newer releases
	df['location_name'] = df['location_name'].replace(
							{'Bahamas': 'The Bahamas',
							'Saint Vincent and Grenadines': 'Saint Vincent and the Grenadines',
							'Brunei Darussalam': 'Brunei',
							'Republic of Korea': 'South Korea',
							'Hong Kong SAR': 'Hong Kong Special Administrative Region of China',
							'Republic of Moldova': 'Moldova',
							'United Kingdom, Northern Ireland': 'Northern Ireland',
							'United Kingdom, Scotland': 'Scotland',
							'Occupied Palestinian Territory': 'Palestine'}
						)

	# now merge gbd location id on with the location_name
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
	# collapse Deaths3-Deaths6 into Deaths3, this is ages 1-4
	df['Deaths3'] = df[['Deaths3', 'Deaths4', 'Deaths5', 'Deaths6']].sum(axis=1)
	df[['Deaths4', 'Deaths5', 'Deaths6']] = 0

	# rename the im_deaths to Deaths91-94
	df.rename(columns={'IM_Deaths1':'Deaths91', 'IM_Deaths2':'Deaths92',
						'IM_Deaths3': 'Deaths93', 'IM_Deaths4': 'Deaths94'},
						inplace=True
			)

	# for countries with no IM deaths, set im_frmat to 8
	df['Deaths91_94'] = df[['Deaths91', 'Deaths92', 'Deaths93', 'Deaths94']].sum(axis=1)
	df['Deaths91_94_tot'] = df.groupby(['location_id', 'Year'],
								as_index=False).Deaths91_94.transform(sum)
	df['Deaths2_tot'] = df.groupby(['location_id', 'Year'],
								as_index=False).Deaths2.transform(sum)
	df.loc[(df.Deaths91_94_tot == 0) &
			(df.Deaths2 != 0),
			'IM_Frmat'] = 8
	# now zero out deaths2 if there are deaths in 91-94
	df.loc[(df.Deaths91_94_tot) > 0, 'Deaths2'] = 0

	# collapse down deaths91 and deaths92 into deaths91
	df['Deaths91'] = df[['Deaths91', 'Deaths92']].sum(axis=1)
	df['Deaths92'] = 0
	df.loc[df.IM_Frmat == 1, 'IM_Frmat'] = 2

	# Where im_frmat is 8, move deaths 2 into deaths 91
	df.loc[(df.Deaths91 > 0) &
			(df.IM_Frmat == 8),
			'Deaths2'] = 0
	df.loc[(df.Deaths2 > 0) &
			(df.IM_Frmat == 8),
			'Deaths91'] = df['Deaths2']

	# many cases where WHO incorrectly assigns frmat with respect to terminal ages
	# see lines 485-498 of the Stata formatting script
	df['deaths23sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							as_index=False).Deaths23.transform(sum)
	df['deaths24sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							as_index=False).Deaths24.transform(sum)
	df['deaths25sum'] = df.groupby(['location_id', 'Year', 'Frmat'],
							as_index=False).Deaths25.transform(sum)

	df.loc[(df.Frmat == 2) & (df.deaths23sum > 0) & (df.deaths24sum == 0) &
		(df.deaths25sum == 0), 'Frmat'] = 1

	df.loc[(df.Frmat.isin([1, 2])) &
		((df.deaths24sum > 0) | (df.deaths25sum > 0)),
		'Frmat'] = 0

	# Sometimes WHO reports frmat 0 when it's actually 85+, collapse into Deaths23, zero 24 and 25
	df.loc[(df.Frmat == 0) & (df.deaths25sum == 0) &
		((df.deaths23sum > 0) | (df.deaths24sum > 0)),
		'flag'] = 1
	df.loc[df.flag == 1, 'Deaths23'] = df[['Deaths23', 'Deaths24', 'Deaths25']].sum(axis=1)
	df.loc[df.flag == 1, ['Deaths24', 'Deaths25']] = 0
	df.loc[df.flag == 1, 'Frmat'] = 1

	df.drop(['deaths23sum', 'deaths24sum', 'deaths25sum', 'flag'], axis=1, inplace=True)

	# seperate out the unknown deaths into their own rows and adjust frmat/im_frmat
	unknowns = df.loc[df.Deaths26 > 0]
	non_unknowns = ["Deaths" + str(x) for x in range(3, 26)] + \
					['Deaths91', 'Deaths92', 'Deaths93', 'Deaths94']
	unknowns[non_unknowns] = 0
	df['Deaths26'] = 0
	df = df.append(unknowns, ignore_index=True)
	df.loc[df.Deaths26 > 0, 'Frmat'] = 9
	df.loc[df.Deaths26 > 0, 'IM_Frmat'] = 8

	# now make an accurate Deaths1 Variable
	death_cols = ["Deaths" + str(x) for x in range(3, 27)] + \
					["Deaths91", "Deaths92", "Deaths93", "Deaths94"]
	df['Deaths1'] = df[death_cols].sum(axis=1)

	# drop the columns we've collapsed out
	assert (df[['Deaths4', 'Deaths5', 'Deaths6', 'Deaths92']].sum(axis=1) == 0).all()
	df.drop(['Deaths4', 'Deaths5', 'Deaths6', 'Deaths92'], axis=1, inplace=True)

	return df

def fix_rows_with_zero_deaths(df):
	"""
	Typically we will keep all zeros, but due to the special handling of WHO data,
	several rows with zero deaths are created with nonsense who_age/frmat combinations.
	We want to drop these, but keep any real zeros in the data. We need to do this to make
	merging age group ids successful later on. May need to continue adding exceptions to
	this with each new WHO release.
	"""
	# rows that have frmat 9 are unknown, so any zero that is not who_age 26 can be dropped
	df = df.loc[~((df.Frmat == 9) & (df.IM_Frmat == 8) & (df.who_age != 26) & (df.deaths == 0))]

	# For IM_Frmat 8, who_ages 93 and 94 are meaningless, we collapsed all these into 91 earlier
	df = df.loc[~((df.IM_Frmat == 8) & (df.who_age.isin([93, 94])) & (df.deaths == 0))]

	# having who_age 26 (unknown), and any frmat besides 9 is nonsense
	df = df.loc[~((df.who_age == 26) & (df.Frmat != 9) & (df.deaths == 0))]

	# who ages 24/25 mean nothing for frmat 1, this fmrat maxes at 23 (85+)
	df = df.loc[~((df.who_age.isin([24, 25])) & (df.Frmat == 1) & (df.deaths == 0))]

	# who_ages 22-25 mean nothing for frmat 4, this frmat maxes at 21 (75+)
	df = df.loc[~((df.who_age.isin([22, 23, 24, 25])) & (df.Frmat == 4) & (df.deaths == 0))]
	return df



def melt_df(df):
	var_cols = ['Cause', 'Frmat', 'IM_Frmat', 'Sex', 'Year', 'location_id']
	death_cols = ["Deaths" + str(x) for x in range(1, 27)] + \
					["Deaths91", "Deaths92", "Deaths93", "Deaths94"]
	for col in ['Deaths4', 'Deaths5', 'Deaths6', 'Deaths92']:
		death_cols.remove(col)
	df = df[var_cols+death_cols]

	df = df.melt(id_vars=var_cols, value_vars=death_cols,
					var_name='who_age', value_name='deaths'
				)
	df['deaths'] = df['deaths'].fillna(0)
	df = df.loc[df.deaths >= 0]
	return df

def get_age_group_ids(df):
	# clean the age column
	df['who_age'] = df['who_age'].str.replace('Deaths', '')
	df['who_age'] = df['who_age'].astype(int)

	# can drop Deaths1 and Deaths2, they're just subtotals
	df = df.loc[~(df.who_age.isin([1, 2]))]
	start_len = len(df)

	# load codebooks that are used to map age group id to who ages
	adult_cb = get_adult_age_codebook()
	infant_cb = get_infant_age_codebook()
	adult_cb.rename(columns={"frmat":"Frmat", 'cod_age':'who_age'}, inplace=True)
	infant_cb.rename(columns={'im_frmat':'IM_Frmat', 'cod_age':'who_age'}, inplace=True)
	adult_cb = adult_cb[['Frmat', 'who_age', 'age_group_id']]
	infant_cb = infant_cb[['IM_Frmat', 'who_age', 'age_group_id']]

	# subset df to infants and adults
	infant_df = df.loc[df.who_age.isin([91, 92, 93, 94])]
	adult_df = df.loc[df.who_age.isin(range(3, 27))]

	# merge age group_ids on with respective codebooks
	infant_df = infant_df.merge(infant_cb, on = ['IM_Frmat', 'who_age'], how='left')
	adult_df = adult_df.merge(adult_cb, on=['Frmat', 'who_age'], how='left')

	# handle the unknown ages
	adult_df.loc[(adult_df.Frmat == 9) & (adult_df.who_age == 26), 'age_group_id'] = 283

	df = pd.concat([infant_df, adult_df], ignore_index=True)
	# make sure we didn't add/drop any rows in this process
	assert len(df) == start_len, "You added/dropped rows in age mapping"
	# first we need to make adjustments to rows with zero deaths
	df = fix_rows_with_zero_deaths(df)

	report_if_merge_fail(df, 'age_group_id', ['Frmat', 'IM_Frmat', 'who_age'])
	assert df.age_group_id.notnull().all()
	return df

def apply_cause_adjustments(df):
	df['Cause'] = df['Cause'].astype(str)
	# drop the all cause variable
	df = df.loc[df.Cause != 'AAA']

	# strip the decimal, comma, or space
	df['Cause'] = df['Cause'].str.replace(".", "")
	df['Cause'] = df['Cause'].str.replace(",", "")
	df['Cause'] = df['Cause'].str.replace(" ", "")

	# For codes that end in an X, remove the X and treat as one digit shorter
	df.loc[(df.Cause.str.len() >= 4) &
		(df.Cause.str[-1] == 'X'),
		'Cause'] = df['Cause'].str[:-1]
	
	# Remap some special codes to R99
	remap_codes = ["HEMO", "PARO", "SEPS", "ENFE", "INSU", "PARO", "TRAU"]
	df.loc[df.Cause.isin(remap_codes), 'Cause'] = 'R99'

	# Ensure there are no S and T codes in the data
	assert len(df.loc[df.Cause.str.startswith(('S', 'T'))]) == 0, "There are" \
				"S and T codes in the data."

	# Various fixes for Iran injury codes
	irn_causes = ['W32', 'W33', 'W34', 'W40', 'W42', 'W43', 'W49']
	df.loc[(df.location_id == 142) &
		(df.Cause.isin(irn_causes)),
		'Cause'] = 'Y36'

	# Various fixes for foreign body codes in Bolivia
	bol_causes = ["Y349", "W849", "X598", "X599", "X596", "X590", "W789", "Y209"]
	df.loc[(df.location_id == 121) &
		(df.Cause.isin(bol_causes)),
		'Cause'] = 'Y34'
	return df


def map_code_id(df, cause_map):
	# apply some cause adjustments first, these are adjustments carried over
	# from the stata code
	df = apply_cause_adjustments(df)

	# merge on code ids, will try at 4 and then 3 digit codes
	df.rename(columns={'Cause':'value'}, inplace=True)
	cause_map = cause_map[['value', 'code_id']]
	cause_map['value'] = cause_map['value'].str.replace(".", "")
	df = df.merge(cause_map, on='value', how='left')

	# retry at 3 digits if it failed
	if len(df.loc[df.code_id.isnull()]) > 0:
		df.loc[df.code_id.isnull(), 'value'] = df['value'].str[:3]
		df = df.merge(cause_map, on='value', how='left')
		df.loc[df.code_id_x.isnull(), 'code_id_x'] = df['code_id_y']
		df.drop('code_id_y', axis=1, inplace=True)
		df.rename(columns={'code_id_x':'code_id'}, inplace=True)
	report_if_merge_fail(df, 'code_id', 'value')
	return df

def map_nids(df, release_date):
	# read the custom nid file for this release data, controls which loc/years are
	# going to be used.
	# Merge right on line 440 since the goal is to format those
	# targeted location/years in the custom nid_loc_year_map
	nid_map_path = "FILEPATH"
	assert os.path.exists(nid_map_path), "You need to create an nid map for this WHO release" \
		"and save it at: FILEPATH"

	nid_map = pd.read_csv(nid_map_path)
	nid_map = nid_map[['parent_nid', 'nid', 'location_id', 'year_id']]

	# merge nid information on
	df = df.merge(nid_map, on=['year_id', 'location_id'], how='right')
	merge_cols = ['year_id', 'location_id']
	report_if_merge_fail(df, 'nid', merge_cols)
	report_if_merge_fail(df, 'parent_nid', merge_cols)
	return df

def cleanup(df):
	# adding some manual columns for finalize formatting
	# also some final renaming + checks
	df.rename(columns={'Sex':'sex_id', 'Year':'year_id'}, inplace=True)
	assert df.sex_id.isin([1, 2, 9]).all()

	df['representative_id'] = 1
	df['code_system_id'] = 1
	df['site'] = ""
	df['data_type_id'] = 9
	return df

def apply_special_adjustments(df):
	# Turkey before 2010 and all Palestine data from WHO should be non-representative
	df.loc[(df.location_id == 155) & (df.year_id < 2010), 'representative_id'] = 0
	df.loc[df.location_id == 149, 'representative_id'] = 0
	return df


def format_source(release_date):
	# read the raw data and the WHO provided country/year map
	df = read_data(release_date)
	country_map = get_country_map(release_date)

	# subset to just the new loc/years
	# also apply location/year restrictions
	df = subset_location_years(df, country_map)

	# map location information
	loc_meta = get_current_location_hierarchy(location_set_id=CONF.get_id('location_set'),
							location_set_version_id=CONF.get_id('location_set_version'),
							force_rerun=False, block_rerun=True
					)
	df = get_gbd_locations(df, country_map, loc_meta)

	# replicating age adjustments for WHO data from
	df = adjust_WHO_ages(df)

	# Limit the dataframe to the columns needed and melt ages wide to long
	df = melt_df(df)

	# assign age group ids
	df = get_age_group_ids(df)

	# map code ids and apply special remaps
	cause_map = get_cause_map(1, force_rerun=False, block_rerun=True)
	df = map_code_id(df, cause_map)

	# add manual cols and cleanup
	df = cleanup(df)

	# apply nids
	df = map_nids(df, release_date)

	# apply any final special adjustments
	df = apply_special_adjustments(df)

	# final grouping and finalize formatting
	df = df[FINAL_FORMATTED_COLS]
	assert df.notnull().values.all()
	df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

	# run finalize formatting
	locals_present = finalize_formatting(df, SYSTEM_SOURCE, write=WRITE)
	nid_meta_df = locals_present['nid_meta_df']

	# update nid metadata status
	if WRITE:
		nid_extracts = nid_meta_df[
	        ['nid', 'extract_type_id']
	    ].drop_duplicates().to_records(index=False)
		for nid, extract_type_id in nid_extracts:
			nid = int(nid)
			extract_type_id = int(extract_type_id)
			update_nid_metadata_status(nid, extract_type_id, is_active=IS_ACTIVE,
	                                   is_mort_active=IS_MORT_ACTIVE)

if __name__ == "__main__":
	release_date = str(sys.argv[1])
	format_source(release_date)