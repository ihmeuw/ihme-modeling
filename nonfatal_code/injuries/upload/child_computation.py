######################################################################
## Purpose: Child Script for COMPUTATION step for COMO-injuries
## 			incidence and prevalence aggregates
######################################################################

# import packages
import pandas as pd
import numpy as np
import os
import errno
import itertools

from sys import argv
from copy import deepcopy
from transmogrifier.super_gopher import SuperGopher
from hierarchies import dbtrees
from adding_machine.summarizers import transform_metric
from adding_machine import agg_locations as al
from dimensions import gbdize
from como.common import apply_restrictions, agg_hierarchy, cap_val
import time

# !! IMPORTANT !!
demographics = pd.read_csv("FILEPATH")

#######################################################
## SETUP PARALLELIZATION OR NOT
#######################################################

# if running in parallel, then task ID will be pull-able
# if not, then task id isn't found (I.E. not running in parallel) and this will fill with dUSERt arguments
try:
	task_id = int(os.environ.get("SGE_TASK_ID"))
	grab_args = True
except TypeError: 
	task_id = 1
	grab_args = False

print "Task id: {}".format(task_id)
print argv

if grab_args:
	root_j_dir = argv[1]
	root_tmp_dir = argv[2]
	date = argv[3]
	code_dir = argv[4]
	in_dir = argv[5]
	out_dir = argv[6]
	ndraws = int(argv[7])
else:
	root_j_dir = "FILEPATH"
	root_tmp_dir = "FILEPATH"
	date = "DATE"
	code_dir = "FILEPATH"
	in_dir = os.path.join(root_j_dir, "FILEPATH")
	out_dir = os.path.join("FILEPATH")
	ndraws = 1000

#######################################################
## HELPER FUNCTIONS
#######################################################

def get_params(codes):
	""" Grabs all e-codes and n-codes in two vectors so that we can
	use them in expand.grid."""
	# get the ecodes and ncodes
	shocks = ["inj_war_warterror", "inj_war_execution", "inj_disaster"]
	ecodes = codes.ix[(codes['aggregate'] == 0) & (-codes['e_code'].isin(shocks)), ['e_code']].dropna().e_code.unique()
	ncodes = codes.ix[(codes['n_code'] != "N29")].n_code.dropna().unique()
	return list([ecodes, ncodes])

def expandgrid(*itrs):
	""" Define function to expand grid.. gets the
	Cartesian products."""
	product = list(itertools.product(*itrs))
	return({'Var{}'.format(i + 1):[x[i] for x in product] for i in range(len(itrs))})

def make_square(data_frame, locations, sexes, years, ages, measure_id, codes, shock):
	""" Function that makes a square dataset with the expand grid data
	frame defined within this function.
	This will either square it for shocks or nonshocks."""
	n = get_params(codes)[1]
	if shock:
		e = ["inj_war_warterror", "inj_war_execution", "inj_disaster"]
	else:
		e = get_params(codes)[0]
	square = pd.DataFrame(expandgrid(e, n, measure_id, locations, sexes, ages, years))
	square.columns = ['ecode', 'ncode', 'measure_id', 'location_id', 'sex_id', 'age_group_id', 'year_id']
	alldata = pd.merge(data_frame, square, how = 'right', on = ['ecode', 'ncode', 'measure_id', 'location_id', 'sex_id', 'age_group_id', 'year_id'])
	alldata = alldata.fillna(0)
	return(alldata)

def resample_if_needed(df, dim, gbdizer, ndraws):
	""" Define funciton that resamples if the dimensions object
	that is passed in from COMO says we should have less than 1000
	draws. This will be used for annual runs."""
	df = df.set_index(dim.index_names)
	# subset data columns
	draw_cols = ["draw_{}".format(i) for i in range(ndraws)]
	draw_cols = [d for d in draw_cols if d in df.columns]
	df = df[draw_cols]
	df = df.reset_index()
	# resample if ndraws is less than 1000
	if len(dim.data_list()) != len(draw_cols):
	    df = gbdizer.random_choice_resample(df)
	return df

def injize(df, dim, cv, measure, years, fixage, codes, shock, ndraws):
	""" Define function to make into injury format.
	This is the same for all epi/annual inc/prev computations."""
	# change from age data to age group id. only need to do this for prevalence
	# because for incidence, I'm already doing this in the get_..._inc scripts
	# when I redistribute the under 1 age groups
	df["measure_id"] = measure
	if fixage:
		df["age"] = df["age"].round(2).astype(str)
		ridiculous_am = {
		        '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7,
		        '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11, '35.0': 12,
		        '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17,
		        '65.0': 18, '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31,
		        '90.0': 32, '95.0': 235}
		df["age"] = df["age"].replace(ridiculous_am).astype(int)
		df.rename(columns={"age": "age_group_id"}, inplace=True)
	df = df.groupby(["location_id", "year_id", "age_group_id", "sex_id", "measure_id", "ncode", "ecode"]).sum().reset_index()
	# get demographics for squaring
	location_id = dim.index_dim.get_level("location_id")
	sex_id = dim.index_dim.get_level("sex_id")
	age_group_id = dim.index_dim.get_level("age_group_id")
	if shock:
		years = dim.index_dim.get_level("year_id")
	else:
		years = years
	# make it square!
	df = make_square(data_frame = df, locations = location_id,
		sexes = sex_id, years = years,  ages = age_group_id,
		measure_id = [measure], codes = codes, shock = shock)
	# merge on hierarchies for e-codes and n-codes
	df = df.merge(cv.cause_list, left_on = "ecode", right_on = "acause")
	df = df.merge(cv.ncode_hierarchy, left_on = "ncode", right_on = "rei")
	# double check that everything is collapsed
	df = df.groupby(["measure_id", "location_id", "year_id", "age_group_id", "sex_id", "cause_id", "rei_id"]).sum().reset_index()
	data_cols = ["draw_{}".format(i) for i in range(1000)]
	gbdizer = gbdize.GBDizeDataFrame(dim)
	df = gbdizer.add_missing_index_cols(df)
	df = gbdizer.gbdize_any_by_dim(df, "age_group_id")
	df.fillna(0, inplace=True)
	if gbdizer.missing_values(df, "year_id"):
	    df = gbdizer.fill_year_by_interpolating(
	        df=df,
	        rank_df=df[df["year_id"] == 2005],
	        data_cols=data_cols)
	df = df[df.year_id.isin(dim.index_dim.get_level("year_id"))]
	# assert statements to make sure that it's the right shape and has the right columns
	if shock:
		rows = 3243
	else:
		rows = 28106
	assert len(df.index) == rows, "The number of rows in the injized DF is not correct."
	assert -df.duplicated(subset = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'cause_id', 'rei_id', 'rei_set_id']).any(), "The id columns do not uniquely identify the observations!"
	# potentially resample draws...
	df = resample_if_needed(df, dim, gbdizer, ndraws)
	return df

########################################################
## DEFINE FUNCTIONS TO GET MOST DETAILED DATA
########################################################

## EPI YEARS PREVALENCE

def get_epi_prev(dim, cv, years, codes, ndraws):
	# grab the short-term EN prevalence
	estim_st = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			"FILEPATH"
		))
	estim_st_df = estim_st.content(
		location_id = dim.index_dim.get_level("location_id"),
		year_id = years,
		sex_id = dim.index_dim.get_level("sex_id"))
	estim_st_df["term"] = "short-term"
	# grab the long-term EN prevalence
	estim_lt = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			"FILEPATH"))
	estim_lt_df = estim_lt.content(
		location_id = dim.index_dim.get_level("location_id"),
		year_id = years,
		sex_id = dim.index_dim.get_level("sex_id"))
	estim_lt_df["term"] = "long-term"
	estim_lt_df = estim_lt_df.loc[(estim_lt_df.ecode != "inj_war_warterror") & (estim_lt_df.ecode != "inj_war_execution") & (estim_lt_df.ecode != "inj_disaster")]
	# append short-term and long-term prevalence datasets together
	estim_df = estim_st_df.append(estim_lt_df)
	# these columns are unnecessary
	estim_df.drop(['prob_draw_', 'inpatient'], axis = 1, inplace = True)
	# injury-ize based on the gbd requirements
	estim_df = injize(estim_df, dim, cv, measure = 5, years = years, fixage = True, codes = codes, shock = False, ndraws = ndraws)
	return estim_df

## ANNUAL YEARS PREVALENCE

def get_annual_prev(dim, cv, years, codes, ndraws):
	# set vars
	location_id = dim.index_dim.get_level("location_id")[0]
	sex_id = dim.index_dim.get_level("sex_id")[0]
	year_id = dim.index_dim.get_level("year_id")[0]
	# get short-term annual results
	annual_st = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			'FILEPATH'
			))
	annual_st_df = annual_st.content(
		location_id = dim.index_dim.get_level("location_id"),
		year_id = dim.index_dim.get_level("year_id"),
		sex_id = dim.index_dim.get_level("sex_id"))
	annual_st_df["term"] = "short-term"
	# get long-term annual results
	annual_lt = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			'FILEPATH'))
	annual_lt_df = annual_lt.content(
		location_id = dim.index_dim.get_level("location_id"),
		year_id = dim.index_dim.get_level("year_id"),
		sex_id = dim.index_dim.get_level("sex_id"))
	annual_lt_df["term"] = "long-term"
	# combine them in one data frame
	annual_df = annual_st_df.append(annual_lt_df)
	# drop them
	annual_df.drop(['inpatient', 'prob_draw_', 'term'], axis = 1, inplace = True)
	# injury-ize based on the gbd requirements
	annual_df = injize(annual_df, dim, cv, measure = 5, years = years, fixage = True, codes = codes, shock = True, ndraws = ndraws)
	return annual_df

## EPI YEARS INCIDENCE

def get_epi_inc(dim, cv, years, codes, ndraws):
	# grab the age ids
	age_ids = pd.read_csv(os.path.join(code_dir, "convert_to_new_age_ids.csv")).rename(columns = {'age_start': 'age'})
	# get the EN incidence -- not short-term or long-term because all incidence is captured in short-term
	estim_sg = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			'FILEPATH'
		))
	estim_df = estim_sg.content(
		location_id = dim.index_dim.get_level("location_id"),
		year_id = years,
		sex_id = dim.index_dim.get_level("sex_id"))
	# merge on age group ids
	estim_df = pd.merge(estim_df, age_ids, on = 'age')
	estim_df.drop('age', inplace = True, axis = 1)
	# keep age group id 2 and triplicate so that we can have 3 sets
	# this is to redistribute the incidence in under 1 age groups
	# with population fractions for age groups 2 3 and 4
	todupe1 = estim_df.ix[(estim_df['age_group_id'] == 2)]
	todupe1['age_group_id'] = 3
	todupe2 = estim_df.ix[(estim_df['age_group_id'] == 2)]
	todupe2['age_group_id'] = 4
	estim_df = estim_df.append(todupe1)
	estim_df = estim_df.append(todupe2)
	# get the population -- don't query database every time
	pops = pd.read_stata(os.path.join(root_j_dir, "FILEPATH"))
	# MAKE POPULATION FRACTIONS
	fullpops = pd.merge(pops, age_ids, on = 'age_group_id')
	fullpops['collapsed_age'] = fullpops['age']
	fullpops.loc[fullpops.age < 1, 'collapsed_age'] = 0
	pops = fullpops.copy()
	pops.loc[pops.age < 1, 'age'] = 0
	pops = pops[['location_id', 'year_id', 'sex_id', 'age', 'population']]
	pops = pops.groupby(['location_id', 'year_id', 'sex_id', 'age']).sum().reset_index()
	pops = pops.rename(columns = {'population': 'total_pop', 'age': 'collapsed_age'})
	popfracts = pd.merge(fullpops, pops, on = ['location_id', 'year_id', 'collapsed_age', 'sex_id'])
	popfracts['pop_fraction'] = popfracts['population'] / popfracts['total_pop']
	popfracts = popfracts[['age_group_id', 'location_id', 'year_id', 'sex_id', 'pop_fraction']]
	# redistribute inc. under 1 (ALL OF THE OTHER POP FRACTIONS SHOULD BE 1)
	estim_df = pd.merge(estim_df, popfracts, how = 'left', on = ['location_id', 'year_id', 'sex_id', 'age_group_id'])
	cols = ["draw_" + `i` for i in range(0, 999)]
	estim_df[cols] = estim_df[cols].multiply(estim_df.pop_fraction, axis = "index")
	estim_df.drop(['pop_fraction', 'inpatient'], inplace = True, axis = 1)
	# injury-ize based on the gbd requirements
	estim_df = injize(estim_df, dim, cv, measure = 6, years = years, fixage = False, codes = codes, shock = False, ndraws = ndraws)
	return estim_df

## ANNUAL YEARS INCIDENCE

# define function to get annual incidence
def get_annual_inc(dim, cv, years, codes, ndraws):
	location_id = dim.index_dim.get_level("location_id")[0]
	sex_id = dim.index_dim.get_level("sex_id")[0]
	year_id = dim.index_dim.get_level("year_id")[0]
	# get inpatient incidence
	annual_inp = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			'FILEPATH'
		))
	annual_inp_df = annual_inp.content(
		location_id = dim.index_dim.get_level("location_id"),
		sex_id = dim.index_dim.get_level("sex_id"))
	annual_inp_df = annual_inp_df.loc[(annual_inp_df.year_id == year_id)]
	# get outpatient incidence
	annual_otp = SuperGopher(
		{'file_pattern': 'FILEPATH'},
		os.path.join(
			'FILEPATH'
		))
	annual_otp_df = annual_otp.content(
		location_id = dim.index_dim.get_level("location_id"),
		sex_id = dim.index_dim.get_level("sex_id"))
	annual_otp_df = annual_otp_df.loc[(annual_otp_df.year_id == year_id)]
	# bind the inpatient and outpatient data frames together and groupby to collapse over inpatient
	annual_df = annual_otp_df.append(annual_inp_df)
	annual_df.drop(['inpatient'], inplace = True, axis = 1)
	annual_df = injize(annual_df, dim, cv, measure = 6, years = years, fixage = True, codes = codes, shock = True, ndraws = ndraws)
	return annual_df

def get_prevalence(dim, cv, years, codes, ndraws):
	location_id = dim.index_dim.get_level("location_id")
	year_id = dim.index_dim.get_level("year_id")
	sex_id = dim.index_dim.get_level("sex_id")
	print "Getting epi prevalence for location {}, sex {}, and year {}".format(location_id, sex_id, year_id)
	epi = get_epi_prev(dim, cv, years, codes, ndraws = ndraws)
	print "Getting annual prevalence for location {}, sex {}, and year {}".format(location_id, sex_id, year_id)
	annual = get_annual_prev(dim, cv, years, codes, ndraws = ndraws)
	alldata = epi.append(annual)
	return alldata

def get_incidence(dim, cv, years, codes, ndraws):
	location_id = dim.index_dim.get_level("location_id")
	year_id = dim.index_dim.get_level("year_id")
	sex_id = dim.index_dim.get_level("sex_id")
	print "Getting epi incidence for location {}, sex {}, and year {}".format(location_id, sex_id, year_id)
	epi = get_epi_inc(dim, cv, years, codes, ndraws = ndraws)
	print "Getting annual incidence for location {}, sex {}, and year {}".format(location_id, sex_id, year_id)
	annual = get_annual_inc(dim, cv, years, codes, ndraws = ndraws)
	alldata = epi.append(annual)
	return alldata

########################################################
## PROCESSING STEPS FOR MOST DETAILED DATA
########################################################

# define function to get the cause tree
def get_cause_tree(cv):
    ct = dbtrees.causetree(cv.cause_set_version_id, None, None)
    return deepcopy(ct)

# define function to compute n-code and e-code aggregates
# on the most detailed datasets
def compute_aggregates(df, dim, cv):
	# grab the cause tree
	cause_tree = get_cause_tree(cv)
	# aggregate ecodes
	df = agg_hierarchy(tree = cause_tree, df = df, index_cols = dim.index_names, data_cols = dim.data_list(),
		dimension = "cause_id")
	df = df[dim.index_names + dim.data_list()]
	# aggregate ncodes
	df = df.merge(cv.ncode_hierarchy)
	df_agg = df.copy()
	df_agg = df.groupby(["age_group_id", "location_id", "year_id", "sex_id", "cause_id", "parent_id", "measure_id"])[dim.data_list()].sum().reset_index()
	df_agg = df_agg.rename(columns = {"parent_id": "rei_id"})
	# set attribute
	df = df.append(df_agg)
	df = df[dim.index_names + dim.data_list()]
	return df

# function to write the resulting draws for a given data frame
def write_result_draws(df, measure_id, dim, out_dir):
    # define parallelism and filepaths
    parallelism = ["location_id", "year_id", "sex_id"]
    for slices in dim.index_slices(parallelism):
        filename = "{mid}_{yid}_{sid}.h5".format(
            mid = measure_id, yid=slices[1], sid=slices[2])
        directory = os.path.join(
            out_dir,
            str(slices[0]))
        try:
            os.makedirs(directory)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        filepath = os.path.join(directory, filename)
        # write the out data frame
        out_df = df.loc[
            (df.location_id == slices[0]) &
            (df.year_id == slices[1]) &
            (df.sex_id == slices[2]),
            dim.index_names + dim.data_list()]
        out_df.to_hdf(
            filepath,
            "draws",
            mode = "w",
            format = "table",
            data_columns = dim.index_names)

## ---------------------------- MAIN! ----------------------------------- ##

def main(root_j_dir, root_tmp_dir, date, code_dir, in_dir, out_dir, ndraws, demographics, task_id):
	dems = demographics.ix[(demographics["task_id"] == task_id)]
	# subset based on task id to the demographic arguments
	location_id = np.asscalar(dems["location_id"].iloc[0])
	year_id = np.asscalar(dems["year_id"].iloc[0])
	sex_id = np.asscalar(dems["sex_id"].iloc[0])

	# import hierarchies from Como
	from como.version import ComoVersion
	cv = ComoVersion("FILEPATH")
	cv.load_cache()

	# get dimensions and replace with what we are parallelizing
	# in this child script
	print "Copying dimensions from cv"
	dim = deepcopy(cv.dimensions)
	dim.index_dim.replace_level("location_id", location_id)
	dim.index_dim.replace_level("year_id", year_id)
	dim.index_dim.replace_level("sex_id", sex_id)

	print "Adding cause to dimensions"
	# add cause to dimensions
	dim.index_dim.add_level("cause_id", cv.cause_restrictions.cause_id.unique().tolist())
	dim.index_dim.add_level("rei_id", cv.ncode_hierarchy.rei_id.unique().tolist())

	# set the years so that we always have 2005 to calibrate
	years = list(set(cap_val(dim.index_dim.levels.year_id,
		[1990, 1995, 2000, 2005, 2010, 2016]) + [2005]))
	print "Years"
	print years

	# get all E-N combinations to use to make square data
	codes = pd.read_csv(os.path.join(code_dir, "FILEPATH.csv"))

	# Get Incidence df
	df_inc = get_incidence(dim, cv, years, codes, ndraws = ndraws)
	df_inc_agg = compute_aggregates(df_inc, dim, cv)
	assert len(df_inc_agg.index) == 50922, "The number of rows in the injized DF is not correct."
	assert -df_inc_agg.duplicated(subset = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'cause_id', 'rei_id']).any(), "The id columns do not uniquely identify the observations!"
	assert -df_inc_agg.isnull().any().any(), "You have null values in the prevalence DF!"
	print "Writing results for incidence"
	write_result_draws(df_inc_agg, measure_id = 6, dim = dim, out_dir = out_dir)

	# Get Prevalence df
	df_prev = get_prevalence(dim, cv, years, codes, ndraws = ndraws)
	df_prev_agg = compute_aggregates(df_prev, dim, cv)
	assert len(df_inc_agg.index) == 50922, "The number of rows in the injized DF is not correct."
	assert -df_prev_agg.duplicated(subset = ['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'cause_id', 'rei_id']).any(), "The id columns do not uniquely identify the observations!"
	assert -df_prev_agg.isnull().any().any(), "You have null values in the prevalence DF!"
	print "Writing results for prevalence"
	write_result_draws(df_prev_agg, measure_id = 5, dim = dim, out_dir = out_dir)

	# save checkfile when all done
	checkpath = os.path.join("FILEPATH", "finished_{}_{}_{}.txt".format(location_id, year_id, sex_id))
	tmp = open(checkpath,'wb')
	tmp.close()

	
# set initial time
tic = time.time()
# call main function
main(root_j_dir, root_tmp_dir, date, code_dir, in_dir, out_dir, ndraws, demographics, task_id)
# set end time
toc = time.time()
elapsed = toc - tic
print "Total time was {} seconds".format(elapsed)

print "You finished properly!"
# END OF SCRIPT ---------------------------------------------------------------------------------------