#######################################################################
Code in this folder is used to perform the crosswalks and data prep
for the LRI etiologies that are modeled as proportions in DisMod.
#######################################################################

** crosswalk_lri_etiologies_mr-brt.R: This is the MASTER file!
It is the single most important piece of code that is new to GBD 2019 
for the LRI etiologies. It does a lot of things, including and in order:
	1) Pulled step 1 data from the bundles, saved in a location for crosswalks
	2) Appends new data for GBD 2019 step 4
	3) Age splits data using an age curve developed in MR-BRT that then
	splits data where the difference in age_start and age_end is greater
	than 25 years. Age splitting depends on a function (age_split_mrbrt_weights.R)
	that is described below and must be sourced.
	4) Estimates crosswalk coefficients in MR-BRT for cv_inpatient and cv_diag_pcr.
	These crosswalks are performed in either log or logit transformations and have
	been updated such that only data from decomp 2 are used in the regressions
	but all data (including new data for decomp 4) are adjusted. Needs:
		"bundle_crosswalk_collapse.R"
	5) Uploads the data for
		upload_bundle_data (raw data including not sex/age split, no crosswalks)
		save_bundle_version (a static version of the bundle at a point in time)
		save_crosswalk_version (a version of data that are linked to a bundle_version
			but have been sex/age split and crosswalked)
In general, the steps listed above should be run sequentially. Output from each of 
the steps are used as inputs in the following ones. Any data adjustments should occur here and 
if there are new data or processes adopted, this file could be used as a guide. There are 
comments throughout the code that are hopefully helpful!

** age_split_mrbrt_weights.R: This file is a function that splits age data using an 
age curve fit in MR-BRT. The modeler can define which age groups should be created
after splitting. More age groups allow for more flexible curves but have more uncertainty.
There are a couple things it requires including
	1) A dataframe to split the denominator. This means an age-specific set of
	values that can be applied to all age data to find the proportion of the 
	original sample size that should be put in each age group. So, for the diarrhea
	etiologies, one could use the population or the total number of diarrhea
	episodes by age group. 
	2) A dummy column in the input data to be split that indicates if the data
	are age-specific or not (cv_age_split). Data that are cv_age_split = 0
	are used in the MR-BRT model to find the age pattern using only age-specific
	data and data that are cv_age_split = 1 are then split where the numerator
	(cases) is adjusted by the proportion of the MR-BRT model that is in each
	age group and the denominator (sample_size) is split by the dataframe described
	above. 
This file also helps to clear these rows if the modeler wants to upload them to the
bundle before modeling. 

** bundle_crosswalk_collapse.R: This is a function used a lot in crosswalk
preparation. What it does is takes a bundle dataset and collapses to reference and alternative
definitions for a given crosswalk indicator. An example would be, cv_inpatient. The function
takes all the data and finds matches at the modeler-specified amount of detail (described below),
finds the ratio of reference to alternative, transforms that ratio into log and logit spaces,
and returns that output which is then intended to be used directly in MR-BRT. Things that the modeler
can specify for the function are:
	1) age_cut: How many age bins do you want and where should they be defined
	2) year_cut: How many year bins do you want and where should they be defined
	3) merge_type: Can data be matched between studies or must they occur within studies only?
	4) location_match: Should locations be matched exactly or can data be aggregated to higher levels