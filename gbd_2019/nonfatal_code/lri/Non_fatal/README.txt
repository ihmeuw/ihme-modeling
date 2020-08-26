################################################################################
## The files in this folder are those relating to non-fatal data prep for LRI.
## Preparing non-fatal data to be modeled in DisMod is relatively intensive for
## LRI. There are a number of different steps for the different data types:
## 1) Survey/self-report, 2) Literature data, 3) Clinical data. 
################################################################################

#################
## Survey data ##

## Main scripts
* lri_collapse_survey_data_2019.R: This file is the master script to collapse
	survey data that have been cleaned in UbCov. It does several things
	including tabulating the survey prevalence by month for use in the
	seasonality scalar, regressing LRI prevalence over seasonality to
	adjust the survey prevalence, and tabulates by age-sex-location-year
	all survey data using the appropriate survey weights. This file finds
	the prevalence using the most specific survey case definition within
	each survey (chest w/diff. breathing and fever, chest w/diff. breathing,
	diff. breathing and fever, and diff. breathing). 

* lri_survey_processing_single_survey.R: This file does something similar to 
	the file above but in a somewhat different way. Instead of loading
	all of the individual survey results into memory at once, this file
	only loads in and tabulates a single survey at a time. This file
	needs the seasonality scalars to be calculated previously. 

* lri_collapse_only_good_data.R: This file does the same things as the file
	above but tabulates the results only among surveys that have all
	four of the possible case defintions. This file is used to determine the
	crosswalks for each survey definition (estimated and applied in the 
	"crosswalk_lri_all_mr-brt.R" file.

* lri_convert_survey_period-point.R: This file converts the survey results from
	period prevalence to point prevalence and prepares the data to be uploaded
	to the Epi database in a bundle.

* prep_survey_upload.R: This file simply prepares the surveys so that they can
	be uploaded to the Epi database.

## Scripts needed in survey collapse files
* lri_month_tabulation.R: This file collapses the survey data into survey-month
	prevalence. These values are used in "lri_month_tabulation.R" to find
	the relationship between seasonality and LRI prevalence by GBD region.

* lri_seasonality_scalar.R: This file uses a generalized additive model with a forced
	periodicity to estimate a relationship between calendar month and LRI
	prevalence by GBD region. The ratio of the predicted monthly prevalence
	and the mean prevalence in the region becomes a scalar that is applied in
	the "lri_collapse_survey_data_2019.R" file to adjust for the fact that
	each survey occurs over a duration shorter than a year.

## Data management scripts ##
* bulk_outlier_susenas_surveys.R: This file should be used to bulk outlier
	the Indonesia National Health Survey (SUSENAS) surveys. These surveys
	use a case definition for LRI that is inconsistent with our other surveys
	and should not be used. They have been excluded in previous GBD iterations
	and it is was not clear how nor expected that they be used in the step 2
	DisMod models. The data should be outliered. As of 9/6/2019 at 9.30 AM,
	the data have been outliered in step 3 models but not in step 4 because
	apparently there needs to be a "best" model for step4 before the data
	can be bulk outliered. This should happen as soon as there is a step 4 model,
	which will then need to be re-run!

##################################
## Clinical data and crosswalks ##

* crosswalk_lri_all_mr-brt.R: This file is pretty comprehensive.
	1) Takes care of some surveys that needed to be replaced for new subnationals
	
	2) Pulls a bundle_version that includes the clinical data (inpatient and claims)
	
	3) Creates crosswalks for the different survey definitions. There are four
	possible defintions from the surveys, so we found the ratio between the best
	defintion and each alternative *within* surveys that had all four defintions 
	available (from "lri_collapse_only_good_data.R"). These values are then run
	through MR-BRT to get a single value for each ratio. Previous GBD iterations had
	this ratio vary by location/age but we did not do this for GBD 2019.
	
	4) Adjust the age-sex-year-location survey data using these crosswalks established
	in step 3. The data here come from "lri_collapse_survey_data_2019.R".
	
	5) Determine a crosswalk for hospital literature data. We want to adjust
	for studies that determined the incidence/prevalence of LRI among hospitalized populations.
	Although most LRI episodes do receive medical treatment, not all do and we want to
	capture all LRI episodes. This crosswalk is done *within* studies that report the
	incidence/prevalence in both hospitalized and non-hospitalized populations.
	
	6) Determine a crosswalk for all self-reported data. After the survey data have
	been crosswalked to the best survey definition, they are still not very specific for
	LRI compared to our case definition of clinician-diagnosed pneumonia or bronchiolitis.
	In this step, we use MR-BRT to find a ratio for self-reported compared to reference.
	
	7) Convert clinical data from incidence to prevalence. All other data are prevalence
	data. We convert from incidence to prevalence for two reasons A) So that we can perform
	crosswalks between these data and our reference data which are prevalence and B) so that
	DisMod can calculate an excess mortality ratio (EMR). The EMR is the ratio of 
	cause specific mortality to prevalence.
	
	8) Crosswalk the inpatient data using MR-BRT.
	
	9) Upload the data as a bundle_version to be used in DisMod!

####################################################
## Custom Excess Mortality function ##

5* lri_emr_central_function.R: This file pulls excess mortality rates (EMR) from a 
	completed DisMod model (should be step 3) and runs an MR-BRT regression to predict
	EMR for all locations, by sex and age, dependent on healthcare access and quality index (HAQI).
	The code then preps and uploads the data either to:
		A) Existing bundle (bundle_id 19) for step 4
		B) New bundle for step 3
