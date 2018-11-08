/*====================================================================
project:       GBD2017
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2017
Output:           Submit script to model mortality due to Ascariasis and process predicted estimates
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}

*Log
	local log_dir "FILEPATH"

*Directory for standard code files
	adopath + "FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submit_ascar_cod_log_`date'_`time'.smcl", replace
	***********************


*Code Testing
	local rebuild 1
	*set equal to 0 if dataset should not be rebuilt




/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

	local cause_id 360
	local covariate_ids 119 142 1099 3 208 33 261 479 1087 160
	* this is heavy model id......
	local prev_model_id 316493
	local get_model_results gbd_team(epi) model_version_id(`prev_model_id') measure_id(5)
	local saveto "FILEPATH"
	local minAge 4


*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear


*--------------------2.3: Custom Variable Transformation

	* left off here
	use "FILEPATH".dta, clear


		**CHECKPOINT
*/		*twoway(scatter mean_value year_id,mcolor(magenta) msize(medium) msymbol(oh))(scatter S6 year_id,mcolor(sand) msize(medium) msymbol(O)), by(location_name)

	*TRANSFORMATIONS PER COVARIATE TESTING
		*Prevalence of Ascariasis
			gen logitPrev27_`prev_model_id'=logit(prevalence_`prev_model_id'_27+(.1*.0001081))

		*Latitude
			gen square_lat=abs_latitude^2

	*VANUATU PREVALENCE FIX (loc_id=30)
		*Apply 2010 prevalence value to all years before 2010
			gen tempVan2=logitPrev27_`prev_model_id' if location_id==30 & year_id==2010
			bysort location_id: egen tempVan3=mean(tempVan2)
			replace logitPrev27_`prev_model_id'=tempVan3 if location_id==30 & year_id<2010
			drop temp*

	save "FILEPATH".dta, replace

}

/*====================================================================
                        3: Run Model
====================================================================*/


	use "FILEPATH".dta, clear

*--------------------3.1: Use Custom CoD Function to Submit Model
* need to run with heavy inf.....and mark best
run_best_model menbreg  study_deaths i.age_group_id i.sex_id logitPrev27_`prev_model_id' health_system_access_capped square_lat pop_dens_under_150_psqkm_pct evi_2000_2012 education_yrs_pc  prop_pop_agg , exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)


*--------------------3.2: Preliminary Model Testing

	//Test dispersion parameter in negative binomial to see if it is significant, if not use poisson
		*test [lndelta]_cons = 1\
	//Test fit of model
		*estat ic

/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz
**Save to both cause 360 (ascar) and 361 (INI)

	local description "menbreg.i.age,i.sex,,logitPrev27_`prev_inf_id',hsac, sq lat,pop_dens150,evi,educ,prop_pop_agg,RE=location_id,R.age"
	process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') description("`description'") multiplier(envelope) saving_cause_ids(360 361)


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.Exact Model from GBD 2017:
poisson study_deaths i.age_cat i.sex i.superregion i.prop_muslim_cat2 hsa, exposure(sample_size) vce(robust)
*egen prop_muslim_cat2 = cut(prop_muslim), at(0,0.2,1) label

2.
3.


Version Control:
