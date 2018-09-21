/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER    
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
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step COD
	*local cause
	local cause ntd_nema
	*local bundle
	local bundle
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submit_ascar_cod_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

*Code Testing
	local rebuild 1
	*set equal to 0 if dataset should not be rebuilt




/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model.ado
	run FILEPATH/process_predictions.ado


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

	local cause_id 360
	local covariate_ids 57 118 119 142 1099 3 463 854 208 109 17 33 66 128 129 130 131 132 133 134 135 47 261 461 473 476 479 845 881 1087 208 160
	local prev_model_id 130874
	local heavy_inf_id 130904
	local get_model_results gbd_team(epi) model_version_id(`prev_model_id') measure_id(5) | gbd_team(epi) model_version_id(`heavy_inf_id') measure_id(5 6) |
	local saveto FILEPATH/ascariasis.dta
	local minAge 4


*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear


*--------------------2.3: Custom Variable Transformation

	use FILEPATH/ascariasis.dta, clear

	*GET MOVING AVERAGE FOR RAINFALL TO MAKE LESS VOLATILE
		get_covariate_estimates,covariate_id(130) clear
		keep location_id year_id mean_value
		tsset location_id year_id, yearly
		tssmooth ma RainSmooth11=mean_value,window(17 1 17)
		rename mean_value RainOriginal
		merge 1:m location_id year_id using FILEPATH/ascariasis.dta, nogen keep(matched using)
		drop rainfall*
		
		*Save
		tempfile new2
		save `new2', replace

		**CHECKPOINT
		*twoway(scatter mean_value year_id,mcolor(magenta) msize(medium) msymbol(oh))(scatter S6 year_id,mcolor(sand) msize(medium) msymbol(O)), by(location_name)

	*TRANSFORMATIONS PER COVARIATE TESTING
		*Prevalence of Ascariasis
			gen logitPrev27_`prev_model_id'=logit(prevalence_`prev_model_id'_27+(.1*.0001081))
			gen logitPrev27_`heavy_inf_id'=logit(prevalence_`heavy_inf_id'_27+(.1*1.11e-09))
					
		*Latitude
			gen square_lat=abs_latitude^2
				
	*VANUATU PREVALENCE FIX (loc_id=30) 
		*Apply 2010 prevalence value to all years before 2010
			gen tempVan2=logitPrev27_130904 if location_id==30 & year_id==2010
			bysort location_id: egen tempVan3=mean(tempVan2)
			replace logitPrev27_130904=tempVan3 if location_id==30 & year_id<2010
			drop temp*
			
	save FILEPATH/ascariasis.dta, replace

}

/*====================================================================
                        3: Run Model
====================================================================*/


	use FILEPATH/ascariasis.dta, clear

*--------------------3.1: Use Custom CoD Function to Submit Model

	run_best_model menbreg  study_deaths i.age_group_id i.sex_id logitPrev27_`heavy_inf_id' health_system_access_capped square_lat pop_dens_under_150_psqkm_pct evi_2000_2012 RainSmooth_w17 education_yrs_pc  prop_pop_agg , exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)


*--------------------3.2: Preliminary Model Testing

	//Test dispersion parameter in negative binomial to see if it is significant, if not use poisson
		*test [lndelta]_cons = 1\
	//Test fit of model
		*estat ic

/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz
**Save to cause 360 (ascar) and 361 (INI)

	local description "menbreg.minage=ag2;covs:i.age,sex,logit_heavyprev,hsac,lat^2,popdens<150,evi,rain17,educ,prop_agg;RE-loc||R.age"
	process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') description("`description'") multiplier(envelope) saving_cause_ids(360 361)







log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


