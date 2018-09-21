/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER  
Output:           Submit script to model mortality due to Cysticercosis and process predicted estimates
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
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step COD
	*local cause
	local cause ntd_cysticer
	*local bundle "/#/"
	local bundle /59/
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
	log using "`log_dir'/submit_cysticer_cod_log_`date'_`time'.smcl", replace
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

	local cause_id 352
	local covariate_ids 137 100 1099 208 57 99 142 118 119 1087 33 463 881 17 160 845
	local get_model_results gbd_team(epi) model_version_id(124196) measure_id(5)
	local saveto FILEPATH/cyst.dta
	local minAge 5

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear
	save FILEPATH/cyst.dta, replace

*--------------------2.3: Add Custom Covariate

	*Get GBD Info
		*Location Metadata
			get_location_metadata,location_set_id(35) clear
			keep location_id parent_id path_to_top_parent level most_detailed location_name region_id
			tempfile metadata
			save `metadata', replace
			levelsof location_id,local(alllocs) clean
		
		*Demographics
			get_demographics,gbd_team(cod) clear
			local codyears `r(year_ids)'
			local codages `r(age_group_ids)'
			local codlocs `r(location_ids)'
			local codsexes `r(sex_ids)'
		
		*Population
			get_population, location_id("`alllocs'") sex_id("`codsexes'") age_group_id("`codages'") year_id("`codyears'") clear
			tempfile pop_env
			save `pop_env', replace
		
	*Get Covariate Info
		*Pull in Covariate Information File
			insheet using "FILEPATH/prop_muslim_gbd2016.csv", clear double
			tempfile muslim_raw
			save `muslim_raw', replace
				
		*Merge with population envelope
			merge 1:m location_id using `pop_env'
			merge m:1 location_id using `metadata', nogen
			drop if level<3
			bysort region_id year_id age_group_id sex_id: egen region_mean=mean(prop_muslim)
			replace prop_muslim=region_mean if prop_muslim==.
		
		*Adjust Missing Locations
			*Missing locations: American Samoa (298), Greenland (349), Guam(351), Northern Mariana Islands (376), Us Virgin Islands (422) per CIA World Factbook
			replace prop_muslim=0 if inlist(location_id,298,349,351,376,422)
			
	*Add to Dataset
		*Format for merge with cod database
			keep location_id age_group_id year_id sex_id prop_muslim_gbd2016
		
		*Merge and Save
			merge 1:m location_id year_id age_group_id sex_id using FILEPATH/cyst.dta, keep (matched using) nogen
			save FILEPATH/cyst.dta, replace


*--------------------2.4: Custom Variable Transformation

	*Sanitation
		gen logit_sanitation=logit(sanitation_prop)	

}

/*====================================================================
                        3: Run Model
====================================================================*/

	use FILEPATH/cyst.dta, clear

*--------------------3.1: se Custom CoD Function to Submit Model

	run_best_model mepoisson study_deaths i.age_group_id i.super_region_id c.health_system_access_capped i.sex_id  logit_sanitation prop_muslim pop_dens_under_150_psqkm_pct, exposure(sample_size) difficult vce(robust) reffects(|| location_id: R.age_group_id)

*--------------------3.2: Preliminary Model Testing

	//Test dispersion parameter in negative binomial to see if it is significant, if not use poisson
		*test [lndelta]_cons = 1\
	//Test fit of model
		*estat ic


/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

	local description "32.mepoisson CF;+i.SA_inc**studydeaths,i.SR,HSAC,i.sex,logit_sanit,prop_muslim,popdens<150;RE-loc||R.newagecat"
	process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') multiplier(envelope) description("`description'")





log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


