/*Dependencies:  IHME
----------------------------------------------------------------------
Creation Date:    28 Jul 2017 - 11:07:08
Do-file version:  GBD2017
Output:           Submit script to estimate prevalence of NCC with epilepsy from NCC among epileptics
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	clear mata
	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}

* Directory Paths
	***SETTINGS***
	*bundle_name
	local bundle_name = "ntd_cysticer"
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2017"
	*model step
	local step 01
	***************
	**LOCAL DIRECTORIES
	local localRoot "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	**CLUSTER DIRECTORES
	local clusterRoot "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"


* Make and Clear Directories
		*set up base directories on shared
		capture shell mkdir "FILEPATH"
		capture shell mkdir `clusterRoot'
		capture shell mkdir `tmp_dir'
		capture shell mkdir `out_dir'
		capture shell mkdir `log_dir'
		capture shell mkdir `progress_dir'

		*make all directories
		local make_dirs code in tmp local_tmp out log progress
		foreach dir in `make_dirs' {
			capture shell mkdir ``dir'_dir'
		}

*directory for standard code files
	adopath + `"FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH.smcl", replace
	***********************

	*print locals to log
		macro dir

/*====================================================================
                        1: Get GBD Info
====================================================================*/

*--------------------1.1: Demographics

	get_demographics, gbd_team("ADDRESS") clear
		local gbdages `r(age_group_id)'
		local gbdyears `r(year_id)'
		local gbdsexes `r(sex_id)'

*--------------------1.2: Location Metadata

	get_location_metadata, location_set_id(35) clear
		save "`local_tmp_dir'/metadata.dta", replace

		keep if most_detailed==1
		levelsof location_id,local(gbdlocs)clean
		save "`local_tmp_dir'/metadata_detailed.dta", replace

*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`local_tmp_dir'/skeleton.dta", replace


/*====================================================================
                        2: Set MEIDs and Model Version IDs
====================================================================*/


*--------------------2.1: Set MEIDs

	local meid 1479
	local epilepsymeid 2403

*--------------------2.2: Get Best Model Version - NCC

	*local nccmodelid 175205
	get_best_model_versions, entity(modelable_entity) ids(`meid') clear
	local nccmodelid = model_version_id

*--------------------2.3: Get Best Model Version - Epilepsy

	*local epilepsymodelid 175718
	get_best_model_versions, entity(modelable_entity) ids(`epilepsymeid') clear
	local epilepsymodelid = model_version_id



/*====================================================================
                        3: Pull Data to Define Population Not At Risk (PNAR)
====================================================================*/
** PNAR = proportion non-Muslim without access to sanitation

*--------------------3.1: Proportion of People with Access to Sanitation (covariate_id = 142) & Proportion of People that are Muslim (covariate_id = 1218)

	local covariates 142 1218
	foreach cov in `covariates'{

	*Get estimates
		get_covariate_estimates,covariate_id(`cov') location_id(`gbdlocs') year_id(`gbdyears') clear

		local name=covariate_name_short
		local name = subinstr("`name'","_"," ",.)
		local name = "`: word 1 of `name''"

	*Format
		foreach measure in mean lower upper{
			rename `measure'_value `name'_`measure'
		}
		keep covariate_id location_id year_id age_group_id sex_id `name'*

		forval d=0/999{
			gen `name'_prop_`d' = `name'_mean
		}


	tempfile cov_`cov'
	save `cov_`cov'', replace

	}
*--------------------3.3: Merge and Save

	*Merge covs into one table for use
		use `cov_142', clear
		merge 1:1 location_id year_id using `cov_1218', nogen
		save "FILEPATH.dta", replace


*--------------------3.4: Calculate Population Not At Risk (PNAR) (GBD2015 equation)

	forval x=0/999{
			generate double notPAR_`x' = 1 - (1 - religion_prop_`x') * (1 - sanitation_prop_`x')
		}

*--------------------3.5: Format and save

	keep location_id year notPAR* religion*
	gen locyear = string(location_id) + "_" + string(year_id)
		*data is all-sex and all-age so there is no variation by those variables

	save "FILEPATH/not_at_risk.dta", replace

/*====================================================================
                        4: Submit Estimation Script to Qsub
====================================================================*/

*--------------------4.1: Identify geographically restricted location-years

	*Rule out geographic restrictions - get locals of restricted location-years
		import delimited "FILEPATH", clear
			replace year_id=2017 if year_id==2016
			*******
		keep if inlist(year_id,1990,1995,2000,2005,2010,2017)
		gen restricted=1

		merge 1:m location_id year_id using "FILEPATH/not_at_risk.dta", nogen keep(matched using)
		replace restricted=0 if restricted==.
		*egen locyear=concat(location_id year_id),p(_)

	*Save
		save "FILEPATH/not_at_risk_restriced.dta", replace


*--------------------5.2: Submit Qsub

		use "FILEPATH/not_at_risk_restriced.dta", clear
		levelsof locyear,local(locyears) clean

		*remove spaces from locals to pass through qsub
		*local gbdyears = subinstr("`gbdyears'", " ", "_", .)
		local gbdages = subinstr("`gbdages'", " ", "_", .)
		local gbdsexes = subinstr("`gbdsexes'", " ", "_", .)


		foreach locyear in `locyears'{

			di ". `locyear'" _continue

			preserve
				quietly keep if locyear=="`locyear'"
				local restrict=restrict

				if `restrict'==0 {
					quietly save `tmp_dir'/not_at_risk_`locyear'.dta, replace
				}

				restore
				*quietly drop if locyear=="`locyear'"

				*submit qsub
					local jobname locyear`locyear'_NCC_sequelae
					!qsub -P proj_tb -pe multi_slot 5 "FILEPATH" "FILEPATH/seq_splits.do" "`gbdages' `gbdsexes' `meid' `epilepsymeid' `nccmodelid' `epilepsymodelid' `locyear' `restrict'"

		}



/*====================================================================
                        6: Save Results
====================================================================*/

/*
	*save the results to the database

		local meid 2656
		local description "Results - NCC `nccmodelid' & Epilepsy `epilepsymodelid' - sanit w/wlisons cc"
		local in_dir = "`out_dir'/2656/"
		local file_pat = "{location_id}_{year_id}.csv"

		local out_dir "FILEPATH"

		run FILEPATH/save_results_epi.ado

		save_results_epi, modelable_entity_id(`meid') input_file_pattern("`file_pat'") description(`description') input_dir(`out_dir'/2656) measure_id (5) clear


*/


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

References:
1.
2.
3.
