/*====================================================================
project:       GBD2017
Dependencies:  IHME
----------------------------------------------------------------------
Creation Date:    28 Jul 2017 - 11:52:10
Do-file version:  GBD2017
Output:           Estimate prevalence of NCC with epilepsy
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
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


	*directory for standard code files
	adopath + "FILEPATH"

** Set locals from arguments passed on to job
	*get task id from environment
	local job_id : env SGE_TASK_ID

	local gbdages = subinstr("`1'","_"," ",.)
	local gbdsexes = subinstr("`2'","_"," ",.)
	local meid "`3'"
	local epilepsymeid "`4'"
	local nccmodelid "`5'"
	local epilepsymodelid "`6'"
	local locyear "`7'"
	local restrict "`8'"
	di "`restrict'"
	di "`locyear'"

	*read in file
	*import delimited "`tmp_dir'/not_at_risk1.csv", clear
	use "FILEPATH/not_at_risk.dta", clear
	di "`locyear'"
	keep if locyear == "`locyear'"
	tempfile not_at_risk_adjustment
	save `not_at_risk_adjustment', replace



	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH.smcl", replace
	***********************

	*print macros in log
	macro dir

/*====================================================================
                        1: RESTRICTED LOCYEARS
====================================================================*/

	*parse locyear to get location,year
	local locyear2 = subinstr("`locyear'","_"," ",.)
	local location `: word 1 of `locyear2''
	local year `: word 2 of `locyear2''


*--------------------1.1: Save zeroes file for every sequelae meid/metric/sex

	if `restrict' == 1 {

		di in red "Location Geographically Restricted"


		*make skeleton using gbd age and sex locals
		local num_sexes : list sizeof local(gbdsexes)
		local num_ages : list sizeof local(gbdages)
		local table_length = `num_sexes'*`num_ages'

clear
set obs `table_length'
egen age_group_id = fill(`gbdages' `gbdages')
egen sex_id = fill(`gbdsexes' `gbdsexes')

		*add empty draws to make zeroes file
		forval x=0/999{
			display in red ". `x' " _continue
			quietly gen draw_`x'=0
		}

		*format
			gen modelable_entity_id=2656
			gen measure_id=5
			gen location_id = `location'
			gen year_id=`year'
			keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*
			order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*

		*export
			capture shell mkdir "`out_dir'/2656"
			outsheet using "`out_dir'/2656/`location'_`year'.csv", comma replace

	}


/*====================================================================
                        2: ENDEMIC LOCYEARS
====================================================================*/


	else if `restrict' != 1 {

		di in red "Location Endemic"

*--------------------2.1: Check location & year

	di "location `location' year `year'"

*--------------------2.2: Get draws for NCC among epileptics

	*Load NCC prevalence among epileptics at risk
	import delimited "FILEPATH/`location'.csv", clear
	keep if measure_id == 5
	keep if year_id == `year'

	*Format
		forvalues i=0/999 {
			di ". `i'" _continue
			quietly rename draw_`i' ncc_prev_`i'
		}

	*Save
		tempfile ncc_prev_draws
		save "`ncc_prev_draws'", replace


*--------------------2.3: Get draws of Epilepsy Envelope

	*Load Epilepsy prevalence
	import delimited "FILEPATH/`location'.csv", clear
	keep if measure_id == 5
	keep if year_id == `year'

	*Format
		forvalues i = 0/999 {
			di ". `i'" _continue
			quietly rename draw_`i' epilepsy_`i'
		}

	*Save
		tempfile epilepsy_draws
		save "`epilepsy_draws'", replace

*--------------------2.4: Merge NCC Draws + Epilepsy Draws + PNAR

	*Merge NCC Among epileptics + Epilepsy
		merge 1:1 location_id year_id sex_id age_group_id using "`ncc_prev_draws'"

		*CHECKPOINT
		quietly sum _merge
		if `r(min)'<3{
			di in red "Merge Issue Between NCC and Epilepsy Draws"
		}
		drop _merge

	*Merge PNAR
		*merge m:1 location_id year_id using `tmp_dir'/not_at_risk_`locyear'.dta, keep(master match)
		merge m:1 location_id year_id using `not_at_risk_adjustment', keep(master match)

		*CHECKPOINT
		quietly sum _merge
		if `r(min)'<3{
			di in red "Merge Issue Between NCC/Epilepsy Draws and PAR"
		}
		drop _merge

		*outsheet using "FILEPATH/issue.csv", comma replace
/*====================================================================
                        3: Calculate NCC with Epilepsy
====================================================================*/

** Multiply NCC prevalence among all epileptics with epilepsy envelope, correcting for population not at risk
*Calculate prevalence of epilepsy due to NCC as: P * (NM-N) / (NM-1), where

*--------------------3.1:

		*calculate final values
			forvalues i = 0/999 {
				di ". `i'" _continue
				quietly gen draw_`i' = epilepsy_`i' * (ncc_prev_`i' * religion_prop_`i' - ncc_prev_`i') / (ncc_prev_`i' * religion_prop_`i' - 1)
			}


		*format
			replace modelable_entity_id=2656
			replace measure_id=5
			keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*
			order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*

		*export
			outsheet using "`out_dir'/2656/`location'_`year'.csv", comma replace


}




***************************

file open progress using "FILEPATH"/`locyear'.txt, text write replace
file write progress "complete"
file close progress



log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:
