/*====================================================================
project:       GBD2017
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2017
Output:           Estimate wasting due to STH
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
	local gbd = "gbd2017"
	*model step
	local step 03
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*directory for output of draws
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

*Directory for standard code files
	adopath + "FILEPATH"

** Set locals from arguments passed on to job

		*get task id from environment
			local job_id : env SGE_TASK_ID

		*get arguments passed through
			local gbdages = subinstr("`1'","_"," ",.)
			local gbdsexes = subinstr("`2'","_"," ",.)
			local gbdyears = subinstr("`3'","_"," ",.)
			local wasting_ages = subinstr("`4'","_"," ",.)

		*get other arguments task-specific
			use "`tmp_dir'/task_directory.dta", clear
			levelsof modelable_entity_id, local(my_meids)
			keep if id==`job_id'
			local locyear = locyear
			local restrict = restrict
			*local meid = modelable_entity_id
			di "`my_meids'"

		*parse locyear to get location,year
			local locyear2 = subinstr("`locyear'","_"," ",.)
			local location `: word 1 of `locyear2''
			local year `: word 2 of `locyear2''


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	capture mkdir "`log_dir'/`meid'"
	log using "`log_dir'/`meid'/`location'_`year'_`date'_`time'.smcl", replace
	***********************

	*print macros in log
	macro dir


*Code Testing
	local testing
	*set equal to "*" if not testing


/*====================================================================
                        1: Non-ENDEMIC LOC-YEARS
====================================================================*/
foreach meid in `my_meids' {

if `restrict' == 1 {



*--------------------1.1: Make zeroes file


		di in red "`Location-Year `locyear' is Restricted"

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

		*add missing columns
		di "`meid'"
		gen measure_id=5
		gen location_id = `location'
		gen year_id = `year'
		gen modelable_entity_id = `meid'


*--------------------1.2: Format & Export .csv's

		*format
		keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
		order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*

		*export
		outsheet using "`out_dir'/`meid'/`location'_`year'.csv", comma replace

}

/*====================================================================
                        2: ENDEMIC LOC-YEARS
====================================================================*/

else if `restrict' != 1 {

		di in red "`Location-Year `locyear' is Endemic"

*--------------------2.1: Prep Z-scores
**Z-score change in weight size per heavy prevalent case.
		local effsize = .493826493
		local effsize_l =	.389863021
		local effsize_u =	.584794532

		local effsize_sd = (`effsize_u' - `effsize_l') / (2*invnorm(0.975))

		forvalues i = 0/999 {
			local z_change_`i' = `effsize' + rnormal() * `effsize_sd'
		}

*--------------------2.2: Calculate Wasting

	di "`meid'"
	use "`tmp_dir'/wasting_prepped_`meid'_all.dta", clear
	keep if locyear == "`locyear'"


	*Calculate total wasting due to heavy worm infection
		forvalues i = 0/999 {
			*Prev of wasting due to worms as function of change in z-score
				quietly replace sumsth_`i' = wast_`i' - normal(invnormal(wast_`i') - `z_change_`i'' * sumsth_`i') if !missing(sumsth_`i')
				quietly replace sumsth_`i' = 0 if missing(sumsth_`i') | wast_`i' == 0
		}

		`testing' tempfile wast_split
		`testing' quietly save `wast_split', replace

	*Attribute wasting to different worm species and write draw files
		`testing' use `wast_split', clear

		forvalues i = 0/999 {
			quietly replace prop_`i' = prop_`i' * sumsth_`i'
			quietly replace prop_`i' = 0 if missing(prop_`i')
		}

		rename prop_* draw_*
		drop sumsth_*

		tempfile ready
		save `ready', replace

*--------------------2.3: Format & Export

	*Make older ages into zero
clear
local num_ages : list sizeof gbdages
local num_sexes : list sizeof gbdsexes
local table_length = `num_ages' * `num_sexes'
set obs `table_length'
egen age_group_id = fill(`gbdages' `gbdages')
sort age_group_id
egen sex_id = fill(`gbdsexes' `gbdsexes')
drop if age_group_id <= 5
forvalue i = 0/999 {
	gen draw_`i' = 0 if age_group_id>5
}

	*append to ready file
		append using `ready'

	*Export csvs of draw files
		replace measure_id = 5
		replace location_id = `location'
		replace year_id=`year'
		replace modelable_entity_id=`meid'
		keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
		order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
		sort sex_id age_group_id

		outsheet using "`out_dir'/`meid'/`location'_`year'.csv", comma replace


}
}


***************************
file open progress using `progress_dir'/`locyear'_`meid'.txt, text write replace
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
