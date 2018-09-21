/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 CLEAN
Output:           Save zeroes files for all output meids for nonendemic countries
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
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
	local step 02
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

	*directory for standard code files
	adopath + FILEPATH

	** Set locals from arguments passed on to job
	local location "`1'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/nonendemiczeroes_`location'_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: CREATE EMPTY DRAWS FOR NON-ENDEMIC COUNTIRES
====================================================================*/


*--------------------1.1:

	*Load zeroes file for this location
		use "`tmp_dir'/nonendemic_allmeids_zeroes_`location'.dta", clear
		levelsof modelable_entity_id,local(meids) clean

	*Output 1 file per meid
		foreach meid in `meids'{
			preserve
			keep if modelable_entity_id == `meid'
			outsheet using "`out_dir'/`meid'_prev/`location'.csv", comma replace
			restore
		}

***************************

file open progress using `progress_dir'/`location'.txt, text write replace
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


