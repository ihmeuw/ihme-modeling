/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER        
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
	local step 04
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
	*directory for output of draws
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH'"
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

** Set locals from arguments passed on to job
		local location "`1'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/trich_log_`date'_`time'.smcl", replace
	***********************

	*print macros in log
	macro dir

/*====================================================================
                        1: Setup
====================================================================*/


*--------------------1.1: Set Locals

	local meid 3001   
	local model 188435 

	*Malaysia, Philippines
		if `location'==13 | `location'==16 { 
		*if inlist(`location', 13, 16) { 
			local inLoc 9
		}
	
	*Kiribai, Marshall Islands, Fiji
		else if `location'==23 | `location'==24 | `location'==22 {
			local inLoc 21
		}
		
	*South Africa + Subnationals, Swaziland
		else if inlist(`location', 196, 482, 483, 484, 485, 486, 487, 488, 489, 490, 197) {
			local inLoc 192
		}
	
	*Jamaica
		else if `location'==115 {
			inLoc 104
		}
		
	*Other locations
		else {
			local inLoc `location'
		}

	macro dir

*--------------------1.3: 

	local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235

	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') location_ids(`inLoc') model_version_id(`model') measure_ids(5) age_group_ids(`ages') source(dismod) clear
	drop model_version_id
		
	replace location_id = `location'

	export delimited using `out_dir'/trich/`location'.csv, replace




*************************************************************************
file open progress using `progress_dir'/`location'.txt, text write replace
file write progress "complete"
file close progress
  
log close
exit
/* End of do-file */
