// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Leprosy India adjustment
// Author:		USERNAME


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILENAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILENAME"
	}

	** set adopath
	adopath + "FILENAME"
	** shell file
	local shell_file "FILENAME/stata_shell.sh"
	** set code directory
	local code_dir "FILENAME"


	local root_j_dir "FILENAME"
	// base directory on share
	local root_tmp_dir "FILENAME"
	// timestamp of current run (i.e. 2014_01_17)
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr("`date'"," ","_",.)

	** set directories
	// directory for external inputs
	local in_dir "`root_j_dir'/02_inputs"
	// directory for output on XXX
	local out_dir "`root_j_dir'/03_steps/`date'/india_adjustment"
	// directory for output on XXX
	capture mkdir "`root_tmp_dir'/03_steps/`date'"
	local tmp_dir "`root_tmp_dir'/03_steps/`date'/india_adjustment"
	capture mkdir "`root_tmp_dir'/03_steps/`date'"
	capture mkdir "`tmp_dir'"
	capture mkdir "`out_dir'"

	** set up log files
	cap mkdir "`tmp_dir'/02_temp"
	cap mkdir "`tmp_dir'/02_temp/02_logs"

	** get location information
	get_location_metadata, location_set_id(35) clear
	keep if most_detailed == 1 & is_estimate == 1
	tempfile locs 
	save `locs', replace

	*********************************************************************************************************************************************************************
	*********************************************************************************************************************************************************************

	** pull in data
	import excel "FILENAME/india_phfi_splits.xlsx", firstrow clear
	gen inc_rate = cases/sample_size
	gen standard_error = sqrt(inc_rate*(1-inc_rate)/sample_size)
	tempfile data
	save `data', replace
	** get locations
	levelsof location_id, local(india_locs)
	** decide the locations we need to pull draws for
	use `locs', clear
	gen keep = 0
	foreach loc of local india_locs {
		replace keep = 1 if location_id == `loc' | parent_id == `loc'
	}

	keep if keep == 1
	levelsof location_id, local(india_detailed)

	foreach location of local india_detailed {
		// submit job
		local job_name "india_adj_`location'"
		di "submitting `job_name'"
		// di in red "location is `location'" // for tests
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/india_adjustment_parallel.do" ///
		"`date' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 100		

	}

