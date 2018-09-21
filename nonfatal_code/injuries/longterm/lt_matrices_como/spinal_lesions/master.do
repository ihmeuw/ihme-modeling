// Parallelize pre-COMO spinal lesion severity splits
// USERNAME
// DATE

// PREP STATA (DON'T EDIT)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

// base directory on FILEPATH
local root_j_dir `1'
// base directory on FILEPATH
local root_tmp_dir `2'
// timestamp of current run (i.e. 2014_01_17)
local date `3'
// step number of this step (i.e. 01a)
local step_num `4'
// name of current step (i.e. first_step_name)
local step_name `5'
// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
local hold_steps `6'
// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
local last_steps `7'
// directory where the code lives
local code_dir `8'
// directory for external inputs
local in_dir "`root_j_dir'/FILEPATH"
// directory for output on the J drive
local out_dir "`root_j_dir'FILEPATH"
// directory for output on clustertmp
local tmp_dir "`root_tmp_dir'FILEPATH"
// directory for standard code files
adopath + "FILEPATH"


get_demographics, gbd_team(epi) clear
global year_ids `r(year_ids)'
global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'


// parallelize by location/year/sex
local code "FILEPATH/FILEPATH.do"
foreach location_id of global location_ids {
	foreach year of global year_ids {
		foreach sex of global sex_ids {
		! qsub -P proj_injuries_2 -N spinal_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
		}
	}
}

// END
