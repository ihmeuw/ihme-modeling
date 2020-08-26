************************************************************
** Description: This script linearlly interpolates prevalence at age 0-6d, 7-27d and 28d (missing value) at the draw level
** for the first DisMod model results for neonatal preterm, enceph and sepsis. The output is prevalence at 28 days. 
************************************************************
// LOAD SETTINGS FROM MASTER CODE

	// prep stata
	clear all
	set more off
	set maxvar 32000
	global prefix "FILEPATH"
	set odbcmgr unixodbc

// *********************************************************************************************************************************************************************

// locals 
local working_dir `c(pwd)'
local me_ids 1594

adopath + "FILEPATH"

// directories
local data_dir "FILEPATH"

// Submit jobs
foreach me_id of local me_ids {

	// get acause

	** new preterm process
	**if `me_id' == 1557 | `me_id' == 1558 | `me_id' == 1559 {
	**	di "Me_id is `me_id'"
	**	local acause "neonatal_preterm"
	**}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local acause "neonatal_enceph"
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local acause "neonatal_sepsis"
	}

	di "Me_id is `me_id' and acause is `acause'"

	!qsub -l fthread=20 -l m_mem_free=20G -l h_rt=24:00:00 -l archive -N get_draws_`me_id' -P proj_neonatal -q all.q -e FILEPATH -o FILEPATH -cwd "FILEPATH/stata_shell.sh" "FILEPATH/04_interpolate_parallel.do" "`me_id' `acause'"

}


// wait for final results files to finish 
foreach me_id of local me_ids {

	// get acause
	if `me_id' == 1557 | `me_id' == 1558 | `me_id' == 1559 {
		di "Me_id is `me_id'"
		local acause "neonatal_preterm"
	}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local acause "neonatal_enceph"
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local acause "neonatal_sepsis"
	}

	capture noisily confirm file "FILEPATH/all_draws.dta"
	while _rc!=0 {
		di "File all_draws.dta for `acause' `me_id' not found :["
		sleep 60000
		capture noisily confirm file "FILEPATH/all_draws.dta"
	}
	if _rc==0 {
		di "File all_draws.dta for `acause' `me_id' found!"
	}		

}

