// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	calculate inpatient and "other" hierarchies from followup studies
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

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
	// SETTINGS TO DO A CHECK-RUN
	local check 99
	if `check' == 1 {
		local 1 _inj
		local 2 gbd2015
		local 3 DATE
		local 4 02b
		local 5 hierarchies
		local 6 "FILEPATH"
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
	// directory for output on the FILEPATH
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	di "`in_dir'"
	di "`tmp_dir'"
	BREAK
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// Settings
	local debug 0
	
	set type double, perm

// Incorporate GBD-related ado functions
	adopath + "FILEPATH"

// Filepaths
	local inj_dir "`code_dir'"
	local steps_dir "`inj_dir'/FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local pooled_fu_input "`steps_dir'/FILEPATH.csv"
	local parameters "`in_dir'/FILEPATH"	
	local checkfile_dir "`out_dir'/FILEPATH"
		
// Get map of DW
	adopath + "`code_dir'/ado"
	get_ncode_dw_map, out_dir("`out_dir'/FILEPATH") category("lt_t") prefix("$prefix")
	
	hierarchy_params, prefix($prefix) repo(`code_dir') steps_dir(`steps_dir')
	
	use "$prepped_filepath", clear
	
// get N for n-codes so we can merge on these numbers for N-codes that we set to 0 disability eventually for one or more of the severities
	drop if inpatient == .
	collapse (sum) INJ_*, by(inpatient)
	reshape long INJ_, i(inpatient) j(ncode) string
	rename INJ_ N_
	reshape wide N_, i(ncode) j(inpatient)

	// drop INJ_ prefix
	replace ncode = subinstr(ncode,"INJ_","",1)

	// account for fact that pooled analysis includes both inpatient and non-inpatient cases
	replace N_2 = N_0 + N_1 + N_2

	// save
	tempfile replacement_Ns
	save `replacement_Ns'
	save "`out_dir'/FILEPATH.dta", replace
	
// Submit jobs to run iterative regression process separately for inpatient, non-inpatient, and pooled analyses
	// set memory (gb) for each job
	local mem 8
	if `mem' < 2 local mem 2
	local slots = ceil(`mem'/2)
	local mem = `slots' * 2
	local k=0
	
	!rm -r "`checkfile_dir'"
	mkdir "`checkfile_dir'"
	
	** submit job for each of inpatient, outpatient, pooled
	forvalues hosp = 0/2 {
		local name hierarchy_`hosp'
		
		if "$prefix"=="J:" {
			do "`code_dir'/`step_name'/FILEPATH.do" "`root_j_dir'" "`root_tmp_dir'" "`date'" "`step_num'" "`step_name'" "`code_dir'" "`hosp'"
		}
		
		di "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `hosp'"
		else !qsub -N `name' -P proj_injuries -pe multi_slot `slots' -l mem_free=`mem'  "FILEPATH.sh" "`code_dir'/`step_name'/FILEPATH.do" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `hosp'"
		
		local ++k
		
	}
	 
	** end hosp loop
	
	** wait for hospital files to be written
	local i = 0
	while `i' == 0 {
		local checks : dir "`checkfile_dir'" files "finished_*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `k' jobs finished"
		if (`count' == `k') continue, break
		else sleep 60000
	}
	
	
// bring in the hospital=2 numbers
use "`out_dir'/FILEPATH.dta", clear

// Merge on the inpatient and non-inpatient datasets
forvalues x = 0/1 {
	merge 1:1 ncode using "`out_dir'/FILEPATH.dta", nogen
}

// Name variables appropriately
rename (*_0 *_1 *_2) (*_noninp *_inp *_pool)


// indicate which n-codes have only-inpatient cases
foreach ncode of global hosp_only {
	assert dw_noninp == . if ncode == "`ncode'"
	replace dw_noninp = 9 if ncode == "`ncode'"
}

preserve
get_ncode_names, prefix("$prefix")
rename n_code ncode
tempfile ncodes_names_map
save `ncodes_names_map'
restore

// merge on names and format results
merge 1:1 ncode using `ncodes_names_map', keep(match master) nogen
assert !regexm(ncode,"GS")

// sort, order, save
gsort - dw_pool
order ncode name *_inp *_noninp *_pool
format dw* %16.0g
outsheet using "`out_dir'/FILEPATH.csv", comma names replace

** run the expert hierarchy code in python, passing it the file path for the empirical hierarchy
confirm file "`code_dir'/`step_name'/FILEPATH.py"

local args "`out_dir'/FILEPATH/ `code_dir'/ `out_dir'/FILEPATH/"
display "`args'"
!FILEPATH "`code_dir'/`step_name'/FILEPATH.py" `args'

copy "`out_dir'/FILEPATH.xls" "`tmp_dir'/FILEPATH.xls", replace

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)
	

// write check file to indicate step has finished
	file open finished using "`out_dir'/FILEPATH.txt", replace write
	file close finished
	
// close log if open
	log close master
		