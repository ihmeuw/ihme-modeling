// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	Get draws of short term durations using draws from treated and untreated for each ncode and draws of health system scores

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "DIRECTORY"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "DIRECTORY"
	}
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "01d"
		local 5 durations

		local 8 "FILEPATH"
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
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	
	local make_durations = 0
	local interpolate_to_annual = 1
	local tester = 0

	local make_pct_treated = 0

// Settings
	local debug 0
	set type double, perm
	set seed 0	
	
// Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local pct_file "`out_dir'/FILEPATH.dta"
	local dur_file "`out_dir'/FILEPATH.dta"
	local output_dir "`tmp_dir'/FILEPATH"
	local checkfile_dir "`tmp_dir'/FILEPATH"
	
// Import functions
	adopath + `code_dir'/ado
	adopath + "`gbd_ado'"
	
// start timer
	start_timer, dir("`diag_dir'") name("`step_name'")

// Get list of years and ncodes
	load_params
	local expandbyn = wordcount("${n_codes}")
	get_demographics, gbd_team(epi) clear

	global year_ids `r(year_ids)'
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'
	global age_group_ids `r(age_group_ids)'

// get percent treated
	if `make_pct_treated' == 1 {
		get_pct_treated, prefix("$prefix") code_dir("`code_dir'")
		save "`pct_file'", replace
	}
	
// Get durations
	import excel ncode=A mean_inp=C se_inp=D ll_inp=E ul_inp=F mean_otp=G se_otp=H ll_otp=I ul_otp=J mean_mul=K ll_mul=L ul_mul=M using "`in_dir'/FILEPATH.xlsx", cellrange(A10) clear

	foreach i in inp otp {
	* create SE from LL and UL when SE doesn't exist
		replace se_`i' = (ul_`i' - ll_`i') / 3.92 if se_`i' == .
		drop ll_`i' ul_`i'
		
	* convert days to years
		foreach var in mean se {
			replace `var'_`i' = `var'_`i' / 365.25
		}
	}
	
	** keep just multipliers
	preserve
	keep ncode *_mul
	tempfile mults
	save `mults'
	restore
	
	** reshape long
	drop *_mul
	reshape long mean_ se_, i(ncode) j(inp_str) string
	gen inpatient = 0
	replace inpatient = 1 if inp_str == "inp"
	drop inp_str
	
	** merge on untreated multipliers
	merge m:1 ncode using `mults', assert(match) nogen
	calc_se ll_mul ul_mul, newvar(se_mul)
	drop ll_mul ul_mul
	
	** generate treated/untreated duration draws
	forvalues x = 0/$drawmax {
		gen treat_`x' = rnormal(mean_,se_)
		gen untreat_`x' = treat_`x' * rnormal(mean_mul,se_mul)
		
		** fix situation where assumed no difference in treat/untreat
		replace untreat_`x' = treat_`x' * mean_mul if se_mul == 0
		
		** cap at 1 year
		replace untreat_`x' = 1 if untreat_`x' > 1
	}
	drop mean_ se_ mean_mul se_mul
	
	** save
	save "`dur_file'", replace
	

// Parallelize the country-year-specific multiplication

	** set mem/slots and create job checks directory
	
if `tester' == 1 {
	global location_ids 101
	global year_ids 2000 2005
}

// submit jobs
if `make_durations' == 1 {
	local code = "durations/FILEPATH.do"
	local n 0
	foreach location_id of global location_ids {
		foreach year of global year_ids {
			local name `functional'_`step_num'_`location_id'_`year'
			! qsub -o FILEPATH -e FILEPATH -P proj_injuries -N stdur_`location_id'_`year' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year'"
			local n = `n' + 1
		}
	}
}

if `interpolate_to_annual' == 1 {
	local code = "durations/FILEPATH.do"
	local n 0
	foreach location_id of global location_ids {
			local name `functional'_`step_num'_`location_id'
			! qsub -o FILEPATH -e FILEPATH -P proj_injuries -N ipo_stdur_`location_id' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"
			local n = `n' + 1
	}	
}
	

	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

// write check file to indicate step has finished
	//file open finished using "`out_dir'/FILEPATH.txt", replace write
	//file close finished
	
	end_timer, dir("`diag_dir'") name("`step_name'")
	//sum_times, dir("`diag_dir'")

// close log if open
	log close master
		
