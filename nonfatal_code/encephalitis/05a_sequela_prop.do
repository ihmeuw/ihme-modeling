// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	
// Description:	Generate 1000 draws of each sequela proportion for each etiology-outcome combination, and normalize to sum to 1. This file also reformats 
// 				frequency_distributions file
// Number of output files: 12
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

	// base directory on J
	local root_j_dir `1'
	// base directory on clustertmp
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
	// define if estimate sequelae for long_modsev (Y=1, N=0)
	local mort0 = 1
	local mort1 = 1
	// grouping
	local grouping "long_mild long_modsev" // taking out _vision, uploading in step 04b
	// metric
	local metric 5
	// functional
	local functional "encephalitis"
	// directory for external inputs
	local in_dir "`root_j_dir'/02_inputs"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for standard code files
	adopath + "SHARED FUNCTIONS"

	// get demographics
	get_location_metadata, location_set_id(9) clear
	keep if most_detailed == 1 & is_estimate == 1
	levelsof location_id, local(locations)
	clear

	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	cap erase "`out_dir'/finished.txt"
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/03_steps/`date'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/03_steps/`date'/`dir'/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	foreach group of local grouping {
		local conduct = 0
		if "`group'" == "long_mild" local conduct = `mort0'
		if "`group'" == "long_modsev" local conduct = `mort1'
		if `conduct' == 1 {
		
			insheet using "`in_dir'/frequency_distributions.csv", clear
			keep if regexm(output_code,"other") == 1
			rename output_code grouping
			replace grouping = subinstr(grouping,"other_","",.)	
			keep if grouping == "`group'"
		
			forvalues a = 0/5 {
				capture gen present_`a' = .
				local count = 0
				count if healthstate_`a' == ""
				local count = r(N)
				if `count' == 1 {
					drop healthstate_`a' dw_`a' proportion_`a' lb_`a' ub_`a'
				}
				else {
					replace present_`a' = 1
				}
			}
			
			preserve
			
			forvalues b = 0/5 {
				if present_`b' == 1 {
					levelsof healthstate_`b', local(state) clean
					levelsof proportion_`b', local(proportion_`b')
					levelsof lb_`b', local(lb_`b')
					levelsof ub_`b', local(ub_`b')
					clear

					local m = `proportion_`b''
					local l = `lb_`b''
					local u = `ub_`b''
					local s = (`u'-`l') / (2*1.96)
					local alpha1 = (`m'*(`m'-`m'^2-`s'^2))/`s'^2
					local alpha2 = (`alpha1'*(1-`m'))/`m'
					set obs 1000
					gen all_major = rbeta(`alpha1',`alpha2')
					xpose, clear
					
					gen state = "`state'"
					gen code = "`group'"
					gen measure_id = `metric'
					order measure_id code state
					tempfile state_`b'
					save `state_`b'', replace
					clear
					
					restore, preserve
				}
				else {
					capture erase `state_`b''
				}
			}
			capture restore, not
			clear
			
			forvalues c = 0/5 {
				capture append using `state_`c''
				capture erase `state_`c''
			}
			forvalues d = 1/1000 {
				local e = `d' - 1
				qui rename v`d' v_`e'
			}
			
			preserve
			collapse (sum) v_*, by(code measure_id) fast
			rename v_* total_*
			tempfile freq_`group'
			save `freq_`group'', replace
			restore
			
			merge m:m code measure_id using `freq_`group'', keep(3) nogen
			erase `freq_`group''
			
			forvalues f = 0/999 {
				if total_`f' > 0 {
					qui replace v_`f' = v_`f' / total_`f'
					drop total_`f'
				}
				else {
					qui drop total_`f'
				}
			}
			save "`tmp_dir'/03_outputs/01_draws/`functional'_`group'.dta", replace
			clear
		}
		clear
	}
	
	di "`step_num' sequential runs completed"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// write check file to indicate step has finished
		file open finished using "`out_dir'/finished.txt", replace write
		file close finished
		
	// if step is last step, write finished.txt file
		local i_last_step 0
		foreach i of local last_steps {
			if "`i'" == "`this_step'" local i_last_step 1
		}
		
		// only write this file if this is one of the last steps
		if `i_last_step' {
		
			// account for the fact that last steps may be parallel and don't want to write file before all steps are done
			local num_last_steps = wordcount("`last_steps'")
			
			// if only one last step
			local write_file 1
			
			// if parallel last steps
			if `num_last_steps' > 1 {
				foreach i of local last_steps {
					local dir: dir "`root_j_dir'/03_steps/`date'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "`root_j_dir'/03_steps/`date'/`dir'/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "`root_j_dir'/03_steps/`date'/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	