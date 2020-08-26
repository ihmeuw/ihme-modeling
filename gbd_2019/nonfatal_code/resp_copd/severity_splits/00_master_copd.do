//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Title: Master script for COPD severity split
// Authod: USER, updated by USER, inherited by USER
// Date updated: 6/13/2017
// Step 1: Calculate conversion factor for Gold Class to IHME severities. Only affects USA 2005 and implies some
//					Gold Class Squeezing. Output is age/sex specific factors.
// Step 2: Squeeze gold class draws, apply conversion factors to convert from gold class to severity.
// Step 3: Upload data.
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//prepare stata
	clear all
	set more off
	macro drop _all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set odbcmgr unixodbc
		local code "FILEPATH"
		local stata_shell "FILEPATH"
		local output "FILEPATH"
		local output2 "FILEPATH"
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
		local code "FILEPATH"
	}

	local c_date = c(current_date) // gets current date for folder name
	local date_string = subinstr("`c_date'", " " , "_", .) // reformat c_date
	local ver_desc = "v`date_string'"
	*// local ver_desc = "v5_Oct_2019" // uncomment and change if you want to use a ver_desc that isn't from today
	local folder_name = "`ver_desc'"

	// default to run all stages, but use these toggles to only run one. (do one at a time)
	local stage1 0
	local stage2 0
	local stage3 1

	//me_ids for folder creation
	local asympt 3065
	local mild 1873
	local moderate 1874
	local severe 1875



/////////////////////////////////////////////////
//Stage 1: Create new gold class severity splits
/////////////////////////////////////////////////
	if `stage1' ==1 {
		disp("Stage 1 --------------------------")
		do "`code'/01_gc_to_sev_conversion.do" "`ver_desc'"
	}

////////////////////////////////////////////////////////////////////////////////
//Stage 2: Apply the conversion to all countries and create COPD severity draws
////////////////////////////////////////////////////////////////////////////////
	if `stage2' == 1 {
		run "FILEPATH/get_location_metadata.ado"
		disp("Stage 2 -----------------------")
		//make a folder to store the results
			cap mkdir "`output2'/`folder_name'"
		//now populate the me_ids for the various splits
			cap mkdir "`output2'/`folder_name'/`asympt'"
			cap mkdir "`output2'/`folder_name'/`mild'"
			cap mkdir "`output2'/`folder_name'/`moderate'"
			cap mkdir "`output2'/`folder_name'/`severe'"
			di "`output2'/`folder_name'/`severe'"

		//pull epi locations
		qui{
			do "FILEPATH/get_location_metadata.ado"
		}
		get_location_metadata, location_set_id(35) gbd_round_id(6) clear
		keep if most_detailed==1
		levelsof location_id, local(location_ids)

		local iter 0 // this helps us number the jobs
		// for each location, submit a job to run 02_squeeze_apply_conversion.do
		// these jobs create COPD severity draws
		//local loc_ids 44651 //use these two lines if you only want to run for specific locations
		//foreach loc_id of local loc_ids {
		*// local location_ids 374 //or use this one line
		foreach loc_id of local location_ids{

			! qsub -N sq_`loc_id'_`iter' -l m_mem_free=7G  -l fthread=3 -l archive -l h_rt=1:30:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/02_squeeze_apply_conversion.do" "`folder_name' `loc_id' `output2' `asympt' `mild' `moderate' `severe'"

			di "`loc_id' | sq_`loc_id'_`iter'"
			local old_id `old_id',sq_`loc_id'_`iter'
			local iter = `iter'+1

		}

	}

////////////////////////////
//Stage 3: Upload results
////////////////////////////

	if `stage3'== 1{
		disp("Stage 3 ----------------------------------------")
		if `stage2' ==1{
			// ! qsub -N copd_mild_save_results -hold_jid "`old_id'" -l m_mem_free=50G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' mild"
			// ! qsub -N copd_moderate_save_results -hold_jid "`old_id'"-l m_mem_free=50G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' moderate"
			// ! qsub -N copd_severe_save_results -hold_jid "`old_id'" -l m_mem_free=50G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_yld -q all.q -o FILEPATH -e /FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' severe"
			// ! qsub -N copd_asympt_save_results -hold_jid "`old_id'" -l m_mem_free=50G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' asympt"
		}
		else{
			! qsub -N copd_mild_save_results -l m_mem_free=70G  -l fthread=4 -l archive -l h_rt=24:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' mild"
			! qsub -N copd_moderate_save_results -l m_mem_free=70G  -l fthread=4 -l archive -l h_rt=24:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' moderate"
			! qsub -N copd_severe_save_results -l m_mem_free=70G  -l fthread=4 -l archive -l h_rt=24:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' severe"
			! qsub -N copd_asympt_save_results -l m_mem_free=70G  -l fthread=4 -l archive -l h_rt=24:00:00 -P proj_yld -q all.q -o FILEPATH -e FILEPATH "`stata_shell'" "`code'/03_check_outputs.do" "`output2' `folder_name' asympt"
		}
	}

	clear
