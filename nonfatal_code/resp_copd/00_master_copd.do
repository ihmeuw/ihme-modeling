////////////////////////////////////////////////
// Title: Master script for COPD severity split
// Date updated: 6/13/2017
// Step 1: Calculate conversion factor for Gold Class to GBD severities. Only affects USA 2005 and implies some Gold Class Squeezing. Output is age/sex specific factors.
// Step 2: Squeeze gold class draws, apply conversion factors to convert from gold class to severity.
// Step 3: Upload data.
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//prepare stata
	clear all
	set more off
	macro drop _all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
	
	local c_date = c(current_date) // gets current date for folder name
	local date_string = subinstr("`c_date'", " " , "_", .) // reformat c_date
	local ver_desc = "v`date_string'"
	local folder_name = "`ver_desc'"
	// dUSERt to run all stages, but use these toggles to only run one.
	local stage1 1
	local stage2 1
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
		do "FILEPATH/01_gc_to_sev_conversion.do" "`ver_desc'"
	}

////////////////////////////////////////////////////////////////////////////////
//Stage 2: Apply the conversion to all countries and create COPD severity draws
////////////////////////////////////////////////////////////////////////////////
	if `stage2' == 1 {
		//make a folder to store the results
			cap mkdir "FILEPATH/`folder_name'"
		//now populate the me_ids for the various splits
			cap mkdir "FILEPATH/`folder_name'/`asympt'" 
			cap mkdir "FILEPATH/`folder_name'/`mild'" 
			cap mkdir "FILEPATH/`folder_name'/`moderate'"
			cap mkdir "FILEPATH/`folder_name'/`severe'"
			
		//pull epi locations
		qui{
			do "FILEPATH/get_location_metadata.ado"
		}
		get_location_metadata, location_set_id(9) clear
		levelsof location_id, local(location_ids)

		local iter 0 // this helps us number the jobs
		// for each location, submit a job to run 02_squeeze_apply_conversion.do
		// these jobs create COPD severity draws
		foreach loc_id of local location_ids{

			! qsub -N sq_`loc_id'_`iter' -pe multi_slot 4 -P proj_custom_models -l mem_free=4G "`stata_shell'" "FILEPATH/02_squeeze_apply_conversion.do" "`folder_name' `loc_id' `output' `asympt' `mild' `moderate' `severe'"
			
			di "`loc_id' | sq_`loc_id'_`iter'"
			
			local old_id `old_id',sq_`loc_id'_`iter'
			
			local iter = `iter'+1		
			
		}
	}

////////////////////////////
//Stage 3: Upload results
////////////////////////////

	if `stage3'==1{
		if `stage2' ==1{
			! qsub -N copd_save_results -hold_jid "`old_id'" -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "FILEPATH/03_check_outputs.do" "`output' `folder_name'"
		}
		else{
			! qsub -N copd_save_results -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "FILEPATH/03_check_outputs.do" "`output' `folder_name'"
		}
	}

	clear
	
