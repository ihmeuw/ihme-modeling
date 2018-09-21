//Master Script for Updated Pneumo Exclusions -- Coal Worker's pneumoconiosis

//prepare stata
	clear all
	set more off
	
	macro drop _all
	set maxvar 32000
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not

	adopath + "FILEPATH" // load in shared functions
	
	//set locals
	local code "FILEPATH"
	local stata_shell "FILEPATH"
	local stata_mp_shell  "FILEPATH"
	local output "FILEPATH"

	local c_date = c(current_date) // gets current date for folder name
	local date_string = subinstr("`c_date'", " " , "_", .) // reformat c_date
	local ver_desc = "v`date_string'"
	local folder_name = "`ver_desc'"
	local generate_files 0
	
	//make directories
	cap mkdir "`output'"
	cap mkdir "`output'/`ver_desc'"
	
	//me_ids (ac = all country DisMod model, end = final ME with exclusions applied)
	local coal_workers_ac 1893
	local coal_workers_end 3052
	
	//Zero out exclusions
	if `generate_files' == 1{
		
		get_covariate_estimates, covariate_id(15) clear
				
		gen no_coal = mean_value ==0
		bysort location_id: egen coal_id = total(no_coal)
		keep if coal_id >30
				
		keep location_id
		duplicates drop
			
		//get the list of excluded countries
		levelsof location_id, local(excluded_locs) clean

		get_location_metadata, location_set_id(9) clear
		levelsof location_id, local(location_ids)
			
		//convert list to dataframe
		clear
		set obs 1000
		gen location_id = .
		local row = 0
		foreach i of local location_ids {
			local row = `row' + 1
			qui replace location_id = `i' in `row'
		}
		drop if location_id == .
			
		// generate variable "exclusion" and set to 1 for excluded locations
		gen exclusion =0
		foreach loc of local excluded_locs{
			qui replace exclusion = 1 if location_id == `loc'
		}
			
		//launch the jobs
		local jobs
		foreach location_id of local location_ids {
			preserve
				keep if location_id == `location_id'
				local excluded = exclusion[1]
				! qsub -N coal_workers_`location_id' -pe multi_slot 1 -P proj_custom_models "`stata_mp_shell'" "`code'/01_pneumo_to_csv.do" "`location_id' `output' `ver_desc' `excluded'"
				local jobs `jobs',coal_workers_`location_id'
			restore
		}
		
		! qsub -N save_results_coal -hold_jid "`jobs'" -pe multi_slot 4 -P proj_custom_models "`stata_mp_shell'" "`code'/02_save_pneumo_results.do" "`output' `ver_desc'"
	

	} 

	else {  // end loop for "if `'generate files' == 1"
		do "`code'/02_save_pneumo_results.do" "`output'" "`ver_desc'"
	}

	
