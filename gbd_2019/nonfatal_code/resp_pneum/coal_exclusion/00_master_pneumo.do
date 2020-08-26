//Master Script for Updated Pneumo Exclusions -- Coal Worker's pneumoconiosis
//Author: USER GBD 2015
//Edited by: USER GBD 2016
//Note: GBD 2015 used exclusions for asbestosis as well, but those exclusions were removed
//		for GBD 2016 because we changed the asbestos covariate to an asbestos consumption
//		covariate that has no zeros by country (was previously asbestos production)

// NOTE FOR USERS: Before running, check (1) path for shared functions is current,
//	(2) output folder is set as desired


//prepare stata
	clear all
	set more off

	*// change directory --------------------------------
	*// cd FILEPATH


	macro drop _all
	set maxvar 32000
*// Set to run all selected code without pausing
	set more off
*// Remove previous restores
	cap restore, not
*// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		local prefix_h "FILEPATH"
		set odbcmgr unixodbc
	}
	else{
		local prefix J:/
	}
	adopath + `prefix'FILEPATH
	qui do "FILEPATH/get_location_metadata.ado"
	*//if local
	*//qui do "FILEPATH/get_location_metadata.ado"

	*//set locals
	local code "`prefix_h'FILEPATH"
	local stata_shell "`prefix_h'FILEPATH"
	local stata_mp_shell  "`prefix_h'FILEPATH"
	local output "FILEPATH"

	*// local c_date = c(current_date) // gets current date for folder name
	*// local date_string = subinstr("`c_date'", " " , "_", .) // reformat c_date
	*// local ver_desc = "v`date_string'"
	local ver_desc = "v1_step4" // uncomment and change if you want to use a ver_desc that isn't from today
	*// local folder_name = "`ver_desc'"
	local generate_files 1 //set generate_files to 0 to bypass to save results

	*//make directories
	cap mkdir "`output'"
	cap mkdir "`output'/`ver_desc'"

	*//me_ids (ac = all country DisMod model, end = final ME with exclusions applied)
	local coal_workers_ac 24658 // 10/4/2019 changed to EMR bundle, previously 1893
	local coal_workers_end 3052

	*//Zero out exclusions
	if `generate_files' == 0 {
		*//find countries to 0 out
		*//get_covariate_estimates, covariate_name_short(coal_prod_cont_pc) clear
		get_covariate_estimates, covariate_id(15) gbd_round_id(6) decomp_step("step4") clear

		*//if over 30 years have 0 coal for a country, declare it coal free (mostly to deal with precision)
		gen no_coal = mean_value ==0
		bysort location_id: egen coal_id = total(no_coal)
		keep if coal_id >30

		keep location_id
		duplicates drop

		*//tempfile exclusion
		*//save `exclusion', replace

		*//get the list of excluded countries
		levelsof location_id, local(excluded_locs) clean

		*// get locations with >0 deaths coded in vital registration and remove them from exclusions
		qui do "FILEPATH/get_cod_data.ado"
		get_cod_data, cause_id(513) age_group_id(22) decomp_step("step4") clear
		keep if cf_raw>0
		keep if data_type=="Vital Registration"
		levelsof location_id, local(locations_from_vr)

		*// get all locations to loop over
		get_location_metadata, location_set_id(9) clear
		levelsof location_id, local(location_ids)

		*//convert list to dataframe
		clear
		set obs 2000
		gen location_id = .
		local row = 0
		foreach i of local location_ids {
			local row = `row' + 1
			qui replace location_id = `i' in `row'
		}
		drop if location_id == .

		*// generate variable "exclusion" and set to 1 for excluded locations
		gen exclusion =0
		foreach loc of local excluded_locs{
			qui replace exclusion = 1 if location_id == `loc'
		}
		*// reset exclusion to 0 if there's >0 VR deaths in that location
		foreach loc of local locations_from_vr{
			qui replace exclusion = 0 if location_id ==`loc'
		}

		*//launch the jobs
		local jobs
		di "`location_ids'"
		*// local location_ids 102 //change this to specific location ids or comment out for all locations
		disp("Launch jobs ----------------------------------")
		foreach location_id of local location_ids {
			preserve
				keep if location_id == `location_id'
				local excluded = exclusion[1]
				cap mkdir "`output'/`ver_desc'/`coal_workers_end'/"
				di `location_id'
				! qsub -N coal_workers_`location_id' -l m_mem_free=5G  -l fthread=2 -l archive -l h_rt=1:00:00 -P proj_custom_models -q all.q -o FILEPATH -e FILEPATH "`stata_mp_shell'" "`code'/01_pneumo_to_csv.do" "`location_id' `output' `ver_desc' `excluded'"
				local jobs `jobs',coal_workers_`location_id'
			restore
		}

		// disp("Save Jobs -------------------------------")
		// ! qsub -N save_results_coal -hold_jid "`jobs'" -l m_mem_free=20G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_custom_models -q all.q -o FILEPATH -e FILEPATH "`stata_mp_shell'" "`code'/02_save_pneumo_results.do" "`output' `ver_desc'"


	}

	else {  // end loop for "if `'generate files' == 1"
		disp("Only Saving ---------------")
		do "`code'/02_save_pneumo_results.do" "`output'" "`ver_desc'"
		*//! qsub -N save_results_coal -l m_mem_free=20G  -l fthread=1 -l archive -l h_rt=4:00:00 -P proj_custom_models -q all.q -o FILEPATH -e FILEPATH "`stata_mp_shell'" "`code'/02_save_pneumo_results.do" "`output' `ver_desc'"

	}

*// End script
