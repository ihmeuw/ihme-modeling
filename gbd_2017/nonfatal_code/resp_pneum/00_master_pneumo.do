//Master Script for pneumoconiosis exclusions

	clear all
	set more off
	macro drop _all
	set maxvar 32000
	set more off
	cap restore, not
	local prefix "FILEPATH"
	local prefix_h "FILEPATH"
	set odbcmgr unixodbc
	
	adopath + `prefix'/FILEPATH
	qui do "FILEPATH/get_location_metadata.ado"
	
	local code "FILEPATH"
	local stata_shell "FILEPATH/stata.sh"
	local stata_mp_shell  "FILEPATH/stata_shell.sh"
	local output "FILEPATH"

	local c_date = c(current_date)
	local date_string = subinstr("`c_date'", " " , "_", .)
	local ver_desc = "v`date_string'"
	local folder_name = "`ver_desc'"
	
	cap mkdir "`output'"
	cap mkdir "`output'/`ver_desc'"
	
	local coal_workers_ac 1893
	local coal_workers_end 3052

		//get_covariate_estimates, covariate_name_short(coal_prod_cont_pc) clear
		get_covariate_estimates, covariate_id(15) clear
				
		//if over 30 years have 0 coal for a country, declare it coal free (mostly to deal with precision)
		gen no_coal = mean_value ==0
		bysort location_id: egen coal_id = total(no_coal)
		keep if coal_id >30
				
		keep location_id
		duplicates drop
			
		//get the list of excluded countries
		levelsof location_id, local(excluded_locs) clean

		// get locations with >0 deaths coded in vital registration and remove them from exclusions
		qui do "FILEPATH/get_cod_data.ado"
		get_cod_data, cause_id(513) age_group_id(22) clear
		keep if cf_raw>0
		keep if data_type=="Vital Registration"
		levelsof location_id, local(locations_from_vr)

		// get all locations to loop over
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
		// reset exclusion to 0 if there's >0 VR deaths in that location
		foreach loc of local locations_from_vr{
			qui replace exclusion = 0 if location_id ==`loc'
		}
			
		//launch the jobs
		local jobs
		foreach location_id of local location_ids {
			preserve
				keep if location_id == `location_id'
				local excluded = exclusion[1]
				cap mkdir "`output'/`ver_desc'/`coal_workers_end'/"
				! qsub -N coal_workers_`location_id' -pe multi_slot 1 -P proj_custom_models "`stata_mp_shell'" "`code'/01_pneumo_to_csv.do" "`location_id' `output' `ver_desc' `excluded'"
				local jobs `jobs',coal_workers_`location_id'
			restore
		}
		
		! qsub -N save_results_coal -hold_jid "`jobs'" -pe multi_slot 4 -P proj_custom_models "`stata_mp_shell'" "`code'/02_save_pneumo_results.do" "`output' `ver_desc'"
	
