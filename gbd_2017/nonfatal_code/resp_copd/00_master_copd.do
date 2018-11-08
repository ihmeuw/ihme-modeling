// Master script for COPD severity split

// setup
	clear all
	set more off
	macro drop _all
	set maxvar 32000
	set more off
	cap restore, not

	local prefix "FILEPATH"
	set odbcmgr unixodbc
	local code "FILEPATH"
	local stata_shell "FILEPATH/stata_shell.sh"
	local output "FILEPATH"
	
	local c_date = c(current_date)
	local date_string = subinstr("`c_date'", " " , "_", .)
	local ver_desc = "v`date_string'"
	local folder_name = "`ver_desc'"

	local asympt 3065
	local mild 1873
	local moderate 1874
	local severe 1875

	cap mkdir "`output'/`folder_name'"
	cap mkdir "`output'/`folder_name'/`asympt'" 
	cap mkdir "`output'/`folder_name'/`mild'" 
	cap mkdir "`output'/`folder_name'/`moderate'"
	cap mkdir "`output'/`folder_name'/`severe'"
	
	do "FILEPATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(9) clear
	levelsof location_id, local(location_ids)

// Stage 1: Calculate conversion from GOLD class proportion to GBD severities
	do "`code'/01_gc_to_sev_conversion.do" "`ver_desc'"

// Stage 2: Apply the conversion to all countries and create COPD severity draws
	local iter 0
	foreach loc_id of local location_ids{
		! qsub -N sq_`loc_id'_`iter' -pe multi_slot 4 -P proj_custom_models -l mem_free=4G "`stata_shell'" "`code'/02_squeeze_apply_conversion.do" "`folder_name' `loc_id' `output' `asympt' `mild' `moderate' `severe'"
		di "`loc_id' | sq_`loc_id'_`iter'"
		local old_id `old_id',sq_`loc_id'_`iter'
		local iter = `iter'+1			
	}

//Stage 3: Upload results
! qsub -N copd_mild_save_results -hold_jid "`old_id'" -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "`code'/03_check_outputs.do" "`output' `folder_name' mild"
! qsub -N copd_moderate_save_results -hold_jid "`old_id'" -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "`code'/03_check_outputs.do" "`output' `folder_name' moderate"
! qsub -N copd_severe_save_results -hold_jid "`old_id'" -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "`code'/03_check_outputs.do" "`output' `folder_name' severe"
! qsub -N copd_asympt_save_results -hold_jid "`old_id'" -pe multi_slot 15 -P proj_custom_models "`stata_shell'" "`code'/03_check_outputs.do" "`output' `folder_name' asympt"

clear
	
