
// 
// Description:	Standardizes format of manually formatted cancer data_sources
//
** **************************************************************************
** CONFIGURATION 
** **************************************************************************
// Clear memory and set STATA to run without pausing
	clear all
	set more off, permanently

// Accept Arguments
	args dataset_id data_type_id

// Load and run set_common function based on the operating system 
//		(will load common functions and filepaths relevant for registry intake)
	if "$h" == "" & c(os) == "Windows" global h = <filepath>
    else if "$h" == "" global h = <filepath>
	run "$h/cancer_estimation/_common/set_common_roots.do" "registry_input"  

// Get dataset and type name based on the ids
	preserve
	do "$code_prefix/_database/load_database_table" "dataset"
	levelsof dataset_name if dataset_id == `dataset_id', clean local(dataset_name)
	if "`dataset_name'" == "" {
		noisily di in red "ERROR: could not match dataset_name to dataset_id `dataset_id'"
		BREAK
	}
	do "$code_prefix/_database/load_database_table" "data_type"
	levelsof data_type if data_type_id == `data_type_id', clean local(data_type)
	local data_type = lower("`data_type'")
	if "`data_type'" == "" {
		noisily di in red "ERROR: could not match data_type to data_type_id `data_type_id'"
		BREAK
	}
	restore
// Set common macros
	set_common, process(1) dataset_name("`dataset_name'") data_type("`data_type'")

// set folders
	local metric = r(metric)
	local data_folder = r(data_folder)
	local temp_folder = r(temp_folder)
	local error_folder = r(error_folder)

// set location of subroutines
	local standardize_causes = "$standardize_causes_script"
		
** ****************************************************************
** Part 1: Get data. 
** ****************************************************************
// GET DATA
	use "`data_folder'/$step_0_output", clear
	capture confirm var registry_id
	if !_rc {
		capture confirm var registry_index
		if !_rc drop registry_id
		else rename registry_id registry_index
	} 
	else {
		capture confirm var registry_index 
		if _rc {
			noisily di "ERROR: no registry_index information exists for this dataset"
		}
	}

	// Ensure that any extra variables have been dropped
	foreach v in registry_name dataset_name {
		capture drop `v'
	}

** ****************************************************************
** Part 2: Standardize Causes
** ***************************************************************
// // // Standardize cause and cause_name
	do `standardize_causes'
	
** ******************************************************************
** Part 3: Standardize order and variable widths. Upadate variable names
** ******************************************************************
// Standardize column widths
	// long strings
		format %30s cause_name
	
	// short strings
		format %10s cause coding_system

		
** ******************************************************************
** SAVE
** ******************************************************************
// SAVE
	save_prep_step, process(1)
	capture log close


** *********************************************************************
** End standardize_format.do
** *********************************************************************
 exit, STATA clear 

