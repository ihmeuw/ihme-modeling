// *********************************************************
// Author:	
// Date:	
// Purpose:	Calculate bacterial vs viral meningitis ratio
// *********************************************************

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

// define locals from qsub command
	local date 			"`1'"
	local etiology 		"`2'"
	local group 		"`3'"

	run "SHARED FUNCTION UPLOAD DATA"
	run "SHARED FUNCTION GET DATA"

	//define locals for bundle_ids
	if "`etiology'_`group'" == "meningitis_pneumo_long_modsev" local bundle_id 31 
	if "`etiology'_`group'" == "meningitis_hib_long_modsev" local bundle_id 35 
	if "`etiology'_`group'" == "meningitis_meningo_long_modsev" local bundle_id 39 
	if "`etiology'_`group'" == "meningitis_other_long_modsev" local bundle_id 43 
	if "`etiology'_`group'" == "meningitis_pneumo__epilepsy" local bundle_id 32 
	if "`etiology'_`group'" == "meningitis_hib__epilepsy" local bundle_id 36 
	if "`etiology'_`group'" == "meningitis_meningo__epilepsy" local bundle_id 40 
	if "`etiology'_`group'" == "meningitis_other__epilepsy" local bundle_id 44 

	get_epi_data, bundle_id(`bundle_id') clear
	keep bundle_id seq
	capture export excel "EXPORT DELETE ALL DATA", sheet("extraction") sheetreplace firstrow(variables)
	if !_rc {
		upload_epi_data, bundle_id(`bundle_id') filepath("DELETE ALL DATA") clear
	}
	upload_epi_data, bundle_id(`bundle_id') filepath("UPLOAD DATA") clear
	