
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
	local functional	"`2'"
	local group 		"`3'"

	run "/home/j/temp/central_comp/libraries/current/stata/upload_epi_data.ado"
	run "/home/j/temp/central_comp/libraries/current/stata/get_epi_data.ado"

	//define locals for bundle_ids
	if "`functional'_`group'" == "encephalitis__epilepsy" local bundle_id 401
	if "`functional'_`group'" == "encephalitis_long_modsev" local bundle_id 400
	
	get_epi_data, bundle_id(`bundle_id') clear
	keep bundle_id seq
	capture export excel "/home/j/WORK/12_bundle/`functional'/`bundle_id'/03_review/02_upload/delete_all_`date'.xlsx", sheet("extraction") sheetreplace firstrow(variables)
	if !_rc {
		upload_epi_data, bundle_id(`bundle_id') filepath("/home/j/WORK/12_bundle/`functional'/`bundle_id'/03_review/02_upload/delete_all_`date'.xlsx") clear
	}
	//upload new bundle 36 data
	upload_epi_data, bundle_id(`bundle_id') filepath("$prefix/WORK/12_bundle/`functional'/upload/`date'/dm_custom_input_`functional'_`group'_`date'.xlsx") clear
	//upload_epi_data, bundle_id(`bundle_id') filepath("/home/j/WORK/12_bundle/encephalitis/upload/2017_3_22/dm_custom_input_`functional'_`group'_`date'.xlsx") clear
