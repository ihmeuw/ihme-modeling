// Uploading all necessary lit data for birth prevalence models, post-review week
// {AUTHOR NAME}
//----------------------------------------

// setup 
clear all 
set more off
set maxvar 30000
cap restore, not
cap log close 

	// set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}


run "$dl/{FILEPATH}/upload_epi_data.ado"
run "$dl/{FILEPATH}/get_epi_data.ado"

local dir "$dl/{FILEPATH}"
local date {DATE}

		
foreach b in {BUNDLE ID LIST} {

	if `b'==603 {
		local c cong_msk
		}

	if `b'==617 {
		local c cong_urogenital
		}

	if `b'==619 {
		local c cong_urogenital
		}

	if `b'==226 {
		local c cong_cleft
		}

	if `b'==621 {
		local c cong_digestive
		}

	if `b'==228 {
		local c cong_turner
		}

	if `b'==229 {
		local c cong_klinefelter
		}
		
	if `b'==605 { 
		local c cong_msk
		}

	if `b'==607 {
		local c cong_msk
		}		
				
	if `b'==623 {
		local c cong_digestive
		}

	if `b'== 625 {
		local c cong_digestive
		}

	if `b'== 627 { 
		local c cong_digestive
		}

	di in red "`c' `b'" 

	//-----------------------------------
	// upload from recent review sheet
	
		local f "`dir'/{FILEPATH}" 
			local files : dir "`f'" files *
			local max 0
				foreach ff of local files{
   				 local chooseme = substr("`ff'", 9,4)
    				di "`chooseme'"
    				if(`max' < `chooseme'){
       				 local max `chooseme'
   					 }
					}
			local final_file = "`f'/request_`max'.xlsx"
			di "`final_file'"

		import excel using "`final_file'", firstrow clear 

		keep seq bundle_id 
		export excel using "`dir'/{FILEPATH}/deleting_old_`b'_data_`date'.xls", firstrow(variables) replace sheet("extraction")

		upload_epi_data, bundle_id("`b'") filepath("`dir'/{FILEPATH}/deleting_old_data_`date'.xls") clear 

	// upload all files from bundle folders 
		// NOTE this uploads all files, not just the most recent ones; need to remove any files you don't want uploaded from this folder.
		local f "`dir'/{FILEPATH}"
		local files : dir "`f'" files *
			foreach ff in `files' {
				local name = substr("`ff'", 1, 5)
				import excel using "`f'/`ff'", firstrow clear
					foreach var in table_num uncertainty_type uncertainty_type_value specificity input_type page_num case_diagnostics underlying_field_citation file_path sampling_type note_modeler {
						cap tostring `var', replace
						}
					cap replace input_type= "extracted" if input_type==.
					cap replace uncertainty_type="" if uncertainty_type==".", replace 
					tempfile `name'
					save ``name'', replace 
					di "`ff'"
				di "`name' imported"
			}

		local f2 "`dir'/{FILEPATH}"
		local files : dir "`f2'" files *
			foreach ff in `files' {
				local name = substr("`ff'", 1, 15)
				import excel using "`f2'/`ff'", firstrow clear
					foreach var in table_num uncertainty_type uncertainty_type_value specificity input_type page_num case_definition case_diagnostics underlying_field_citation file_path sampling_type note_modeler prenatal_fd_def extractor {
						cap tostring `var', replace
						}
					foreach var in smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demo unit_value_as measure_issue response cv_* {
						cap destring `var', replace			
							}

					cap replace input_type= "extracted" if input_type=="."
					replace extractor="{USERNAME}" if extractor==""
					tempfile `name'
					save ``name'', replace 
				di "`name' imported"
				}

//----------------------------------	
			use `euroc', clear
				append using `china'
				append using `world' 
				append using `icbds'
				cap append using `nbdpn'
					append using `lit_data_from_n'  
					append using `lit_data_from_m'

				cap drop count 
				cap drop count2 
				cap drop ihme_loc_id 
				cap drop cv_inpatient 
				cap drop parent_id 
				cap drop cv_no_still_births 
				cap drop data_sheet_filepath 
				cap drop is_outlier 
				cap drop outlier_type_id
					gen outlier_type_id=0
					replace source_type = "Facility - other/unknown" if source_type==""

				foreach var in sampling_type uncertainty_type uncertainty_type_value spec input_type {
					replace `var' ="" if `var'=="."
					}
				replace measure = lower(measure)
				replace urbanicity_type = "Mixed/both" if urbanicity_type =="Mixed/Both"
				replace uncertainty_type_value = "" if uncertainty_type==""

		gen cv_excludes_chromos = 1 if cv_includes_chromos==0
		replace cv_excludes_chromos =0 if cv_includes_chromos==1

	// export 
		export excel using "`dir'/{FILEPATH}/all_current_`b'_data_`date'.xlsx", replace firstrow(variables) sheet("extraction")
		di in red "`c' `b' input data saved"

		upload_epi_data, bundle_id(`b') filepath("`dir'/{FILEPATH}/all_current_`b'_data_`date'.xlsx") clear 
		
		// clean up tempfiles: 
			cap erase `euroc'
			cap erase `china'
			cap erase `world'
			cap erase `icbds'
			cap erase `nbdpn'
				cap erase `lit_data_from_n'
				cap erase `lit_data_from_m'

		}
