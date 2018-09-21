
** Purpose: upload all current birth prev data to the corresponding full models 
** {AUTHOR NAME} 
**---------------------------

** setup
clear all
set more off
cap restore, not

	// set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}

local date {DATE}

local function_dir "$dl/{FILEPATH}"
	run "`function_dir'/get_epi_data.ado"
	run "`function_dir'/upload_epi_data.ado"

local out_dir "$dl/{FILEPATH}"
local map_file "$dl/{FILEPATH}/Congenital_bundle_model_and_cause_map_CURRENT.xlsx"


** 0. pull in bundle to model mapping file 
	preserve
		import excel using "`map_file'", firstrow clear 
		keep cause description birthprev_bundle fullmod_bundle birthprev_ME fullmod_ME birthprev_regcorrect

			** uploading the bundle with registry-crosswalked data for the congenital heart causes only
				replace birthprev_bundle = birthprev_regcorrect_bundle if cause=="cong_heart"
				drop birthprev_regcorrect 

			keep if birthprev_ME !="-" & birthprev_ME !="" & birthprev_bundle !=""
			foreach var of varlist birthprev_bundle fullmod_bundle birthprev_ME fullmod_ME {
				destring `var', replace 
				}

		tempfile bundle_map
		save `bundle_map', replace
	restore 


**---------------------------------------------------
** 1. loop over bundles; get data, save and upload 

local birthprev_bundle_list {BUNDLE ID LIST}

	foreach b in `birthprev_bundle_list' {

		get_epi_data, bundle_id("`b'") clear 
		tempfile `b'_data
		save ``b'_data', replace
		noi di "`b' data imported"

			rename bundle_id birthprev_bundle
			merge m:1 birthprev_bundle using `bundle_map', keepusing(fullmod_bundle) keep(3) nogen
				local fb = fullmod_bundle
				drop birthprev_bundle
				rename fullmod_bundle bundle_id

				** formatting fixes for Uploader
				foreach v of varlist * {
					cap confirm string variable `v'
					if !_rc {
						replace `v'="" if `v'=="."
						}
					}

			local destination_file "`out_dir'/all_`b'_data_for_`fb'_upload_`date'.xlsx"
			export excel using "`destination_file'", firstrow(variables) sheet("extraction") replace

			upload_epi_data, bundle_id("`fb'") filepath("`destination_file'") clear 

			noi di "`b' data uploaded to bundle `fb'"
	
		}

