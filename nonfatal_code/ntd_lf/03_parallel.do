// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step pulls saved draws of hydrocele and lymphedema prevalence to convert into prevalence of ADL, and upload results
// Author:		USERNAME


/* things that this code does:

- pulls in draws of lymphedema and hydrocele
- convert the combination into proportion of ADL cases
- adjust for time period
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
*/

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

	local location 		`1'
	local out_dir		`2'
	local in_dir 		`3'
	local date			`4'
	local tmp_dir 		`5'
	local step_num 		`6'
	local root_j_dir 	`7'

	** write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	** check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			** remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

	** set adopath
	adopath + "FILEPATH"

	** get locations
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate == 1 & most_detailed == 1
	levelsof location_id, local(location_ids)

	*********************************************************************************************************************************************************************
	*********************************************************************************************************************************************************************
	** WRITE CODE HERE

	** pull in lymphedema draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1492) source(epi) measure_ids(5) location_ids(`location') status(best) sex_ids(1 2) clear

	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * 0.95 * (4 * 4 / 365)
		rename draw_`i' lymph_`i'
	}
	tempfile lymph
	save `lymph', replace

	** pull in hydrocele draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1493) source(epi) measure_ids(5) location_ids(`location') status(best) sex_ids(1 2) clear

	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * 0.7 * (2 * 4 / 365)
	}

	merge 1:1 age_group_id sex_id year_id location_id using `lymph', nogen assert(3)
	** add them up
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' + lymph_`i'
		drop lymph_`i'
	}

	keep draw_* location_id age_group_id sex_id year_id measure_id
	export delimited "FILEPATH/`location'.csv", replace

	** write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

	** close logs
	if `close' log close
	clear

	