******************************
** Purpose: Deletes existing EMR results and launches smr_to_emr_parallel.do script for each GBD location
** smr_to_emr_parallel.do script calculates EMR for each location and uses a for-loop to calculate EMR for each acause 
** within the location
** After submitting the location-specific qsubs, this script waits until all location-specific results are exported by the parallel scripts, then concatenates them.
** It then formats them for upload to the DisMod ME for long-term impairment.
*********************************

** Inputs: After smr_to_emr_parallel.do runs: FILEPATH/[location_id]_emr.csv

** Outputs: FILEPATH/[long_term_outcome_ME]_mtexcess.xlsx

*********************************

clear all
set graphics off
set more off
set maxvar 32000

** Locals
local in_dir "FILEPATH"
local out_dir "FILEPATH"

** To delete existing EMR files, change to 1. If testing, use as 0.
local delete_flag 0

*local acause_list " "neonatal_enceph" "neonatal_preterm" "neonatal_hemolytic" "neonatal_sepsis" "
local acause_list "neonatal_sepsis"

** Functions
adopath + "FILEPATH"

** Print working directory
di "Working directory: `working_dir'"

*********************************
di "Delete flag is `delete_flag'. If 1, will delete all existing EMR files."

if `delete_flag' == 1 {

	di "Deleting existing EMR results"

	foreach acause of local acause_list {

		cd "`out_dir'"	
		local datafiles: dir "`out_dir'" files "*.csv"
	
		foreach datafile of local datafiles {
        	rm `datafile'
		}
	}
}

*********************************
di "Submit a QSUB for each GBD location"

*import delimited "FILEPATH/old_gbd2019_loc_ids.csv", clear
get_location_metadata, location_set_id(35) gbd_round_id(6) clear
*keep if inlist(location_id,44909)
levelsof location_id, local(location_id_list)
tempfile locations 
save `locations'

foreach location_id of local location_id_list {

	di "Submitting qsub for location_id `location_id'"
	!qsub -N smr_to_emr_`location_id' -P proj_neonatal -l fthread=3 -l m_mem_free=2G -l h_rt=01:00:00 -l archive -q all.q -e FILEPATH -o FILEPATH  "FILEPATH/stata_shell.sh" "FILEPATH/smr_to_emr_parallel.do" "`location_id' `in_dir' `out_dir'"
	sleep 1000
}	


*********************************

foreach acause of local acause_list {

	di "Waiting until results from each GBD location exist in: FILEPATH/[location_id]_emr.csv"

	foreach location_id of local location_id_list {
		capture noisily confirm file "`out_dir'/`location_id'_emr.csv"
		while _rc!=0 {
			di "File `location_id'_emr.csv not found :["
			sleep 600
			capture noisily confirm file "`out_dir'/`location_id'_emr.csv"
		}
	}
	
	di "Append all location-specific EMR files for `acause'"
	cd "`out_dir'/"
	!cat *csv > emr_all_locations.csv
	import delimited "`out_dir'//emr_all_locations.csv", varnames(1) clear
	drop if sex_id == "sex_id"
 
	ds 
	local _all = "`r(varlist)'"
	foreach var of varlist _all {
		di "Var is `var'"
		gen `var'_copy = real(`var')
	}
	keep *copy
	foreach var of local _all {
		di "Var is `var'"
		rename `var'_copy `var'
	}

	** Match acause with the long-term impairment DisMod ME (labeled as the target_me_id) and the appropriate NID
	di "finding target_me_ids and nids"
	if "`acause'" == "neonatal_preterm" {
		local target_me_ids 8623 8622 8621
		local nid 256379
		local bundle_id 491
	}
	if "`acause'" == "neonatal_enceph" {
		local target_me_ids 8653
		local nid 256556
		local bundle_id 497
	}
	if "`acause'" == "neonatal_hemolytic" {
		local target_me_ids 3962 
		local nid 256559
		local bundle_id 458
	}
	if "`acause'" == "neonatal_sepsis" {
		local target_me_ids 8674
		local nid 256563
		local bundle_id 499
	}

	tempfile data
	save `data'

	di "Formatting for DisMod for `acause' `target_me_id'"
	
	get_location_metadata, location_set_id(35) gbd_round_id(6) clear
	merge 1:m location_id using `data', keep(3) nogen
	keep if most_detailed == 1
	keep location_id location_name age_group_id sex_id year_id mean lower upper
	save `data', replace

	** Get age_start and age_end for age_group_id. Need to convert to get_age_metadata shared functions
	import delimited "FILEPATH/age_group_metadata.csv", clear
	keep age_group_id age_group_years_start age_group_years_end
	merge 1:m age_group_id using `data', keep(3) nogen
	rename age_group_years_start age_start
	rename age_group_years_end age_end
	keep location_id location_name sex_id year_id mean lower upper age_start age_end
	save `data', replace

	rename year_id year_start
	gen year_end = year_start

	rename sex_id sex
	tostring sex, replace
	replace sex = "Male" if sex == "1"
	replace sex = "Female" if sex == "2"

	gen bundle_id = `bundle_id'
	gen seq = .
	gen nid = `nid'
	gen underlying_nid = .
	gen input_type = "extracted"
	gen source_type = "Surveillance - other/unknown"
	gen smaller_site_unit = 0
	gen sex_issue = 0
	gen year_issue = 0
	gen age_issue = 0
	gen age_demographer = 0
	gen measure = "mtexcess"
	gen unit_type = "Person"
	gen unit_value_as_published = 1
	gen measure_issue = 0
	gen recall_type_value = .
	gen measure_adjustment = 0
	gen uncertainty_type = "Confidence interval"
	gen uncertainty_type_value = 95
	gen urbanicity_type = "Unknown"
	gen recall_type = "Not Set"
	gen extractor = "steeple"
	gen is_outlier = 0
	gen standard_error = . 
	gen effective_sample_size = . 
	gen cases = . 
	gen sample_size = .
	gen design_effect = .
	gen sampling_type = .
	gen representative_name = "Nationally and subnationally representative"
	gen response_rate = . 
	gen case_name = ""
	gen case_definition = ""
	gen case_diagnostics = ""
	gen note_modeler = ""
	gen note_SR = ""
	gen specificity = .
	gen group = .
	gen group_review = .


	di "Export excel per long-term impairment ME"
	foreach target_me_id of local target_me_ids {	
		export excel "FILEPATH/`target_me_id'_mtexcess.xlsx", firstrow(variables) sheet("extraction") replace 
	}

}
