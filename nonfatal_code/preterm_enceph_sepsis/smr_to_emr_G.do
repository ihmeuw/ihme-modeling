
clear all
set graphics off
set more off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */

// priming the working environment
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		ssc install estout, replace 
		ssc install metan, replace
	} 
	di in red "J drive is `j'"
	
// locals
local acause_list " "neonatal_enceph" "neonatal_preterm" "neonatal_hemolytic" "neonatal_sepsis" "

// functions

adopath + /*FILEPATH*/
run /*FILEPATH*/
run /*FILEPATH*/

// directories
local in_dir /*FILEPATH*/
local out_dir /*FILEPATH*/

local log_dir /*FILEPATH*/
local upload_dir /*FILEPATH*/ 

// Create timestamp for logs
local c_date = c(current_date)
local c_time = c(current_time)
local c_time_date = "`c_date'"+"_" +"`c_time'"
display "`c_time_date'"
local time_string = subinstr("`c_time_date'", ":", "_", .)
local timestamp = subinstr("`time_string'", " ", "_", .)
display "`timestamp'"

//log
capture log close
log using /*FILEPATH*/, replace

**************************************************************************

// clear directories where our final results will be saved
foreach acause of local acause_list {
	cd /*FILEPATH*/
	pwd

	local datafiles: dir /*FILEPATH*/ files "*.csv"

	foreach datafile of local datafiles {
        rm `datafile'
	}

}

// generate location local to loop over 
get_location_metadata, location_set_id(35) clear
levelsof ihme_loc_id, local(ihme_loc_id_list)
levelsof location_id, local(location_id_list)
tempfile locations 
save `locations'



// submit jobs
foreach ihme_loc_id of local ihme_loc_id_list {

	// generate location_id argument
	use `locations', clear
	keep if ihme_loc_id == "`ihme_loc_id'"
	local location_id = location_id
	di "Submitting job for Ihme_loc_id `ihme_loc_id', location_id `location_id'"

	// submit jobs
	/* QSUB TO NEXT SCRIPT */

}	


// wait until jobs are done 
foreach acause of local acause_list {
	foreach location_id of local location_id_list {
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ not found"
			sleep 60000
			capture noisily confirm file /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ found!"
		}
	}

	/* The following lines of code are
	a quick way to append a lot of files
	via Unix. Starting with 'drop if sex_id
	== "sex_id", we're dealing with the fact
	that all vars are turned into strings 
	because their varnames are appended as well.*/


	
	di "appending files for `acause'"
	cd /*FILEPATH*/
	!cat *csv > /*FILEPATH*/
	import delimited /*FILEPATH*/, varnames(1) clear
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

	// format for upload to DisMod

	// find target me_ids
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

	di "formatting for DisMod for `acause' `target_me_id'"
	
	get_location_metadata, location_set_id(35) clear
	merge 1:m location_id using `data', keep(3) nogen
	keep if most_detailed == 1
	keep location_id location_name age_group_id sex_id year_id mean lower upper
	save `data', replace

	import delimited /*FILEPATH*/, clear
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
	gen extractor = "USERNAME"
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


	di "begin target_me_id loop"
	foreach target_me_id of local target_me_ids {
		// save
		export excel /*FILEPATH*/, firstrow(variables) sheet("extraction") replace 
	}

}


