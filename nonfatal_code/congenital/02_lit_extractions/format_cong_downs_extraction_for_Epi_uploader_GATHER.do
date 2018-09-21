// Formatting the lit 2016 extraction for Down Syndrome
// {AUTHOR NAME}

clear all
set more off
set maxvar 10000
cap restore, not

local date {DATE}

// Down Syndrome 
	import excel using "{FILEPATH}", firstrow clear

	drop if modelable_entity_name==""
	drop in 1
	replace modelable_entity_name = "Down Syndrome"

	// destring vars
	foreach var in nid page_num table_num location_id year_start year_end year_issue age_start age_end age_issue age_demographer ///
		mean effective_sample_size sample_size group is_outlier {
			destring `var', replace 
		}


// assign to birth prev and non-birth prev bundle_id's 
	gen bundle_id=.
		replace bundle_id = 227 if age_start ==0 & age_end ==0
		replace bundle_id = 436 if age_end !=0


// rename covariates
drop cv 
	rename cv_includes_top cv_topnotrecorded
	rename cv_1_facility_only cv_1facility_only


	// re-code covariates where necessary 
		replace cv_aftersurgery = "0" if cv_aftersurgery =="not applicable"
		replace cv_prenatal = "1" if regexm(cv_prenatal, "mixed")
			replace cv_prenatal = "0" if cv_prenatal=="not specified"

			destring cv_prenatal, replace
			destring cv_aftersurgery, replace 


// rename metadata variables
	rename cases_stillbirth prenatal_fd
	rename cases_top prenatal_top


	// rename cv's so that Epi uploader will allow them
			rename cv_1facility_only future_cv_1facility_only
			rename cv_aftersurgery future_cv_aftersurgery
			rename cv_prenatal future_cv_prenatal 


//-----------------------------------------------------------------------------
// save files separately to each bundle_id folder
	gen cause = "cong_downs"

	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				levelsof modelable_entity_name, local(me) clean
				di in red "Exported `me' data for upload"
			restore
		}

