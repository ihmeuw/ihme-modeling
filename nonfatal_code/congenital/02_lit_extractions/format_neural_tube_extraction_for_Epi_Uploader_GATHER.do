
// Formatting the lit 2016 extraction for neural tube defects for upload via Epi Uploader 
// {AUTHOR}
//----------------------------------

clear all 
set more off
set maxvar 10000
cap restore, not

local date {DATE}
//-----------------------------------

import excel using "{FILEPATH}", firstrow clear 
	drop if case_name==""
	drop cv

	gen is_birth_prev =1 if age_start==0 & age_end==0 & measure=="prevalence"
		replace is_birth_prev = 0 if is_birth_prev ==.

	rename cv_1_facility_only keep_cv_1facility_only
		drop cv_*only 
		rename keep_* *
	drop modelable_entity_id

// covariate adjustments
	replace cv_pre = "1" if regexm(cv_pre, "mixed - 1 case")
	replace cv_pre = "1" if regexm(cv_pre, "mixed")
		destring cv_prenatal, replace

 // map to data bundles
 rename modelable_entity_name old_model

	// export and prepare case name mapping file:
		preserve
			duplicates drop case_name old_model modelable_entity_name is_birth_prev, force
			keep  case_name old_model modelable_entity_name is_birth_prev
			sort old_model case_name is_birth_prev
			export excel using "{FILEPATH}\2016_lit_case_name_mapping.xls", sheet(neural_tube) firstrow(variables) sheetreplace 
		restore 

	// merge in case names
	preserve
		import excel using "{FILEPATH}\2016_lit_case_name_mapping.xls", firstrow clear 
		tempfile case_map 
		save `case_map', replace
	restore

	merge m:1 case_name old_model is_birth_prev using `case_map', nogen keep(3) 
		drop if modelable_entity_name=="drop"
		drop old_model

	replace case_name = subinstr(case_name, `"""', "", . )
		replace note_SR = subinstr(note_SR, `"""', "", . )
		replace case_definition = subinstr(note_SR, `"""', "", . )			

tempfile pre_collapse
save `pre_collapse', replace 
	
	keep if measure != "prevalence"
		tempfile mtwith
		save `mtwith', replace 

//-------------------------------
// collapse to modelable entities
use `pre_collapse', clear 
	drop if measure !="prevalence"
	drop is_birth_prev
	egen collapse_group = group(seq seq_parent input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier data_sheet_file modelable_entity_name bundle_id cause ///
			), missing

bysort collapse_group: gen count = _N

		sort collapse_group modelable_entity_name
		// case_name 
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

		// prenatal_fd_def
		by collapse_group : gen new_prenatal_fd_definition = prenatal_fd_def[1]
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_n-1] + ", " + prenatal_fd_def if _n > 1
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_N]

	//----------------
	// collapse
	collapse (sum) cases mean (max) measure_adj cv_*, by(seq seq_parent input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier data_sheet_file modelable_entity_name bundle_id cause ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def)

		rename new_* *

		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

tempfile collapsed1
save `collapsed1', replace 

//--------------------------------
// collapse to causes
use `pre_collapse', clear 
	drop if measure !="prevalence"
	keep if inlist(cause, "heart", "neural", "msk", "digest")
		
		// fill in "total" ME and bundle informaation 
			gen is_total =1 if regexm(modelable, "otal")
			replace modelable = "" if is_total !=1
			replace bundle = . if is_total !=1
			sort cause is_birth_prev is_total
		carryforward modelable bundle_id, replace 	
		drop if nid==.
	
	drop is_total is_birth_prev

	egen collapse_group = group(seq seq_parent input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier data_sheet_file modelable_entity_name bundle_id cause ///
			), missing

bysort collapse_group: gen count = _N

		sort collapse_group modelable_entity_name
		// case_name 
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

		// prenatal_fd_def
		by collapse_group : gen new_prenatal_fd_def = prenatal_fd_def[1]
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_n-1] + ", " + prenatal_fd_def if _n > 1
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_N]

	//----------------
	// collapse
	collapse (sum) cases mean (max) measure_adj cv_*, by(seq seq_parent input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier data_sheet_file modelable_entity_name bundle_id cause ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def)

		rename new_* *
		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

//-----------------------------------------------------------------------
append using `collapsed1'
append using `mtwith'

//-------------------------		
// code cv_subset
			preserve 
				duplicates drop cause modelable case_name bundle_id, force
				keep cause modelable case_name bundle
				sort cause modelable case_name
				export excel using "{FILEPATH}", firstrow(variables) sheetreplace
			restore 

	preserve
		import excel using "{FILEPATH}", clear firstrow
			keep if cv_subset !=.
			tempfile cv_subset
		save `cv_subset', replace
	restore

	merge m:1 case_name modelable using `cv_subset', nogen

	rename cv_includes_top cv_topnotrecorded
	rename cv_subset cv_under_report
	rename is_outlier outlier_type_id
	rename cases_top prenatal_top 
	rename cases_stillbirth prenatal_fd
		drop is_birth_prev

	replace group_review =1 if group !=. & group_review==. // these are the "specific NTD type and all NTD" groups 

// order variables to match the 2016 extraction template
	order bundle_id modelable_entity_name seq seq_parent underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
		 input_type underlying_field_citation_value design_effect cv* ///
		 prenatal_fd prenatal_top prenatal_fd_definition

drop if nid==.

replace sample_size = round(sample_size)	


//-----------------------------------------------------------------------
// split into bundles for upload 

	// format "cause" var for folder structure
		replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"

	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}/lit_data_from_neural_tube_screening_sheet_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				local me = modelable_entity_name
				di in red "Exported `me' data for upload"
			restore
		}







