
/*  Formatting the lit 2016 extractions for:
	 - congenital heart anomalies
	 - cleft lip / palate
	 - multiple congenital anomalies, including msk and digestive
		  (all from {USERNAME}) 
	  - more multiple congenital anomalies, extracted by {USERNAME}

	  ... all for upload via Epi Uploader */
	
// {AUTHOR NAME}
//----------------------------------

clear all 
set more off
set maxvar 10000
cap restore, not

local date {DATE}

// set toggle for drive access
	local D_access 1

	if `D_access'==1 {
		local dir "{FILEPATH}"
		}

	if `D_access'==0 {
		local dir "{FILEPATH}"
		}

//---------------------------------------------------------------------------------------------
// 1. import and append extraction sheets together... 

	// heart 
	import excel using "`dir'\{FILEPATH}", firstrow clear 
		foreach var of varlist * {
			tostring `var', force replace
				}
				replace extractor = "{USERNAME}"
				gen source = "heart"
		tempfile heart
		save `heart', replace

	// cleft 
	import excel using "`dir'\{FILEPATH}", firstrow clear
		foreach var of varlist * {
			tostring `var', force replace
				}
			gen source = "cleft"
		tempfile cleft
		save `cleft', replace

	// multiple causes, from {USERNAME}
	import excel using "`dir'\{FILEPATH}", firstrow clear
		foreach var of varlist * {
			tostring `var', force replace
				}
			gen source = "mult"
		tempfile mult 
		save `mult', replace

	// multiple causes, from {USERNAME}
		import excel using "`dir'\{FILEPATH}", firstrow clear 
		foreach var of varlist * {
			tostring `var', force replace
				}
			gen source = "{USERNAME}"
		tempfile mult2
		save `mult2', replace

//--------
use `heart', clear
	append using `cleft'
	append using `mult'
	append using `mult2'

	
//---------------------------------------------------------
// 2. fix issues, and standardize variable names 
		foreach var in mean cases sample_size upper lower nid {
				destring `var', replace 
				}

drop seq seq_parent modelable_entity_id cv data_sheet_filepath bundle_id bundle_name
		drop if modelable_entity_name=="" // extra blank rows
		drop if Exclude=="1" // articles that should have been excluded
		drop Exclude

 	// covariates for specific case definitions
 		rename cv_1_* temp_cv_1_*
 		rename cv_1* temp_cv_1*
 		drop cv_*only 
 		rename temp_* *

 	// map the modelable_entity field to causes
		gen cause = ""
			replace modelable = lower(modelable)
			replace modelable = substr(modelable, 6, .)

			replace cause = "digest" if regexm(model, "abdom") | regexm(model, "digest") | model=="cdh"
			replace cause = "chromo" if inlist(modelable, "chromo", "edpatau")
			replace cause = "neural" if model=="neuro"
			replace cause = model if inlist(model, "heart", "msk", "urogenital", "cleft", "turner")
			replace cause = "urogenital" if model=="urogentical" | model=="urinary" 
			replace cause = "down" if regexm(case_name, "down") | regexm(case_name, "Down")
				drop if model=="total" // can't currently use data on the total envelope of all congenital anomalies
		
		// covariates: standardizing to names as they appear in the Epi Uploader
			replace cv_livestill = cv_includes_stillbirths if cv_livestill=="" & cv_includes_stillbirths !=""
				drop cv_includes_stillbirths
			replace cv_includes_chromos = cv_includes_chromo if cv_includes_chromos=="" & cv_includes_chromo !=""
				drop cv_includes_chromo 
			replace cv_1_fac = cv_1fac if cv_1_fac !="" & cv_1fac==""
				drop cv_1_fac

			gen prenatal_top = .
				destring TOP_count, replace
				destring cases_top, replace
					replace prenatal_top = TOP_count if TOP_count !=.
					replace prenatal_top = cases_top if cases_top !=.
					drop TOP_count cases_top

			gen prenatal_fd=.
				destring cases_stillbirth, replace
				destring stillbirth_count, replace
					replace prenatal_fd = cases_stillbirth if cases_stillbirth !=.
					replace prenatal_fd = stillbirth_count if stillbirth_count !=.
					drop stillbirth_count cases_stillbirth

			rename cv_includes_top cv_topnotrecorded

		replace source_type = source_typeS if source_type=="" & source_typeS !=""
			drop source_typeS

		replace is_outlier="0"
	
	
		// data with missing sex, mean, sample size information and note_SR's from {USERNAME}
			replace mean = (85/(1020/365)) if nid==281257 & mean==.

			replace sex = "Male" if inlist(case_name, "Ambiguous genitalia (46,XY DSD); Partial androgen insensitivity", "Ambiguous genitalia (46,XY DSD); 17  beta -hydroxysteroid dehy. def.", "CAH: 46, XY", "micropenis")
			replace sex = "Female" if inlist(case_name, "CAH: 46, XX")
			replace sex = "Both" if regexm(case_name, "dysmorph") & sex==""
			replace sex = "Both" if sex=="" & nid==272473

			local male_prop 0.5 // assume 50/50 gender split according to {USERNAME}'s note_SR 
				replace sample_size = 20000/`male_prop' if nid==270489 & sex =="Male"
				replace sample_size = 20000/(1-`male_prop') if nid==270489 & sex=="Female"
				replace sample_size = 20000 if nid==270489 & sex =="Both"
					replace note_modeler = "assumed 50/50 gender split among births, so set sample_size as 10,000" if nid==270489 & sex !="Both"


			// addressing data with measure_issue=1
				replace measure_issue="0" if nid==272339 | nid==272389 | nid==272347 | nid==274732
					replace note_modeler = "measure_issue marked as 1 by SR, but no indication of why" if nid==272389
					replace note_modeler = "extraction done to include both singleton and multiple births" if nid==272413

			// one source without the NID information filled in...
				replace nid=274745 if nid==.

			// location info...
				replace smaller_site_unit="1" if smaller_site_unit=="0"

				// filling in UK extration info:
					replace location_name = "Scotland" if regexm(site_memo, "Glasgow") & location_name==""
						replace location_id = "434" if regexm(location_name, "Scotland")
						replace location_id = "4749" if regexm(location_name, "England")
		
tempfile pre_collapse
save `pre_collapse', replace 


//-----------------------------------------------------
// 3. map to modelable entities, bundles and causes

use `pre_collapse', clear 

			// duplicate data points for causes reported together; id multiple anomalies in one child
			preserve
				keep if regexm(case_name, "&") & cause=="heart"
				keep case_name 
				split case_name, parse("&")
				duplicates drop
					rename case_name ccc
					gen blerg = 1
					reshape long case_name, i(ccc) j(ttt) string
					destring ttt, replace
					drop if case_name ==""
					rename case_name uniq_case_name
					rename ccc case_name
					drop ttt blerg
				tempfile dup_cases
				save `dup_cases', replace
			restore

			joinby case_name using `dup_cases', unmatched(master)
			replace case_name = uniq_case_name if uniq_case_name !=""
			drop uniq_case_name _m

			// export and prepare cause mapping file:
			preserve
				keep case_name cause 
				duplicates drop
				sort cause case_name

			export excel using "{FILEPATH}\2016_lit_case_name_mapping.xls", sheet("multiple_{DATE}") sheetreplace firstrow(variables)
			restore

			preserve
				import excel using "{FILEPATH}\2016_lit_case_name_mapping.xls", firstrow sheet("multiple_{DATE}") clear 
				drop E
					replace case_name=trim(case_name)
					replace cause=trim(cause)
					duplicates drop
				tempfile cause_map
				save `cause_map', replace 
			restore

		// merge in cause mapping file:
			replace case_name=trim(case_name)
			replace cause=trim(cause)
				drop modelable_entity_name
		merge m:1 case_name cause using `cause_map', nogen 

			replace cause = new_cause if new_cause !=""
			drop new_cause

			// exclude case names for which we don't have explicit models
			drop if inlist(cause, "exclude", "drop", "other", "exclude OR split")
				drop if inlist(modelable, "exclude", "ignore", "other")
		
	//------------------------------
	// merge in bundle information
	preserve
		import excel using "{FILEPATH}", firstrow clear 
			gen is_birth =1 if regexm(modelable_entity_name, "irth prevalence")
			drop if is_dental==1
			keep if is_2016=="Y"
			replace modelable_entity_name_old = trim(modelable_entity_name_old)
			tempfile bundles
			save `bundles', replace 
	restore

		// split into birth and non-birth prevalence
			gen is_birth = 1 if age_start=="0" & age_end=="0"
			rename modelable_entity_name modelable_entity_name_old
			replace modelable_entity_name_old = trim(modelable_entity_name_old)

		merge m:1 modelable_entity_name_old is_birth using `bundles', nogen keep(3)
			drop modelable_entity_name_old

			destring cv_* measure_adj, replace // necessary for later collapses

tempfile pre_collapse2
save `pre_collapse2', replace 

	// save non-prevalence data because not collapsing this
	keep if measure != "prevalence"
		tempfile mtwith
		save `mtwith', replace 

//-------------------------------------------------------
// 4. collapse (prevalence only) to the modelable entity level

use `pre_collapse2', clear 
	drop if measure !="prevalence"
	drop is_birth
	egen collapse_group = group(input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier modelable_entity_name bundle_id cause ///
			), missing

bysort collapse_group: gen count = _N

		sort collapse_group modelable_entity_name case_name
		// case_name 
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]
			replace new_case_def = subinstr(new_case_def, ", ,", "", .)
			replace new_case_def = "" if new_case_def==", "

			// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]
			replace new_note_SR = subinstr(new_note_SR, ", ,", "", .)
			replace new_note_SR = "" if new_note_SR==", "

		// prenatal_fd_def
		by collapse_group : gen new_prenatal_fd_definition = prenatal_fd_def[1]
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_n-1] + ", " + prenatal_fd_def if _n > 1
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_N]
			replace new_prenat = subinstr(new_prenat, ", ,", "", .)
			replace new_prenat = "" if new_prenat==", "

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

	collapse (sum) cases prenatal_top prenatal_fd mean (max) measure_adj cv_*, by(input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier modelable_entity_name bundle_id cause ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def extractor)

		rename new_* *

		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

tempfile collapsed1
save `collapsed1', replace 

//------------------------------------------------------
// 4.a collapse (prevalence only) to the cause level

// collapse to causes
use `pre_collapse2', clear 
	drop if measure !="prevalence"
	keep if inlist(cause, "heart", "neural", "msk", "digest")
		
		// fill in "total" ME and bundle informaation 
			gen is_total =1 if regexm(modelable, "otal")
			replace modelable = "" if is_total !=1
			replace bundle = . if is_total !=1
			sort cause is_birth is_total
		carryforward modelable bundle_id, replace 	
	
	egen collapse_group = group( input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier modelable_entity_name bundle_id cause ///
			), missing

	bysort collapse_group: gen count = _N

		sort collapse_group modelable_entity_name case_name
		// case_name 
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]
			replace new_case_def = subinstr(new_case_def, ", ,", "", .)
			replace new_case_def = "" if new_case_def==", "

			// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]
			replace new_note_SR = subinstr(new_note_SR, ", ,", "", .)
			replace new_note_SR = "" if new_note_SR==", "

		// prenatal_fd_def
		by collapse_group : gen new_prenatal_fd_definition = prenatal_fd_def[1]
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_n-1] + ", " + prenatal_fd_def if _n > 1
		by collapse_group : replace new_prenatal_fd_def = new_prenatal_fd_def[_N]
			replace new_prenat = subinstr(new_prenat, ", ,", "", .)
			replace new_prenat = "" if new_prenat==", "

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

	collapse (sum) cases prenatal_top prenatal_fd mean (max) measure_adj cv_*, by(input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier modelable_entity_name bundle_id cause ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def extractor)

		rename new_* *

		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

tempfile collapsed2
save `collapsed2', replace

append using `collapsed1'
append using `mtwith'

//-----------------------------------------------------------------------
// check for duplicates and collapse issues
//-----------------------------------------------------------------------
// 4.b: export and code cv_subset
		
		**export and prepare mapping file
		preserve 
				duplicates drop cause modelable case_name bundle_id, force
				keep cause modelable case_name bundle
				sort cause modelable case_name
				export excel using "{FILEPATH}\multiple_cong_lit_cv_subset_coding.xls", firstrow(variables) sheetreplace
		restore 

		preserve
			import excel using "{FILEPATH}\multiple_cong_lit_cv_subset_coding.xls", clear firstrow 
			keep if cv_subset !=""
			tempfile cv_subset
			duplicates drop
			save `cv_subset', replace
		restore

	merge m:1 case_name bundle cause modelable using `cv_subset' , nogen
		rename cv_subset cv_under_report


//---------------------------------------------------------------------------------
// 4.c: format & order variables to match the 2016 extraction template

gen seq =.
gen seq_parent =.

	replace design_effect = "" if design_effect=="Person*year"
	foreach var in underlying_nid recall_type_value effective_sample_size design_effect uncertainty_type_value uncertainty_type group_review location_id input_type group group_review {
		destring `var', replace
		}

	replace sampling_type = "" if sampling_type=="."
	replace group_review =1 if group !=. & group_review==.

	order bundle_id modelable_entity_name seq seq_parent underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
		 input_type underlying_field_citation_value design_effect cv* ///
		 prenatal_fd prenatal_top prenatal_fd_definition is_outlier 

		 	drop source is_2015 is_2016 is_dental row_num is_birth parent_id
		 
//-----------------------------------------------------
// 5. upload / split into bundle-specific files for upload 

// export all, including insufficient case names
	export excel using "{FILEPATH}", firstrow(variables) replace
		drop if cv_under=="drop" // where the collection of case_names is not sufficient for the entire category; eg. "Dandywark snydrome" for other chromosomal anomalies


	// format "cause" var for folder structure
		replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"
			// fix for previous error
				replace cause = "cong_turner" if bundle==228

	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				local me = modelable_entity_name
				di in red "Exported `me' data for upload"
			restore
		}

