
/*  Formatting the final lit GBD 2016 extractions for:
	 - multiple congenital anomalies (extracted by {USERNAME})
	 - multiple congenital anomalies (a handful extracted by {USERNAME})
	 - screening sheets created for Turner Syndrome, Klinefelter and other chromosomal anomalies 
	 
		  all for upload via Epi Uploader */
	
// {AUTHOR NAME}
//---------------------------------------------------


clear all 
set more off
set maxvar 10000
cap restore, not

	local dir {FILEPATH}
	local date {DATE}

//--------------------
// 0. pull in the male/female birth proportion estimates for later use in sex-splitting denominators
	preserve
		local birth_file "{FILEPATH}"

		import delimited using "`birth_file'", clear 	
		drop covariate_id covariate_name_short lower_value upper_value
		
		rename year_id year_end  
		gen sex = "Both" if sex_id==3
			replace sex = "Male" if sex_id==1
			replace sex = "Female" if sex_id==2 
			drop sex_id
		rename mean_value births
		
			reshape wide births, i(location_id year_end) j(sex) string 
				gen prop_Male = birthsMale/birthsBoth
				gen prop_Female = birthsFemale/birthsBoth
					drop births*

			reshape long prop_, i(location_id year_end) j(sex) string 
				rename prop_ birth_prop

		tempfile births
		save `births', replace
	restore

//---------------------------------------------------------------------------------------------
// 1. import and append extraction sheets together 

	// multiples from {USERNAME}
	import excel using "`dir'\epi_lit_multiples_{USERNAME}_{DATE}.xlsm", firstrow clear 
		drop if nid ==.
		foreach var of varlist * {
			tostring `var', force replace
				}
				
		local source mult
			gen source = "`source'"
			tempfile `source'
			save ``source'', replace


	// multiples from {USERNAME} 
	import excel using "`dir'\multiple_cong_extraction_{USERNAME}_{DATE}.xlsm", firstrow clear 
		drop if nid ==.
		foreach var of varlist * {
			tostring `var', force replace
				}
				
		local source mult_{USERNAME}
			gen source = "`source'"
			tempfile `source'
			save ``source'', replace

	// msk from {USERNAME} 
	import excel using "`dir'\msk_extraction_{USERNAME}_{DATE}.xlsm", firstrow clear 
		drop if nid==.
		foreach var of varlist * {
			tostring `var', force replace
				}
				
		local source msk_{USERNAME}
			gen source = "`source'"
			tempfile `source'
			save ``source'', replace

	// chromosomal screening sheets
	import excel using "`dir'\epi_lit_TurnerKlineOther_{USERNAME}_{DATE}.xlsm", firstrow clear 
		drop in 1
		drop if nid==""
		foreach var of varlist * {
			tostring `var', force replace
				}
				
		local source chromo
			gen source = "`source'"
			tempfile `source'
			save ``source'', replace

//-----------
	use `mult', clear
		append using `mult_{USERNAME}'
		append using `msk_{USERNAME}'
		append using `chromo'

			// drop duplicates that are already included in other extractions;
				preserve
					keep nid field_cit source 
					duplicates drop 
					duplicates tag nid, gen(dup)
						levelsof nid if dup==1, local(nids_to_drop)
					restore

					foreach n in `nids_to_drop' {
						drop if nid=="`n'" & regexm(source, "{USERNAME}")
						}

				// drop duplicate NIDs from earlier screening sheets
					drop if inlist(nid, "274575", "274577") & regexm(source, "{USERNAME}")

//-----------------------------------------------------
// 2. fix issues and standardize variable names 
		foreach var in mean cases sample_size upper lower nid {
				destring `var', replace ignore( " ")
				}

	// drop excess vars
		drop cv BG row_num

	// drop covariates for specific case definitions 
		rename cv_1_* temp_cv_1_*
 			rename cv_1* temp_cv_1*
 			drop cv_*only 
 			rename temp_* *
 

 	// covariates: standardizing to names as they appear in the Epi Uploader
			replace cv_livestill = cv_includes_stillbirths if cv_livestill=="" & cv_includes_stillbirths !=""
				drop cv_includes_stillbirths
			replace cv_includes_chromos = cv_includes_chromo if cv_includes_chromos=="" & cv_includes_chromo !=""
				drop cv_includes_chromo 
			replace cv_1_fac = cv_1fac if cv_1_fac !="" & cv_1fac==""
				drop cv_1_fac 
			rename cv_includes_top cv_topnotrecorded

			gen cv_excludes_chromos = "1" if cv_includes_chromos =="0"
				replace cv_excludes_chromos ="0" if cv_includes_chromos=="1"

			gen prenatal_top = ""
				replace prenatal_top = TOP_count if TOP_count !=""
				replace prenatal_top = cases_top if cases_top !=""
				replace prenatal_top = cv_TOP_count if cv_TOP_count !=""
					drop TOP_count cases_top cv_TOP_count cv_TOP_count

			gen prenatal_fd=""
					replace prenatal_fd = cases_stillbirth if cases_stillbirth !=""
					replace prenatal_fd = stillbirth_count if stillbirth_count !=""
					replace prenatal_fd = cv_stillbirth_count if cv_stillbirth_count !=""
						drop stillbirth_count cases_stillbirth cv_stillbirth_count


		// other note_SR comments that need to be addressed
			replace age_issue="0" if age_issue=="1"

			// fix group_review and specificity accordingly
				replace group_review ="1" if regexm(specificity, "specific_") & (group_review=="0" | group_review=="")


	// map the bundle_name and modelable_entity_name fields to causes
		gen cause = ""
			replace bundle_name = modelable_entity_name if bundle_name==""
			replace bundle_name = trim(bundle_name)
			replace bundle_name = subinstr(bundle_name, "cong_", "", .)
			replace cause = "digest" if inlist(bundle_name, "abdomwall", "cdh", "digestive", "digestive_atresia")
			replace cause = "urogenital" if inlist(bundle_name, "genital", "genitourin", "urin")
			replace cause = "heart" if inlist(bundle_name, "heart", "heart?")
			replace cause = "msk" if regexm(bundle_name, "msk")
			replace cause = "turner" if bundle_name=="Turner syndrome"
			replace cause = "edpatau" if bundle_name=="edward_patau"
				replace cause = bundle_name if cause ==""

				drop bundle_name modelable_entity_name modelable_entity_id

	// drop "total congenital anomalies" data 
		drop if cause=="total"

tempfile pre_collapse
save `pre_collapse', replace 


//-------------------------------------------------
// 3. map case names to modelable entities, bundle_ids and causes

use `pre_collapse', clear
	replace case_name = trim(case_name)
	replace case_name = proper(case_name)
	
			// prepare cause mapping file:
			preserve
				keep case_name cause 
				replace case_name = proper(case_name)
				duplicates drop
				sort cause case_name

			export excel using "`dir'\{FILEPATH}\2016_lit_case_name_mapping.xls", sheet("{DATE}") sheetreplace firstrow(variables)
			restore 

	// merge in mapping file 
		preserve
			import excel using "`dir'\{FILEPATH}\2016_lit_case_name_mapping.xls", firstrow sheet("{DATE}") clear 
					replace case_name=trim(case_name)	
					duplicates drop  case_name cause, force 
				tempfile cause_map
				save `cause_map', replace 
			restore

		merge m:1 case_name cause using `cause_map', nogen
			replace cause = new_cause if new_cause !=""
			drop new_cause

		// expand rows where there are >1 modelable entities, for combined case names 
			expand 2 if modelable_entity_name2 !="", gen(new)
				replace modelable_entity_name = modelable_entity_name2 if new==1
				drop modelable_entity_name2 new


	// merge in bundle information 
		preserve
		import excel using "{FILEPATH}\Bundle_to_ME_map_{USERNAME}.xlsx", firstrow clear 
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

		merge m:1 modelable_entity_name_old is_birth using `bundles', nogen keep(1 3)
			drop if modelable_entity_name_old=="exclude"
			drop modelable_entity_name_old

		// destring vars as necessary for later collapses
		destring cv_* measure_adj prenatal_top prenatal_fd, force replace 
		
		foreach var of varlist cv* {
			destring `var', force replace
		}

	tempfile pre_collapse2
	save `pre_collapse2', replace 

	//-----------------
	// save non-prevalence data; it will not be collapsed
	keep if measure != "prevalence"
		tempfile mtwith
		save `mtwith', replace 

//-------------------------------------------------------
// 4. collapse (prevalence only) to the modelable entity level

use `pre_collapse2', clear 
	drop if measure !="prevalence"
	drop is_birth

	drop if modelable_entity_name=="" // null modelable_entity_names indicate "other" heart, digest, and msk sub-categories at this stage; 

	local collapse_vars input_type underlying_nid nid underlying_field field file_path ///
			source_type location_name location_id ihme_loc_id smaller site sex sex_issue year_start year_end year_issue ///
			age_start age_end age_issue age_demo measure sample_size effective_sample_size design_effect unit_type unit_val ///
			measure_issue uncertainty_type uncertainty_type_val representative urbanicity recall_type recall_type_val sampling ///
			response_rate group spec group_review note_modeler is_outlier modelable_entity_name bundle_id cause
				
	egen collapse_group = group(`collapse_vars'), missing

	bysort collapse_group: gen count = _N

	//--------------------------
	// drop the rows that have already been totaled;
			 drop if regexm(case_name, "Combined:") & count >1

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


collapse (sum) cases prenatal_top prenatal_fd mean lower upper (max) measure_adj cv_*, by( `collapse_vars' ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def extractor)
				replace lower =. if (lower==0 & upper==0)
				replace upper =. if (lower==. & upper==0)


		rename new_* *	

		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

tempfile collapsed1
save `collapsed1', replace 

//------------------------------------------------------
// 4.5. collapse (prevalence only) to the cause level

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

egen collapse_group = group(`collapse_vars'), missing

	bysort collapse_group: gen count = _N

	//--------------------------
	// drop the rows that have already been totaled;
			 drop if regexm(case_name, "Combined:") & count >1

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


	collapse (sum) cases prenatal_top prenatal_fd lower upper mean (max) measure_adj cv_*, by(`collapse_vars' ///
			new_case_name new_case_definition new_note_SR new_table_num new_page_num new_prenatal_fd_def extractor)
				replace lower =. if (lower==0 & upper==0)
				replace upper =. if (lower==. & upper==0)


		rename new_* *

		replace mean = cases/sample_size if (cases >0 & mean ==0 & sample_size !=.)
		replace cases = mean * sample_size if cases==0 & mean >0 
		replace sample_size = cases/mean if sample_size==.

tempfile collapsed2
save `collapsed2', replace

append using `collapsed1'
append using `mtwith'


//----------------------------------------------------------------------
// 4.3: code cv_underreport, drop under-reported data points as necessary 
		
		//export and create cv_underreport mapping file 
			preserve 
				duplicates drop cause modelable case_name bundle_id, force
				keep cause modelable case_name bundle measure
				sort cause modelable measure case_name
				order cause model bundle measure case_name
				export excel using "`dir'/{FILEPATH}/final_lit_cv_subset_coding.xls", firstrow(variables) sheet("{DATE}") sheet replace
			restore 

		preserve
			import excel using "`dir'/{FILEPATH}/final_lit_cv_subset_coding.xls", clear sheet("{DATE}") firstrow
			keep if cv_underreport !=""
			tempfile cv_subset
			duplicates drop
			save `cv_subset', replace
		restore

	merge m:1 case_name bundle cause modelable measure using `cv_subset', nogen keep(3)
		rename cv_underreport cv_under_report


			/* // when needing to add more case names to the cv_underreport mapping file: 
			preserve
			keep if _m==1
			keep cause modelable case_name bundle measure
				duplicates drop 
				sort cause modelable measure case_name
				order cause model bundle measure case_name
					br 
					restore */

			// cv_underreport is coded as 0 if the case names are reasonably complete for a given ME; 
				// coded as 1 if the case names are not complete for a given ME but still complete enough to use and cross-walk to the more complete data points
				// "drop" if the case names are so limited for a given ME that the data should not be used; mostly used for the total-cause MEs where case names are specific to a sub-cause 


//-------------------------------------------------------------
// 5. format & order variables to match the 2016 extraction template

replace seq =""
replace seq_parent =""

order bundle_id modelable_entity_name seq seq_parent underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
		 input_type underlying_field_citation_value design_effect cv* ///
		 prenatal_fd prenatal_top prenatal_fd_definition is_outlier 

		 drop source parent_id is_birth is_2015 is_2016 is_dental data_sheet_filepath


// address string/numeric matches for Epi uploader data validation checks 
	foreach var in underlying_nid input_type recall_type recall_type_value sampling_type uncertainty_type uncertainty_type_value is_outlier group_review effective_sample_size specificity group group_review design_effect underlying_field_citation_value {
		replace `var' = "" if `var'=="."
		}
	replace recall_type = "Not Set" if recall_type ==""

	replace note_SR = substr(note_SR, 1, 1998) // the max character limit for Epi uploader is 2000 characters 
	replace case_definition = substr(case_definition, 1, 1998) // the max character limit for Epi uploader is 2000 characters 

	replace lower = .  if cases !=. & sample_size !=.
	replace upper = .  if cases !=. & sample_size !=.
		replace uncertainty_type_value="" if lower==. & upper==.

// ensure group and group_review vars are coded as expected; code in where missing 
	replace group_review = "0" if (inlist(specificity, "isolatedEA", "allYears_allProvinces"  ) & group_rev=="")
		replace group_review = "1" if spec !="" & group_rev=="" 

	replace is_outlier = "0" if is_outlier==""

tempfile ready
save `ready', replace

//---------------------------------------------------------------
// 6. upload / split into bundle-specific files for upload 

// export all, including insufficient case names
	export excel using "`dir'\{FILEPATH}\last_mult_extraction_all_bundles_combined_`date'.xls", firstrow(variables) replace
		drop if cv_under=="drop" // where the collection of case_names is not sufficient for the entire category; eg. "Dandywark snydrome" for other chromosomal anomalies


	// format "cause" var for folder structure
		replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"
		replace cause = "chromo" if cause=="edpatau" 

	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}/lit_data_from_final_2016_mult_screening_sheets_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				local me = modelable_entity_name
				di in red "Exported `me' data for upload"
			restore
		}
