
** Purpose: append together and upload full life-course data for all congenital non-birth prevalence bundles
** {AUTHOR NAME}
**----------------------------------------------------------

** setup
clear all 
macro drop _all
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

run "$dl/{FILEPATH}/get_location_metadata.ado"
local dir "$dl/{FILEPATH}"
local date {DATE}

local fullmod_bundle_list {BUNDLE ID LIST} 
 
foreach b in `fullmod_bundle_list' {

 ** define the cause name 
 		if `b'==435 {
			local c cong_cleft
			}

 		if `b'==436 {
			local c cong_downs
			}

		if `b'==437 {
			local c cong_turner
			}

		if `b'==438 {
			local c cong_klinefelter 
			}

		if (`b'==439 | `b'==638) {
			local c cong_chromo
			}

		if (`b'==602 | `b'==604 | `b'==606  ) {
			local c cong_msk
			}

		if (`b'==608 | `b'==610 | `b'==612 | `b'==614 ) {
			local c cong_neural
			}

		if (`b'==616 | `b'==618 ) {
			local c cong_urogenital
			}

		if (`b'==620 | `b'==622  `b'==624 | `b'==626 ) {
			local c cong_digestive
			}


		if (`b'==628 | `b'==630 | `b'==632 | `b'==634 |`b'==636 )  {
			local c cong_heart 
			}

	di in red "`c' `b'" 
	
**-----------------------------------------------------------------------
** 1. Upload all prepped files from each bundle folder

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
				gen source= "`name'"
					tempfile `name'
					save ``name'', replace 
					di "`ff'"
				di in red "`name' imported"

				local `b'_file_names `name'
			}

		local f2 "`dir'/{FILEPATH}"
		local files : dir "`f2'" files *
			foreach ff in `files' {
				local name = substr("`ff'", 1, 18)
				import excel using "`f2'/`ff'", firstrow clear
					foreach var in table_num representative_name unit_type uncertainty_type uncertainty_type_value specificity input_type page_num case_definition case_diagnostics underlying_field_citation file_path sampling_type note_modeler prenatal_fd_def extractor {
						cap tostring `var', replace
						}
					foreach var in smaller_site_unit group cases location_id standard_error sex_issue year_start year_end year_issue age_start age_end age_issue age_demo unit_value_as measure_issue is_outlier group_review measure_adjustment response cv_* {
						cap destring `var', replace			
							}

					cap replace input_type= "extracted" if input_type=="."
					replace extractor="{USERNAME}" if extractor==""
				gen source= "`name'"
					tempfile `name'
					save ``name'', replace 
				di in red "`name' imported"

				local `b'_file_names = "``b'_file_names'" + " " + "`name'"
				}


di "``b'_file_names'"

**--------------------------------------------------
** 2. Append data together 

	local i 0
	foreach f of local `b'_file_names {
		 di "`f'"
		 local f = "``f''" 
			** di `"`f'"'
			
		 if `i'==0 {
			use "`f'", clear
			} 
		if `i' > 0 {
	 	append using "`f'"
	 	} 
			
		local i= `i'+1 
		} 

	 use `china', clear
		append using `lit_data_from_mult'
		append using `lit_data_from_neur'
		append using `lit_data_from_fina'
		append using `lit_data_from_2013'
		append using `{USERNAME}_downsyn_e'

		** address duplicate vars
			foreach var in 1facility_only aftersurgery prenatal {
				cap replace cv_`var' = future_cv_`var' if future_cv_`var' !=.
				cap drop future_cv_`var'
				}

			rename prenatal_fd_definition keepme
			cap replace keepme =  prenatal_fd_def if keepme !=""
				cap drop prenatal_fd_def
				rename keepme prenatal_fd_definition

			cap replace outlier_type_id = is_outlier if is_outlier !=.
				cap drop is_outlier

			replace cv_excludes_chromos =1 if (cv_includes_chromos==0 & cv_excludes_chromos==.)
				replace cv_excludes_chromos =0 if (cv_includes_chromos==1 & cv_excludes_chromos==.)


		** address variable formatting issues
			foreach var in sampling_type uncertainty_type uncertainty_type_value spec input_type {
					replace `var' ="" if `var'=="."
					}

			replace measure = lower(measure)
			replace urbanicity_type = "Mixed/both" if urbanicity_type =="Mixed/Both"
			replace uncertainty_type_value = "" if uncertainty_type==""
			replace sampling_type = proper(sampling_type)


			** other adjustments for extracted data

				** Down Syndrome lit extraction 
				replace lower = lower/1000 if nid==138940 & mean < upper
				replace upper = upper/1000 if nid==138940 & mean < upper
					replace uncertainty_type_value = "95" if lower !=. & upper !=.

				expand 2 if nid== 273002 & location_name=="United Kingdom", gen(temp)
					replace location_id = 4749 if (nid== 273002 & location_name=="United Kingdom" & temp==0)
					replace location_id = 4636 if (nid== 273002 & location_name=="United Kingdom" & temp==1)
					replace site_memo = "England, Wales - one copy uploaded for each location" if (nid== 273002 & location_name=="United Kingdom")
					drop temp

				drop if sample_size < 0
				drop if (cases== 0 & mean== 0 & sample_size==.) 
				drop if (cases==0 & mean==0 & sample_size==0 )

				replace representative_name = "Unknown" if representative_name=="."

				replace unit_type = "Person" if measure=="prevalence" & unit_type=="."
				replace unit_type = "Person*year" if measure=="mtwith" & unit_type=="."

				replace group = 1 if source=="lit_data_from_2013" & spec !="" & group==.
				replace group_review = 1 if source=="lit_data_from_2013" & spec !="" & group_review==.

				replace case_definition = substr(case_definition, 1, 1998)
				replace note_SR = substr(note_SR, 1, 1998)

				replace sampling_type = "Simple random" if sampling_type=="Simple Random"

				replace nid=281233 if nid==281191 & regexm(field_citation, "Kasai hepatoportoenterostomy")


		** order according to the extraction template
		order bundle_id modelable_entity_name seq nid field_citation_value underlying_nid underlying_field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type design_effect input_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
			cv* prenatal_top prenatal_fd prenatal_fd_definition outlier_type_id 

		** drop extra vars; no necessary information
			foreach var in cv_postnatal source seq_parent modelable_entity_id data_sheet_filepath  count ihme_loc_id cv_inpatient parent_id cv_no_still_births{
				cap drop `var'
				}

		** check the covariates included; rename as necessary
			** chromosomal causes shouldn't have cv_includes_chromos
			if (`b'==436 | `b'==437 | `b'==438 | `b'==439 | `b'==638) {
				drop cv_includes_chromos cv_excludes_chromos
				}

		** 2. Save appended file for upload to the database

		export excel using  "`dir'/{FILEPATH}/all_current_`b'_data_`date'.xlsx", replace firstrow(variables) sheet("extraction")
			di in red "`c' `b' input data saved"

	
			** clean up tempfiles 
			cap erase `china'
			cap erase `lit_data_from_mult'
			cap erase `lit_data_from_neur'
			cap erase `lit_data_from_fina'
			cap erase `lit_data_from_2013'
			cap erase `{USERNAME}_downsyn_e'

** close the loop over bundles 
}
run "{FILEPATH}/upload_epi_data.ado"
local {DATA}

local fullmod_bundle_list {BUNDLE ID LIST}	

foreach b in `fullmod_bundle_list' {

	local file "{FILEPATH}/all_current_`b'_data_`date'.xlsx"
	upload_epi_data, bundle_id("`b'") filepath("`file'") clear 
	
}
