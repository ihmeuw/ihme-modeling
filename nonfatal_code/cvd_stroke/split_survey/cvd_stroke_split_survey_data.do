//Set prefix based on operating system
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	
	else if c(os) == "Windows" {
		global prefix "J:"
	}
adopath + "FILEPATH"

import excel using "FILEPATH/request_49985.xlsx", firstrow clear
keep if measure=="prevalence"
tempfile prev
gen year_id = round(year_start, 5)
replace year_id=1990 if year_start<1990
replace year_id=2016 if year_id==2015
gen age_id = round(age_start, 5)
replace age_id =1 if age_id==0
drop if input_type=="group_review"
drop if is_outlier ==1
drop if (location_name=="Saudi Arabia" | location_name=="Indonesia" | location_name=="England") & note_sr=="extraction/tabulation via ubcov on 2017-04-22NA"
drop if location_name=="Brazil" & case_name=="rec_stroke"
drop if location_name=="Distrito Federal" & case_name=="rec_stroke"
drop if location_name=="England" & (case_name=="ever_stroke" | case_name=="rec_stroke")
drop if location_name=="Northern Ireland" & case_name=="rec_stroke"
drop if location_name=="Scotland" & (case_name=="ever_stroke" | case_name=="rec_stroke")
drop if location_name=="Sweden" & case_name=="rec_stroke"
drop if location_name=="United States" & (case_name=="ever_stroke" | case_name=="rec_stroke")
drop if nid==120814 
drop if nid==120819 
drop if nid==120812 
replace is_outlier=1 if nid==125159
save `prev', replace

get_ids, table(age_group) clear
tempfile ages
save `ages', replace

use FILEPATH/ratios_02Jun2017.dta, clear
preserve
collapse (mean) mean_ischemic mean_cerhem , by(location_id year_id age_group_id)
gen sex_id = 3
tempfile both
save `both', replace
restore
append using `both'

merge m:1 age_group_id using `ages', keep(3) nogen
split(age_group_name), parse(" to ")
rename age_group_name1 age_start
rename age_group_name2 age_end
drop if age_end==""
destring age_start, replace
destring age_end, replace
generate age_id = age_start
generate sex = "Male" if sex_id==1
replace sex = "Female" if sex_id==2
replace sex = "Both" if sex_id==3

merge 1:m age_id year_id location_id sex using `prev', keep(3) nogen

preserve
replace mean = mean*mean_ischemic
replace lower = .
replace upper = .
replace cases = .
replace uncertainty_type=""
replace uncertainty_type_value=.
generate response_rate=.
# delimit ;
keep location_id age_start age_end sex bundle_id seq nid underlying_nid input_type page_num table_num source_type ihme_loc_id 
		location_name uncertainty_type_id smaller_site_unit site_memo sex_issue input_type_id year_start year_end urbanicity_type_id
		year_issue unit_type_id age_issue age_demographer measure mean lower upper standard_error effective_sample_size 
		cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment uncertainty_type 
		uncertainty_type_value representative_name urbanicity_type source_type_id recall_type recall_type_value
		sampling_type case_name case_definition case_diagnostics group specificity group_review note_modeler note_sr  extractor
		is_outlier design_effect field_citation_value underlying_field_citation_value response_rate;
# delimit cr;
export excel using "FILEPATH/split_survey_02Jun2017_nodups.xlsx", replace firstrow(variables) sheet("extraction")
restore

replace mean = mean*mean_cerhem
replace lower = .
replace upper = .
replace cases = .
replace uncertainty_type=""
replace uncertainty_type_value=.
generate response_rate=.
# delimit ;
keep location_id age_start age_end sex bundle_id seq nid underlying_nid input_type page_num table_num source_type ihme_loc_id 
		location_name uncertainty_type_id smaller_site_unit site_memo sex_issue input_type_id year_start year_end urbanicity_type_id
		year_issue unit_type_id age_issue age_demographer measure mean lower upper standard_error effective_sample_size 
		cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment uncertainty_type 
		uncertainty_type_value representative_name urbanicity_type source_type_id recall_type recall_type_value
		sampling_type case_name case_definition case_diagnostics group specificity group_review note_modeler note_sr  extractor
		is_outlier design_effect field_citation_value underlying_field_citation_value response_rate;
# delimit cr;
export excel using "FILEPATH/split_survey_02Jun2017_nodups.xlsx", replace firstrow(variables) sheet("extraction")


