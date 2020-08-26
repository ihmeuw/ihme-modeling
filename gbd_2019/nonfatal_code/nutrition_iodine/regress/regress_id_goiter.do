
** Purpose: Predict cretinism prevalence from goiter prevalence 
** Key steps: (1) impute missing visible goiter prevalence based on total goiter prevalence; 
**(2) regress cretinism prevalence on visible goiter prevalence ;
**(3) predict cretinism prevalence by country, age, sex, year using dismod output on visible goiter prevalence; 
**(4) set cretinism prevalence to zero for countries where total goiter prevalence is <20% or national hh consumption of iodized salt is >=90%; 
**(5) keep children 1-4 yrs only and run cretinism prevalence as incidence in DisMod 
** Note: Need to incorporate Intellectual Disability (ID) assumptions (see line 235) before running DisMod. 
**Please ask the modeler of Intellectual Disability to get the ID assumptions

// Settings//
	clear all
	set more off 
	set maxvar 32000
	cap restore, not 
	cap log close 
	pause on
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}	
	adopath + "FILEPATH"	
	run "FILEPATH/get_model_results.ado"
	run "FILEPATH/get_covariate_estimates.ado"
	run "FILEPATH/get_location_metadata.ado"


// Define locals
	local gbd_round GBD2019
	local input_dir "FILEPATH/`gbd_round'/nonfatal/regress_cretin_goiter_inputs"
	local in_fil "`input_dir'/cretins_lit_review_data.csv"
	local goit_fil "`input_dir'/idd_input_v12_with_grades.csv" 
    local save_dir "$FILEPATH/`gbd_round'/nonfatal/outputs"
	
    local log_fil "`save_dir'/goiter_cretin_regress.smcl"
	
	get_population, location_id("-1") year_id(-1) sex_id("1 2") age_group_id("-1") location_set_id(22) decomp_step(step1) clear
	tempfile pop_data_all
	save `pop_data_all', replace

    get_location_metadata, location_set_id(22) clear
    keep location_id location_name
    tempfile iso3
    save `iso3', replace

** Step 1: prep the data and impute missing visible goiter prevalence based on total goiter prevalence
	import delim using "`in_fil'", clear 
	replace total_goiter_prev=goiter_prev if total_goiter_prev == .	

	replace total_goiter_prev = total_goiter_prev/100

	replace visible_prev = visible_prev/100
	
	replace cret_prev = cret_prev/100

	gen logit_total_goiter_prev=logit(total_goiter_prev)

	gen logit_visible_prev=logit(visible_prev)
	
	gen logit_cret_prev = logit(cret_prev)

	regress logit_visible_prev logit_total_goiter_prev

	predict pred_logit_visible_prev
	
	replace logit_visible_prev=pred_logit_visible_prev if logit_visible_prev==.

	/* gen visible_prev_new=invlogit(pred_logit_visible_prev) */
	
	tempfile cret_goit_data
	save `cret_goit_data', replace
	
// Prep the dismod output file
   get_location_metadata, location_set_id(22) clear
   keep location_id super_region_id super_region_name region_id region_name ihme_loc_id
   tempfile region_sr
   save `region_sr', replace

   // location set id 35 temporary fix
   get_model_results, gbd_team("epi") gbd_id("1609") gbd_round_id("6") measure_id("5") decomp_step("step4") clear 
//   use `dismod_fil', clear
   save "`save_dir'/dismod_goiter_prev.dta", replace
   drop if age_group_id>21
   keep location_id year_id age_group_id sex_id measure mean
   rename mean visible_prev
   gen logit_visible_prev = logit(visible_prev)
   gen logit_cret_prev =.
// merge on regions
   merge m:1 location_id using `region_sr', keep(3) nogen
   tempfile dismod_goiter_prev
   

   save `dismod_goiter_prev', replace

  // Prep the goiter file which we use as an indicator for countries with less than 20% total goiter 
	import delim using "`goit_fil'", clear
	keep country_iso3_code year_start year_end sex age_start age_end parameter_value grade_total
	keep if grade_total == 1
	gen year_id = year_start 
	drop year_start year_end 
	
    // Recode variables for merging with the dismod dataset so that years correspond 
	rename country_iso3_code iso3 
	replace year = 1990 if year <1995
	replace year = 1995 if year >=1995 & year <2000
	replace year = 2000 if year >= 2000 & year <2005
	replace year = 2005 if year >=2005 & year <2010
	
	
    // Take the highest value in each country
	collapse (max) parameter_value, by(iso3) 
	tempfile totgoit
	save `totgoit', replace 


// Prep the salt dataset 
	//covariate_name_short(hh_iodized_salt_pc) 
	get_covariate_estimates, covariate_id(46) decomp_step("step4") clear

//	use "`salt_fil'", clear 
	rename mean_value hh_iodized_salt_pc
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017) 
	
	// merge on iso3
	// merge on regions
	merge m:1 location_id using `region_sr', keepusing(ihme_loc_id) keep(3)nogen
	rename ihme_loc_id iso3
	keep iso3 location_id year_id hh_iodized_salt_pc
	tempfile salt
	save `salt', replace 	
	
	
** Step 2: regress cretinism prevalence on visible goiter prevalence  
    use `cret_goit_data', clear
	log using "`log_fil'", replace
	regress logit_cret_prev logit_visible_prev
	cap log close
	

	gen ref = 1
	drop sex
	gen sex=3
	append using `dismod_goiter_prev'
	
** Step 3: predict cretinism prevalence by country, age, sex, year using dismod output on visible goiter prevalence	
	predict pred_logit_cret_prev 
	gen inv_cret_prev = invlogit(pred_logit_cret_prev)
	rename inv_cret_prev pred_cret_prev 
	drop if ref==1
	rename ihme_loc_id iso3
// Merge on countries with total goiter 
	merge m:1 iso3 using `totgoit', nogen


** Step 4: set cretinism prevalence to zero for countries where total goiter prevalence is <20% or national hh consumption of iodized salt is >=90%

// Replace prevalence as zero in countries with less than 20% total goiter prevalence 
	replace pred_cret_prev = 0 if parameter_value < 0.2
	replace pred_cret_prev = 0 if goiter_prev == 0 	
	
// Merge on iodized salt data and set cretinism prev to zero for countries with greater than 90% salt iodization
	merge m:1 iso3 year_id using `salt', nogen
	replace pred_cret_prev = 0 if hh_iodized_salt_pc >= 0.9
	
// Replace cretinism as zero in high income countries 
   	replace pred_cret_prev = 0 if super_region_id==64
		
// Mark other regions as indicated in the UNICEF SOWC reports that have sufficient household salt iodization
	replace pred_cret_prev = 0 if (year_id == 2005 | year_id == 2010 | year_id == 2015) & (region_name == "Central Latin America" | region_name == "Central Europe" | region_name == "Southeast Asia" | region_name == "East Asia")  
	
	
// Drop missing data for BMU, PRI, India rural, India urban and ZAF subnationals. They are missing because we do not have dismod output for them.	
    drop if pred_cret_prev ==. 

	
** Step 5: keep children 1-4 yrs only and run cretinism prevalence as incidence in DisMod 
// Keep ages 1-4
	keep if age_group_id == 5		
    sort iso3 year_id
    save "`save_dir'/cretin_prev_step4.dta", replace

	

// prep for DisMod

keep location_id iso3 year_id age_group_id sex_id pred_cret_prev
gen measure="incidence"

// bring in population data

merge m:1 location_id year_id age_group_id sex_id using `pop_data_all', keepusing(population) keep(3)nogen


// add location_name
merge m:1 location_id using `iso3', keepusing(location_name) keep(3)nogen


// format for dismod
gen seq=.
gen seq_parent=.
gen data_sheet_file_path=.
gen input_type="adjusted"
gen underlying_nid=.
gen nid=151700
gen underlying_field_citation_value=.
gen field_citation_value=""
gen page_num=.
gen table_num=.
gen source_type="Mixed or estimation"
gen smaller_site_unit=0 
gen site_memo=.
gen representative_name="Nationally representative only"
replace representative_name="Representative for subnational location only" if strpos(iso3,"BRA_")>0 | strpos(iso3,"CHN_")>0 | strpos(iso3,"GBR_")>0 | strpos(iso3,"IND_")>0 | strpos(iso3,"JPN_")>0 | strpos(iso3,"KEN_")>0 | strpos(iso3,"MEX_")>0 | strpos(iso3,"SAU_")>0 | strpos(iso3,"USA_")>0 | strpos(iso3,"ZAF_")>0 | strpos(iso3,"SWE_")>0
rename iso3 ihme_loc_id	
gen urbanicity_type=""
replace urbanicity_type="Unknown"
gen sex_issue=0
gen year_issue=0
gen age_issue=0
gen age_demographer=0
gen lower=.
gen upper=.													
gen design_effect=.
gen unit_type="Person"
gen unit_value_as_published=1
gen measure_issue=0
gen measure_adjustment=.
gen uncertainty_type="Standard error"
gen uncertainty_type_value=.
gen recall_type="Point"
gen recall_type_value=.
gen sampling_type=""
gen response_rate=.
gen case_name=""
gen case_definition=""
gen case_diagnostics=""
gen group=""
gen specificity=""
gen group_review=.
gen note_modeler=""
gen note_SR=""	
gen extractor="hkl1"
gen is_outlier=0
gen sex=""
replace sex="Male" if sex_id==1
replace sex="Female" if sex_id==2
gen age_start=1
gen age_end=4
gen year_start=year_id
gen year_end=year_id
rename pred_cret_prev mean
			
rename population sample_size
gen standard_error=sqrt(mean*(1-mean)/sample_size)
gen effective_sample_size=.
gen cases=.

// drop countries where total goiter prevalence is < 20% or national hh consumption of iodized salt is >90%
drop if mean==0

sort year_start sex age_start
keep seq seq_parent input_type underlying_nid nid underlying_field_citation_value field_citation_value page_num	table_num source_type location_name	location_id	ihme_loc_id	smaller_site_unit site_memo	representative_name	urbanicity_type sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size design_effect unit_type unit_value_as_published measure_issue measure_adjustment uncertainty_type uncertainty_type_value recall_type recall_type_value	sampling_type response_rate	case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier data_sheet_file_path 

order seq seq_parent input_type underlying_nid nid underlying_field_citation_value field_citation_value page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo representative_name urbanicity_type sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size design_effect	unit_type unit_value_as_published measure_issue	measure_adjustment uncertainty_type uncertainty_type_value recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor is_outlier data_sheet_file_path 

tempfile ready
save `ready', replace

// bring in ID assumptions

import excel using "`input_dir'/ID_assumptions.xlsx", firstrow clear
tostring ihme_loc_id, replace
tostring sampling_type, replace
tostring case_name, replace
tostring case_definition, replace
tostring case_diagnostics, replace
tostring note_SR, replace

// append prepped cretin data

append using `ready'

replace sampling_type = "" if sampling_type == "."
replace case_name = "" if case_name == "."
replace case_definition = "" if case_definition == "."
replace case_diagnostics = "" if case_diagnostics == "."
replace note_SR = "" if note_SR == "."

save "`save_dir'/prepped_for_upload.dta", replace
export excel using "`save_dir'/prepped_for_upload.xlsx", firstrow(variables) sheet("extraction") nolabel replace

