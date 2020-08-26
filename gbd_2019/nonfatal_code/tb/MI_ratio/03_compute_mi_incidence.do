// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Process MI-ratios from MR-BRT to compute incidence adjusted for risk of HIV
// Author:		USERNAME
// Edited:      USERNAME
// Description:	collates inputs for MI-ratio based incidence
//				adjusts for the risk of death associated with HIV
//				back calculate incidence using estimated MI-ratios and CodCorrected deaths
// 				age-splits 65+ incidence data using Brazil age-pattern
// Variables:	mvid, decomp_step, rr_med, model
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************

**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************
 
	// clear memory and establish settings
	clear all
	set more off

	// create necessary locals 
	local mvid v.1
	local decomp_step step4
	local rr_med 2.420979
	local model mrbrt_age_dummy_haq_sr_`mvid'
	local intermediate_dir "FILEPATH/`mvid'"

**********************************************************************************************************************
** PULL INPUTS
**********************************************************************************************************************
 
	// get predictions from MR-BeRT
	insheet using "`intermediate_dir'/preds/cfr_pred_`model'.csv", clear 
	keep location_id location_name year_id sex_id haq age mean_prop lower_prop upper_prop se
	sort location_id year_id age sex_id

	// clean data
	gen cfr_se=(upper_prop-lower_prop)/(2*1.96)
	rename (mean_prop lower_prop upper_prop) (cfr_pred cfr_lower cfr_upper) 

	// save tempfile
	tempfile cfr
	save `cfr', replace

	// Update this
	import delimited "FILEPATH\TB_TBHIV_mortality_tb_codcorrect_v101_v1.6_custom_ages.csv", clear

	// clean
	keep location_id year_id sex_id age all_tb_death_mean all_tb_death_lower all_tb_death_upper
	rename all_tb_death_mean all_tb_death

	// merge location meta-data
	merge m:1 location_id using "FILEPATH\locations_22.dta", keepusing(ihme_loc_id region_name) keep(3)nogen
	rename ihme_loc_id iso3

	// save tempfile
	tempfile death
	save `death', replace

**********************************************************************************************************************
** PULL TB NO-HIV PREVALENCE
**********************************************************************************************************************

	// Pull TB no-HIV prevalence
	use "FILEPATH/tb_prev.dta", clear 
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	keep location_id year_id age_group_id sex_id mean 

	// Pull in populations
	merge m:1 age_group_id using "FILEPATH/age_group_ids.dta", keep(3)nogen
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/population_decomp1.dta", keepusing(population) keep(3)nogen

	// compute cases
	gen cases=mean*population
	drop if age_group_id==22

	// aggregate 65+ age group
	preserve
		qui keep if age_group_id>=18
		collapse (sum) cases population, by(location_id year_id sex_id)
		qui gen age=65
		tempfile tmp_65
		save `tmp_65', replace
	restore

	// create under 5 age group
	preserve
		qui keep if age_group_id<=5
		collapse (sum) cases population, by(location_id year_id sex_id)
		qui gen age=0
		tempfile tmp_0
		save `tmp_0', replace
	restore
			
	// prep to create 10 year age-bins
	drop if age_group_id<=5
	drop if age_group_id>=18

	// create age indicator for aggregation
	split age_group_name,p( to )
	gen age=age_group_name1
	destring age, replace

	// loop through each age-bin and aggregate
	forvalues i=5(10)55 {
		preserve
			local k=`i'+5
			keep if age>=`i' & age<=`k'
			collapse (sum) cases population, by(location_id year sex_id)
			gen age=`i'
			tempfile tmp_`i'
			save `tmp_`i'', replace 
		restore
	}

	// append the files
	use "`tmp_0'", clear
	forvalues i=5(10)55 {
		qui append using "`tmp_`i''"
	}

	// aggregate
	append using "`tmp_65'"	
	qui drop if year<1990

	// clean
	gen prev= cases/population
	tempfile prev
	save `prev', replace

**********************************************************************************************************************
** PULL HIVTB PREVALENCE
**********************************************************************************************************************

	// pull HIVTB prevalence
	use "FILEPATH/hivtb_prev.dta", clear 
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	keep location_id year_id age_group_id sex_id mean 

	// Pull in populations
	merge m:1 age_group_id using "FILEPATH/age_group_ids.dta", keep(3)nogen
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/population_decomp1.dta", keepusing(population) keep(3)nogen

	// compute cases
	gen cases=mean*population
	drop if age_group_id==22

	// aggregate 65+ age group
	preserve
		qui keep if age_group_id>=18
		collapse (sum) cases population, by(location_id year_id sex_id)
		qui gen age=65
		tempfile tmp_65
		save `tmp_65', replace
	restore

	// create under 5 age group
	preserve
		qui keep if age_group_id<=5
		collapse (sum) cases population, by(location_id year_id sex_id)
		qui gen age=0
		tempfile tmp_0
		save `tmp_0', replace
	restore
			
	// prep to create 10 year age-bins
	drop if age_group_id<=5
	drop if age_group_id>=18

	// create age indicator for aggregation
	split age_group_name,p( to )
	gen age=age_group_name1
	destring age, replace

	// loop through each age-bin and aggregate
	forvalues i=5(10)55 {
		preserve
			local k=`i'+5
			keep if age>=`i' & age<=`k'
			collapse (sum) cases population, by(location_id year sex_id)
			gen age=`i'
			tempfile tmp_`i'
			save `tmp_`i'', replace 
		restore
	}

	// append the files
	use "`tmp_0'", clear
	forvalues i=5(10)55 {
		qui append using "`tmp_`i''"
	}

	// aggregate
	append using "`tmp_65'"	
	qui drop if year<1990

	// compute cases
	gen hivtb_prev= cases/population

	// merge in TB no-HIV
	merge 1:1 location_id year_id age sex_id using `prev', keepusing(prev) keep(3)nogen

	// compute adjustment factor for HIVTB
	gen hiv_prop=hivtb_prev/prev
	gen RR_median=`rr_med'
	gen hiv_rr_cyas=hiv_prop*RR_median+(1-hiv_prop)*1

	// save tempfile
	tempfile hiv
	save `hiv', replace

**********************************************************************************************************************
** ADJUST FOR HIVTB
**********************************************************************************************************************

	// pull in mortality
	use `death', clear
	merge 1:1 location_id year_id age sex_id using `prev', keep(3)nogen
	merge m:1 location_id year_id age sex_id using `cfr', keep(3)nogen
	merge m:1 location_id year_id age sex_id using `hiv', keep(3)nogen

	// adjust for risk of HIV
	gen cfr_pred_hiv=cfr_pred*hiv_rr_cyas
	gen cfr_pred_hiv_lower=cfr_lower*hiv_rr_cyas
	gen cfr_pred_hiv_upper=cfr_upper*hiv_rr_cyas

	// save intermediate file
	export excel using "`intermediate_dir'/temp/cfr_`model'.xlsx", firstrow(variables) nolabel replace

	// compute CSMR
	gen csmr=all_tb_death/population
	gen csmr_lower=all_tb_death_lower/population
	gen csmr_upper=all_tb_death_upper/population
	gen csmr_se=(csmr_upper-csmr_lower)/(2*1.96)

	// compute incidence
	gen inc_cases=all_tb_death/cfr_pred
	gen inc_cases_hiv=all_tb_death/cfr_pred_hiv
	gen inc_rate=inc_cases_hiv/population

	// create combined se
	gen inc_rate_se=sqrt((csmr^2/cfr_pred_hiv^2)*((csmr_se^2/csmr^2)+(cfr_se^2/cfr_pred_hiv^2)))

	// format
	gen sex=""
	replace sex="Male" if sex_id==1
	replace sex="Female" if sex_id==2

	// fix age groups
	gen age_start=age
	gen age_end=age_start+9
	replace age_end=4 if age_start==0
	replace age_end=100 if age_start==65

	// compute hiv incidence
	gen inc=inc_cases/population
	gen inc_hiv=inc_cases_hiv/population
	gen duration=prev/inc
	gen duration_hiv=prev/inc_hiv

	// order variables
	order location_id location_name iso3 year_id sex_id age_start age_end haq all_tb_death cfr_pred cfr_pred_hiv inc_cases inc_cases_hiv duration duration_hiv

	// save
	save "`intermediate_dir'/temp/inc_from_death_cfr_`model'.dta", replace

**********************************************************************************************************************
** IDENTIFY LOCATIONS WHERE MI-RATIO BASES INCIDENCE IS LOWER THAN NOTIFICATIONS
**********************************************************************************************************************

	// pull in notifications
	use "FILEPATH", clear
	keep iso3 year age sex_id CN_bact_xb
	replace year=2010 if year==2008 & iso3=="PER"

	// formate age groups
	gen age_start=age
	gen age_end=age_start+9
	replace age_end=4 if age_start==0
	replace age_end=100 if age_start==65

	// rename variables
	rename CN_bact_xb cases_notification
	rename year year_id

	// save tempfile
	tempfile tmp
	save `tmp', replace 

	// read in MI-based incidence
	use "`intermediate_dir'/temp/inc_from_death_cfr_`model'.dta", clear

	// clean
	replace duration_hiv=prev/inc_hiv
	order location_id location_name iso3 year_id age_start age_end sex sex_id haq cfr_pred cfr_pred_hiv inc_cases inc_cases_hiv all_tb_death inc_rate_se
	keep location_id location_name iso3 year_id age_start age_end sex sex_id haq cfr_pred cfr_pred_hiv cfr_pred_hiv_lower cfr_pred_hiv_upper se inc_cases inc_cases_hiv all_tb_death inc_rate_se

	// merge in notifications
	merge 1:1 iso3 year_id age_start age_end sex_id using `tmp', nogen

	// clean
	drop age
	drop if cfr_pred_hiv==.
	sort location_id year_id age_start sex

	// compute MI-ratios for notification and GBD estimated incidence
	gen cfr_notification=all_tb_death/cases_notification
	gen cfr_predicted_incidence=all_tb_death/inc_cases_hiv

	// compute ratios of CFR
	preserve
		keep if year_id==2010
		gen cfr_difference=cfr_notification/cfr_predicted_incidence
		keep location_id age_start age_end sex cfr_difference
		tempfile cfr_diff
		save `cfr_diff', replace
	restore

	// merge in the ratios
	merge m:1 location_id age_start age_end sex using `cfr_diff', keep(3)nogen
	sort location_id year_id age_start age_end sex
	
	// Save
	outsheet using "`intermediate_dir'/temp/CFR_`model'.csv", comma names replace

**********************************************************************************************************************
** FORMAT MI RATIOS FOR DISMOD
**********************************************************************************************************************

	// EMR format for DisMod
	insheet using "`intermediate_dir'/temp/CFR_`model'.csv", clear
	keep location_id iso3 year_id age_start age_end sex sex_id cfr_pred_hiv cfr_pred_hiv_lower cfr_pred_hiv_upper se
	merge m:1 location_id using "FILEPATH/locations_22.dta", keepusing(location_name) keep(3)nogen

	// compute summaries for MI-Ratio
	replace cfr_pred_hiv_lower = cfr_pred_hiv - 1.96*se
	replace cfr_pred_hiv_upper = cfr_pred_hiv + 1.96*se
	replace cfr_pred_hiv_lower = 0 if cfr_pred_hiv_lower < 0
	replace cfr_pred_hiv_upper = 1 if cfr_pred_hiv_lower > 1

	// clean
	rename (cfr_pred_hiv cfr_pred_hiv_lower cfr_pred_hiv_upper iso3 se) (mean lower upper ihme_loc_id standard_error)

	// save
	order seq seq_parent input_type	underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_sr extractor is_outlier cv_*
	export excel using "`intermediate_dir'/output/emr_`model'.xlsx", firstrow(variables) sheet("extraction") nolabel replace

**********************************************************************************************************************
** CLEAN MI-RATIO BASED INCIDENCE
**********************************************************************************************************************

	// Pull results
	insheet using "`intermediate_dir'/temp/CFR_`model'.csv", clear
	drop location_name

	// clean
	gen ihme_loc_id=iso3
	gen age=age_start
	gen year=year_id

	// merge locations and populations
	merge m:1 ihme_loc_id using "FILEPATH/locations_22.dta", keepusing(location_id location_name) keep(3) nogen
	merge 1:1 iso3 year age sex_id using "FILEPATH/pop_custom_age.dta", keep(3)nogen
	keep location_id location_name iso3 year_id sex_id age age_start cases_adjusted pop inc_rate_se

	// formate sex columns
	gen sex=""
	replace sex="Male" if sex_id==1
	replace sex="Female" if sex_id==2

	// save tempfile
	tempfile all
	save `all', replace

	// preserve a copy of 10 year aggregate data
	preserve
		keep if age_start>=5 & age_start<=55
		gen age_end=age_start+9
		tempfile tmp_1
		save `tmp_1', replace
	restore

	// clean
	keep if age_start==0
	gen age_end=4
	append using `tmp_1'
	rename pop sample_size

	// save tempfile
	tempfile tmp_0to64
	save `tmp_0to64', replace

**********************************************************************************************************************
** CLEAN MI-RATIO BASED INCIDENCE
**********************************************************************************************************************

	// pull in star ratings
	insheet using "FILEPATH/custom_four_plus_stars.csv", comma names clear

	// save tempfile
	tempfile stars
	save `stars', replace

	// pull populations
	insheet using "FILEPATH/population_step4.csv", comma names clear

	// drop aggregate locations
	drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
	  
	// merge in age group metadata
	merge m:1 age_group_id using "FILEPATH/age_group_ids.dta", keep(3)nogen
	keep if age_group_id==1| (age_group_id>=6 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	replace age_group_name="0 to 4" if age_group_name=="Under 5"
	replace age_group_name="95 to 100" if age_group_name=="95 plus"

	// create aggregate 90+ age group
	preserve
		keep if age_group_name=="90 to 94" | age_group_name=="95 to 100"
		collapse (sum) population, by (location_id year_id sex_id) fast
		gen age_group_name="90 to 100"
		tempfile tmp_90
		save `tmp_90', replace
	restore

	// append new age group
	drop if age_group_name=="90 to 94" | age_group_name=="95 to 100"
	append using `tmp_90'

	// prep age indicator variable
	split age_group_name, p( to )
	rename age_group_name1 age_start
	rename age_group_name2 age_end

	// destring
	destring age_start, replace
	destring age_end, replace

	// format sex variable
	gen sex=""
	replace sex="Male" if sex_id==1
	replace sex="Female" if sex_id==2

	// save tempfile
	tempfile pop_all
	save `pop_all', replace

	// only 65+ data
	preserve
		keep if age_start>=65
		tempfile pop_65_plus
		save `pop_65_plus', replace
	restore

	// crate a 65+ only data set
	drop if age_start>=65
	tempfile pop_under65
	save `pop_under65', replace

**********************************************************************************************************************
** OUTPUT AN EXCEL SHEET READY FOR INCIDENCE UPLOAD
**********************************************************************************************************************

	// clean
	gen nid=305682
	keep nid location_id location_name iso3 year_id age_start age_end sex cases_adjusted sample_size inc_rate_se
	gen mean=cases_adjusted/sample_size
	gen standard_error=. 
	rename iso3 ihme_loc_id

	// merge in location data
	merge m:1 location_id using "FILEPATH/location_id_parent.dta" 
	drop if nid==.
	gen year_start=year_id
	gen year_end=year_id

	// generate needed columns
	gen cases=.
	gen lower=.
	gen upper=.

	// change representative_name
	gen representative_name="Nationally representative only"
	replace representative_name="Representative for subnational location only" if parent=="Brazil" | parent=="China" | parent=="India" | parent=="Japan" | parent=="Kenya" | parent=="Mexico" | parent=="Sweden" | parent=="United Kingdom" | parent=="United States" | parent=="South Africa" | parent=="Indonesia"  
	replace representative_name="Representative for subnational location only" if regexm(ihme_loc_id,"IRN_") | regexm(ihme_loc_id,"ETH_") | regexm(ihme_loc_id,"RUS_") | regexm(ihme_loc_id,"UKR_")| regexm(ihme_loc_id,"NOR_") | regexm(ihme_loc_id,"NZL_")
		
	duplicates drop 

	// save
	sort location_name year_start sex age_start
	keep seq seq_parent input_type underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_sr extractor is_outlier cv_* inc_rate_se
	order seq seq_parent input_type	underlying_nid nid underlying_field_citation_value field_citation_value file_path page_num	table_num	source_type	location_name	location_id	ihme_loc_id	smaller_site_unit	site_memo	representative_name	urbanicity_type	sex	sex_issue	year_start	year_end	year_issue	age_start	age_end	age_issue	age_demographer	measure	mean	lower	upper	standard_error	effective_sample_size	cases	sample_size	design_effect	unit_type	unit_value_as_published	measure_issue	measure_adjustment	uncertainty_type uncertainty_type_value	recall_type	recall_type_value	sampling_type	response_rate	case_name	case_definition	case_diagnostics group specificity group_review note_modeler note_sr extractor is_outlier cv_* inc_rate_se


	// drop IND states without urban/rural differentiation
	drop if location_id>=4841 & location_id<=4875
	sort location_id year_start age_start sex

	// compute cases
	replace cases=mean*sample_size
	drop if mean==.

	// get se
	insheet using "`intermediate_dir'/temp/CFR_`model'.csv", comma clear
	keep if age_start==65 & age_end==100
	keep location_id year_id sex inc_rate_se
	rename inc_rate_se se_65

	// save tempfile
	tempfile se
	save `se', replace

	// pull in data
	import excel using "`intermediate_dir'/output/incidence_`model'.xlsx", firstrow clear

	// merge in se
	gen year_id=year_start
	merge m:1 location_id year_id sex using `se', keep(3)nogen 
	replace inc_rate_se=se_65 if age_start>=65
	replace standard_error=inc_rate_se
	drop se_65 inc_rate_se year_id
	sort location_id year_start age_start sex

	// save
	export excel using "`intermediate_dir'\incidence_`model'.xlsx", firstrow(variables) sheet("extraction") nolabel replace
