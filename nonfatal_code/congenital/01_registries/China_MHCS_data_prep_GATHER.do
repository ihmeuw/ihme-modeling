

// Purpose: prepare the China National Maternal and Child Health Surveillance System Data for upload to the database, GBD 2016
// {AUTHOR NAME}

// setup
	clear all
	set more off
	set maxvar 10000

	cap restore, not
	cap log close


// file paths
	local in_dir "{FILEPATH}"
	local out_dir "{FILEPATH}"

	local var_map "{FILEPATH}"
	local prov_map 

// set local for date 
	local date {DATE}


//---------------------------------------------------------------------------------
// load data

	import delimited using "{FILEPATH}", clear 	
		tempfile raw1
			save `raw1', replace 

	import delimited using "{FILEPATH}", clear 
		tempfile raw2
		save `raw2', replace 

//-----------------------------------------
	use `raw1', clear
		append using `raw2'


// map to causes
	preserve
		import excel using "`out_dir'/MCHS mapping instructions - causes - GBD 2016 update.xlsx",  firstrow clear
			keep code cause_2016 modelable_entity description icd_included

				rename icd_included case_definition
				rename description case_name

				rename cause_2016 cause 
				rename code bd 
		tempfile cause_map
		save `cause_map', replace
	restore

	merge m:1 bd using `cause_map' , nogen 

	drop if bd=="total"


	preserve
		import excel using "`var_map'", firstrow clear
			keep variable explanation
		tempfile var_map
		save `var_map', replace
	restore

	
//-------------------------------------
// aggregate across mother's ages... 
	egen m_cases = rowtotal(bd_um1 bd_um2 bd_um3 bd_um4 bd_um5 bd_rm1 bd_rm2 bd_rm3 bd_rm4 bd_rm5), missing 
	egen f_cases = rowtotal(bd_uf1 bd_uf2 bd_uf3 bd_uf4 bd_uf5 bd_rf1 bd_rf2 bd_rf3 bd_rf4 bd_rf5), missing 
	egen unknown_cases = rowtotal(bd_ufm1 bd_ufm2 bd_ufm3 bd_ufm4 bd_ufm5 bd_rfm1 bd_rfm2 bd_rfm3 bd_rfm4 bd_rfm5), missing 
		egen cases_tot = rowtotal(m_cases f_cases unknown_cases)

	egen m_births = rowtotal(um1 um2 um3 um4 um5 rm1 rm2 rm3 rm4 rm5), missing 
	egen f_births = rowtotal(uf1 uf2 uf3 uf4 uf5 rf1 rf2 rf3 rf4 rf5), missing
	egen unknown_births = rowtotal(ufm1 ufm2 ufm3 ufm4 ufm5 rfm2 rfm2 rfm3 rfm4 rfm5), missing 
		egen births_tot = rowtotal(m_births f_births unknown_births)


		drop ufm* uf* um* rm* rf* bd_ufm* bd_uf* bd_um* bd_rf* bd_rm* 

		// assign "unknown" gender cases to m/f split based on m/f ratio:
			egen mf_cases = rowtotal(m_cases f_cases)
			egen mf_births = rowtotal(m_births f_births)

				gen propbirths_m = m_births/mf_births
				gen propbirths_f = f_births/mf_births
				gen propbirths_idk = unknown_births/mf_births

				gen propcases_m = m_cases/mf_cases
				gen propcases_f = f_cases/mf_cases
				gen propcases_idk = unknown_cases/mf_cases

			gen new_m_births = (m_births + (propbirths_m*unknown_births))
			gen new_f_births = (f_births + (propbirths_f*unknown_births))

			gen new_m_cases = (m_cases + (propcases_m*unknown_cases))
			gen new_f_cases = (f_cases + (propcases_f*unknown_cases))
		
				// clean up 
				drop m_births f_births unknown_births m_cases f_cases unknown_cases mf_cases mf_births prop* birth 

				rename new_*births births_*
				rename new_*cases cases_*
				rename *_ *

			// reshape to be long on gender
			reshape long births cases, i(year province case_name modelable_entity_name cause) j(sex) string 
				replace sex="Female" if sex=="_f"
				replace sex="Male" if sex=="_m"
				replace sex="Both" if sex=="_tot"


	// outcome data and diagnosis information is only available for both sexes combined
		foreach var in outc_1 outc_2 outc_3 outc_4 dig_ch1 dig_ch2 dig_nonch1 dig_nonch2 dig_nonch3 {
			replace `var'= . if sex!="Both"
				}


//------------------------------------------------------------------------------------------
// code in the diagnosis type information 

	// chromosomal diagnosis

gen temp1 = "autopsy" if dig_nonch1 > 0 & dig_nonch1 !=.
gen temp2 = "ultrasound" if dig_nonch1 > 0 & dig_nonch2 !=.
gen temp3 = "other nonchromosomal diagnoses" if dig_nonch2 > 0 & dig_nonch3 !=.
gen temp4 = "AFP or chromosomal examination" if dig_ch1 >0 & dig_ch1 !=.
gen temp5 = "other chromosomal diagnosis" if dig_ch2 >0 & dig_ch2 !=.
gen temp6 = "cases missing diagnostic information" if dig_chmiss >0 & dig_chmiss !=.
	egen case_diagnostics = concat(temp1 temp2 temp3 temp4 temp5 temp6), punct(", ")
		replace case_diag = subinstr(case_diag, ", ,", "", .)
		replace case_diag = subinstr(case_diag, " , ", "", .)
		replace case_diag = subinstr(case_diag, " ,", "", .)
		replace case_diag = subinstr(case_diag, "diagnosis,", "diagnosis", .)
		replace case_diag = "diagnosis information not provided" if case_diag==" "

	replace case_diagnostics = "includes " + case_diagnostics 

	// dropping the counts of cases by diagnosis type
	drop dig*
		 

//-------------------------------------------------------------
	// "Outcome" variables (stillbirths, deaths; no information provided on terminations)

		// note that the "live births" var includes only cases that lived past 7 days of age; 
			egen cases_fd = rowtotal(outc_2 outc_3), mi 
				rename outc_1 cases_surv
				rename outc_4 deaths
				drop outc_2 outc_3


	// drop extra info 
		drop bd
		drop if modelable_entity=="exclude"


tempfile ready_to_collapse
save `ready_to_collapse', replace

//-------------------------------------------------------------------------------------
// collapse to modelable entities
use `ready_to_collapse', clear

	// concatenate the case names together by ME 
	gen new_case_name=""
		egen me_num = group(modelable)
		levelsof me_num, local(MEs)
			foreach me in `MEs' {
				di in red `me'
					levelsof case_name if me_num==`me', local(case_names_`me') clean separate(", ")
						replace new_case_name = "`case_names_`me''" if me_num==`me'
						}

			replace case_definition = "Includes ICD-10 codes " + case_definition

	// collapse
		collapse (sum) cases cases_fd cases_surv deaths outc_miss, ///
			by(year province sex modelable_entity_name new_case_name cause case_definition births)

	tempfile collapsed1
	save `collapsed1', replace 


//---------------------------------------------------------------------------------------------
// collapse to the cause level 

use `ready_to_collapse', clear
	keep if inlist(cause, "heart", "neural", "digest", "msk")

	replace modelable= "Total congenital heart birth defects" if cause=="heart"
		replace modelable= "Total neural tube defects" if cause=="neural"
		replace modelable= "Total congenital musculoskeletal birth defects" if cause=="msk"
		replace modelable= "Total congenital digestive birth defects" if cause=="digest"
		
// create new case names and case definitions here 
gen new_case_name=""
		egen me_num = group(modelable)
		levelsof me_num, local(MEs)
			foreach me in `MEs' {
				di in red `me'
					levelsof case_name if me_num==`me', local(case_names_`me') clean separate(", ")
						replace new_case_name = "`case_names_`me''" if me_num==`me'

					levelsof case_definition if me_num==`me', local(case_defs_`me') clean separate(", ")
						replace case_definition = "`case_defs_`me''" if me_num==`me'
						}

			replace case_definition = "Includes ICD-10 codes " + case_definition

	collapse (sum) cases cases_fd cases_surv deaths outc_miss, ///
			by(year province sex modelable_entity_name new_case_name cause case_definition births)
	
tempfile collapsed2
save `collapsed2', replace 

//----------------------------------------------------------------------------------------------
// calculate 7-day mortality rate with the "outc_4" variable, then append this to the rest of the dataset 
	// can only calcualte mortality rate for both sexes combined 
	// denominator is the number of cases with known outcomes, not all cases 

	preserve
	use `collapsed1', clear 
		keep if sex=="Both"

			gen measure="mtwith"
			gen age_start = 0
			gen age_end = 7/365
			gen age_demographer = 1

		gen cases_new = cases - outc_miss
			drop if cases_new ==0

				gen prop_dead = deaths/cases_new
				bysort year province modelable_entity_name: gen mean = (-ln(1-(prop_dead))/(age_end-age_start))

				drop prop_dead cases births

				rename deaths cases
				rename cases_new sample_size

		tempfile mtwith1
		save `mtwith1', replace
	restore

//----------------------------------
// calc mortality for the cause level

preserve
	use `collapsed2', clear 
			gen measure="mtwith"
			gen age_start = 0
			gen age_end = 7/365
			gen age_demographer = 1

		gen cases_new = cases - outc_miss
			drop if cases_new ==0

				gen prop_dead = deaths/cases_new
				bysort year province modelable_entity_name: gen mean = (-ln(1-(prop_dead))/(age_end-age_start))

				drop prop_dead cases births

				rename deaths cases
				rename cases_new sample_size

		tempfile mtwith2
		save `mtwith2', replace
	restore

//-----------------------------
use `collapsed1', clear
	append using `collapsed2'
	append using `mtwith1'
	append using `mtwith2'

tempfile ready_for_loc_info
save `ready_for_loc_info', replace 

//--------------------------------------------------------------------------------
// map province numbers to GBD province information

use `ready_for_loc_info', clear 		
	
	// China's province code numbers are mapped to GBD 2013 X-ICD codes
		preserve
			import delimited using "{FILEPATH}\China province codes.csv", clear 
			rename region province

			tempfile china_provs
			save `china_provs', replace 
		restore 

		merge m:1 province using `china_provs', nogen

	// merge to older location metadata (X-ICD codes)
			preserve
				use "{FILEPATH}", clear
				keep if gbd_country_name=="China"

				tempfile old_china_locs
				save `old_china_locs', replace
			restore

		merge m:1 iso3 using `old_china_locs', keep(3) nogen keepusing(location_name location_id)

	// merge to current location metadata (get ihme_loc_id)

			preserve
				run "{FILEPATH}\get_location_metadata.ado"
						get_location_metadata, location_set_id(9) clear
						keep if regexm(ihme_loc_id, "CHN")

				tempfile current_locs
				save `current_locs', replace
			restore

		merge m:1 location_name location_id using `current_locs', keepusing(ihme_loc_id) keep(3) nogen

	tempfile almost_ready
	save `almost_ready', replace 


//--------------------------------------------------------------------------------
// other variables and metadata necessary for uploader

use `almost_ready', replace 

// age and measure information for birth prevalence data
	replace age_start = 0 if age_start==.
	replace age_end = 0 if age_end ==.
	replace measure = "prevalence" if measure ==""

		// hide duplicate non-sex split prevalence data
			gen group =1 if measure=="prevalence" & sex !="Both"
				replace group =2 if measure=="prevalence" & sex=="Both"
			gen group_review =1 if group==1
				replace group_review =0 if group==2
			gen specificity ="gender" if group==1
				replace specificity = "both genders" if group==2

//-------------------------------------------
// merge in bundle_id values

	preserve
			import excel using "{FILEPATH}\Bundle_to_ME_map_{USERNAME}.xlsx", firstrow clear 
				gen is_birth_prev = 1 if regexm(modelable_entity_name, "irth prevalence")
					replace is_birth_prev =0 if is_birth_prev==.
					keep if modelable_entity_name_old !=""
					replace modelable_entity_name_old =trim(modelable_entity_name_old)

				tempfile bundle_map
				save `bundle_map', replace
			restore

		rename modelable_entity_name modelable_entity_name_old
			replace modelable_entity_name_old =trim(modelable_entity_name_old)
		gen is_birth_prev =1 if age_end==0
			replace is_birth_prev =0 if is_birth_prev==.

	merge m:1 modelable_entity_name_old is_birth_prev using `bundle_map' , nogen keep(1 3)


//-------------------------------------
// nid and citation information

	gen nid = 135199 

	gen file_path =""
		replace file_path = "{FILEPATH}" ///
			if year >= 1996 & year <= 2006
		replace file_path = "{FILEPATH}" ///
			if year >= 2007 & year <= 2012

	gen field_citation_value = "China National Maternal and Child Health Surveillance System Congenital Anomalies 1996-2012 - MCHS"
		

// add variables in the order of the Epi template 
	
		gen underlying_nid =""
		gen page_num =""
		gen table_num =""

		gen source_type = "Registry - congenital"
		gen smaller_site_unit =0

		gen sex_issue = 0
		gen year_end = year
		gen year_start = year
		gen year_issue = 0
		gen age_issue = 0
		replace age_demographer = 0 if age_demographer ==.
		gen site_memo = location_name

		gen lower =.
		gen upper =.
		gen standard_error =.
		replace sample_size = births if sample_size ==.
		gen effective_sample_size = .
		gen sampling_type = ""
		gen response_rate = ""

		gen unit_type = "Person"
		gen unit_value_as_published = 1
		gen measure_issue = 0
		gen measure_adjustment = 0
		gen uncertainty_type_value = .
		gen uncertainty_type = "Sample size"
		gen recall_type_value =.
		gen design_effect =.

		gen representative_name = "Representative for subnational location only"

		gen urbanicity_type = "Mixed/both"
		gen recall_type = "Point"

		gen case_name = modelable_entity_name
		gen note_SR = "terminations not reported; counts of stillbirths and neonatal mortality only available for both genders combined"
		gen note_modeler =.
		gen extractor = "{USERNAME}"

		rename cases_fd stillbirth_count 
		gen cases_top =.

		rename case_definition case_diagnostics
		rename new_case_name case_definition

		// covariates
		gen cv_low_income_hosp =0
		gen cv_1facility_only =0
		gen cv_topnotrecorded =0
		gen cv_livestill = 1 if measure=="prevalence" & sex!="Both"
			replace cv_livestill =0 if cv_livestill==.
		gen cv_includes_chromos =1 if !inlist(cause, "down", "klinefelter", "turner", "edpatau")
			replace cv_includes = 0 if cv_includes ==.
		gen cv_inpatient=0
		gen cv_prenatal =1
		gen cv_aftersurgery=0

		rename cases_top prenatal_top
		rename stillbirth_count prenatal_fd
		gen prenatal_fd_definition = "stillbirth or stillborn"

		gen seq =.
		gen input_type =.
		gen outlier_type_id=0

		replace sample_size = round(sample_size)

		replace mean = cases/sample_size if measure=="prevalence"
		drop if mean==.

		gen cv_under_report =1 // covariate means that registry has low values in comparison to other available data;


//-------------------------------------------------------
// order variables to match the 2016 extraction template
	order bundle_id modelable_entity_name seq underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
				design_effect input_type cv* prenatal_top prenatal_fd prenatal_fd_definition outlier_type_id


//----------------------------------------------------------

// drop extra vars 
	drop year province births cases_surv deaths outc_miss iso3 is_birth_prev is_2016 is_2015 modelable_entity_name_old


	// format "cause" var for folder structure
	replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"
		replace cause = "cong_chromo" if cause=="cong_edpatau"

//-------------------------------------------------------
// split into bundles and save files for upload 
			
	  levelsof bundle_id, local(bundles)
		foreach b in `bundles' { 

			preserve
			keep if bundle_id==`b'
			local c = cause

				if !regexm(modelable_entity_name, "irth prevalence") {
					drop cv_livestill cv_topnot prenatal_top prenatal_fd
					}

				local file  "{FILEPATH}/China_MCHS_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				levelsof modelable_entity_name, local(me) clean
				di in red "Exported `me' data for upload"
			restore
		}




