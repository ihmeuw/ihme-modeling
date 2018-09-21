// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Cleaning extraction data to be in GBD format
// Author:		USERNAME

/* things that this code does:

- pull in extraction sheet
- adjust for diagnostics
- drop proportion data, not using that at this point in the model
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
*/

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILENAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILENAME"
	}

	// set adopath
	adopath + "FILENAME"

	// set in_path
	local reg_dir "FILENAME.xlsx"
	// set in_path
	local in_dir "FILENAME.xlsm"
	// out directory
	local out_dir "FILENAME.xlsx"

	**************************************************************************************************************************
	** ADD ADJUSTMENTS TO SPECIFICITY AND SENSITIVITY HERE
	**************************************************************************************************************************

	// pull in diagnostic data
	** import file
	import excel "`reg_dir'", firstrow clear
	** set up necessary variables
	gen pr_kk = cases_kk/ sample_size_kk
	gen pr_cca = cases_cca / sample_size_cca
	gen ln_kkoffset = ln(pr_kk) + 5
	generate num_stoolZ = num_stool - 1
	rename num_stool num_stools
	** save this file
	tempfile regression
	save `regression', replace

	**************************************************************************************************************************
	** ADD ADJUSTMENTS TO SPECIFICITY AND SENSITIVITY HERE
	**************************************************************************************************************************

	** pull in file with data and convert to mean if need be
	** pull in extraction sheet
	import excel "`in_dir'", firstrow sheet(extraction) clear
	drop in 1

	drop if measure == "proportion"
	drop if is_outlier == 1
	drop if group_review == 0
	destring sample_size cases mean upper lower standard_error num_samples, replace
	** set up row_num for later merging
	gen row_count = _n
	replace case_diagnostics = "fecal smear" if case_diagnostics == "" & (case_name == "S japonicum" | case_name == "S mansoni")
	replace case_diagnostics = "urine filtration technique" if case_diagnostics == "" & case_name == "S haematobium"
	drop if (cases == . & lower == . & upper == . & standard_error == . & sample_size == .)
	replace standard_error = (upper - lower)/(2*1.96) if mean != . & upper != . & lower != . & cases == .
	replace mean = cases/sample_size if mean == .
	replace sample_size = cases/mean if sample_size == . & cases != . & mean != .
	replace standard_error = sqrt(mean*(1-mean)/sample_size) if standard_error == .
	replace cases = (mean * (mean - mean^2 - standard_error^2))/standard_error^2 if sample_size == .
	replace sample_size = cases/mean if sample_size == .
	tempfile all_data
	save `all_data', replace

	keep if regexm(case_diagnostics, "Kato-Katz") | case_diagnostics == "fecal smear"
	replace num_samples = 2 if num_samples == .
	append using `regression'
	replace ln_kkoffset = ln(mean) + 5 if ln_kkoffset == .
	replace ln_kkoffset = 5 if mean == 0 
	replace num_stoolZ = num_samples - 1 if num_stoolZ == .

	** run regression
	regress pr_cca c.num_stoolZ#c.ln_kkoffset ln_kkoffset, nocons
	** predict out adjusted mean
	predict adjusted_mean
	predict adjusted_se, stdp
	replace adjusted_se = sqrt(standard_error^2 + adjusted_se^2)

	replace adjusted_mean = 0 if mean == 0
	replace adjusted_mean = mean if adjusted_mean < 0
	replace mean = adjusted_mean
	replace standard_error = adjusted_se
	drop adjusted* num_stoolZ pr_kk pr_cca ln_kkoffset
	drop if standard_error == .

	replace cases = (mean * (mean - mean^2 - standard_error^2))/standard_error^2 
	replace cases = 0 if cases < 0
	replace sample_size = cases/mean if mean != 0
	** rename diagnostic
	replace case_diagnostic = "CCA adjusted"
	tempfile adjusted
	save `adjusted'

	** merge back onto dataset
	use `all_data', clear
	drop if regexm(case_diagnostics, "Kato-Katz") | case_diagnostics == "fecal smear"
	append using `adjusted'
	sort row_count

	**************************************************************************************************************************
	** AGGREGATE TO GBD LOCATION LEVEL
	**************************************************************************************************************************

	preserve
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure num_samples num_slides, gen(keep)
	drop if keep == 0
	duplicates drop nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure num_samples num_slides, force
	gen species_group = _n
	keep nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics species_group measure num_samples num_slides
	tempfile dupes
	save `dupes', replace
	restore

	merge m:1 nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics num_samples num_slides using `dupes', nogen keep (1 3)

	** clean data, collapse by source, year, age, sex, gbd location, case diagnostic, measure, and species to get totals
	replace standard_error = (upper - lower)/(2*1.96) if mean != . & upper != . & lower != . & cases == .
	replace cases = (mean * (mean - mean^2 - standard_error^2))/standard_error^2 if mean != . & upper != . & lower != . & cases == .
	replace sample_size = cases/mean if mean != . & upper != . & lower != . & sample_size == .
	replace cases = 0 if mean == 0
	
	** collapse to get totals by source, year, age, sex, gbd_location, case diagnostic
	collapse (sum) cases sample_size, by(nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure)
	gen diag_class = 1
	tempfile collapsed
	save `collapsed', replace

	** run through each species type to get species-specific data
	*****************
	** S japonicum **
	*****************
	preserve
	keep if case_name == "S japonicum"
	rename cases cases_j
	rename sample_size sample_size_j
	rename case_diagnostics case_diagnostics_j
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	replace diag_class = 2 if dup == 1 & (case_diagnostics_j == "IHA")
	drop dup
	tempfile j_cases
	save `j_cases', replace
	*****************
	*** S mansoni ***
	*****************
	restore, preserve
	keep if case_name == "S mansoni" 
	rename cases cases_m
	rename sample_size sample_size_m
	rename case_diagnostics case_diagnostics_m
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	replace diag_class = 2 if dup == 1 & (case_diagnostics_m == "sedimentation")
	drop dup
	tempfile m_cases
	save `m_cases', replace
	********************
	** S intercalatum **
	********************
	restore, preserve
	keep if case_name == "S intercalatum"
	rename cases cases_i
	rename sample_size sample_size_i
	rename case_diagnostics case_diagnostics_i
	tempfile i_cases
	save `i_cases', replace
	*******************
	** S haematobium **
	*******************
	restore, preserve
	keep if case_name == "S haematobium"
	rename cases cases_h
	rename sample_size sample_size_h
	rename case_diagnostics case_diagnostics_h
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure, gen(dup)
	replace diag_class = 2 if dup == 1 & (case_diagnostics_h == "urine filtration technique")
	drop dup
	tempfile h_cases
	save `h_cases', replace
	*****************
	*** S mekongi ***
	*****************
	** no duplicates here
	restore, preserve
	keep if case_name == "S mekongi"
	rename cases cases_k
	rename sample_size sample_size_k
	rename case_diagnostics case_diagnostics_k
	tempfile k_cases
	save `k_cases', replace
	restore

	** merge all the species together
	use `h_cases', clear
	** add mansoni
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `m_cases', nogen
	** add japonicum
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `j_cases', nogen
	** add intercalatum
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `i_cases', nogen
	** add mekongi
	merge m:m nid underlying_nid diag_class field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex measure using `k_cases', nogen

	** save file
	tempfile merged_species
	save `merged_species', replace

	export delimited "FILENAME.csv", replace

	**************************************************************************************************************************
	******create draws where necessary, and output prevalence with upper and lower confidence intervals for co-infection******
	**************************************************************************************************************************
		
	** keep data where two species are measured (only occurs for haematobium and mansoni or intercalatum)
	preserve
	keep if cases_h != . & (cases_m != . | cases_i != .)

	foreach var in m i h {
		replace cases_`var' = cases_`var' + 1e-4 if cases_`var' == 0
		replace cases_`var' = cases_`var' - 1e-4 if cases_`var' == sample_size_`var'
		forvalues i = 0/999 {
			qui gen gammaA = rgamma((cases_`var'), 1)
			qui gen gammaB = rgamma((sample_size_`var' - cases_`var'), 1)
			qui gen mean_`var'_`i' = (gammaA / (gammaA + gammaB))
			replace mean_`var'_`i' = 0 if mean_`var'_`i' == .
			drop gamma*
		}
	}

	forvalues i = 0/999 {
		gen mean_hm_`i' = mean_h_`i' + mean_m_`i' - (mean_h_`i')*(mean_m_`i')
		gen mean_hi_`i' = mean_h_`i' + mean_i_`i' - (mean_h_`i')*(mean_i_`i')
		drop mean_h_`i' mean_i_`i' mean_m_`i'
	}


	egen mean_hi = rowmean(mean_hi_*)
	egen lower_hi = rowpctile(mean_hi_*), p(2.5)
	egen upper_hi = rowpctile(mean_hi_*), p(97.5)
	egen mean = rowmean(mean_hm_*)
	egen lower = rowpctile(mean_hm_*), p(2.5)
	egen upper = rowpctile(mean_hm_*), p(97.5)
	replace mean = mean_hi if mean_hi > mean
	replace lower = lower_hi if mean_hi > mean
	replace upper = upper_hi if mean_hi > mean
	drop mean_* lower_* upper_*

	tempfile all_schisto
	save `all_schisto', replace
	restore

	merge 1:1 nid underlying_nid diag_class location_name ihme_loc_id location_id field_citation_value sex year_start year_end age_start age_end measure case_diagnostics* using `all_schisto', nogen keep(1 3)
	gen sample_size = .
	gen cases = .

	** replace in single-species cases and sample size
	foreach type in h i j k m {
		replace sample_size = sample_size_`type' if sample_size_`type' != . & mean == .
		replace cases = cases_`type' if cases_`type' != . & mean == .
	}

	sort location_name nid year_start year_end age_start

	egen case_diagnostics = concat(case_diagnostics_i case_diagnostics_h case_diagnostics_m case_diagnostics_j), punct(" ")
	replace case_diagnostics = subinstr(case_diagnostics, "  ", " ", .)
	gen file_path = ""
	gen page_num = .
	gen table_num = .
	gen source_type = "Survey - cross-sectional"
	gen smaller_site_unit = 1
	gen site_memo = ""
	gen sex_issue = 0
	gen year_issue = 0
	gen age_issue = 0
	gen age_demographer = 1
	gen standard_error = .
	gen unit_type = "Person"
	gen unit_value_as_published = 1
	gen measure_issue = 0
	gen measure_adjustment = 0
	gen uncertainty_type_value = 95 if mean != .
	gen representative_name = "Unknown"
	gen urbanicity_type = "Unknown"
	gen recall_type = "Point"
	gen recall_type_value = ""
	gen sampling_type = ""
	gen response_rate = .
	gen case_definition = ""
	gen specificity = ""
	gen group_review = .
	gen bundle_id = 725
	gen extractor = "USERNAME"
	gen effective_sample_size = sample_size
	gen input_type = "extracted"
	gen uncertainty_type = "Sample size"
	replace uncertainty_type = "Confidence interval" if uncertainty_type_value == 95
	gen design_effect = ""
	gen underlying_field_citation_value = ""
	gen seq = .
	gen outlier_type_id = 0
	gen group = .




	order bundle_id seq nid field_citation_value file_path page_num table_num source_type location_id location_name ihme_loc_id smaller_site_unit site_memo sex sex_issue year_start year_end year_issue age_start age_end age_issue ///
	age_demographer measure mean lower upper standard_error cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment uncertainty_type_value representative_name ///
	urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics specificity group_review

	** clean up
	drop case_diagnostics_* cases_* sample_size_*
	** removes extracted french guiana data
	drop if location_name == ""
	** this will have to be addressed at some point
	replace nid = "285210" if nid == "" | nid == "151571" | nid == "restricted"

	** study level covariates
	gen cv_diag_blood = 0
	replace cv_diag_blood = 1 if regexm(case_diagnostics, "haematuria")
	gen cv_diag_elisa = 0
	replace cv_diag_elisa = 1 if regexm(case_diagnostics, "ELISA")
	gen cv_diag_antigen = 0
	replace cv_diag_antigen = 1 if regexm(case_diagnostics, "IHA")

	export excel "`out_dir'", replace sheet("extraction") firstrow(var)


