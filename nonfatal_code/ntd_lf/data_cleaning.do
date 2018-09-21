// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Takes GAHi data and puts it into the custom template form (without NIDs) AND FIRSTLY all the new lit data
// Author:		USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

	// set adopath
	adopath + "FILEPATH"

	// set central functions directory
	local central_dir "FILEPATH"
	run "FILEPATH"

	// pull in extracted data
	import delimited "FILEPATH", clear
	replace measure = "prevalence" if measure == ""
	keep if measure == "prevalence"

	// gets tempfile for totals that is labeled with species_group
	preserve
	duplicates tag nid modelable_entity_id field_citation year_start year_end age_start age_end location_name sex case_name case_diagnostics measure urbanicity, gen(keep)
	drop if keep == 0
	duplicates drop nid modelable_entity_id field_citation year_start year_end age_start age_end location_name sex case_name case_diagnostics measure urbanicity, force
	gen species_group = _n
	keep nid field_citation modelable_entity_id year_start year_end age_start age_end location_name sex case_name case_diagnostics species_group measure urbanicity
	tempfile dupes
	save `dupes', replace
	restore

	merge m:1 nid field_citation modelable_entity_id year_start year_end age_start age_end location_name sex case_name case_diagnostics urbanicity using `dupes', nogen keep (1 3)

// pull out hydrocele and lymphedema and elephantiasis numbers and save those separately

	preserve
	keep if modelable_entity_id == 1493
	tempfile hydro
	save `hydro'
	restore, preserve
	// lymphedema and elephantiasis
	keep if modelable_entity_id == 1492
	tempfile lymph
	save `lymph'
	restore

	// set up LF only dataset
	keep if modelable_entity_id == 1491
	destring group, replace
	levelsof group, local(groups)
	preserve
	drop if group != .
	tempfile clean
	save `clean', replace
	restore
	foreach g of local groups {
		preserve
		keep if group == `g'
		levelsof specificity, local(specs)
		local count: word count `specs'
		if `count' > 1 {
			drop if specificity == "total"
		}
		append using `clean'
		tempfile clean
		save `clean', replace
		restore
	}
	use `clean', clear

	destring sample_size cases mean upper lower standard_error effective_sample_size, replace
	// separate out by combination of values that we have
	// cases, sample size
	preserve
	keep if cases != . & sample_size != .
	collapse (sum) cases sample_size, by(nid field_citation year_start year_end age_start age_end location_name sex case_name case_diagnostics urbanicity)
	tempfile c_ss
	save `c_ss', replace
	restore
	drop if cases != . & sample_size != .

	// mean, sample size
	preserve
	keep if mean != . & sample_size != .
	replace cases = mean*sample_size
	collapse (sum) cases sample_size, by(nid field_citation year_start year_end age_start age_end location_name sex case_name case_diagnostics urbanicity)
	tempfile m_ss
	save `m_ss', replace
	restore
	drop if mean != . & sample_size != .

	// mean, cases
	preserve
	keep if mean != . & cases != .
	replace sample_size = cases/mean
	collapse (sum) cases sample_size, by(nid field_citation year_start year_end age_start age_end location_name sex case_name case_diagnostics urbanicity)
	tempfile m_c
	save `m_c', replace
	restore
	drop if mean!= . & cases != .


	use `c_ss', clear
	append using `m_ss'
	append using `m_c'
	** append using `m_ul'

	export delimited "FILEPATH", replace
