// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Creates the most up to date covariates for schisto models
// Author:		USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

	// set central functions directory
	local central_dir "FILEPATH"
	
	// load central functions
	run "FILEPATH/get_demographics.ado"
	run "FILEPATH/get_population.ado"
	run "FILEPATH/get_location_metadata.ado"
	run "FILEPATH/get_covariate_estimates.ado"
	run "FILEPATH/get_best_model_versions.ado"

	local gbd_year 2016

	// start pulling necessary information for LF proportion of the population at risk covariate
	get_location_metadata, location_set_id(9) clear
	keep if is_estimate == 1
	tempfile all_locs
	save `all_locs', replace

	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// population at risk estimates for schisto

	// adjust for UTLAs
	use `all_locs', clear
	keep if parent_id == 4749
	expand 26
	gen year_id = 1990
	bysort location_id: replace year_id = year_id + (_n - 1)
	keep ihme_loc_id year_id location_name location_id
	gen include = 0
	tempfile utlas
	save `utlas', replace

// pull population at risk numbers from WHO for schisto

	// schisto population at risk numbers 
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if japonicum != "" & mansoni != "" & haematobium != "" & other != ""
	keep location_id
	// exclude these
	gen exclude = 1
	merge 1:1 location_id using `all_locs', nogen
	// drop regions and superregions
	drop if most_detailed == 0
	replace exclude = 0 if exclude == .
	keep exclude location_id ihme_loc_id location_name
	levelsof location_id if exclude == 0, local(pop_locs)
	keep if exclude == 0
	tempfile locs_keep
	save `locs_keep', replace

	// pull in population estimates
	get_population, location_id(`pop_locs') year_id(1990 1995 2000 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016) age_group_id(22) clear
	merge m:1 location_id using `locs_keep', nogen keep(3)
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	rename year_id year

	// sum the population in the subnationals that are endemic
	foreach iso3 in CHN BRA KEN SAU ZAF IDN {
		preserve
		keep if ihme_loc_id == "`iso3'"
		collapse (sum) population, by(year ihme_loc_id)
		tempfile `iso3'_aggregated
		save ``iso3'_aggregated', replace
		restore
	}

	// append to dataset
	foreach iso3 in CHN BRA KEN SAU ZAF IDN {
		append using ``iso3'_aggregated'
	}

	tempfile pops
	save `pops', replace

	// pull in pct population at risk numbers
	import delimited "FILEPATH/pct_coverage_who_2016.csv", clear
	replace pop_at_risk = "" if pop_at_risk == "-"
	destring pop_at_risk, replace
	merge m:1 location_id using `all_locs', nogen keep(3)
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)

	preserve 
	keep location_id ihme_loc_id
	duplicates drop location_id, force
	expand 26
	bysort location_id: gen year = 1990 + _n - 1
	tempfile template
	save `template', replace
	restore	
	merge 1:1 location_id year using `template', nogen
	keep country year pop_at_risk ihme_loc_id

	// merge to population numbers -- merging on ihme_loc_id takes care of subnational issues in China, Brazil, Kenya, South Africa, and 
	merge 1:m year ihme_loc_id using `pops', nogen keep(2 3)
	replace pop_at_risk = . if pop_at_risk == 0 // correcting for the WHO data that puts 0s where none is estimated
	keep location_id year pop_at_risk ihme_loc_id population location_name
	gen prop_at_risk = pop_at_risk / population
	tempfile who_par
	save `who_par', replace
	// add in population at risk numbers and adjustments from Chitsulo et al 2000
	import delimited "FILEPATH/par_adjustments.csv", clear
	gen adjusted = unadjusted * adjustment
	merge 1:m ihme_loc_id using `who_par', nogen

	// expand proportion of the population at risk numbers to applicable subnationals
	foreach iso3 in CHN BRA KEN SAU ZAF IDN {
		preserve
		keep if ihme_loc_id == "`iso3'" & location_id == .
		rename prop_at_risk prop_at_risk_`iso3'
		keep prop_at_risk_`iso3' ihme_loc_id year
		tempfile prop_risk_`iso3'
		save `prop_risk_`iso3'', replace
		restore
		merge m:1 year ihme_loc_id using `prop_risk_`iso3'', nogen keep(1 3)
		replace prop_at_risk = prop_at_risk_`iso3' if ihme_loc_id == "`iso3'"
		drop prop_at_risk_`iso3'
		drop if ihme_loc_id == "`iso3'" & location_id == .
	}

	// run it back (push proportion of the population at risk numbers from 2010 backwards)
	preserve
	keep if year == 2010
	expand 5
	bysort location_id: replace year = 1990 + (_n - 1)*5
	tempfile new_years
	save `new_years', replace
	restore
	// SSD only 2013 forward
	preserve
	keep if year == 2013 & location_name == "South Sudan"
	expand 8
	replace year = 1990 + (_n - 1)*5
	replace year = 2011 if year == 2015
	replace year = 2012 if year == 2020
	replace year = 2013 if year == 2025
	tempfile sudan
	save `sudan', replace
	restore

	sort location_id year
	bysort location_id: replace prop_at_risk = prop_at_risk[_n-1] if year == 2016

	// merge old years propagated backwards
	drop if year < 2011
	append using `new_years'
	drop if year < 2014 & location_name == "South Sudan"
	append using `sudan'
	replace prop_at_risk = adjusted / population if prop_at_risk == .

	replace prop_at_risk = 0.001 if prop_at_risk == 0 | prop_at_risk == .
	keep if inlist(year, 1990, 1995, 2000, 2005, 2010, 2016)
	keep location_name ihme_loc_id year location_id prop_at_risk
	// save file
	export delimited "FILEPATH/prop_pop_at_risk.csv", replace

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// schisto cumulative treated according to WHO PCT databank

	get_location_metadata, version_id(149) clear

// schisto cumulative treated according to WHO PCT databank
	import delimited "FILEPATH/pct_coverage_who_2016.csv", clear
    replace cov_national = "" if cov_national == "-"
    destring cov_national, replace

    preserve
      keep location_id
      duplicates drop location_id, force
      expand 36
      bysort location_id: generate year = 1980 + _n-1
      tempfile template
      save `template', replace
    restore
    merge 1:1 location_id year using `template', nogen
    
    replace cov_national = 0 if missing(cov_national) & year != `gbd_year'
    bysort location_id (year): replace cov_national = cov_national[_n-1] if year == `gbd_year'
    generate cov_cumm = cov_national if year == 2006
    bysort location_id: replace cov_cumm = cov_cumm[_n-1] + cov_national if year > 2006
    replace cov_cumm = 0 if cov_cum == .

    merge m:1 location_id using `all_locs', keepusing(ihme_loc_id) keep(1 3) nogen
    save `template', replace
	
  //merge with iso3s data to fill in subnational geographies with national coverage where necessary (BRA, CHN, KEN, SAU, ZAF, IDN)
	use `cov_locs', clear
	keep if is_estimate == 1
	expand 36
	bysort location_id: generate year = 1980 + _n-1
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	joinby ihme_loc_id year using "`template'", unmatched(none)
  
	tempfile template_filled
	save `template_filled', replace	

	preserve
	use `cov_locs', clear
	keep if is_estimate == 1
	tempfile detailed_locs
	save `detailed_locs', replace
	restore

	merge m:1 location_id using `detailed_locs', nogen
	expand 36 if cov_cumm == .
	bysort location_id: replace year = 1980 + _n - 1 if cov_cumm == .
	replace cov_cumm = 0 if cov_cumm == .
    
    keep location_id year cov_national cov_cumm location_name

    replace cov_national = 0 if cov_national == .
    expand 2 if year == 2015, gen(new)
    replace year = 2016 if new == 1
    drop new
    gen covariate_id = 1110
    gen nid = 144319
    rename year year_id
    gen age_group_id = 22
    gen sex_id = 3
    gen covariate_name_short = "schisto_treatments"
    rename cov_cumm mean_value
    gen lower_value = mean_value
    gen upper_value = mean_value
    drop location_name
        sort location_id year_id

    keep nid location_id mean_value upper_value lower_value covariate_name_short sex_id age_group_id year_id covariate_id

    // for saving the covariate
    export delimited "FILEPATH/schisto_cummulative_treatments_cov.csv", replace
