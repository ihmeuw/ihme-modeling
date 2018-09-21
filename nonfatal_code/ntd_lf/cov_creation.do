// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Creates the most up to date covariates for LF and schisto models
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

	// pull in LF geographic restrictions
	import delimited "FILEPATH", clear
	tempfile lf_restrictions
	save `lf_restrictions', replace

	import excel "FILEPATH", firstrow clear
	// rename Country location_name

	// clean country names
	replace location_name = "Cape Verde" if location_name == "Cabo Verde"
	replace location_name = "Brunei" if location_name == "Brunei Darussalam"
	replace location_name = "Cote d'Ivoire" if location_name == "CâŒ te d'Ivoire"
	replace location_name = "The Gambia" if location_name == "Gambia"
	replace location_name = "Laos" if location_name == "Lao People's Democratic Republic"
	replace location_name = "Federated States of Micronesia" if location_name == "Micronesia (Federated States of)"
	replace location_name = "Tanzania" if location_name == "United Republic of Tanzania"
	replace location_name = "Vietnam" if location_name == "Viet Nam"

	drop if location_name == "Cook Islands" | location_name == "French Polynesia" | location_name == "New Caledonia" | location_name == "Niue" | ///
	location_name == "Palau" | location_name == "Tuvalu" | location_name == "United Republic of Tanzania (Zanzibar)" | location_name == "Wallis and Futuna"

	// clean variables
	drop Nationalcoverage NumberofIUscovered Geographicalcoverage TotalpopulationofIUs Programmedrugcoverage Mappingstatus
	rename Reportednumberofpeopletreate num_treated
	rename PopulationrequiringPCforLF pop_at_risk
	rename Year year_id
	gen surveillance = 0
	replace surveillance = 1 if TypeofMDA == "Surveillance"

	replace pop_at_risk = "0" if pop_at_risk == "-"
	destring pop_at_risk, replace
	replace num_treated = "0" if num_treated == "-"
	destring num_treated, replace

	// fixing different types of MDA for India and BanglUSER
	collapse (sum) num_treated, by(location_name year_id pop_at_risk surveillance)

	// create proportion of the at risk population who receives MDA
	gen prop_mda = num_treated/pop_at_risk

	// set year_ids local
	numlist "1990/`gbd_year'"
	local year_ids `r(numlist)'

	levelsof location_name, local(mda_locs)

	// getting the correct format
	preserve
	get_covariate_estimates, covariate_id(255) clear
	tempfile cov_old
	save `cov_old'
	restore
	merge 1:m year_id location_name using `cov_old', nogen

	// more cleaning
	drop if year_id < 1990
	replace prop_mda = 0 if prop_mda == .

	// generate cumulative variable
	bysort location_id (year_id): gen cum_mda_treated = sum(prop_mda)
	order location_name year_id prop_mda cum_mda_treated

	replace mean_value = cum_mda_treated
	replace upper_value = cum_mda_treated
	replace lower_value = cum_mda_treated

	keep location_name year_id mean_value upper_value lower_value location_id age_group_id age_group_name sex_id covariate_id covariate_name_short

	export delimited "FILEPATH", replace


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// population at risk estimates for LF

	// adjust for UTLAs
	use `all_locs', clear
	keep if parent_id == 4749 // keeps all UTLAs
	expand 26
	gen year_id = 1990
	bysort location_id: replace year_id = year_id + (_n - 1)
	keep ihme_loc_id year_id location_name location_id
	gen include = 0
	tempfile utlas
	save `utlas', replace

import delimited "FILEPATH", clear
// rename columns
local year = 1990
forvalues i = 4/30 {
	rename v`i' year_`year'
	local year = `year' + 1
}
// reshape
reshape long year_, i(ihme_loc_id) j(year_id)
rename year_ include
append using `utlas'
tempfile inclusions
save `inclusions', replace
// save it
export delimited "FILEPATH", replace
keep if include == 1
levelsof location_id, local(pop_locs)

// pull in WHO pop at risk numbers 

	import excel "FILEPATH", firstrow clear

	replace location_name = "Cape Verde" if location_name == "Cabo Verde"
	replace location_name = "Brunei" if location_name == "Brunei Darussalam"
	replace location_name = "Cote d'Ivoire" if location_name == "CâŒ te d'Ivoire"
	replace location_name = "The Gambia" if location_name == "Gambia"
	replace location_name = "Laos" if location_name == "Lao People's Democratic Republic"
	replace location_name = "Federated States of Micronesia" if location_name == "Micronesia (Federated States of)"
	replace location_name = "Tanzania" if location_name == "United Republic of Tanzania"
	replace location_name = "Vietnam" if location_name == "Viet Nam"

	drop if location_name == "Cook Islands" | location_name == "French Polynesia" | location_name == "New Caledonia" | location_name == "Niue" | ///
	location_name == "Palau" | location_name == "Tuvalu" | location_name == "United Republic of Tanzania (Zanzibar)" | location_name == "Wallis and Futuna"

	// clean variables
	drop Nationalcoverage NumberofIUscovered Geographicalcoverage TotalpopulationofIUs Programmedrugcoverage Mappingstatus
	rename Reportednumberofpeopletreate num_treated
	rename PopulationrequiringPCforLF pop_at_risk
	rename Year year_id
	gen surveillance = 0
	replace surveillance = 1 if TypeofMDA == "Surveillance"

	replace pop_at_risk = "0" if pop_at_risk == "-"
	destring pop_at_risk, replace
	replace num_treated = "0" if num_treated == "-"
	destring num_treated, replace
		
	// add ihme_loc_id
	merge m:m location_name using `all_locs', nogen keep(3)
	duplicates drop year_id location_name pop_at_risk ihme_loc_id, force
	levelsof ihme_loc_id, local(isos)

	// save this
	tempfile pop_at_risk
	save `pop_at_risk', replace
	// pull in population numbers
	get_population, location_id(`pop_locs') year_id(1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016) age_group_id(22) clear
	merge m:1 location_id using `all_locs', nogen keep(3)
	keep location_id ihme_loc_id population year_id
	// only use the subnationals that are endemic for certain years
	merge 1:1 location_id year_id using `inclusions', nogen keep(1 3)
	keep if include == 1
	// collapse by country-year so we take into account change in subnational endemicity
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	collapse (sum) population, by(year_id ihme_loc_id)
	// add on population at risk numbers
	merge 1:1 year_id ihme_loc_id using `pop_at_risk', nogen keep(3)
	gen prop_at_risk = pop_at_risk / population
	replace prop_at_risk = 1 if prop_at_risk > 1
	// propagate these proportions backwards
		// create template
		preserve
		drop if year != .
		tempfile template
		save `template', replace
		restore

		// keep lowest presenting location and propagate it backwards to 1990
	levelsof location_id, local(locs)
	foreach loc of local locs {
		preserve
		keep if location_id == `loc'
		summarize year_id
		// keep the lowest presenting year
		keep if year_id == `r(min)'
		local ex = 2016 - `r(min)'
		if `ex' > 15 {
			expand 3, gen(new)
			replace year_id = 1990 + (_n-2)*5 if new == 1
		}
		else if `ex' > 10 {
			expand 4, gen(new)
			replace year_id = 1990 + (_n-2)*5 if new == 1
		}
		else if `ex' > 5 {
			expand 5, gen(new)
			replace year_id = 1990 + (_n-2)*5 if new == 1
		}
		else if `ex' < 6 {
			expand 6, gen(new)
			replace year_id = 1990 + (_n-2)*5 if new == 1
		}
		drop if new == 0
		drop new
		append using `template'
		save `template', replace
		restore
	}

	append using `template'
	sort location_id year_id
	expand 2 if year_id == 2015, gen(new)
	replace year_id = 2016 if new == 1
	drop new
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
	drop if TypeofMDA == "Eliminated as a public health problem"
	replace prop_at_risk = .01 if TypeofMDA == "Not required MDA"
	// merge with geographic restrictions
	merge 1:1 location_id year_id using `inclusions', nogen keep(2 3)
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
	keep if include == 1
	preserve
	keep if inlist(ihme_loc_id, "BRA", "IND", "IDN", "CHN", "KEN")
	rename prop_at_risk parent
	tempfile parents
	save `parents', replace
	restore
	drop if inlist(ihme_loc_id, "BRA", "IND", "IDN", "CHN", "KEN")
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	merge m:1 ihme_loc_id year_id using `parents', nogen
	replace prop_at_risk = parent if parent != .
	replace prop_at_risk = 0.1 if prop_at_risk == .
	// cleaning
	keep ihme_loc_id location_id year_id prop_at_risk
	drop if prop_at_risk == 0
	// save it
	export delimited "FILEPATH", replace


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// pull population at risk numbers from WHO for schisto

	// schisto population at risk numbers 
	import delimited "FILEPATH", clear
	keep if japonicum != "" & mansoni != "" & haematobium != "" & other != ""
	keep location_id
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
	import delimited "FILEPATH", clear
	replace pop_at_risk = "" if pop_at_risk == "-"
	destring pop_at_risk, replace
	merge m:1 location_id using `all_locs', nogen keep(3)
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)

	preserve // sets template to 1990 and up
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
	replace pop_at_risk = . if pop_at_risk == 0
	keep location_id year pop_at_risk ihme_loc_id population location_name
	gen prop_at_risk = pop_at_risk / population
	tempfile who_par
	save `who_par', replace
	// add in population at risk numbers and adjustments from Chitsulo et al 2000
	import delimited "FILEPATH", clear
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
	preserve
	keep if year == 2010
	expand 5
	bysort location_id: replace year = 1990 + (_n - 1)*5
	tempfile new_years
	save `new_years', replace
	restore
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

	drop if year < 2011
	append using `new_years'
	drop if year < 2014 & location_name == "South Sudan"
	append using `sudan'
	replace prop_at_risk = adjusted / population if prop_at_risk == .

	replace prop_at_risk = 0.001 if prop_at_risk == 0 | prop_at_risk == .
	keep if inlist(year, 1990, 1995, 2000, 2005, 2010, 2016)
	keep location_name ihme_loc_id year location_id prop_at_risk
	// save file
	export delimited "FILEPATH", replace




	get_location_metadata, version_id(149) clear


	import delimited "FILEPATH", clear
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
    export delimited "FILEPATH", replace

