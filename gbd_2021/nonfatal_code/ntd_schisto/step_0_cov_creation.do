// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Creates the most up to date covariates for schisto models

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


* schisto cumulative treated according to WHO PCT databank

	clear all
	set more off
	set maxvar 32000
	if c(os) == ADDRESS {
		global prefix "FILEPATH"
		set ADDRESS
	}
	else if c(os) == ADDRESS {
		global prefix "FILEPATH"
	}

	run "FILEPATH/get_location_metadata.ado"
	run "FILEPATH/get_demographics.ado"
	run "FILEPATH/get_population.ado"
	run "FILEPATH/get_covariate_estimates.ado"
	run "FILEPATH/get_best_model_versions.ado"
	
	
	local gbd_year 2019
	
	get_location_metadata, location_set_id(9) clear
	keep if is_estimate == 1
	tempfile all_locs
	save `all_locs', replace
	
*this gets the covariate location list
get_location_metadata, location_set_version_id(418) gbd_round_id(ADDRESS) clear
tempfile cov_locs
save `cov_locs', replace

*import pct data bank dataset - the variable used is nat_coverage and this is the percentage of coverage (populated TREATED/ population at risk)
 *for cluster
 import excel "FILEPATH/pct_databank_schisto.xlsx", firstrow clear
 
 tempfile master
 save `master'
	
get_location_metadata, location_set_id(35) clear
keep location_id location_name region_name region_id ihme_loc_id parent_id

merge 1:m location_id using `master', assert(1 3) keep(3) nogenerate

tempfile masternew
save `masternew'
 
 expand 2 if year_id == 2017, gen(new)
    replace year_id = 2018 if new == 1
    drop new
   
   expand 2 if year_id == 2018, gen(new)
    replace year_id = 2019 if new == 1
    drop new
 
 destring nat_coverage, replace

	
	*sets template to 2006 and up
    preserve 
      keep location_id
      duplicates drop location_id, force
      expand 40
      bysort location_id: generate year_id = 1980 + _n-1
      tempfile template
      save `template', replace
    restore
    merge 1:1 location_id year_id using `template', nogen
    
	
    replace nat_coverage = 0 if missing(nat_coverage) & year_id != `gbd_year'
    bysort location_id (year_id): replace nat_coverage = nat_coverage[_n-1] if year_id == `gbd_year'
	*making an assumption that national coverage was the same in 2019 as it was in 2017
    generate cov_cumm = nat_coverage if year_id == 1980
    bysort location_id: replace cov_cumm = cov_cumm[_n-1] + nat_coverage if year_id > 1980
    replace cov_cumm = 0 if cov_cumm == .

    merge m:1 location_id using `all_locs', keepusing(ihme_loc_id) keep(1 3) nogen
    save `template', replace
	
  *merge with iso3s data to fill in subnational geographies with national coverage where necessary (BRA, CHN, KEN, SAU, ZAF, IDN)
	use `cov_locs', clear
	keep if is_estimate == 1
	expand 40
	bysort location_id: generate year_id = 1980 + _n-1
	replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
	joinby ihme_loc_id year_id using "`template'", unmatched(none)
  
	tempfile template_filled
	save `template_filled', replace	

	preserve
	use `cov_locs', clear
	keep if is_estimate == 1
	tempfile detailed_locs
	save `detailed_locs', replace
	restore

	merge m:1 location_id using `detailed_locs', nogen
	expand 40 if cov_cumm == .
	bysort location_id: replace year_id = 1980 + _n - 1 if cov_cumm == .
	replace cov_cumm = 0 if cov_cumm == .
    
    keep NID location_id year_id nat_coverage cov_cumm location_name

	rename NID nid
    replace nat_coverage = 0 if nat_coverage == .
   
   gen covariate_id = 1110
   * gen nid = ADDRESS
   * rename year year_id
    gen age_group_id = 22
    gen sex_id = 3
    gen covariate_name_short = "schisto_treatments"
    rename cov_cumm mean_value
    gen lower_value = mean_value
    gen upper_value = mean_value
    drop location_name
        sort location_id year_id

    keep nid location_id mean_value upper_value lower_value covariate_name_short sex_id age_group_id year_id covariate_id


    *for saving the covariate
	export delimited "FILEPATH/schisto_cummulative_treatments_cov.csv", replace

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// creating files to run the mapping function to get proportion of the population at risk pre and post mda

	// Schisto

	import delimited "FILEPATH/schisto_gr_map.csv", clear
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
	keep if year_id == 1990 | year_id == 2016
	tempfile ex
	save `ex', replace

	import delimited "FILEPATH/prop_pop_at_risk.csv", clear
	drop ihme_loc_id
	rename year year_id
	merge 1:1 year_id location_id using `ex', nogen keep(2 3)
	drop if inlist(ihme_loc_id, "CHN", "KEN", "IND", "IDN", "BRA")
	replace prop_at_risk = 0 if include == 0
	// drop if we don't have estimates
	// LOOK AT THE ADJUSTMENTS
	preserve
	import delimited "FILEPATH/par_adjustments.csv", clear
	merge 1:m ihme_loc_id using `all_locs', nogen keep(3)
	levelsof location_id, local(drops)
	restore
	foreach loc of local drops {
		drop if location_id == `loc'
	}
	drop if regexm(ihme_loc_id, "SAU")

	// pre MDA
	preserve
	keep if year_id == 1990
	keep ihme_loc_id prop_at_risk
	rename prop_at_risk mapvar
	export delimited "FILEPATH\pre_MDA_par.csv", replace
	restore
	keep if year_id == 2016
	keep ihme_loc_id prop_at_risk
	rename prop_at_risk mapvar
	export delimited "FILEPATH\post_MDA_par.csv", replace	

	
