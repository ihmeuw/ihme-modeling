
** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off

** **********************
** Filepaths 
** **********************

	local version_id `1'
	global main_dir FILEPATH
	global u5_raw_file = FILEPATH
	global u5_est_file = FILEPATH
	global save_file = FILEPATH


** **********************
** Calculate child comp 
** **********************	
// Grab locations for merging on later

	import delim using FILEPATH, clear
	keep if location_name == "Old Andhra Pradesh"
	tempfile ap_old 
	save `ap_old', replace

	import delim using FILEPATH, clear
	keep if level_all == 1 // Drop England and India states -- this doesn't do anything ...
	append using `ap_old'
	keep ihme_loc_id
	tempfile codes
	save `codes'

** Format estimates 
	use "$u5_est_file", clear
	rename med u5_est
	rename lower u5_est_lower
	rename upper u5_est_upper

	tempfile est
	save `est', replace 

** Format raw data 
	use "$u5_raw_file", clear
	replace source = upper(source)
	replace source = "DSP" if regexm(source, "DSP") == 1 
	replace source = "SRS" if regexm(source, "SRS") == 1 
	replace source = "SSPC-DC" if source == "1% INTRA-CENSUS SURVEY" 
	replace source = "SSPC-DC" if source == "1 PER 1000 SURVEY ON POP CHANGE" 
    replace source = "RapidMortality" if ihme_loc_id == "ZAF" & year == 2009.5 & q5 == .056
    replace source = "RapidMortality" if ihme_loc_id == "ZAF" & year == 2010.5 & q5 == .052
    replace source = "RapidMortality" if ihme_loc_id == "ZAF" & year == 2011.5 & q5 == .04
    replace source = "RapidMortality" if ihme_loc_id == "ZAF" & year == 2012.5 & q5 == .041
	replace source = "VR" if regexm(source, "VR") == 1
	rename q5 u5_obs
	replace year = floor(year) + .5
        
    // Exclude marked shocks and outliers, along with discretionary 5q0 points
        **drop if regexm(indirect, "indirect") == 1 //Dropped when pulled from DB	
        drop if outlier == 1
        * drop if ihme_loc_id == "IRN" & year > 2003
        * drop if ihme_loc_id == "SAU" & inlist(year,2009.5,2012.5) & source == "VR" // Two very high child completeness points
        drop if ihme_loc_id == "IRQ" & source == "SRS"
        

	keep if inlist(source, "VR", "SRS", "DSP", "CENSUS", "SURVEY", "UNKNOWN", "SSPC-DC", "SUSENAS") | inlist(source, "HOUSEHOLD", "SUPAS", "MCCD", "CR")
    keep ihme_loc_id year_id year u5_obs source nid underlying_nid

** Mark duplicates and take average
	duplicates tag ihme_loc_id year year_id source, gen(dup)
	replace dup = 0 if mi(dup)
	preserve
	keep if dup != 0
	collapse (mean) u5_obs, by(ihme_loc_id year year_id source)
	tempfile raw_data_avg
	save `raw_data_avg', replace
	restore
	drop if dup != 0
	append using `raw_data_avg'
	drop dup

** Merge estimates and raw data 
	merge m:1 ihme_loc_id year_id using `est'
	drop if _m == 2
	drop _m 

	
** Calculate comp 
** You use the est lower bounds to calculate the comp upper bounds 
	g comp = u5_obs/u5_est
	g comp_upper = u5_obs/u5_est_lower
	g comp_lower = u5_obs/u5_est_upper

	preserve
	keep if comp < .1
	export delim using FILEPATH, replace
	restore

	drop if comp < .1
	
** Merge on ihme_loc_id
	merge m:1 ihme_loc_id using `codes', keep(3) nogen

** save
	keep ihme_loc_id year comp* source nid underlying_nid
	gen comp_type = "u5"
	rename source source_type
	compress
	saveold "$save_file", replace

** output list of complete/not for stillbirths model
	replace year = floor(year)
	gen complete  = 1 if comp >= 0.95
	replace complete = 0 if mi(complete)
	count if mi(complete)

** keep highest comp value
	gsort ihme_loc_id year source_type -comp
	duplicates tag ihme_loc_id year source_type, gen(dd)
	bysort ihme_loc_id year source_type: gen ob = _n
	drop if dd == 1 & ob != 1
	drop dd ob

	keep ihme_loc_id year complete source_type
	export delimited using FILEPATH, replace

