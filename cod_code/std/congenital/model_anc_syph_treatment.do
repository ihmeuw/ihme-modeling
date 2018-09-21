	
** **************************************************************************
** PREP STATA
** **************************************************************************
	
	// prep stata
	clear all
	set more off
	set maxvar 32000

	// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}
	sysdir set PLUS "`h'/ado/plus"

	// load functions
	run "GET_COVARIATE SHARED FUNCTION"
	run "GET_DEMOGRAPHICS SHARED FUNCTION"
	run "GET_LOCATION_METADATA SHARED FUNCTION"
	run "FASTCOLLAPSE SHARED FUNCTION"
	
	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, root_dir(string) date(string) out_dir(string)
		c_local out_dir "`out_dir'"
		c_local date "`date'"
		c_local root_dir "`root_dir'"
	end
	parse_syntax, `0'
	
** **************************************************************************
** prep for regression
** **************************************************************************

	// make estimation template
	clear
	get_demographics, gbd_team(cod) clear
	local year_ids = "`r(year_ids)'"
	gen year_id = .
	foreach year of local year_ids {
		set obs `=_N +1'
		replace year_id = real("`year'") in `=_N'
	}
	tempfile template
	save `template', replace
	get_location_metadata, location_set_id(35) clear
	keep if location_type != "nonsovereign" & level >= 3
	keep location_id ihme_loc_id region_id
	cross using `template'
	save `template', replace

	// get log ldi covariate
	get_covariate_estimates, covariate_id(57) clear
	gen ldi = mean_value
	gen ln_ldi = log(mean_value)
	keep location_id year_id ln_ldi ldi
	tempfile ldi
	save `ldi', replace
	
	// import dataset
	import delimited using "`out_dir'/inputs/anc_syphilis_treament_formatted.csv", clear
	drop if mean == .
	replace mean=0.001 if mean==0
	replace mean=0.999 if mean==1
	gen lt_mean = logit(mean)
	keep location_id year_id lt_mean mean
	
	// merge onto template and add covariates
	joinby year_id location_id using `template', unmatched(both)
	merge m:1 location_id year_id using `ldi', nogen keep(3)

** **************************************************************************
** run for regression
** **************************************************************************

	// run model
	xtmixed lt_mean ln_ldi || region_id:
			
	// predict linear effects
	predict xb, xb
	predict xb_sd, stdp
	
	// predict random effects
	predict re, reffects 
	predict re_sd, reses

	// create 1000 draw distribution
	forvalues i = 0/999 {
		gen xb_`i' = rnormal(xb, xb_sd)
		gen re_`i' = rnormal(re, re_sd)

		gen anc_syph_treat_`i' = invlogit(xb_`i' + re_`i') if !mi(re_`i')
		replace anc_syph_treat_`i' = invlogit(xb_`i') if mi(re_`i')
		
		drop xb_`i' re_`i'
	}
	drop xb xb_sd re re_sd lt_mean
	
	// collapse to get rid of input data overlap
	fastcollapse anc* , type(mean) by(year_id location_id ihme_loc_id)

	// generate dummy variable for merge (this proportion will only apply to those who have 1-3 antenatal visits
	gen visits = "_13"

	// save resutls
	export delimited using "`out_dir'/models/anc_syph_treatment.csv", replace
	save "`out_dir'/models/anc_syph_treatment.dta", replace

