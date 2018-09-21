	
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
	run "SHARED FUNCTIONS"

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
** get covariate data
** **************************************************************************

	// get covariate data for ANC1+ covariate
	get_covariate_estimates, covariate_id(7) clear
	gen se = (upper_value - lower_value)/(2*1.96)
	keep location_id year_id mean_value se
	rename (mean_value se) (mean_1 se_1)
	tempfile anc1
	save `anc1', replace
	
	// get proportion with no visit
	gen mean_0 = 1-mean_1
	keep location_id year_id mean_0 se_1
	rename se_1 se_0

	// get beta distribution
	keep location_id year_id mean_0 se_0
	forvalues i = 0/999 {
		gen draw_`i' = rnormal(mean_0,se_0)
		replace draw_`i' = mean_0 if mi(draw_`i')
		replace draw_`i' = 0 if draw_`i' < 0
		replace draw_`i' = 1 if draw_`i' > 1
	}
	gen visits = "_0"
	keep location_id year_id draw* visits
	tempfile anc0
	save `anc0', replace
	
	// get proportion with 4+ visits
	get_covariate_estimates, covariate_id(8) clear
	gen se = (upper_value - lower_value)/(2*1.96)
	keep location_id year_id mean_value se
	rename (mean_value se) (mean_4 se_4)

	// get proportion with 1-3 visits
	merge 1:1 location_id year_id using `anc1', nogen
	replace mean_4 = mean_1 if mean_4 > mean_1 
	gen mean_13 = mean_1 - mean_4
	gen se_13 = mean_13 * sqrt((se_1/mean_1)^2 + (se_4/mean_4)^2)

	// get draws from normal distribution
	keep location_id year_id mean_13 se_13
	forvalues i = 0/999 {
		gen draw_`i' = rnormal(mean_13,se_13)
		replace draw_`i' = mean_13 if mi(draw_`i')
		replace draw_`i' = 0 if draw_`i' < 0
		replace draw_`i' = 1 if draw_`i' > 1
	}
	gen visits = "_13"
	keep location_id year_id draw* visits
	
	// append
	append using `anc0'
	tempfile unsqueezed
	save `unsqueezed', replace
	
	// collapse to find values
	collapse (sum) draw*, by(location_id year_id) fast
	rename draw* draw*_sum
	tempfile summed
	save `summed', replace
	
	// any draws greater than 1 get scaled to the sum of the means
	merge 1:1 location_id year_id using `summed', nogen
	merge 1:m location_id year_id using `unsqueezed', nogen

	// scale to 1 if greater
	forvalues i=0/999 {
		replace draw_`i' = draw_`i' / draw_`i'_sum if draw_`i'_sum > 1
	}
	drop draw*_sum
		
	// save to disk
	export delimited using "`out_dir'/models/anc_visit_props.csv", replace
	save "`out_dir'/models/anc_visit_props.dta", replace
	

