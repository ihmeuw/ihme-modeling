// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Runs GDP - sequelae proportion regression
// Author:		
// Last updated:	12/8/2015
// Description:	Running regression between GDP and outcome proportions

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}
	run "SHARED FUNCTION"
	


// *********************************************************************************************************************************************************************
// run code

// load covariate data for GDP per capita in international dollars
	get_covariate_estimates, covariate_id(851) clear
	drop covariate_name_short model_version_id covariate_id age_group_id age_group_name sex_id lower_value upper_value location_name
	tempfile gdp_cov
	save `gdp_cov'

// load results from Edmonds
	import delimited "MAJOR_SEQUELAE.CSV", clear
	rename studyyear year_id
	merge m:1 location_id year_id using `gdp_cov', nogen
	rename mean_value gdp
	rename proportionwith1majorsequela prop_major
	gen cases = n * prop_major
	gen lngdp = ln(gdp)

// run regression --  this is a linear regression when in the form of binomial outputs proportion values in logit space so they're bounded between 0 and 1
	glm cases lngdp, family(binomial n)

// save file only with values from Edmonds
	preserve
	drop if missing(prop_major)
	export delimited "[FILENAME]/prop_major_gdp.csv", replace
	restore
	
// create prediction variables -- want to predict these values still in logit space because that's where the linear model is
	predict pred_mean, xb
	predict pred_se, stdp

	keep year_id location_id pred_mean pred_se
	keep if year_id == 1990 | year_id == 1995 | year_id == 2000 | year_id == 2005 | year_id == 2010 | year_id == 2016

// create 1000 draws -- need to invlogit these because they've been calculated in logit space
	forvalues i = 0/999 {
		gen draw_`i' = invlogit(rnormal(pred_mean, pred_se))
	}

// clean
	sort location_id year_id
	drop pred*

// save
	export delimited "[FILENAME]/major_prop_draws.csv", replace
	
