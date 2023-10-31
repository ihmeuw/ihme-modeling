// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Runs GDP - sequelae proportion regression
// Author:		USERNAME
// Last updated:	12/8/2015
// Description:	Running regression between GDP and outcome proportions; this only needs to be changed for years in which we are publishing results 

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// prep stata
	clear all
	set more off
	set mem 2g
	#set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	run "FILEPATH"
	

// *********************************************************************************************************************************************************************
// run code

// load covariate data for GDP per capita in international dollars
	get_covariate_estimates, covariate_id(851) gbd_round_id(7) decomp_step("iterative") clear
	drop covariate_name_short model_version_id covariate_id age_group_id age_group_name sex_id lower_value upper_value location_name
	tempfile gdp_cov
	save `gdp_cov'

// load results from <NAME>
	import delimited "FILEPATH", clear
	rename studyyear year_id
	merge m:1 location_id year_id using `gdp_cov', nogen
	rename mean_value gdp
	rename proportionwith1majorsequela prop_major
	gen cases = n * prop_major
	gen lngdp = ln(gdp)

// run regression --  this is a linear regression when in the form of binomial outputs proportion values in logit space so they're bounded between 0 and 1
	glm cases lngdp, family(binomial n)

// save file only with values from <NAME>
	preserve
	drop if missing(prop_major)
	export delimited "FILEPATH", replace
	restore
	
// create prediction variables -- want to predict these values still in logit space because that's where the linear model is
	predict pred_mean, xb
	predict pred_se, stdp

	keep year_id location_id pred_mean pred_se

	keep if year_id == 1990 | year_id == 1995 | year_id == 2000 | year_id == 2005 | year_id == 2010 | year_id == 2015 | year_id == 2017 | year_id == 2019 | year_id == 2020 | year_id == 2021 | year_id == 2022

// create 1000 draws -- need to invlogit these because they've been calculated in logit space
	forvalues i = 0/999 {
		gen draw_`i' = invlogit(rnormal(pred_mean, pred_se))
	}

// clean
	sort location_id year_id
	drop pred*

// save
	export delimited "FILEPATH", replace
	
	
/*
// comparison between 2019 results and this year

	import delimited "FILEPATH", clear
	drop in 1/7
	rename v1 iso
	keep if regexm(iso, "2010") // looking at 2010 estimates for consistency

	forvalues i = 2/1001 {
		local a = `i' - 2
		rename v`i' draw_`a'
	}

	gen iso3 = substr(iso, 1, 3)
	order iso iso3
	replace iso3 = substr(iso, 1, 7) if regexm(iso, "CHN")
	replace iso3 = substr(iso, 1, 8) if regexm(iso, "MEX")
	replace iso3 = substr(iso, 1, 7) if regexm(iso, "GBR_43")
	replace iso3 = substr(iso, 1, 8) if regexm(iso, "GBR_46")
	drop iso

	preserve
	get_location_metadata, location_set_id(9) clear
	keep ihme_loc_id location_id
	rename ihme_loc_id iso3
	tempfile ids
	save `ids'
	restore

	merge 1:1 iso3 using `ids', nogen keep(1 3)
	order location_id
	drop iso3
	egen mean_2019 = rowmean(draw_*)
	drop draw_*
	tempfile mean2019
	save `mean2019'

	import delimited "FILEPATH", clear
	keep if year_id == 2010
	egen mean_2015 = rowmean(draw_*)
	drop draw_*
	merge 1:1 location_id using `mean2019', nogen keep(3)

	twoway (scatter mean_2020 mean_2019, color(forest_green) mlabel(location_id)) (line mean_2020 mean_2020), ///
	legend(off) xtitle("2019 Mean") ytitle("2020 Mean") title("Comparison between 2019 and 2020 estimates for 2010")
	
