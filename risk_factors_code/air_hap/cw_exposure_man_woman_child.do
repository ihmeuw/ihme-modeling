//Date: March 4 2017
//Purpose: Prep crosswalk values from the mixed effect model to generate Relative Risks

//Housekeeping
clear all
set more off
set maxvar 20000

//Set directories
	if c(os) == "Windows" {
		global j "FILEPATH"
		global i "FILEPATH"
		set mem 1g
	}
	if c(os) == "Unix" {
		global j "FILEPATH"
		global i "FILEPATH"
		set mem 2g
		set odbcmgr unixodbc
	}

//Set relevant locals
	local exp_factor_dir	"filepath"
	local pm_output_2016	"filepath"
	local out_dir			"filepath"
	set seed 				74658
	local filedate			"061617"
	adopath + "filepath"

	
//Prep exposure factor extractions 
	insheet using "`exp_factor_dir'/personal_exposure_factor_PM2.5_03022015.csv", comma clear

// Calculate means for each group and draw from normal distribution
	summ personal_pm
	local grand_mean `r(mean)'

	summ personal_pm if group == "Female" & age_start >15
	local f_mean `r(mean)'

	summ personal_pm if group == "Male" & age_start >15
	local m_mean `r(mean)'

	summ personal_pm if age_start <= 15
	local c_mean `r(mean)'

/*	gen weight = .
	tempfile temp
	save `temp', replace
	
	foreach group in "F" "M" "C" {
		preserve
		keep if group_new == "`group'"
		local n = _N
		restore
		replace weight = 1/`n' if group_new == "`group'"
		save `temp', replace
	}
	egen wt_mean = wtmean(personal_pm), weight(weight)
	local grand_mean = wt_mean
*/
	forvalues n = 0/999 {
		gen female_exp_`n' = rnormal(`f_mean', 20)
		gen male_exp_`n' = rnormal(`m_mean', 20)
		gen child_exp_`n' = rnormal(`c_mean', 20)
		gen mean_exp_`n' = rnormal(`grand_mean', 20)
	}

//Store draws of the ratio for each group
	forvalues n = 0/999 {
		local f_ratio_`n' = female_exp_`n'/mean_exp_`n'
		local m_ratio_`n' = male_exp_`n'/mean_exp_`n'
		local c_ratio_`n' = child_exp_`n'/mean_exp_`n'
	}

//Insheet linear model results
	import delimited "`pm_output_2016'/lm_pred_`filedate'.csv", clear

	keep location_id ihme_loc_id year_id draw_*
	rename draw_1000 draw_0
	
	forvalues n = 0/999 {
		gen women_`n' = draw_`n'*`f_ratio_`n''
		gen men_`n' = draw_`n'*`m_ratio_`n''
		gen child_`n' = draw_`n'*`c_ratio_`n''
	}

	foreach group in "women" "men" "child" 	{
		fastrowmean `group'_*, mean_var_name(`group'_mean)
		fastpctile `group'_*, pct(2.5 97.5) names(`group'_lower `group'_upper)
		gen `group'_se = (`group'_upper - `group'_lower)/(2*1.96)
	}

	fastrowmean draw_*, mean_var_name(personal_mean)
	fastpctile draw_*, pct(2.5 97.5) names(personal_lower personal_upper)

		preserve
			keep *_mean *_lower *_upper ihme_loc_id year_id
			save "`out_dir'/crosswalk_summary_`filedate'", replace
		restore

//Clean up data (draws of PM2.5)
keep ihme_loc_id year child_* women_* men_*
drop *mean *lower *upper *se

//Save 
export delimited "`out_dir'/PM2.5_draws_`filedate'.csv", replace

**************************************
************End of Code****************
**************************************
