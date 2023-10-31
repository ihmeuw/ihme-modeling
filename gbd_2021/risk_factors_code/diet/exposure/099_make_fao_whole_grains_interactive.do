
*****************************************************************************************************************************
**  Description: Code to compile whole grains 
**
****************************************************************************************************************************
* Set preferences for STATA
	** Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
	** Set to run all selected code without pausing
		set more off
	** Remove previous restores
		cap restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

**call in arguments being passed in
args placeholder c_version gbd_round
local compiled_data "FILEPATH/FAO_sales_clean_compiled_`c_version'.csv"
local proc_ratio_output "FILEPATH/proc_ratio_`c_version'.csv"
local whole_grains_output "FILEPATH/whole_grains_`c_version'.csv"


import delimited "`compiled_data'
drop back_se variance
	preserve
	keep if ihme_risk=="diet_refined_grains"
	tempfile refined_grains_unadj
	save `refined_grains_unadj', replace
	restore
	keep if ihme_risk=="diet_total_grains"
	
	
	//rename year_id year
	cap rename gpr_mean total_grain_mean
	gen total_grain_se = (gpr_upper - gpr_lower) / (2 * 1.96)
	forvalues d = 0/999 {
		gen total_grain_draw_`d' = rnormal(total_grain_mean, total_grain_se)
	}
		drop gpr_* total_grain_mean total_grain_se
	merge 1:1 location_id year_id age_group_id sex_id using `refined_grains_unadj', keep(3) nogen
	cap rename gpr_mean refined_grain_mean
	gen refined_grain_se = (gpr_upper - gpr_lower) / (2 * 1.96)

	forvalues d = 0/999 {
		gen refined_grain_draw_`d' = rnormal(refined_grain_mean, refined_grain_se)
	}
		drop gpr_* refined_grain_mean refined_grain_se

	preserve
	forvalues d = 0/999 {
		gen proc_ratio_`d' = refined_grain_draw_`d' / total_grain_draw_`d' 
			drop total_grain_draw_`d' refined_grain_draw_`d'
			replace proc_ratio_`d' = . if proc_ratio_`d' > 1
	}
		replace ihme_risk = "grain_proc_ratio"
		egen gpr_mean = rowmean(proc_ratio_*)
		egen gpr_upper = rowpctile(proc_ratio_*), p(97.5)
		egen gpr_lower = rowpctile(proc_ratio_*), p(2.5)
			drop proc_ratio_*
			drop if gpr_mean == .
		gen back_se = (gpr_upper - gpr_lower)/(2*1.96)
		gen variance = (back_se)^2
	export delimited "`proc_ratio_output'", replace
	restore

	forvalues d = 0/999 {
		gen wg_draw_`d' = total_grain_draw_`d' - refined_grain_draw_`d'
			drop total_grain_draw_`d' refined_grain_draw_`d'
		replace wg_draw_`d' = . if wg_draw_`d' < 0
	}
	replace ihme_risk = "diet_whole_grains"
	replace gbd_cause = "whole_grains_g_unadj"
	egen gpr_mean = rowmean(wg_draw_*)
	egen gpr_upper = rowpctile(wg_draw_*), p(97.5)
	egen gpr_lower = rowpctile(wg_draw_*), p(2.5)
		drop wg_draw_*
		drop if gpr_mean == .
	gen back_se = (gpr_upper - gpr_lower)/(2*1.96)
	gen variance = (back_se)^2
	export delimited "`whole_grains_output'", replace
