** Compile and summarize


clear all
set more off
cap restore, not

if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}


local in_dir "FILEPATH"

cd "`in_dir'"

use "compiled_partial_env_0.dta", clear
forvalues i = 1/9 {
	local i = `i'*100
	merge 1:1 age sex year location_id using "compiled_partial_env_`i'.dta", nogen assert(3)
}

rename year year_id


saveold "FILEPATH/compiled_env.dta", replace
saveold "FILEPATH/compiled_env_$S_DATE.dta", replace
saveold "FILEPATH/compiled_env_$S_DATE.dta", replace


adopath + "FILEPATH"

rename env_unscaled* unscaled_env*
rename env_* envelope_*

** summarize unscaled results
fastpctile(unscaled_env*), pct(2.5 97.5) names(enve_unscaled_lower enve_unscaled_upper)
fastrowmean(unscaled_env*), mean_var_name(enve_unscaled_mean)

** summarize scaled results
fastpctile(envelope*), pct(2.5 97.5) names(enve_lower enve_upper)
fastrowmean(envelope*), mean_var_name(enve_mean)

drop envelope*
drop unscaled_env*

saveold "FILEPATH/compiled_env_summary.dta", replace
saveold "FILEPATH/compiled_env_summary_$S_DATE.dta", replace 
