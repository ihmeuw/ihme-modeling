
clear all
set more off
cap restore, not

if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local sim `1'
		local in_dir "`2'"
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local sim 1
		local in_dir "FILEPATH"
	}

cd "`in_dir'"
local sim = `sim'*100
local simplus = `sim' + 99
local simone = `sim' + 1

use "envelope_sim_`sim'.dta", clear


forvalues i = `simone'/`simplus' {
	merge 1:1 age sex year location_id using "envelope_sim_`i'.dta", nogen assert(3)
}

saveold "compiled_partial_env_`sim'.dta", replace
