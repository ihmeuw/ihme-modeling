
clear all
set more off
cap restore, not

if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
		local dat_dir "`1'"
		local htype "`2'"
		local in_dir "`dat_dir'"
		local save_dir "`dat_dir'"
		local param "`3'"
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}
	


use "`in_dir'/partial_compile_0.dta", clear
keep ihme_loc_id sex year age `param'*

local simone = 1
local simplus = 9

forvalues i = `simone'/`simplus' {
	local i = `i'*100
	di `i' 
	merge 1:1 ihme_loc_id sex year age using "`in_dir'/partial_compile_`i'.dta", assert(3) nogen
	keep ihme_loc_id sex year age `param'*
}

keep ihme_loc_id sex year age `param'*
isid ihme_loc_id sex year age

saveold "`save_dir'/compiled_lt_`param'.dta", replace
saveold "`save_dir'/archive/compiled_lt_`param'_$S_DATE.dta", replace
if ("`htype'" == "hiv_free") saveold "FILEPATH/compiled_lt_`param'_$S_DATE.dta", replace
if ("`htype'" == "with_hiv") saveold "FILEPATH/compiled_lt_`param'_$S_DATE.dta", replace

adopath + "FILEPATH"

fastrowmean(`param'*), mean_var_name(mean_`param')

if ("`param'" == "mx" & "`htype'" == "hiv_free") {
	preserve
	keep ihme_loc_id sex year age mean*
	sort ihme_loc_id sex year age
	saveold "`save_dir'/compiled_lt_`param'_mean.dta", replace
	restore
}

fastpctile(`param'*), pct(2.5 97.5) names(lower_`param' upper_`param')

keep ihme_loc_id sex year age mean* lower* upper*
sort ihme_loc_id sex year age

saveold "`save_dir'/compiled_lt_`param'_summary.dta", replace
saveold "`save_dir'/archive/compiled_lt_`param'_summary_$S_DATE.dta", replace



