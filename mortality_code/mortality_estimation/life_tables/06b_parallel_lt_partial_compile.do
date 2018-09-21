
clear all
set more off
cap restore, not

if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local sim `1'
		local htype "`2'"
		local in_dir "`3'"
		local save_dir "`4'"
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local sim 0
	}
	

local sim = `sim'*100
local simplus = `sim' + 99
local simone = `sim' + 1

if ("`htype'" == "hiv_free") local filename "/LT_sim_nohiv"
if ("`htype'" == "with_hiv") local filename "/LT_sim_withhiv_withsim"

insheet using "`in_dir'/`filename'_`sim'.csv", clear
keep ihme_loc_id sex year age ax* mx* qx*
rename mx mx`sim'
rename ax ax`sim'
rename qx qx`sim'
tempfile compile
save `compile', replace

forvalues i = `simone'/`simplus' {
	di `i' 
	insheet using "`in_dir'/`filename'_`i'.csv", clear
	merge 1:1 ihme_loc_id sex year age using `compile', assert(3) nogen
	if ("`htype'" == "hiv_free") {
		rename mx mx`i'
		rename ax ax`i'
		rename qx qx`i'
	}
	if ("`htype'" == "with_hiv") {
		rename mx mx`i'
		rename ax ax`i'
		rename qx qx`i'
	}
	keep ihme_loc_id sex year age mx* ax* qx*
	save `compile', replace
}


keep ihme_loc_id sex year age mx* ax* qx*
isid ihme_loc_id sex year age

replace sex = "1" if sex == "male"
replace sex = "2" if sex == "female"
destring sex, replace 

saveold "`save_dir'/partial_compile_`sim'.dta", replace

