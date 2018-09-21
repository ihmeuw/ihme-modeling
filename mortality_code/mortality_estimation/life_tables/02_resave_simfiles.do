** resave draw-level entry files to envelope process

clear 
set more off

di "`1'"

	if (c(os)=="Unix") {
		global j "FILEPATH"
		set odbcmgr unixodbc
		local draw = `1'
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global j "FILEPATH"
		qui do "FILEPATH/get_locations.ado"
		local draw = 1
	}
	

local begin = `draw'*50 
local end = `draw'*50 + 49


clear
set seed 1234567
set matsize 1000
set obs 1000
gen sim=_n-1
expand 10
sort sim
bysort sim: gen secsim=_n
set seed 1234567
sample 10
mkmat sim secsim, matrix(w)


** load data
use "FILEPATHall_entry_sims.dta", clear

forvalues i = `begin'/`end' {

	local j = `i' + 1
	preserve
	keep if simulation == w[`j',1]
	gen secsim=w[`j',2]
	saveold "FILEPATH/entry_`i'.dta", replaceS
	restore
	
}

exit, clear


