
*** by sim
clear all
set more off
set memory 6000m

	if (c(os)=="Unix") {
		** global arg gets passed in from the shell script and is parsed to get the individual arguments below
		global root "FILEPATH"
		set odbcmgr unixodbc
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global root "J:"
		qui do "FILEPATH/get_locations.ado"
	}

	
get_locations, level(estimate)
keep if level_all == 1
keep ihme_loc_id 
levelsof ihme_loc_id, local(locs)

clear
tempfile compiled
save `compiled', replace emptyok
foreach loc of local locs {
	di "`loc'"
	append using "FILEPATH/shock_u5_pop_mean_`loc'.dta"
}	
	
drop if year>2016
drop if year<1950

saveold "FILEPATH/u5_pop_iteration.dta", replace
saveold "FILEPATH/u5_pop_iteration_$S_DATE.dta", replace
saveold "FILEPATH/u5_pop_iteration.dta", replace
saveold "FILEPATH/u5_pop_iteration_$S_DATE.dta", replace
