
clear all
set more off
set memory 6000m

	if (c(os)=="Unix") {
		global root "FILEPATH"
		set odbcmgr unixodbc
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global root "FILEPATH"
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
	append using "FILEPATH/u5_agessex_`loc'.dta"
}	
	
drop if year>2016
drop if year<1950

saveold "FILEPATH/u5_agesex_summary.dta", replace
saveold "FILEPATH/u5_agesex_summary_$S_DATE.dta", replace
saveold "FILEPATH/u5_agesex_summary.dta", replace
saveold "FILEPATH/u5_agesex_summary_$S_DATE.dta", replace
