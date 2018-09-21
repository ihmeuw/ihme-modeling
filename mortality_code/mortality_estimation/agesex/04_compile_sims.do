	
	clear all 
	set more off
	capture log close
	
	if (c(os)=="Unix") {
		global ctmp "FILEPATH"
		global j "FILEPATH"
		set odbcmgr unixodbc
		local code_dir "`1'"
		qui do "FILEPATH/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global j "J:"
		qui do "FILEPATH/get_locations.ado"
	}

	import delimited "FILEPATH/u5_env_locs.csv" 
	keep if level_all == 1
	keep ihme_loc_id region_name super_region_name
	tempfile gbds
	save `gbds', replace
	

keep ihme_loc_id
duplicates drop ihme_loc_id, force

local num = _N
local toloop = ceil(`num'/45)

forvalues i=1/`toloop' {
	local j = `i' - 1
	local top = `i'*45 
	local bottom = `j'*45 + 1
	di "`bottom' to `top'"
	preserve
	keep if _n <= 45*`i' & _n > 45*(`j')
	levelsof ihme_loc_id, local(isos`i')
	restore
}

local missing ""

forvalues j=1/`toloop' {
	clear
	local count = 1
	tempfile part`j'
	foreach i of local isos`j' {	
		if (`count' == 1) cap use "FILEPATH/noshock_u5_deaths_`i'.dta", clear
		if (`count' > 1) cap append using "FILEPATH/noshock_u5_deaths_`i'.dta"
		cap assert _rc == 0
		if (_rc != 0) { 
			noisily display in red "`i' file not available"
			noisily display in red "FILEPATH/noshock_u5_deaths_`i'.dta"
			local missing "`missing' `i'"
		}
		noisily dis "`i' appended to part `j'"
		local count = `count' + 1
    }
	save `part`j'', replace
	noisily dis in red "part `j' done"
}

	clear
	local len = wordcount("`missing'")
	if (`len' == 0) {
		local len = 1
		local missing "NOTHINGMISSING"
	}
	set obs `len'
	gen ihme_loc_id = ""
	gen row = _n
	local count = 1
	foreach unit of local missing {
		replace ihme_loc_id = "`unit'" if row == `count'
		local count = `count' + 1
	}
	drop row
	outsheet using "FILEPATH/missing_countries.csv", replace
	local run_anyway = 0
	if (`len' > 0 & "`missing'" != "NOTHINGMISSING"	& `run_anyway' != 1) assert 1==2


use `part1',clear
forvalues j=2/`toloop' {
    append using `part`j''
}

saveold "FILEPATH/allnonshocks.dta", replace
saveold "FILEPATH/allnonshocks_$S_DATE.dta", replace
