cap program drop onehundredyears
program define onehundredyears


syntax, data(string) varname(string) iso3(string)

clear
set mem 300m
set more off

use "`data'", clear

if (_N > 0) { 

	if("`iso3'" ~= "all") {
		keep if ihme_loc_id == "`iso3'"
	}

	forvalues j = 0(5)800 {
		local jplus = `j'+4
		lookfor DATUM`j'to`jplus'
		return list
		if("`r(varlist)'" == "") {
			local max = `j'
			continue, break
		}
	}
	local maxminus = `max'-5
	di `max'
	di `maxminus'

	di "RENAME AGE GROUPS"
	di _N
	 quietly {
	forvalues j = 0(5)`maxminus' {
		local jplus = `j'+4
		local k = `j'/5
		rename agegroup`j'to`jplus' agegroup`k'	
		rename DATUM`j'to`jplus' DATUM`k'
	}
	 }

	di "RENAME OPEN INTERVAL"
	di _N
	 quietly {
	forvalues j = 0(5)`max' {
		capture: rename agegroup`j'plus agetemp_`j'plus
		capture: rename DATUM`j'plus temp_`j'plus
	}
	forvalues j = 0(5)`max' {
		local k = `j'/5
		capture: rename agetemp_`j'plus agegroup`k'plus
		capture: rename temp_`j'plus DATUM`k'plus
	}
	 }

	di "CREATE NEW VARIABLES"
	di _N
	 quietly {
	forvalues j = 0/100 {
		capture: g agegroup`j' = ""
		capture: g DATUM`j' = .
	}
	 }

	di "FILL IN THE NEW VARIABLES"
	di _N
	 quietly {
	forvalues j = 0/100 {
		capture: replace DATUM`j' = DATUM`j'plus if agegroup`j'plus ~= ""
		capture: replace agegroup`j' = agegroup`j'plus if agegroup`j'plus ~= "" 
	}

	drop *plus
	 }

	di "FILL IN THE AGE GROUP VARIABLES"
	di _N
	 quietly {
	forvalues j = 0/100 {
		replace agegroup`j' = substr(agegroup`j',6,strpos(agegroup`j',"to")-6) if strpos(agegroup`j',"to") ~= 0 & agegroup`j' ~= ""
		replace agegroup`j' = substr(agegroup`j',6,strpos(agegroup`j',"plus")-6) if strpos(agegroup`j',"plus") ~= 0 & agegroup`j' ~= ""
		}


	forvalues j = 0/100 {	
		destring agegroup`j', replace
	}

	forvalues j = 0/100 {
		g deaths`j' = .
	}
	 }

	di "DISTRIBUTE `varname' OVER 100 YEARS"
	di _N
	 quietly {
	forvalues j = 0/99 {
		local jplus = `j'+1
		forvalues k = 0/99 {
			local kplus = `k'+1
			replace deaths`j' = DATUM`k'/(agegroup`kplus'-agegroup`k') if `j' >= agegroup`k' & `j' < agegroup`kplus' & deaths`j' == . & DATUM`kplus' ~= .
		}
	}

	forvalues j = 0/99 {
		forvalues k = 0/99 {
			local kplus = `k' + 1
			replace deaths`j' = DATUM`k'/(101-agegroup`k') if `j' >= agegroup`k' & DATUM`kplus' == .
		}
	}
	forvalues j = 0/100 {
		replace deaths100 = DATUM`j' if agegroup`j' == 100
	}
	forvalues j = 0/100 {
		replace deaths100 = deaths99 if deaths100 == .
	}

	drop DATUM*
	 }

	forvalues j = 0/100 {
		capture: rename deaths`j' `varname'`j'
	}

	if("`varname'" == "pop") {
		
		forvalues j = 0/99 {
			local jplus = `j'+1
			g diff`j' = agegroup`jplus'-agegroup`j' if agegroup`j' ~= . & agegroup`jplus' ~= . 	
		}

		egen meandiff = rowmean(diff*)
		drop diff*

		sort meandiff

		duplicates drop ihme_loc_id year sex source_type precise_year pop*, force

		sort meandiff

		duplicates drop ihme_loc_id year sex source_type precise_year pop_source, force

		duplicates drop ihme_loc_id year sex source_type precise_year agegroup* pop0-pop100, force

		noi di "`iso3'"
		noi di "before duplicate drop"

		duplicates tag ihme_loc_id year sex source_type precise_year, g(dup)
		drop if pop_source == "IPUMS" & dup == 1
		drop dup

		sort meandiff
		
		capture: duplicates drop ihme_loc_id year sex source_type precise_year if ihme_loc_id ~= "CHN", force

		drop meandiff
	}
	else if("`varname'" == "deaths") {

		forvalues j = 0/99 {
			local jplus = `j'+1
			g diff`j' = agegroup`jplus'-agegroup`j' if agegroup`j' ~= . & agegroup`jplus' ~= . 	
		}

		egen meandiff = rowmean(diff*)
		drop diff* 

		sort meandiff

		duplicates drop ihme_loc_id year sex source_type deaths_source, force

		drop meandiff

		egen rmiss = rowmiss(death*)
		drop if rmiss == 101
		drop rmiss

	}
	noi di "outside of if statements"
	local newname = subinstr("`data'","vORDERED","vFORMATTED",1)
	noi di "after newname var generation"
	
	egen openendinterval = rowmax(agegroup*)
	
	noi di "after egen openedinterval"
	drop if openendinterval == 15
	noi di "after drop if openedinterval"
	drop openendinterval

} 

end
