// Author:NAME	
// Date:		2/25/2015
// Purpose:	Format IISS dataset for inclusion in the war database

	clear all
	cap restore not
	set more off

	if c(os) == "Windows" {
		global prefix ""
	}
	else if c(os) == "Unix" {
		global prefix ""
	}
	
	global outdir "FILEPATH"
	// reformatted webscrape data
	import delimited "FILEPATH", clear

	
	local year 1997
	foreach var of varlist v3-v22 {
		local new_name = "deaths`year'"
		rename `var' `new_name'
		local year = `year' + 1
	}
	rename conflict countryname
	keep countryname deaths*
	drop if _n<=2
	foreach var of varlist deaths* {
		capture replace `var' = subinstr(`var', " ", "", .)
		destring `var', replace
	}

	// format specific subnational locations 
	split countryname, p("(")
	gen subdiv = ""
	replace subdiv = "Xinjiang" if regexm(countryname, "Xinjiang")==1
	replace subdiv = "Northern Ireland" if regexm(countryname, "Northern Ireland")==1
 	replace subdiv = countryname2 if regexm(countryname, "India") & !regexm(countryname, "Pakistan")
	replace subdiv = countryname2 if regexm(countryname, "Indonesia")
	replace subdiv = subinstr(subdiv, ")", "", .)
	
	replace countryname = countryname1
	replace countryname = trim(countryname)
	drop countryname1 countryname2 countryname3
	drop if countryname =="" | regexm(countryname, "Copyright")
	expand 2 if regexm(countryname, "[A-Za-z]-[A-Za-z]") & !regexm(countryname, "(Qaeda)|(Timor)|(Brazza)"), gen(new)
	split countryname if regexm(countryname, "[A-Za-z]-[A-Za-z]") & !regexm(countryname, "(Qaeda)|(Timor)|(Brazza)"), p("-")
	replace countryname2 = countryname3 if regexm(countryname2, "Hizbullah")
	drop countryname3
	replace countryname1 = "Cambodia" if countryname1=="Cambodian"
	replace countryname1 = "Syria" if countryname1=="Syrian"
	
	foreach var of varlist deaths* {
		replace `var' = `var'/2 if countryname1 != ""
		
	}
	replace countryname=countryname1 if new==0 & countryname1 != ""
	replace countryname=countryname2 if new==1
	replace countryname="Thailand" if countryname=="Southern Thailand"
	replace countryname="Congo" if countryname=="Congo-Brazzaville"
	tempfile data
	save `data'
	
	use "FILEPATH", clear
	keep countryname countryname_ihme iso3
	merge 1:m countryname using `data'
	drop if _merge==1
	
	replace iso3 = "COD" if countryname=="DRC"
	replace iso3 = "PSE" if countryname=="Palestine"
	replace iso3 = "SYR" if countryname == "Syrian"
	replace iso3 = "XKX" if countryname=="Kosovo"
	drop if iso3==""
	keep iso3 deaths* subdiv
	
	collapse (sum) deaths*, by(iso3 subdiv)
	reshape long deaths, i(iso3 subdiv) j(year)
	drop if deaths == .	
	
	gen dataset_ind = 4 
	rename deaths war_deaths_best

	drop if war_deaths_best == 0

	// 6/25/12: for lower and upper bounds, keep them as missing.  In code 06_combine, the percent difference between low-best and high-best are calculated

	gen war_deaths_low = .
	gen war_deaths_high = .
	order iso3 subdiv year war_deaths_best war_deaths_low war_deaths_high
	gen cause="war"
	gen nid = 93128 // GBD 2016
	
	saveold "$outdir/IISS_alldeaths.dta", replace
