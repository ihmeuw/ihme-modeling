// Author:NAME	
// Date:		5/18/2012
// Purpose:	make final war and disaster file

clear all 
pause on

	if c(os) == "Windows" {
		global prefix ""
	}
	else {
		global prefix ""
	}
	
	
	local input_folder "FILEPATH"
	local output_folder "FILEPATH"

	do "FILEPATH"

	** database setup
	do "FILEPATH"
	create_connection_string, server("modeling-cod-db") database("cod")
	local conn_string `r(conn_string)'


local date = c(current_date)
* FOR THE FILE GOING INTO PATH\WAR_AND_DISASTER, GO INTO THAT FOLDER AND ADD THE DATE TO THE FILE NAME, 
// 	in the way that the data team likes it

// bring in disaster file
	use "FILEPATH", clear
	
	gen deaths_high = u_disaster_rate * pop
	gen deaths_low = l_disaster_rate * pop
	
	collapse (sum) disaster deaths*, by(iso3 location_id year cause pop) fast
	
	gen float l_disaster_rate = deaths_low / pop
	gen float u_disaster_rate = deaths_high / pop
	gen float disaster_rate = disaster / pop
		
	keep iso3 location_id cause disaster disaster_rate l_disaster_rate u_disaster_rate year
	rename (disaster disaster_rate l_disaster_rate u_disaster_rate) (deaths_best rate l_rate u_rate)
	
	drop if iso3 == ""
	drop if deaths_best == .
	drop if deaths_best == 0
	//drop if source == "VR" & inlist(location_id, 135,6,95,163,67,130,152,93,102,196)
	
	// For merge
	replace location_id = . if !inlist(iso3, "CHN", "MEX", "GBR", "IDN", "IND", "BRA", "JPN") & !inlist(iso3,"SAU","SWE","USA","KEN","ZAF")
	//replace location_id = 6.66 if location_id == .

	// Assert the relationship between low, best, high
	count if l_rate>rate
	assert `r(N)'==0
	count if rate>u_rate
	assert `r(N)'==0

	collapse (sum) deaths_best *rate, by(iso3 location_id year cause) fast
	
	tempfile disaster
	save `disaster', replace

// bring in the final war database
	use "`FILEPATH", clear

	replace cause = "inj_war_execution" if cause == "legal intervention"
	replace cause = "inj_war_war" if cause == "war"
	replace cause = "inj_war_terrorism" if  cause == "terrorism"

	// count if war_deaths_low>war_deaths_best
	// assert `r(N)'==0
	// count if war_deaths_best>war_deaths_high
	// assert `r(N)'==0

	** separate the location ids from the iso3 codes
	split iso3, p("_")
	destring iso32, replace
	assert iso32 == location_id if strlen(iso3) > 3
	drop iso3
	rename iso31 iso3
	drop iso32


	// Collapse to iso3 year
	destring location_id, replace
	replace location_id = . if !inlist(iso3, "CHN", "MEX", "GBR", "IDN", "IND", "BRA", "JPN") & !inlist(iso3,"SAU","SWE","USA","KEN","ZAF")

	collapse (sum) war_deaths_best war_deaths_low war_deaths_high, by(iso3 location_id year tot cause) fast

	** 3/31/17 : use lat/long from GED to split SDN/SSD pre independence (2011) (do this for war and terrorism) ****
	**4/1/17   : Checked. This works as intended.
	preserve
		import delimited using "FILEPATH", clear
		tempfile sdn_ratios
		save `sdn_ratios', replace
		import delimited using "FILEPATH", clear
		append using `sdn_ratios'
		save `sdn_ratios', replace
	restore
	preserve
		keep if iso3 == "SDN" & year < 2011 & year > 1982 & cause != "inj_war_execution" 
		isid year cause
		merge m:1 year using `sdn_ratios', assert(3) nogen
		// create two copies, one for SDN, one for SSD.
		expand 2
		by year cause, sort: gen tag = _n
		foreach v of varlist war_deaths_low war_deaths_high war_deaths_best {
			replace `v' = `v' * sdn_proportion if tag == 1
			replace `v' = `v' * ssd_proportion if tag == 2
		}
		replace iso3 = "SSD" if tag == 2
		drop tag ssd_proportion sdn_proportion

		tempfile sdn_split
		save `sdn_split', replace

	restore
	drop if iso3 == "SDN" & year < 2011 & year > 1982  & cause != "inj_war_execution" 
	append using `sdn_split'
	******** end Sudan fix *************************************

	assert war_deaths_low<=war_deaths_best
	assert war_deaths_best<=war_deaths_high
	
	gen war_rate = war_deaths_best / tot
	gen l_war_rate = war_deaths_low / tot
	gen u_war_rate = war_deaths_high / tot

	keep iso3 location_id year cause war_deaths_best war_rate l_war_rate u_war_rate 
	rename (war_deaths_best war_rate l_war_rate u_war_rate) (deaths_best rate l_rate u_rate)
	
	// For merge
	//replace location_id = 6.66 if location_id == .
	
	// Assert the relationship between low deaths high
	assert l_rate<=rate
	assert rate<=u_rate
	
	merge 1:1 iso3 location_id year cause using `disaster'
	drop _merge
	gen sex = "both"
	//replace location_id = . if location == 6.66
	replace iso3 = iso3 + "_" + string(location_id) if location_id != .

	replace iso3 = "IDN" if iso3 == "IDN_11"
	
	tempfile all
	save `all', replace

// grab location names
	**  12/27/16 using connection string, not dsn()
	odbc load, `conn_string' exec("SELECT location_id, ihme_loc_id, location_name FROM shared.location_hierarchy_history WHERE location_set_version_id IN (SELECT max(location_set_version_id) AS location_set_version_id FROM shared.location_set_version WHERE location_set_id=35)") clear 

	tempfile loc_names
	save `loc_names', replace 

	// add location name for china 
	odbc load, `conn_string' exec("SELECT 'CHN_44533' AS ihme_loc_id, location_name FROM shared.location WHERE location_id=44533") clear
	append using `loc_names'
	save `loc_names', replace

// merge in population
	* get_population, year_id("-1") sex_id("3") location_id("-1") location_set_id(35) age_group_id("-1") clear
	get_population, age_group_id(22) sex_id(3) location_id(-1) location_set_id(35) year_id(-1) gbd_round_id(4) status("recent") clear
	rename year_id year 

	* // turn iso3 + location_id into ihme_loc_id
	* gen ihme_loc_suffix = "_" + string(location_id) if location_id != .
	* replace ihme_loc_suffix = "" if location_id==.
	* gen ihme_loc_id = iso3 + ihme_loc_suffix

	// add location names
	merge m:1 location_id using `loc_names', assert(2 3) keep(3) nogen

	// collapse to iso3 year
	collapse (sum) pop, by(ihme_loc_id year location_id location_name)
	gen sex = "both"
	rename ihme_loc_id iso3
	
	merge 1:m iso3 year sex using `all'
	// _m==1: country-years with no disaster or war
	drop if year < 1970
	** assert _m != 2
 
	drop if _m != 3
	drop _m
	
	drop location_id

	//rename tot pop
	//replace pop = pop*1000
	
	/* Don't need this if doing by cause
	// generate all deaths rate
	gen all_deaths = war+disaster
	gen all_deaths_rate = all_deaths/pop
	
	order iso3 year cause war disaster all_deaths war_rate l_war_rate u_war_rate disaster_rate l_disaster_rate u_disaster_rate all_deaths_rate pop
	keep iso3 year cause war disaster all_deaths war_rate l_war_rate u_war_rate disaster_rate l_disaster_rate u_disaster_rate all_deaths_rate pop
	
	replace war_rate = 0 if war_rate == . 
	replace disaster_rate = 0 if disaster_rate == .
	
	// generate a variable to capture the 10 year running average of death rates
	reshape wide war disaster all_deaths war_rate l_war_rate u_war_rate disaster_rate l_disaster_rate u_disaster_rate all_deaths_rate pop, i(iso3) j(year)
	aorder
	
	
	local vars = "war disaster all_deaths pop" 
	foreach  var of local vars {
		forvalues i = 1970/2015 {
			local x = `i' - 9
			if `i' > 1980 {
				egen sum_`var'`i' = rowtotal(`var'`x'-`var'`i')
			}
			if `i' <= 1960 {
				egen sum_`var'`i' = rowtotal(`var'1950-`var'`i')
			}
		}
	}
	
	reshape long all_deaths all_deaths_rate disaster disaster_rate l_disaster_rate u_disaster_rate pop war war_rate l_war_rate u_war_rate sum_war sum_disaster sum_all_deaths sum_pop, i(iso3) j(year)  
	
	// generate new running average variables
	gen war_rate_10yr_avg = sum_war/sum_pop
	gen disaster_rate_10yr_avg = sum_disaster/sum_pop
	gen all_deaths_rate_10yr_avg = sum_all_deaths/sum_pop
	*/
	
	rename population total_population
	tostring year, gen(year2)
	drop year
	rename year2 year
	destring year, replace
	
	
	//order iso3 year war disaster all_deaths war_rate l_war_rate u_war_rate disaster_rate l_disaster_rate u_disaster_rate all_deaths_rate war_rate_10yr_avg disaster_rate_10yr_avg all_deaths_rate_10yr_avg total_population
	//keep iso3 year war disaster all_deaths war_rate l_war_rate u_war_rate disaster_rate l_disaster_rate u_disaster_rate all_deaths_rate war_rate_10yr_avg disaster_rate_10yr_avg all_deaths_rate_10yr_avg total_population
	order iso3 year cause deaths_best rate l_rate u_rate total_population 
	keep iso3 year cause deaths_best rate l_rate u_rate total_population 
	
	isid iso3 year cause
	
	// save the final dataset
	saveold "FILEPATH", replace
	saveold "FILEPATH", replace
