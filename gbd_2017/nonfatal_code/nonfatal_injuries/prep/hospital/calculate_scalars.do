// Generate file with adjustment factor for E/N reporting proportion by iso3/year/age/sex 
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
	}
	set more off
	clear all
	cap restore, not
	cap set maxvar 20000
	set seed 80085
	adopath + "`prefix'/FILEPATH"
	adopath + "`prefix'/FILEPATH"
	adopath + "`prefix'/FILEPATH"
	adopath + "FILEPATH"

	local function _inj
	local out_dir "`prefix'/FILEPATH"
	local tmp_dir "`clustertmp'/FILEPATH"

** ADJUSTING NUMERATOR BY E-N FACTOR		
** **************************************************************************************
	local cutoff 0.15
	local single_year 0
	cap mkdir "`out_dir'/FILEPATH"
	
	// load hospital data and save in temporary folder
	import delimited using "`prefix'/FILEPATH.csv", varnames(1) clear
		tempfile original
		save `original'

		// replace platform names so that they're the same
		replace facility_id = "1" if facility_id == "inpatient unknown" | facility_id == "hospital"
		replace facility_id = "2" if facility_id == "outpatient unknown" | facility_id == "emergency" | facility_id == "day clinic"

		drop if year_start < 1988
		drop if regexm(source, "NHAMCS")

	if !`single_year' {
		replace year_start = 1988 if inrange(year_start, 1988, 1992)
		replace year_start = 1993 if inrange(year_start, 1993, 1997)
		replace year_start = 1998 if inrange(year_start, 1998, 2002)
		replace year_start = 2003 if inrange(year_start, 2003, 2007)
		replace year_start = 2008 if inrange(year_start, 2008, 2012)
		replace year_start = 2013 if inrange(year_start, 2013, 2017)

		replace year_end = 1992 if year_start == 1988
		replace year_end = 1997 if year_start == 1993
		replace year_end = 2002 if year_start == 1998
		replace year_end = 2007 if year_start == 2003
		replace year_end = 2012 if year_start == 2008
		replace year_end = 2017 if year_start == 2013
	}

		gen cause = regexm(nonfatal_cause_name, "e-code") | regexm(nonfatal_cause_name, "n-code")

		keep if cause == 1 
		replace cause = 0
		replace cause = regexm(nonfatal_cause_name, "n-code")
		gen code = "Ncode" if cause == 1
		replace code = "Ecode" if cause == 0
		drop cause
		rename val numerator
		rename facility_id platform

		fastcollapse numerator, type(sum) by(location_id platform year_start year_end code diagnosis_id)

		egen total = sum(numerator), by(location_id platform year_start year_end)
		egen total_prim = total(numerator) if diagnosis_id == 1, by(location_id platform year_start year_end)

		gen prop = numerator / total
		gen prop_prim = numerator / total_prim

		tempfile data
		save `data', replace

		get_location_metadata, location_set_id(9) clear
		keep location_id location_name

		merge 1:m location_id using `data', keep(3) assert(1 3) nogen

		egen diag_total = sum(diagnosis_id), by(location_id platform year_start year_end code)

		tempfile factors
		save `factors', replace

	// Now work on merging on the super region if the sum of diagnosis id is 3

	use `factors', clear

		keep if diag_total == 3 & code == "Ecode"
		gen marginal_prop = prop/prop_prim

		fastcollapse marginal_prop, type(mean) by(platform)

		tempfile supers
		save `supers', replace

	use `factors', clear

		merge m:1 platform using `supers', keep(3) assert (2 3) nogen

		drop if code == "Ncode" & prop_prim != 1

		gen remove = (code == "Ncode" & prop_prim == 1)

		replace prop = prop * marginal_prop if diag_total == 1
		
		gen factor = 1 / prop

		replace remove = 1 if prop < `cutoff'
		keep if diagnosis_id == 1

		gen cv_outpatient = 1 if platform == "2"
		replace cv_outpatient = 0 if platform == "1"

		if !`single_year' {
			export delimited "`out_dir'/FILEPATH.csv", delim(",") replace
		}
		else {
			export delimited "`out_dir'/FILEPATH.csv", delim(",") replace
		}

***********************************************************************
	* NOW WORK ON THE NHAMCS DATA
	use `original', clear
	local single_year 0
	keep if regexm(source, "NHAMCS")
	replace facility_id = "1" if facility_id == "inpatient unknown" | facility_id == "hospital"
	replace facility_id = "2" if facility_id == "outpatient unknown" | facility_id == "emergency" | facility_id == "day clinic"

	drop if year_start < 1988

if !`single_year' {
	replace year_start = 1988 if inrange(year_start, 1988, 1992)
	replace year_start = 1993 if inrange(year_start, 1993, 1997)
	replace year_start = 1998 if inrange(year_start, 1998, 2002)
	replace year_start = 2003 if inrange(year_start, 2003, 2007)
	replace year_start = 2008 if inrange(year_start, 2008, 2012)
	replace year_start = 2013 if inrange(year_start, 2013, 2017)

	replace year_end = 1992 if year_start == 1988
	replace year_end = 1997 if year_start == 1993
	replace year_end = 2002 if year_start == 1998
	replace year_end = 2007 if year_start == 2003
	replace year_end = 2012 if year_start == 2008
	replace year_end = 2017 if year_start == 2013
}

	gen cause = regexm(nonfatal_cause_name, "e-code") | regexm(nonfatal_cause_name, "n-code")

	keep if cause == 1 
	replace cause = 0
	replace cause = regexm(nonfatal_cause_name, "n-code")
	gen code = "Ncode" if cause == 1
	replace code = "Ecode" if cause == 0
	drop cause
	rename val numerator
	rename facility_id platform

	fastcollapse numerator, type(sum) by(location_id platform year_start year_end code diagnosis_id)

	egen total = sum(numerator), by(location_id platform year_start year_end)
	egen total_prim = total(numerator) if diagnosis_id == 1, by(location_id platform year_start year_end)

	gen prop = numerator / total
	gen prop_prim = numerator / total_prim

	tempfile data
	save `data', replace

	get_location_metadata, location_set_id(9) clear
	keep location_id location_name

	merge 1:m location_id using `data', keep(3) assert(1 3) nogen

	egen diag_total = sum(diagnosis_id), by(location_id platform year_start year_end code)

	tempfile factors
	save `factors', replace

	use `factors', clear

		keep if diag_total == 3 & code == "Ecode"
		gen marginal_prop = prop / prop_prim

		fastcollapse marginal_prop, type(mean) by(platform)

		tempfile supers
		save `supers', replace

	use `factors', clear

		merge m:1 platform using `supers', keep(3) assert (2 3) nogen

		drop if code == "Ncode" & prop_prim != 1

		gen remove = (code == "Ncode" & prop_prim == 1)

		replace prop = prop * marginal_prop if diag_total == 1
		gen factor = 1 / prop

		replace remove = 1 if prop < `cutoff'
		keep if diagnosis_id == 1

		gen cv_outpatient = 1 if platform == "2"
		replace cv_outpatient = 0 if platform == "1"

	if !`single_year' {
		export delimited "`out_dir'/FILEPATH.csv", delim(",") replace
	}
	else {
		export delimited "`out_dir'/FILEPATH.csv", delim(",") replace
	}


