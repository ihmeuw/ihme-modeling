** Purpose: Save each table in the passed excel sheet for SRS data to a formatted, cleaned, stata file

** set the prefix for whichever os we're in
if c(os) == "Windows" {
	global j "J:"
}
if c(os) == "Unix" {
	global j "/home/j"
	set odbcmgr unixodbc
}
set more off

local read_dir = "$j/WORK/03_cod/01_database/03_datasets/India_SRS_states_report"
local out_dir = "$j/WORK/03_cod/01_database/03_datasets/India_SRS_states_report/data/raw/tables"

import excel using "`read_dir'/data/raw/VA_Report_2010-2013_Release.xlsx", sheet("Tables") clear

** DROP BLANK ROWS
	gen all_missing=1
	local droplist = ""
	foreach var of varlist * {
		if "`var'" != "all_missing" {
			replace all_missing = 0 if `var' != ""
			count if `var'!=""
			if `r(N)'==0 {
				local droplist = "`droplist' `var'"
			}
			replace `var' = trim(`var')
		}
	}
	drop if all_missing==1
	drop all_missing `droplist'
	ds *
	local letters = "`r(varlist)'"

** SAVE SORT ORDER
	gen sort_order = _n
	format A-F %20s

** DETERMINE TABLE NUMBER FROM TITLE (CONSISTENT ACROSS SRS REPORTS)
	gen table = regexs(2) if regexm(A, "^(Table )([0-9]\.[0-9].)")
	replace table = regexs(2) if regexm(B, "^(Table )([0-9]\.[0-9].)")
	replace table = subinstr(table, " ", "A", .) if inlist(table, "3.1 ", "5.1 ")
	replace table = subinstr(table, " ", "", .)
	replace table = subinstr(table, "-", "", .)
	replace table = table[_n-1] if table==""

** DETERMINE REGION MAPPING
	gen region = regexs(2) if regexm(A, "(Major Region)(.*)(:)")
	replace region = subinstr(region, "-", "", .)
	replace region = trim(region)
	tempfile s
	save `s', replace
	keep region table
	keep if region != ""
	tempfile tr_map
	save `tr_map', replace
	use `s', clear

sort sort_order 
gen is_title = 1 if regexm(A, "^(Table )([0-9]\.[0-9].)") | regexm(B, "^(Table )([0-9]\.[0-9].)")

** STRUCTURAL LEARNING

	gen has_number = 1 if real(A) !=. | real(B) !=. | real(C) !=. | real(D) !=. | real(E) !=. | real(F) !=.
	replace has_number = 0 if has_number==.

	gen is_sub_title= 0
	local end = _N
	local prev_table = "none"
	forval i=1/`end' {
		if table[`i'] != "`prev_table'" {
			local found_last=0
		}
		replace is_sub_title=1 in `i' if has_number==0 & `found_last'==0 & is_title!=1
		local hn = has_number in `i'
		if `hn'==1 {
			local found_last=1
		}
		local prev_table = table in `i'
	}
	replace is_sub_title = 0 if is_sub_title==1 & !inlist(A, "Age Group", "Rank", "S.No.", "CAUSE OF DEATH") & A!=""
	replace is_sub_title = 0 if is_sub_title==1 & (regexm(table, "2\.1")|regexm(table, "^4")) & !inlist(B, "Cause of Death", "Major cause groups") & B!=""

	local prev_var A
	ds table sort_order region is_title has_number is_sub_title, not
	foreach var of varlist `r(varlist)' {
		gen in`var' = 1 if `var'!= ""
		bysort table: egen `var'count = total(in`var')
		replace `var' = `prev_var' if `var'=="" & is_sub_title==1 & `var'count!=0
		drop in`var' `var'count
		local prev_var `var'
	}

tempfile f
save `f', replace

** USE STURCTURAL LEARNING TO MAKE A ROW THAT RENAMES COLUMNS BY TABLE
	keep if is_sub_title ==1
	drop region is_title has_number is_sub_title
	sort table sort_order
	by table: gen ti = _n
	drop sort_order
	ds table ti, not
	local letters = "`r(varlist)'"
	rename (`letters') =col
	reshape wide *col, i(table) j(ti)
	foreach letter of local letters {
		rename `letter'col* col*`letter'
	}
	reshape long col1 col2 col3, i(table) j(old) string
	reshape long col, i(table old) j(num)

	replace col = lower(col)
	replace col = "age_group" if col=="age group"
	replace col = "male_pct" if regexm(col, "male %") & !regexm(col, "female %")
	replace col = "female_pct" if regexm(col, "female %")
	replace col = "person_pct" if regexm(col, "person %")
	replace col = "female" if regexm(col, "female") & !regexm(col, "pct")
	replace col = "male" if regexm(col, "male") & !regexm(col, "female") & !regexm(col, "pct")
	replace col = "person" if regexm(col, "person") & !regexm(col, "pct")
	replace col = "rural" if regexm(col, "rural")
	replace col = "urban" if regexm(col, "urban")
	replace col = "eag_states" if regexm(col, "eag states")
	replace col = "pct_deaths" if regexm(col, "deaths") & regexm(col, "%")
	replace col = "cause" if regexm(col, "causes* of death")
	replace col = "num_in_sample" if col=="number of sample units"
	replace col = "pop" if regexm(col, "population")
	replace col = "oth_st" if regexm(col, "other states")
	replace col = subinstr(col, "-", "_", .)
	replace col = subinstr(col, "+", "plus", .)
	replace col = subinstr(col, " ", "_", .)
	replace col = subinstr(col, ".", "_", .)
	replace col = subinstr(col, "/", "_", .)
	replace col = "_" + col if num>1 & col!=""
	reshape wide col, i(table old) j(num)
	gen newname = col1+col2+col3
	drop col*
	reshape wide newname, i(table) j(old) string
	rename newname* *
	gen name_row = 1
	append using `f'

** CLEAN UP
	drop region
	merge m:1 table using `tr_map', assert(1 3) nogen
	sort table name_row sort_order
	drop if is_sub_title ==1
	drop if is_title ==1
	drop is_title has_number is_sub_title sort_order
	compress

** SAVE ALL
	save "`read_dir'/data/raw/va_report_2010_2013_cleaned.dta", replace

** SAVE BY TABLE WITH RENAMES
	tempfile all
	save `all', replace
	levelsof table, local(tables)
	foreach table of local tables {
		use `all', clear
		keep if table=="`table'"

		local droplist = ""
		foreach var of varlist `letters' region {
			count if `var'!= ""
			if `r(N)'==0 {
				local droplist ="`var' `droplist'"
			}
		}
		drop `droplist'
		sort name_row
		foreach var of local letters {
			capture confirm variable `var'
			if _rc==0 {
				local newname = `var' in 1
				rename `var' `newname'
			}
		}
		drop if name_row==1
		drop name_row
		compress
		local n = subinstr("`table'", ".", "_", .)
		save "`out_dir'/table`n'.dta", replace
	}