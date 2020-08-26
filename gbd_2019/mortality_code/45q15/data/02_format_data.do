// Purpose:		Format 45q15 data for upload and attach all NIDs

** **************************************************************************
  ** Configure Sata
** **************************************************************************
  
  // Set preferences for STATA
clear all
set more off
** Set directories
if c(os) == "Windows" {
  global j "FILEPATH"
}
if c(os) == "Unix" {
  global j "FILEPATH"
  set odbcmgr unixodbc
  
}

local new_run_id = "`1'"

local run_folder = "FILEPATH"
local out_dir = "`run_folder'/outputs"

local db_dir = "FILEPATH"

** ***************************************************************************
  ** load_45q15_data
** ***************************************************************************
  import delimited using "`out_dir'/raw_45q15.csv", clear
local total_rows = _N

rename exclude outlier
// Format data
generate sex_id = 1 if sex == "male"
replace sex_id = 2 if sex == "female"
replace deaths_source = lower(deaths_source)

replace nid = 21173 if source_date == 2007 & ihme_loc_id == "GHA" & regexm("dhs", deaths_source) & source_type == "SIBLING_HISTORIES"

// Separate VR/DDM data and outliers
preserve
keep if filename == "d10_45q15.dta" | outlier == 1
tempfile others
save `others', replace
restore
drop if filename == "d10_45q15.dta" | outlier == 1

// Only source the non-outliered, non-VR data
//drop if outlier == 1
tempfile data
save `data', replace

// Make a map of location_ids and ihme_loc_ids
preserve
keep location_id ihme_loc_id
tempfile loc_map
duplicates drop
save `loc_map', replace
restore
	
// nids to merge

import excel using "FILEPATH", firstrow clear
	drop if source_type == "SIBLING_HISTORIES"
	keep ihme source_type deaths_source year nid Parent
	rename nid NID
	duplicates drop
	tempfile nid
	save `nid', replace

use `data', clear
	merge m:1 ihme source_type deaths_source year using `nid'
drop if _m == 2

gen nid_new = Parent if Parent != .
replace nid_new = NID if NID != . & nid_new == .
replace nid_new = nid if nid != . & nid_new == .
replace underlying_nid = NID if NID != . & Parent != .
replace underlying_nid = nid if nid != . & Parent != .

drop _m Parent NID nid
rename nid_new nid

** type_id variable
/* type_id	type_short
1	VR
2	SRS
3	DSP
4	DSS
5	Census
6	Standard DHS
7	Other DHS
8	RHS
9	PAPFAM
10	PAPCHILD
11	MICS
12	WFS
13	LSMS
14	MIS
15	AIS
16	Other
34  MOH SURVEY
36  SSPC
37  DC
38  FFPS
39  SUPAS
40  SUSENAS
42  HOUSEHOLD
43  HHC
*/
  // Bring back in outliers for reformatting
append using `others'
	
	generate type_id = .
	replace type_id = 1 if (source_type == "VR" | source_type == "VR-SSA") & type_id == .
	replace type_id = 2 if source_type == "SRS" & type_id == .
	replace type_id = 3 if source_type == "DSP" & type_id == .
	replace type_id = 4 if source_type == "DSS" & type_id == .
	replace type_id = 5 if source_type == "CENSUS" | source_type == "2000_CENS_SURVEY" | regexm(lower(deaths_source), "census") | regexm(lower(deaths_source), "ipums") | deaths_source == "dyb" & type_id == .
	replace type_id = 6 if (regexm(lower(deaths_source), "dhs") & regexm(deaths, "sp_dhs") != 1) & type_id == .
	replace type_id = 7 if regexm(deaths_source, "sp_dhs") & type_id == .
	replace type_id = 8 if deaths_source == "cdc-rhs" & type_id == .
	replace type_id = 9 if regexm(lower(deaths_source), "papfam") & type_id == .
	replace type_id = 10 if regexm(lower(deaths_source), "papchild") & type_id == .
	replace type_id = 11 if regexm(lower(deaths_source), "mics") & type_id == .
	replace type_id = 13 if deaths_source == "93806#tza_lsml_2008" & type_id == .
	replace type_id = 13 if regexm(deaths_source, "lsms") & type_id == .
	replace type_id = 15 if deaths_source == "133219#ken_ais_2007" & type_id == .
	replace type_id = 34 if source_type == "MOH survey" & type_id == .
	replace type_id = 36 if source_type == "SSPC" & type_id == .
	replace type_id = 37 if source_type == "DC" & type_id == .
	replace type_id = 38 if source_type == "FFPS" & type_id == .
	replace type_id = 39 if source_type == "SUPAS" & type_id == .
	replace type_id = 40 if source_type == "SUSENAS" & type_id == .
	replace type_id = 43 if source_type == "HHC" & type_id == .
	replace type_id = 16 if type_id == .
	replace type_id = 42 if regexm(lower(source_type), "household") & inlist(type_id, 10, 16, 7, 6)
	** method_id variables	
	/* 
	1	Dir-Unadj
	2	Dir-Adj
	3	CBH
	4	SBH
	5	Sibs
	6	U5-Comp. (for completeness)
	7	SEG (for completeness)
	8	GGB (for completeness)
	9	GGBSEG (for completeness)
	*/

	generate method_id = .
	replace method_id = 1 if method_id == . & inlist(adjust, "complete", "unadjusted") & !(inlist(source_type, "SIBLING_HISTORIES", "direct", "indirect", "indirect, MAC only"))
	replace method_id = 2 if method_id == . & inlist(adjust, "ddm_adjusted", "adjusted") & !(inlist(source_type, "SIBLING_HISTORIES", "direct", "indirect", "indirect, MAC only"))
	replace method_id = 11 if method_id == . & source_type == "DSS"
	replace method_id = 5 if source_type == "SIBLING_HISTORIES" & method_id == .
	
	** reshape to get adjusted and unadjusted data in long form
	drop country source_type adjust comp natl_pop correction sex
	ren (adj45q15 obs45q15) (data_adj45q15 data_obs45q15)
	
	replace data_obs45q15 = data_adj45q15 if data_obs45q15 == .
	replace data_adj45q15 = data_obs45q15 if data_adj45q15 == .
	replace sex_id = _n + 2 if mi(sex_id)
	reshape long data_, i(location_id year deaths_source nid outlier shock sex_id type_id method_id source_date) j(adjusted, string)

	generate adjustment = .
	replace adjustment = 1 if adjusted == "adj45q15" 
	replace adjustment = 0 if adjusted == "obs45q15"
	
	** rename variables
	rename (year deaths_source data_) (year_id source mean)
	generate age_group_id = 155
	gen viz_year = year_id
	replace year_id = floor(year_id)
	rename type_id source_type_id

	keep year_id location_id ihme_loc_id sex_id age_group_id method_id source_type_id nid underlying_nid viz_year  mean adjustment shock outlier source sd exposure filename
	order year_id location_id sex_id age_group_id method_id source_type_id nid underlying_nid viz_year  mean adjustment shock outlier source
	sort location_id year_id sex_id adjustment
	compress
	
	local date = subinstr("$S_DATE", " ", "_", .)
	
	preserve
	keep if filename == "d10_45q15.dta" | outlier == 1
	save `others', replace
restore
drop if filename == "d10_45q15.dta" | outlier == 1

replace nid = 265226 if ihme_loc_id == "BRA" & year_id == 2014 & nid == . & source == "who_causesofdeath"
replace nid = 268267 if ihme_loc_id == "BRA" & year_id == 2015 & nid == . & source == "who_causesofdeath"
replace nid = 148605 if ihme_loc_id == "LBR" & year_id == 1970 & nid == . & source == "dyb"
replace nid = 43008 if ihme_loc_id == "PAK" & year_id == 2003 & nid == . & source == "pak_demographic_survey"
replace nid = 5827 if ihme_loc_id == "IDN" & year_id == 1998 & nid == . & source == "ifls"

save `data', replace
	// Split up dataset by whether or not still have NIDs
	preserve
	keep if !mi(nid)
	tempfile has_nids
	save `has_nids', replace
restore

keep if mi(nid)
tempfile no_nids
save `no_nids', replace
	
	// Bring in NIDs for IDN SUSENAS
	import delimited using "FILEPATH", clear
	rename year year_id
	tempfile sus_nids
	save `sus_nids', replace

use `no_nids', clear
	keep if substr(ihme_loc_id, 1, 3) == "IDN" & source == "susenas"
	merge m:1 year_id using `sus_nids', assert(2 4) keep(4) keepusing(nid) nogen update replace		// Merge NIDs from source table
save `sus_nids', replace
	use `no_nids', clear
merge 1:1 year_id location_id sex_id age_group_id method_id source_type_id viz_year adjustment using `sus_nids', assert(1 4) nogen update replace

	append using `has_nids'
append using `others'

replace nid = 33837 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year > 1980 & year < 1986
replace nid = 68236 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year == 1987
replace nid = 25080 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year == 1990
replace nid = 33841 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year == 1991
replace nid = 33867 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year == 1995
replace nid = 33764 if ihme_loc_id == "IND" & source == "srs" & filename == "d10_45q15.dta" & year == 2009

replace nid = 103973 if ihme_loc_id == "LAO" & source == "mics"
replace nid = 161662 if ihme_loc_id == "MWI" & source == "mics"

replace nid = 128820 if ihme_loc_id == "CHN_44533" & source == "dsp" & inlist(year, 1986, 1987, 1988)

preserve
keep if mi(nid)
if _N > 0 {
  noisily di in red "Warning, dropping unsourced data"
  export delimited using "`out_dir'/45q15_unsourced_dropped_data.csv", replace
}
restore
keep if !mi(nid)
drop ihme_loc_id filename

// NID null checks 
assert nid != .

//No duplicates across unique identifiers or, for VR, no duplicates across location/year/sex
sort location_id year_id sex_id method_id source_type_id nid underlying_nid adjustment
quietly by location_id year_id sex_id method_id source_type_id nid underlying_nid adjustment :  gen dup = cond(_N==1,0,_n)
tabulate dup if dup > 0
if `r(N)' > 0 {
			di in red "There are duplicates across unique identifiers"
			BREAK
	}
	cap drop dup
	
	export delimited using "`out_dir'/45q15_data.csv", replace	
	