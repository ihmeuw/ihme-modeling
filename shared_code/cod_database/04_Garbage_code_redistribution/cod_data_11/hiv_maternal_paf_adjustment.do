** ********************************************************************************
** Purpose: Use calculated proportions of maternal that is hiv positive maternal, hiv positive hiv to adjust maternal parent and create maternal_hiv observations
** ********************************************************************************

** set odbc manager
	set odbcmgr unixodbc

** set location_set_version_id
local lsvid 69
local testing = 0


** save the input data
tempfile data
save `data', replace
** make sure this cause does not exist before this step
count if acause=="maternal_hiv"
assert `r(N)'==0

** use age restrictions to figure out which portion of the data to reserve for the adjustment
use yll_age_start yll_age_end acause using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta" if acause=="maternal", clear
local age_start = yll_age_start in 1
local age_end = yll_age_end in 1

use `data', clear

count if acause=="maternal" & age>=`age_start' & age<=`age_end' & sex==2 & year>=1980
** only do anything if there is any maternal data that would be adjusted
if `r(N)'>0 {

keep if acause=="maternal" & age>=`age_start' & age<=`age_end' & sex==2 & year>=1980
tempfile matdata
save `matdata', replace
** determine years in the data with maternal
levelsof year, local(years)

** save ages for matching maternal props with data
odbc load, exec("SELECT age_group_id, age_group_name_short FROM shared.age_group where (age_group_id>=2 and age_group_id<=22) or age_group_id=27") strConnection clear
replace age_group_name = "0" if age_group_name == "EN"
replace age_group_name = "0.01" if age_group_name == "LN"
replace age_group_name = "0.1" if age_group_name == "PN"
** all ages
replace age_group_name = "99" if age_group_id==22
** age standardized
replace age_group_name = "98" if age_group_id==27
destring age_group_name, replace
rename age_group_name age
tempfile ages
save `ages', replace

** save locations for matching location_id to dumb iso3/location_id combo in cod data
odbc load, exec("SELECT location_id, ihme_loc_id AS iso3, level FROM shared.location_hierarchy_history WHERE location_set_version_id=`lsvid' AND level>=3") strConnection clear
replace iso3 = substr(iso3, 1, 3)
tempfile locations
save `locations', replace

clear
tempfile props
gen foo = .
save `props', replace

foreach year of local years {
	capture import delimited using "/ihme/cod/prep/01_database/maternal_hiv_props/maternal_hiv_props_`year'.csv", clear
	if _rc {
		!python "/home/j/WORK/03_cod/01_database/02_programs/maternal_custom/calc_percent_to_hiv_for_cod.py" `year'
		import delimited using "/ihme/cod/prep/01_database/maternal_hiv_props/maternal_hiv_props_`year'.csv", clear
	}
	append using `props'
	save `props', replace
}
drop foo
save `props', replace

** fix age variable to match with data
merge m:1 age_group_id using `ages', assert(2 3) keep(3) nogen
merge m:1 location_id using `locations', assert(2 3) keep(3) nogen
replace location_id = . if level==3
drop level age_group_id

**
capture merge 1:m iso3 age location_id year using `matdata', assert(1 3) keep(3) nogen
if _rc {
	global source = source in 1
	capture mkdir "/home/j/WORK/03_cod/01_database/03_datasets/$source/explore"
	save "/home/j/WORK/03_cod/01_database/03_datasets/$source/explore/mathiv_merge_fail.dta", replace
}

gen pct_maternal = 1-pct_hiv-pct_maternal_hiv
replace pct_maternal = 1 if pct_maternal==.
replace pct_hiv = 0 if pct_hiv==.
replace pct_maternal_hiv = 0 if pct_maternal_hiv==.
count if pct_maternal <0
assert `r(N)'==0
count if pct_maternal + pct_hiv + pct_maternal_hiv==.
assert `r(N)'==0
count if abs(pct_maternal + pct_hiv + pct_maternal_hiv)-1>.0001
assert `r(N)'==0
** cannot be above 13%; otherwise this would suggest the percentage of maternal deaths that were hiv positive is >1
count if pct_maternal_hiv_vr>.13
assert `r(N)'==0
** there shouldn't be maternal_hiv before this step
count if acause=="maternal_hiv"
assert `r(N)'==0

** Now determine which percentages to use
** Sib histories, Census, and Survey should only have maternal parent; this parent should be split into maternal, maternal_hiv, and hiv; the hiv should be dropped and the maternal_hiv is a new cause
** All others should just take a percentage of all maternal & create maternal_hiv
gen split_maternal = 1 if source_type == "Survey" | source_type == "Census" | source_type == "Self Reported CoD" | source_type == "Sibling history, survey" | source_type == "Sibling history"

replace split_maternal = 0 if split_maternal==.
** keep all maternal parent in this case
replace pct_maternal = 1 if split_maternal==0
** this intentionally adds deaths to the sample size of vr/va, which should have hiv removed from its sample size; these deaths theoretically come from maternal deaths that are codes to hiv
replace pct_maternal_hiv = pct_maternal_hiv_vr if split_maternal==0
replace pct_hiv = 0 if split_maternal==0
** now drop pct_maternal_hiv_vr, which was only used for that replace
drop pct_maternal_hiv_vr

** save death totals for split_maternal observations
foreach stage in raw corr rd final {
	capture drop deaths_`stage'
	gen deaths_`stage' = cf_`stage'*sample_size
	su deaths_`stage' if split_maternal==1
	local `stage'_deaths = `r(sum)'
	drop deaths_`stage'
}

foreach stage in raw corr rd final {
	foreach acause in maternal hiv maternal_hiv {
		gen cf_`stage'`acause' = cf_`stage'*pct_`acause'
	}
	drop cf_`stage'
}

drop acause pct_*
reshape long cf_raw cf_corr cf_rd cf_final, i(iso3 location_id year age sex NID source_label subdiv) j(acause) string

local diff = 0
foreach stage in raw corr rd final {
	capture drop deaths_`stage'
	gen deaths_`stage' = cf_`stage'*sample_size
	su deaths_`stage' if split_maternal==1
	local `stage'_deaths_new = `r(sum)'
	drop deaths_`stage'
	di "OLD: ``stage'_deaths', NEW: ``stage'_deaths_new'"
	local diff = `diff'+ abs(``stage'_deaths'-``stage'_deaths_new')
}
** make sure deaths aren't being changed too much for split_maternal observations, should only be by floating point stuff
assert `diff'/`final_deaths'<.01 | `diff'/`final_deaths'==.

** drop the hiv deaths created from splitting
if `testing'==0 {
	drop if acause=="hiv"
}

tempfile adjmatdata
save `adjmatdata', replace

** now add maternal_hiv to maternal
** keep the maternal_hiv, split_maternal 0 observations and call them maternal
keep if split_maternal==0
count if !inlist(acause, "maternal", "maternal_hiv")
assert `r(N)'==0
drop if acause=="maternal"
replace acause="maternal"
** append them on to the original and collapse
append using `adjmatdata'
collapse (sum) cf_final cf_corr cf_rd cf_raw, by(iso3 region year age sex source source_label source_type national list subdiv NID location_id sample_size acause split_maternal) fast
save `adjmatdata', replace

use `data', clear
drop if acause=="maternal" & age>=`age_start' & age<=`age_end' & sex==2 & year>=1980

append using `adjmatdata'
if `testing'==0 {
	** make sure what was an id before is an id now
	isid iso3 region year age sex source source_label source_type national list subdiv NID location_id sample_size acause, missok
	drop split_maternal
}

if `testing'==1 {
	** just remove the hiv portion
	gen deaths_final = cf_final*sample_size
	count if split_maternal==0
	if `r(N)'>0 {
		di "ADDING MATERNAL HIV DEATHS"
		table year acause if split_maternal==0 & regexm(acause, "maternal"), c(sum deaths_final)
	}
	count if split_maternal==1
	if `r(N)'>0 {
		di "MATERNAL SPLIT PROPORTIONS"
		table year acause if split_maternal==1 & (regexm(acause, "maternal") | acause=="hiv"), c(sum deaths_final)
	}
}
}
