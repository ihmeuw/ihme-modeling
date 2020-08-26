// NAME
// February 2014
// Prep pooled ALPHA network data, UNAIDS Weibull, and ZAF Miners cohort from Todd's paper so baseline mortality can be subtracted to achieve HIV relative mortality

**************************************************************
** SET UP
**************************************************************
clear all
set more off
cap log close
cap restore, not

if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}

// Filepaths
local alpha_path "FILEPATH"
local zaf_path "FILEPATH"

local extract_dir "$FILEPATH"
local raw_dir "FILEPATH"
local out_dir "FILEPATH"

**************************************************************
** BRING IN RAW DATA AND FORMAT FOR BACKGROUND MORTALITY SUBTRACTION
**************************************************************
// WEIBULL
insheet using "`raw_dir'/weibull.csv", clear comma names

reshape long prop_, i(time) j(age)
rename prop_ surv
rename time yr_since_sc

rename age age_start
generate age_end = 25 if age_start == 15
replace age_end = 35 if age_start == 25
replace age_end = 45 if age_start == 35
replace age_end = 100 if age_start == 45

generate age_cat = 1 if age_start == 15
replace age_cat = 2 if age_start == 25
replace age_cat = 3 if age_start == 35
replace age_cat = 4 if age_start == 45

generate mort = 1-surv
generate iso3 = "WEIBULL"
generate sex = 3
generate high_income = 0

generate pubmed_id = 1111
generate year_start = 1990
generate year_end = 2006

tempfile weibull
save `weibull', replace

// ALPHA NETWORK
insheet using "`alpha_path'", clear comma names
duplicates drop
reshape long time_ prop_, i(id merge) j(age)
drop merge
rename age age_start
generate age_end = 25 if age_start == 15
replace age_end = 35 if age_start == 25
replace age_end = 45 if age_start == 35
replace age_end = 100 if age_start == 45

generate age_cat = 1 if age_start == 15
replace age_cat = 2 if age_start == 25
replace age_cat = 3 if age_start == 35
replace age_cat = 4 if age_start == 45

rename prop_ surv
generate mort = 1-surv
rename time yr_since_sc
generate iso3 = "ALPHA"
generate sex = 3
generate high_income = 0

generate year_start = 1990
generate year_end = 2006

generate pubmed_id = 2222
tempfile ea
save `ea', replace

// MINERS COHORT
insheet using "`zaf_path'", clear comma names
duplicates drop
reshape long time_ prop_, i(id merge) j(age)
drop merge
rename age age_start
generate age_end = 25 if age_start == 15
replace age_end = 35 if age_start == 25
replace age_end = 45 if age_start == 35
replace age_end = 100 if age_start == 45

generate age_cat = 1 if age_start == 15
replace age_cat = 2 if age_start == 25
replace age_cat = 3 if age_start == 35
replace age_cat = 4 if age_start == 45

rename prop_ surv
generate mort = 1-surv
rename time yr_since_sc
generate iso3 = "ZAF"
generate sex = 3
generate high_income = 0

generate year_start = 1990
generate year_end = 2002

generate pubmed_id = 3333
tempfile miners
save `miners', replace

	
// COMBINE
append using `ea'
append using `weibull'	
	
drop if yr_since_sc == . | surv == .
drop if yr_since_sc < 1

// Convert floats to doubles
tostring yr_since_sc, replace force
destring yr_since_sc, replace
replace yr_since_sc = round(yr_since_sc, 0.01)

**************************************************************
** SPECIFIC FORMATTING TO MESH WITH BACKGROUND MORTALITY SUBTRACTION CODE
**************************************************************
// Align data to start at year 1
replace yr_since_sc = 1 if yr_since_sc == 1.05 & iso3 == "ZAF" & age_cat == 4
replace yr_since_sc = 1 if yr_since_sc == 1.04 & iso3 == "ALPHA" & inlist(age_cat, 1, 4)

drop id

// Survival can't increase with a time-step - we have noisy data.
sort iso3 age_start yr_since_sc
by iso3 age_start: replace surv = surv[_n-1] if surv > surv[_n-1]

// For mortality adjustment it's going to be easier to take 1-year intervals instead of the continuously extracted data
generate closest_int = round(yr_since_sc)
generate diff = yr_since_sc - closest_int
bysort iso3 age_start closest_int: egen min_diff = min(abs(diff))
generate to_keep = 1 if abs(diff) == min_diff
keep if to_keep == 1

duplicates drop iso3 age_start closest_int, force

drop closest_int diff min_diff to_keep

replace surv = 0.005 if surv == 0
replace mort = 1-surv

**************************************************************
** SAVE
**************************************************************
outsheet using "`out_dir'/weibull_alpha_zaf.csv", replace comma names
	
	

	