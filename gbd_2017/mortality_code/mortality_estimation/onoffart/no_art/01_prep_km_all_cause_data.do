// February 2014
// Prep NAME data extracted from HIV seroconversion studies w/o provision of ART for analysis

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


local gbd2013_dir "FILEPATH"
local gbd2015_dir "FILEPATH"
local out_dir "FILEPATH"

**************************************************************
** DATA FIXES
**************************************************************
insheet using "FILEPATH/km_data_7feb2014.csv", clear comma names

// If/when we add new data for GBD2015, add and append here, and drop extra columns not used between both processes

keep if include == 1

// TZA - 50% males, and the median age was 30 for both sexes, so there will be equal numbes in the 15-30 group as in the 30+ group
replace sc_number = round((0.5*390)/2) if pubmed_id == 18032939 & (age_start == 15 & age_end == 30) | (age_start == 30 & age_end == 100)

// THA Blood Donors
// Median age is 24 (assume same for both sexes), so we'll assign half of the females and half of the males to each age group
replace sc_number = round(0.613*150/2) if sex == 1 & ((age_start == 18 & age_end == 24) | (age_start == 25 & age_end == 56)) & pubmed_id == 18032938
replace sc_number = round((1-0.613)*150/2) if sex == 2 & ((age_start == 15 & age_end == 24) | (age_start == 25 & age_end == 52)) & pubmed_id == 18032938

// THA Female CSW has sex coded backwards
replace sex = 2 if pubmed_id == 10823759

// UGA-Rakai - pubmed ID is wrong
replace pubmed_id = 18032934 if pubmed_id == 18032935 & site == "Rakai"

// ZAF miners - will estimate age-specific sample size based on person years
replace sc_number = round(2015/11951*1950) if age_start == 15 & age_end == 24	& pubmed_id ==  17314525
replace sc_number = round(6786/11951*1950) if age_start == 25   & age_end == 34 & pubmed_id ==  17314525
replace sc_number = round(2667/11951*1950) if age_start == 35   & age_end == 44 & pubmed_id ==  17314525
replace sc_number = round(482/11951*1950)  if age_start == 45  & age_end == 100 & pubmed_id ==  17314525

// KEN Mombasa CSW study - for now drop because the time since seroconversions aren't exact even integers
drop if pubmed_id == 16586394

// KEN - Nairobi CSW study - need to provide age ranges and calendar year ranges
replace age_start = 15 if pubmed_id == 15319739
replace age_end = 100 if pubmed_id == 15319739
replace year_start = 1983 if pubmed_id == 15319739
replace year_end = 1993 if pubmed_id == 15319739
replace sex = 2 if pubmed_id == 15319739

**************************************************************
** VARIABLE CREATION AND FORMATTING
**************************************************************
// Super region
generate super = "HI" if cohort == "38 studies"
replace super = "LOW" if inlist(iso3, "UGA", "CIV", "KEN", "RWA", "TZA", "ZAF", "HTI", "PHL", "THA")

// Keep only 12 years of follow-up
drop if sc_end_yr > 12 & sc_end_yr != .

// Calendar year - for now don't worry about differences across calendar years
replace year_start = 1980 if year_start == 0
drop if cohort == "38 studies" & (year_end != 1996 | year_start != 1980)
	
// Sample sizes
bysort pubmed_id nid age_start age_end year_start year_end sex: egen num_study = max(sc_number)
order baseline num_study sc_number pubmed_id age_start age_end year_start year_end sex
drop if baseline == 1

// Mortality
replace param_value = 1-param_value if parameter == "survival"
rename param_value mort
generate logit_mort = logit(mort)

// Ages
// Linear - use midpoint age of group
generate midpoint_age = (age_start + age_end)/2
	
// Age groups
generate age_cat = 1 if midpoint_age > 15 & midpoint_age < 25
replace  age_cat = 2 if midpoint_age > 25 & midpoint_age < 35
replace  age_cat = 3 if midpoint_age > 35 & midpoint_age < 45
replace  age_cat = 4 if midpoint_age > 45 & midpoint_age != .
replace  age_cat = 0 if inlist(age_start, 0, 15, 18) & age_end == 100

drop if age_cat == . // people are too young
	
// Hi vs. low income
generate high_income = 1 if super == "HI"
replace high_income = 0 if super == "LOW"

// Year
rename sc_end_yr yr_since_sc

// Keep only relevant variables
keep logit_mort mort sex high_income age_start age_end midpoint_age age_cat yr_since_sc num_study pubmed_id nid iso3 year_start year_end

//formatting for new unaids 2017 data 
replace high_income = 1 if iso3 == "UNAIDs E/NA" 
* 96 percent male
replace sex = 1 if iso3 == "UNAIDs Asia"


**************************************************************
** SAVE FOR BACKGROUND MORTALITY ADJUSTMENT
**************************************************************
save "`out_dir'/cum_mort_all_cause.dta", replace

