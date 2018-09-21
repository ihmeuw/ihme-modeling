** *************************************************************************
** Example of code to reformat survey data in order to run analysis on sibling data
** *************************************************************************
clear
clear matrix
pause on
set more off

// list of variables created from each survey
// svy: identify the survey, country, and year
// id: unique identifier for each family of siblings
// sibid: unique identifier for each sibling in a family, sibid = 0 is respondent
// v021: PSU
// v005: sampling weight
// v008: cmc date of interview
// yr_interview: 4 digit year of interview
// sex: sex of sibling 0=female 1=male
// yob: 4 digit yob of sibling
// yod: 4 digit yod of sibling
// alive: 0=dead 1=alive

// Every observation should be a unique sibling

** *****************************************************************
** Example using the 2011-2012 Laos Multiple Indicator Cluster Survey (UNICEF MICS)
use "strLaosInputDataFile.dta", clear 

gen svy = "MICS_LAO_2011_2012"

// Generate unique person ID
tostring HH1, replace
tostring HH2, replace
tostring ln, replace
gen id = HH1 + " " + HH2 + " " + ln

rename mmln sibid // sibling id

// create indicators for sibling's sex and alive status
rename MM6 alive
rename MM5 sex
replace alive = 0 if alive == 2 // No is coded as 2 originally
replace sex = 0 if sex == 2

// Year interviewed
gen year = 1900 + int((wdoi - 1) / 12)
rename wdoi v008

rename wmweight v005
rename HH1 v021 

// Convert birth and death dates to years of birth/death
gen yob = 1900 + int((MM7C - 1)/12)
gen yod = 1900 + int((MM8C - 1)/12)
gen resp_yob = 1900 + int((wdob - 1)/12)

// Generate an observation for the respondent
expand 2 if sibid == 1, gen(new)
replace yob = resp_yob if new == 1 
replace sex = 0 if new == 1
replace yod = . if new == 1
replace alive = 1 if new == 1
replace sibid = 0 if new == 1
drop new

// correct some implausible years of birth
replace yob = . if (yr_interview - yob) > (yr_interview - resp_yob + 35) // We set the maximum allowable bound for ages to be 35 years' age gap between the two
replace yob = . if (yr_interview - yob) < (yr_interview - resp_yob - 35) // Also if the sibling is 35 years younger than the interviewee

// generate stratum variable
decode HH6, gen(new1)
decode HH7, gen(new2)
gen stratum = new1 + new2
encode stratum, gen(strata)

// keep only needed variables
keep svy id sibid v021 v005 v008 yr_interview sex yob yod alive
label drop _all
destring v021, replace

save "$datadir/allsibhistories.dta", replace
