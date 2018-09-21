** This code corrects potential bias created by sibships where there are zero alive siblings (and therefore no one alive to be sampled).
** This is run for surveys where the respondents are all female.

clear
clear matrix
pause on
set mem 1g
set more off

// create tempfiles 
tempfile sisters weights missing1 missing final_missing proportions by_sibsize psu

// bring in the data from step 1.
use "$datadir/allsibhistories.dta", clear
levelsof svy, local(surveys)

quietly {
// loop through all of the surveys that were prepped together
foreach svy of local surveys {

	n di _newline in red "`svy'"
	use if svy == "`svy'" using "$datadir/allsibhistories.dta", clear
	
	// generate standard variables and correct any potential date errors
	gen iso3 = substr(svy,5,3)
	local iso3 = iso3[1]

	replace yob = . if yob < 0

	egen min_yr_interview = min(yr_interview)
	local yr_interview = min_yr_interview
	
	// count number of females in a sibship
	gen female = 1 if sex == 0
	bysort id: egen sibsize = total(female)

	// assign cohorts
	gen cohort = . 
	forvalues cohort = 0(5)85   {
		replace cohort = `cohort' if (yr_interview - yob) - `cohort' < 5 & (yr_interview - yob) - `cohort' >= 0
		}
    gen age_yr = yr_interview - yob
    
	// drop all sibships where the yob of the respondent is missing
	sort id sibid
	
	** ********************************************************************************
	// DECISION POINT: change the age range if the respondents have ages outside the typical 15-49 age range
	** ********************************************************************************
	by id: gen drop = 1 if yob[1]==. | age_yr[1]<15 | age_yr[1]>49
	drop if drop == 1
	lab define agecat 0 "0-4" 5 "5-9" 10 "10-14" 15 "15-19" 20 "20-24" 25 "25-29" 30 "30-34" 35 "35-39" 40 "40-44" 45 "45-49" 50 "50-54" 55 "55-59" 60 "60-64" 65 "65-69" 70 "70-74" 75 "75-79" 80 "80-84" 85 "85-89"
	lab val cohort agecat

	// get proportions of deaths in each time period and age group for assigning missing deaths later on
	preserve
	// for those that have died
	// assign periods of death
	gen period = .
	forvalues period = 0(5)85   {
		replace period =`period' if (yr_interview - yod) - `period' < 5 & (yr_interview - yod) - `period' >= 0
		}
	// assign age at death
	gen age_at_death = . 
	forvalues age = 0(5)85   {
		replace age_at_death = `age' if (yod - yob) - `age' < 5 & (yod - yob) - `age' >= 0
		}
	** ********************************************************************************
	// DECISION POINT: change the age range if the respondents have ages outside the typical 15-49 age range
	** ********************************************************************************
	contract age_at_death period cohort if alive == 0 & female == 1 & age_yr >= 15 & age_yr < 50
	
	bysort cohort: egen denom = total(_freq)
	gen prop = _freq / denom
	
	keep cohort period age_at_death prop
	save `proportions', replace
	restore
	
	// get proportions of sibs in each psu for assigning later on
	preserve
	contract v021
	count 
	if r(N) > 1 {
		egen denom = total(_freq)
		gen prop = _freq / denom
		gsort - v021
		gen cum_prop = prop if _n == 1
		replace cum_prop = prop + cum_prop[_n-1] if _n != 1
		gen n = 1
		keep n v021 cum_prop
		reshape wide cum_prop, i(n) j(v021)
		save `psu', replace
		}
		di "`r(N)'"
	else {
		gen n = 1
		gen cum_prop = 1
		drop _freq v021
		save `psu', replace
		pause
		}
	restore

	// only keep female siblings in sibships with 1 or 2 females
	keep if (sibsize == 2 | sibsize == 1) & sex == 0
	sort id sibid
	by id: gen sister = _n
	keep id sister cohort alive age_yr
	save `sisters', replace

	// get gk weighted age specific death rates
	drop if alive == .
	
	** ********************************************************************************
	// DECISION POINT: change the age range if the respondents have ages outside the typical 15-49 age range
	** ********************************************************************************
	gen eligible_alive = 1 if alive == 1 & age_yr >= 15 & age_yr < 50
	bysort id: egen si_eligible = total(eligible_alive)
	gen birth = 1 / si_eligible
	gen dead = 1 / si_eligible if alive == 0
	collapse (sum) dead birth, by(cohort)
	gen q = dead / birth
	keep cohort q dead birth
	save `weights', replace

	// loop through below code until convergence
	local i = 1
	while `i' < 1000 {
		use `sisters', clear
		merge m:1 cohort using `weights'
		drop _merge dead birth
		sort id cohort alive
		bysort id: replace sister = _n		// make it so the younger sibling is always first so that we don't get any repeats in sibship composition
		reshape wide cohort age_yr alive q, i(id) j(sister)
		contract alive* age_yr* cohort* q*

		** ********************************************************************************
		// DECISION POINT: change the age range if the respondents have ages outside the typical 15-49 age range
		** ********************************************************************************
		// if both sibs are within 15 - 49 age range
		gen miss1 = _freq / (1 - q1*q2) * q1*q2 if age_yr1 >= 15 & age_yr1 < 50 & age_yr2 >= 15 & age_yr2 < 50
		gen miss2 = _freq / (1 - q1*q2) * q1*q2 if age_yr1 >= 15 & age_yr1 < 50 & age_yr2 >= 15 & age_yr2 < 50

		// if one sib is outside 15 - 49 age range
		replace miss1 = (_freq / (1 - q1) * (q1 * q2)) + (_freq / (1 - q1) * (q1 * (1 - q2))) if age_yr2 < 15 | age_yr2 >= 50
		replace miss2 = (_freq / (1 - q2) * (q1 * q2)) + (_freq / (1 - q2) * (q2 * (1 - q1))) if age_yr1 < 15 | age_yr1 >= 50

		// if there is only one sib
		replace miss1 = _freq / (1 - q1) * q1 if alive2 == .

		// compare missing counts with last iteration
		keep cohort* miss*
		gen n = _n
		reshape long cohort miss, i(n) j(sister)
		rename miss miss`i'
		save `missing1', replace
		
		if `i' > 1 {
			local j = `i' - 1
			merge 1:1 n cohort sister using `missing'
			drop _merge
			egen total`i' = total(miss`i')
			egen total`j' = total(miss`j')
			gen total_diff = total`i' - total`j'
			n di in red total_diff
			if total_diff < 0.1 {		
				keep cohort sister n miss`i'
				save `final_missing', replace
				continue, break
				}
			drop total* total_diff
			}
		save `missing', replace
		use `missing1', clear
		
		// recalculate qx to update weights dataset
		collapse (sum) miss, by(cohort)
		merge 1:1 cohort using `weights'
		gen dead`i' = miss`i' + dead
		gen birth`i' = miss`i' + birth
		replace q = dead`i' / birth`i'
		keep cohort q dead birth
		save `weights', replace
		local i = `i' + 1
		}

	use `final_missing', clear
	drop if miss == .
	bysort n cohort: replace sister = _N
	collapse (sum) miss, by(cohort sister)
	
	// save dataset of missing sibs by sibsize, cohort and iso3
	preserve
	rename miss miss
	gen svy = "`svy'"
	capture append using `by_sibsize'
	save `by_sibsize', replace
	restore

	collapse (sum) miss, by(cohort)
	merge 1:m cohort using `proportions'
	keep if _merge == 3
	drop _merge
	gen missing = round(miss`i' * prop)
	gen yob = `yr_interview' - (cohort + 2)
	gen yod = floor(`yr_interview' - (period / 2) - ((cohort - age_at_death) / 2) - 1)
	expand missing
	keep yob yod
	gen alive = 0 
	gen sex = 0
	gen yr_interview = `yr_interview'
	gen surveyyear = yr_interview - 1
	gen svy = "`svy'"
	gen sibid = _n

	// assign primary sampling units
	gen n = 1 
	merge m:1 n using `psu', nogen
	gen v021 = ""
	gen rand = runiform()
	foreach var of varlist cum_prop* {
		replace v021 = substr("`var'",9,.) if rand <= `var'
		}
	
	destring v021, replace
	drop cum* n
	
	save "$datadir/`svy'_missing sibs.dta", replace
	}
	}

// format the misisng sibling files 
use "$datadir/allsibhistories.dta", clear
levelsof svy, local(surveys)
tempfile all_surveys
local i = 1

foreach svy of local surveys {
	use "$datadir/`svy'_missing sibs.dta", clear
	gen id = "`i'"
	replace id = id + string(sibid)	// make id_sm a unique identifier
	replace sibid = 0	// make the missing siblings like a respondent
	local i = `i' + 1
	capture append using `all_surveys'
	save `all_surveys', replace
	}
	
use `all_surveys', clear
gen iso3 = substr(svy,5,3)
save `all_surveys', replace

// merge with country names
use "$locdir/strLocationFileName.dta", clear
drop if (indic_epi != 1 & indic_cod != 1)
replace iso3 = gbd_country_iso3 + "_" + string(location_id) if gbd_country_iso3 != iso3 & gbd_country_iso3 != ""
duplicates drop
drop if iso3 == ""
keep iso3 location_name
rename location_name country

merge 1:m iso3 using `all_surveys', keep(3) nogen

save "$datadir/missing sibs.dta", replace

// look at the number of missing sibs by sibship size
use `by_sibsize', clear
save "$datadir/Missing sibs by iso3 cohort sibsize.dta", replace
collapse (sum) miss, by(sister svy)
reshape wide miss, i(svy) j(sister)
gen pct1 = miss1 / (miss1 + miss2) * 100
gen pct2 = miss2 / (miss1 + miss2) * 100



