//// Create cholera files for draws runs, launched by 'launch_draws.do' on cluster ////
// This file is performed 1000 times as it produces a single draw for 
// cholera fatal and non-fatal PAFs which are then saved centrally //
// That structure requires all the input draws to be pulled in advance. //

clear all
set more off

// Set J //
if c(os)=="Unix" global j "/FILEPATH"
else global j "/FILEPATH"
local project cholera_Draws


* Set up test run
 local testing "F"
 local save_intermed "F"

* Passing in argument from srun
if "`testing'" == "F"{
	local iter `1'
	local iter2 = `iter' - 1
	//local version `2'
}
else {
	local iter 1
	local iter2 0
	local version "2024-12-17"
}

local output_fp "FILEPATH/eti_diarrhea_cholera/by_draw"

cap log close

//// Prepare necessary covariates, location data ////
qui do "FILEPATH/get_population.ado"
// Get countries //
qui do "FILEPATH/get_location_metadata.ado"
get_location_metadata, location_set_id(35) release_id(16) clear

gen iso3 = ihme_loc_id
tempfile countries
save `countries'

// Get covariates // 
use "FILEPATH/covariates.dta", clear
tempfile hs
save `hs'

//// Start main process ////
cap log close
log using "FILEPATH/logfile_`iter'.smcl", replace

local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024

insheet using "FILEPATH/population_data.csv", clear
tempfile pop
save `pop'

// Merge population and Incidence, then Prevalence data //
use age_group_id sex_id location_id measure_id year_id draw_`iter2' if measure_id==6 using "/FILEPATH/gbd_2023_inc_prev_draws.dta", clear
	gen measure = "incidence"
	rename draw_`iter2' meanincidence
	merge 1:1 age_group_id year_id sex_id location_id using `pop', keep(3) nogen
	tempfile inc
	save `inc'

use age_group_id sex_id location_id measure_id year_id draw_`iter2' if measure_id==5 using "FILEPATH/gbd_2023_inc_prev_draws.dta", clear
	gen measure = "prevalence"
	rename draw_`iter2' meanprevalence
	merge 1:1 age_group_id sex_id location_id year_id using `inc', nogen keep(3)
	keep meanincidence meanprevalence pop location_id age_group_id sex_id year_id

// Reshape so that incidence and prevalence can be interpolated //
	reshape wide meanincidence meanprevalence pop,i( location_id age_group_id sex_id ) j(year_id)

	cap foreach v in mean* pop {
			forval i = 1990/2024 {
				cap gen `v'`i' = .
			}
	}
	aorder
	order location_id age_group_id sex_id  mean*  pop*
	reshape long meanincidence meanprevalence pop,i( location_id age_group_id sex_id ) j( year_id)

// Interpolate between years in log space //
	foreach var of varlist meanincidence meanprevalence pop {
			gen ln`var' = ln(`var')
			by location_id age_group_id sex_id, sort : ipolate ln`var' year_id , generate(int_`var') epolate
			replace `var' = exp(int_`var') if `var' == .
			drop ln`var' int_`var'
	}


merge m:1 location_id using `countries', keep(3) nogen

tempfile prv_pop
save `prv_pop'
sleep 60000 // sleep for 1 minute to let stata save prv_pop completely before moving on. Avoids "file is read only" error.

/// Load age pattern from DisMod proportion model at global level
	use age_group_id sex_id draw_`iter2' using "FILEPATH/age_pattern_draws.dta", clear
	keep if sex_id==2
	merge m:1 age_group_id using "/FILEPATH/age_mapping_current.dta", nogen keep(3)
	rename draw_`iter2'  shp
	egen total_shp = total(shp)
	tempfile shape
	save `shape'

	use `prv_pop'

	merge m:1 age_group_id using `shape', nogen

	replace shp = shp

tempfile prv
save `prv'
sleep 20000

/// Load Odds Ratios from GEMS, apply to Cholera Data ///
	use modelable_entity_id age_group_id odds_`iter' using "FILEPATH/odds_ratios_gbd_2016.dta", clear
	keep if modelable_entity_id == 1182
	merge m:1 age_group_id using "FILEPATH/age_mapping_current.dta", nogen keep(3)
	tempfile coefs
	save `coefs'
	gen pf = 1-1/odds_`iter'
	replace pf = 0 if pf<0
	mkmat pf, matrix(coefs) rownames(age_group_id)
	mat list coefs

/// Import Proportion data from literature ///
// The idea here is to make sure that a PAF is calculated for the proportion data 
// using odds ratios from GEMS. We also account for how frequently 
// cholera is detected in inpatient populations (more frequently than non-inpatient)
// and crosswalk data to the non-inpatient reference category. The 
// last step is to account for the age groups from the source data.
// Input data do not always cover the entire age range 0-99 years
// so we standardize each point by taking the proportion of all cholera
// cases represented by that age group. So there is an age curve for cholera,
// and, for example, a study covers 0-5 years. We determine the proportion
// of all cholera cases that occur in that age group and divide by that proportion.
// Then, we sum all cases for each location/year based on the PAF for expected cases.
	use nid-cv_inpatient draw_`iter' using "FILEPATH/latest_cholera_data.dta", clear

	keep if is_outlier==0
	keep if measure == "proportion"
	drop cases

	rename mean meas_value_old
	rename draw_`iter' meas_value
	replace meas_value = logit(meas_value)

	mixed meas_value cv_inpatient || location_name:
	replace meas_value = invlogit( meas_value + (0 - cv_inpatient)*_b[cv_inpatient])
	keep if meas_value != .

	gen order = _n
	gen n_years = year_end-year_start + 1
	expand n_years
	bysort order: gen year = year_start + _n - 1

	gen predo = .
	gen coef_corr = coefs[5,1]
	replace coef_corr = coefs[1,1] if age_end<=1
	gen year_id = year

	merge m:m location_id year_id using `prv', keep(3) nogen
	keep if age_upper >= age_start
	keep if age_lower <= age_end

	replace meanprevalence = 1*10^-9 if meanprevalence < 0
	gen prv = meanprevalence * pop
	tempfile proportions
	save `proportions'
	levelsof order, local(order)
sleep 10000
	clear
	tempfile loop
	save `loop', emptyok

	foreach o in `order' {
		use `proportions', clear
		keep if order == `o'
		di "`o'"
		if _N > 0 {
			qui mean shp [pw=prv] 
			gen prop = _b[shp]
			gen paf = coef_corr * meas_value // / prop
			gen fraction_cases = paf * shp / total_shp
			gen new_cases = paf * meanincidence * pop // * shp / total_shp
			bysort year_id: egen expected = total(new_cases)
			replace predo = expected
			append using `loop'
			save `loop', replace
		}
	}
		
	// Case notifications don't have subnationals! 	
	replace location_id = 161 if regex(location_name, "Urban")
	replace location_id = 161 if regex(location_name, "Rural")
	tostring location_id, generate(str_id)
	gen first_digit = substr(str_id, 1, 2)
	replace location_id = 180 if first_digit== "35"

merge m:1 location_id year_id using "/FILEPATH/gbd_2020_case_notifications.dta", nogen force
	keep if year_id >= 1990
	format %28s location_name
	duplicates drop nid year_id location_id expected, force // expected, force	
	
	bysort nid year_id location_name: egen all_ages = total(expected)
	duplicates drop nid year_id location_id all_ages, force 
			
	gen under_reportpct = cases * 100 / expected
	drop prop

	merge m:1 location_id using `countries', keep(3) nogen
	tempfile main
	save `main'
	saveold "/FILEPATH/checkpoints/main.dta", replace
sleep 10000
	merge m:1 location_id year_id using `hs', nogen force

// NOTE: 'multi' is the 'multiplier' 
	gen multi = under_reportpct/100
	bysort location_id: egen maxyear= max(year_id)
	gen over_estimate = 0
	replace over_estimate = 1 if under_report > 100
	replace multi = 0.99 if super_region_name == "High-income" & under_report == . & year > 2000 | multi > 0.99 & multi != . 
				
	gen lg = logit(multi)
	gen lnsdi = ln(mean_sdi)
	
// Predict the under-reporting frequency of countries to the WHO //
	mixed lg if under_reportpct !=. || super_region_name: || location_name:
	predict p*, reffect
	/*
	bysort super_region_name: egen s_re = mean(p1)
	replace s_re=0 if s_re==.
	bysort region_name: egen r_re = mean(p2)
	replace r_re=0 if r_re == .
	*/
	replace p1 = 0 if p1 == .
	replace p2 = 0 if p2 == .
	gen prp = _b[_cons] + p1 + p2
	gen predict = invlogit(prp)

	cap drop p1 p2 r_re s_re //g_re g_yre  re
	tempfile main
	save `main'
	saveold "/FILEPATH/main2.dta", replace
sleep 10000
	use if pop != . using `prv_pop', clear

		rename meanincidence mean
		gen age = age_group_id
		drop *prevalence
		drop if sex_id==3
		tempfile pop
		save `pop', replace

		gen case = mean *pop
		collapse (sum) case pop,by(location_id year_id)
	merge 1:m location_id year_id using `main', nogen 
	drop region_name super_region_name
	merge m:1 location_id using `countries', keep(3) nogen
	
// Create a "true_count" dummy for where cases = reported cases to fix issue 
// with cases in high-income and in Latin America // 
	gen true_count = 0
	replace predict = 0.99 if super_region_name=="High-income"
	replace true_count = 1 if super_region_name=="High-income"
	replace true_count = 1 if super_region_name == "Latin America and Caribbean" & year_id < 1991
	replace true_count = 1 if super_region_name == "Latin America and Caribbean" & year_id > 1997 & year_id < 2010
	replace true_count = 1 if region_name == "Caribbean" & year_id < 2010 // per PAHO: 
	replace true_count = 1 if region_name == "Andean Latin America" & year_id >= 2010
	replace true_count = 1 if region_name == "Eastern Europe"
	replace true_count = 1 if region_name == "Central Europe"
	
	gen lncases = ln(cases)
	gen true_cases = cases / predict 
	rename case inc
	gen prop = true_cases / inc
	keep if year_id >=1990

	gen lnprop = logit(prop)
	/* gen lnsev = ln(mean_sev) */
		
		
// Limit regression to proportion values that are likely //
// Predict the overall proportion of diarrhea cases due to cholera //
		mixed lnprop lnsdi if prop < 0.025 || super_region_id: || region_id: || location_id: //& prop > 0.00001 
		predict ltr*,reffect
		gen l_re = ltr3
		bysort super_region_name: egen s_re = mean(ltr1) 
		replace s_re = 0 if s_re == .
		bysort region_name: egen r_re = mean(ltr2) 
		replace r_re = 0 if r_re == .
		replace l_re = 0 if l_re == .
		//replace ltr3 = 0 if ltr3 == .
		//replace ltr4 = 0 if ltr4 == .
		gen finalprop = invlogit(_b[_cons] + s_re + r_re + l_re + _b[lnsdi] * lnsdi)
			
		gen overall = finalprop * inc
		
// Fix for where cholera isn't endemic //	
		replace cases = 0 if true_count==1 & cases == .
		replace overall = cases if true_count==1
		
		table year_id if level==3, contents(sum overall)
		table region_name year_id if level==3 & inlist(year_id, 1990, 1995, 2000,2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024), contents(sum overall)
		
		cap drop mean_ldi mean_hsa mean_sdi
	tempfile master
	save `master'
sleep 10000
/// Apply age pattern again from DisMod ///

use age_group_id sex_id draw_`iter2' using "/FILEPATH/age_pattern_draws.dta", clear
		tempfile age_draws
		save `age_draws'
		
		keep if sex_id == 2
		drop sex_id
		rename draw_`iter2' shp
		replace shp = 1-shp
		merge 1:m age_group_id using `pop', nogen

		gen ncases = mean * pop * shp
	
		drop shp
		gen sex = "Both"
		replace sex = "Female" if sex_id==2
		replace sex = "Male" if sex_id==1
		keep mean sex ncases pop location_id year_id age_group_id

		reshape wide mean ncases pop,i(location_id year_id age_group_id) j(sex) string
		reshape wide mean* ncases* pop*,i(location_id year_id) j(age_group_id) 
		egen totalmean= rowtotal(mean*)
		egen totalncases= rowtotal(ncases*)
		egen totalpop= rowtotal(pop*)

		tempfile inc
		save `inc'
		saveold "/FILEPATH/inc.dta", replace
sleep 10000

use `master', clear

		cap drop _m
		drop if overall == .
		duplicates drop location_id year_id, force
	
	merge 1:1 location_id year_id using `inc', keep(1 3)
		foreach var of varlist ncases* {
				gen s_`var' = overall * `var' / totalncases
		}

		keep location_id year_id s_ncases* pop* mean* overall finalprop
		reshape long s_ncasesMale s_ncasesFemale ncasesMale ncasesFemale popMale popFemale meanMale meanFemale, i(location_id year_id) j(age_group_id) string
		rename pop oldpop
		
		reshape long s_ncases ncases pop mean, i(location_id year_id age_group_id) j(sex_id) string
		rename age_group_id age
		gen age_group_id = real(age)
		gen newprop = s_ncases/( mean* pop)
		keep if sex_id == "Female" | sex_id == "Male"
		rename sex_id sex
		gen sex_id = 1
		replace sex_id=2 if sex=="Female"
		merge m:1 location_id using `countries'
		cap drop _m
		sort location_id year_id sex_id age_group_id
		// keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
		
tempfile morbidity
save `morbidity', replace
sleep 10000
// Save checkpoint? //
save "/FILEPATH/morbidity.dta", replace

/////////////////////////////////////////////////////////////////////////
// Note that the code now "Expands" so that we have annual results //////
/////////////////////////////////////////////////////////////////////////

/// Morbidity has been saved, now import Case Fatality ///

 use age_group_id location_id measure_id year_id sex_id draw_`iter2' using "/FILEPATH/death_draws_codcorrect_v455.dta", clear
	keep if measure_id == 1
	drop measure_id
	tab year_id
	rename draw_`iter2' d_diarrhea
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015,  2019, 2020, 2021, 2022, 2023, 2024)
	table year_id if inlist(year_id, 1990, 2010,  2019, 2020, 2021, 2022, 2023, 2024), c(sum d_diarrhea) row
tempfile mortality
save `mortality'
saveold "/FILEPATH/mortality.dta", replace
sleep 10000

use age_group_id location_id year_id sex_id cf_`iter' using "/FILEPATH/cholera_cf_draws.dta", clear
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015,  2019, 2020, 2021, 2022, 2023, 2024)
	
	merge m:m age_group_id location_id year_id sex_id using `mortality', keep(3)
	cap drop _m
	merge m:m location_id year_id sex_id age_group_id using `morbidity'

	rename mean diarr_inc
	replace finalprop = 1 if finalprop>1
	sort location_name year_id sex_id age_group_id
	
	gen n_diarrhea = diarr_inc * pop
	gen s_ndeaths = (1-exp(-diarr_inc* cf_`iter' * newprop)) * pop 
//	replace s_ndeaths = s_ndeaths + age_shock_deaths
	keep if level>=3
	keep if is_estimate == 1 & most_detailed == 1
	
	gen death_fr = 	s_ndeaths / d_diarrhea
	replace death_fr = 0 if death_fr < 0
	replace death_fr = 1 if death_fr > 1
	
	gen deaths_total = death_fr*d_diarrhea
    save "/FILEPATH/full_inc_mort_merge.dta", replace

	table region_name year_id if inlist(year_id, 1990, 2010, 2019, 2020, 2021, 2022, 2023, 2024), c(sum n_diarrhea sum d_diarrhea sum s_ncases  sum s_ndeaths) row
		
	qui replace newprop = 0 if newprop <0
	qui replace newprop = 1 if newprop >1
	gen mortality_`iter' = death_fr
	gen morbidity_`iter' = newprop
	gen deaths_`iter' = deaths_total

	keep if most_detailed == 1
	keep age_group_id year_id sex_id location_id morbidity* mortality* s_ncases s_ndeaths 
	sort location_id year_id sex_id age_group_id 
	
	keep if inlist(year_id, 1990,1995,2000,2005,2010,2015,2019, 2020, 2021, 2022, 2023, 2024)


saveold "`output_fp'/draws_`iter'", replace

log close

