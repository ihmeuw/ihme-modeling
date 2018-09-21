//// Create cholera files for draws runs, launched by 'launch_draws.do' on cluster ////
// This file is performed 1000 times as it produces a single draw for 
// cholera fatal and non-fatal PAFs which are then saved centrally for DALYnator //

// Note that for public use, the internal IHME filepaths have been replaced with 'FILEPATH' //
// Thanks for reading! //

clear all
set more off

local iter `1'

cap log close

//// Prepare necessary covariates, location data ////
// Get countries //
qui do "FILEPATH/get_location_metadata.ado"
//get_location_metadata, location_set_id(9) clear
use "FILEPATH/ihme_loc_metadata_2016.dta", clear
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

use "FILEPATH/population_data.dta", clear
keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
tempfile pop
save `pop'

local iter2 = `iter' - 1
use age_group_id sex_id location_id measure_id year_id draw_`iter2' if measure_id==6 using "FILEPATH/inc_prev_draws.dta", clear
gen measure = "incidence"
rename draw_`iter2' meanincidence
merge 1:1 age_group_id year_id sex_id location_id using `pop', keep(3) nogen
tempfile inc
save `inc'

use age_group_id sex_id location_id measure_id year_id draw_`iter2' if measure_id==5 using "FILEPATH/inc_prev_draws.dta", clear
gen measure = "prevalence"
rename draw_`iter2' meanprevalence
merge 1:1 age_group_id sex_id location_id year_id using `inc', nogen keep(3)
keep meanincidence meanprevalence pop location_id age_group_id sex_id year_id

reshape wide meanincidence meanprevalence pop,i( location_id age_group_id sex_id ) j(year_id)

cap foreach v in mean* pop {
		forval i = 1990/2016 {
			cap gen `v'`i' = .
		}
}
aorder
order location_id age_group_id sex_id  mean*  pop*
reshape long meanincidence meanprevalence pop,i( location_id age_group_id sex_id ) j( year_id)

foreach var of varlist meanincidence meanprevalence pop {
		gen ln`var' = ln(`var')
		by location_id age_group_id sex_id, sort : ipolate ln`var' year_id , generate(int_`var') epolate
		replace `var' = exp(int_`var') if `var' == .
		drop ln`var' int_`var'
}


merge m:1 location_id using `countries', keep(3) nogen
drop if super_region_id == .
drop if region_id == .
tempfile prv_pop
save `prv_pop'

/// Load age pattern from DisMod proportion model at global level

use age_group_id sex_id draw_`iter2' using "FILEPATHs/age_pattern_draws.dta", clear
keep if sex_id==2
merge m:1 age_group_id using "FILEPATH/age_mapping.dta", nogen keep(3)
rename draw_`iter2'  shp
tempfile shape
save `shape'

use `prv_pop'

merge m:1 age_group_id using `shape', nogen

replace shp = shp
tempfile prv
save `prv'

/// Load Odds Ratios from GEMS, apply to Cholera Data ///
use modelable_entity_id age_group_id odds_`iter' using "FILEPATH/odds_ratios_gbd_2016.dta", clear
keep if modelable_entity_id == 1182
merge m:1 age_group_id using "FILEPATH/age_mapping.dta", nogen keep(3)
tempfile coefs
save `coefs'
gen pf = 1-1/odds_`iter'
replace pf = 0 if pf<0
mkmat pf, matrix(coefs) rownames(age_group_id)
mat list coefs

/// Import Proportion data from literature ///
use nid-sample_size extractor is_outlier cv_inpatient cv_community draw_`iter' using "FILEPATH/latest_cholera_data.dta", clear
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

clear
tempfile loop
save `loop', emptyok

qui foreach o in `order' {
	use `proportions', clear
	keep if order == `o'
	qui mean shp [pw=prv] 
	gen prop = _b[shp]
	gen correction = coef_corr * meas_value / prop
	gen new_cases = correction * meanincidence * pop * shp
	bysort year_id: egen expected = total(new_cases)
	replace predo = expected
	append using `loop'
	save `loop', replace
}

/// Generate predicted, expected cases, model under-reporting to WHO ///
		
	// Case notifications don't have subnationals! 	
		replace location_id = 161 if regex(location_name, "Urban")
		replace location_id = 161 if regex(location_name, "Rural")
		tostring location_id, generate(str_id)
		gen first_digit = substr(str_id, 1, 2)
		replace location_id = 180 if first_digit== "35"

		merge m:1 location_id year_id using "FILEPATH/case_notifications.dta", nogen keep(1 3)
	keep if year_id >= 1990
		format %28s location_name
		duplicates drop nid year_id location_id cases, force	
		gen under_reportpct = cases *100/ expected
		drop prop
	
		merge m:1 location_id using `countries', keep(3) nogen
		tempfile main
		save `main'
		
		merge m:1 location_id year_id using `hs', nogen

// NOTE: 'multi' is the 'multiplier' 
		gen multi = under_reportpct/100
		bysort location_id: egen maxyear= max(year_id) 
		replace multi = 0.99 if super_region_name == "High-income" & under_report == . & year > 2000 | multi > 0.99 & multi != . 
					
		gen lg = logit(multi)
		gen lnldi = ln(mean_ldi)
	
// None of the predictors are significant, just use random effects //	
	mixed lg if cases != . || region_name: || location_name:
	predict p*, reffect
	bysort super_region_name: egen s_re = mean(p1)
	replace s_re=0 if s_re==.
	bysort region_name: egen r_re = mean(p2)
	replace r_re=0 if r_re == .
		
	gen prp = _b[_cons] + s_re + r_re
	gen predict = invlogit(prp)
	
	drop r_re s_re //g_re g_yre  re
	tempfile main
	save `main'
		
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
		gen lncases = ln(cases)
		gen true_cases = cases / predict 
		rename case inc
		gen prop = true_cases / inc
		keep if year_id >=1990

		gen lnprop = logit(prop)
		gen lnsev = ln(mean_sev)
		
// Limit regression to proportion values that are likely //
		regress lnprop lnsev if prop < 0.05 & prop > 0.0001
		gen finalprop = invlogit(_b[_cons] + _b[lnsev]*lnsev)
		gen overall = finalprop * inc
		table year_id, contents(sum overall)
		drop mean_ldi mean_hsa
	tempfile master
	save `master'

/// Apply age pattern again from DisMod ///

		use age_group_id sex_id draw_`iter2' using "FILEPATH/age_pattern_draws.dta", clear

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

use `age_draws', clear
	bysort sex_id: gen merge_dummy = _n
	egen total = total(draw_`iter2')
	gen shp = draw_`iter2' / total

// Add fatal discontinuities (shocks) for cholera //
	merge 1:m merge_dummy sex_id using "FILEPATH/shock_deaths.dta", nogen
	gen age_shock_deaths = shock_deaths * shp
tempfile shocks
save `shocks'
	
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
		keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
		
tempfile morbidity
save `morbidity', replace

/// Morbidity has been saved, now import Case Fatality ///

use age_group_id location_id year_id sex_id draw_`iter2' using "FILEPATH/death_draws.dta", clear

	rename draw_`iter2' d_diarrhea
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
tempfile mortality
save `mortality'

use age_group_id location_id year_id sex_id cf_`iter' using "FILEPATH/cholera_cf_draws.dta", clear
	merge m:m age_group_id location_id year_id sex_id using `mortality', keep(3)
	cap drop _m
	merge m:m location_id year_id sex_id age_group_id using `morbidity'
	merge m:1 ihme_loc_id year_id sex_id age_group_id using `shocks', nogen
	replace age_shock_deaths = 0 if age_shock_deaths == .
	rename mean diarr_inc
	replace finalprop = 1 if finalprop>1
	sort location_name year_id sex_id age_group_id
	gen n_diarrhea = diarr_inc * pop

	gen s_ndeaths = (1-exp(-diarr_inc* cf_`iter' * newprop)) * pop 
	replace s_ndeaths = s_ndeaths + age_shock_deaths

	keep if level>=3
	keep if is_estimate == 1 & most_detailed == 1
	
	gen death_fr = 	s_ndeaths / d_diarrhea
	replace death_fr = 0 if death_fr < 0
	replace death_fr = 1 if death_fr > 1
	
	gen deaths_total = death_fr*d_diarrhea
	
	table region_name year_id if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016), c(sum n_diarrhea sum d_diarrhea sum s_ncases  sum deaths_total) row
		
	qui replace newprop = 0 if newprop <0
	qui replace newprop = 1 if newprop >1
	gen mortality_`iter' = death_fr
	gen morbidity_`iter' = newprop
	gen deaths_`iter' = deaths_total

	keep if most_detailed == 1
	keep age_group_id year_id sex_id location_id morbidity* mortality* s_ncases s_ndeaths
	sort location_id year_id sex_id age_group_id 
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)

saveold "FILEPATH/draws_`iter'", replace
log close

// Finished!! Yay! //

