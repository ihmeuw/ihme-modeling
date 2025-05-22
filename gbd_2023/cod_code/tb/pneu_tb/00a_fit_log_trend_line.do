** Description: Fitting a logarithmic trend line 

clear all
run "FILEPATH/get_model_results.ado"
get_model_results, gbd_team("epi") gbd_id(9806) measure_id(6) decomp_step(step1) clear

save "FILEPATH/tb_inc_376565.dta", replace


clear all 
set more off

// compute age_standardized incidence for both sexes combined

use "FILEPATH/tb_inc_376565.dta", clear

drop if inlist(age_group_id,22,27)

keep if measure_id==6

merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/population_decomp1.dta", keepusing(population) keep(3) nogen

gen cases=mean*population

collapse (sum) cases population, by (location_id year_id age_group_id) fast

gen sex_id=3

gen mean=cases/population

merge m:1 age_group_id using "FILEPATH/age_weights.dta", keep(3)nogen

gen age_std_inc=mean*weight

collapse (sum) age_std_inc, by (location_id year_id) fast


// Prepare to project back to 1980
expand 2 if year==1990, gen(exp)
replace year=1980 if exp==1
drop exp 

// interpolate

egen panel = group(location_id)
tsset panel year_id
tsfill, full

bysort panel: ipolate age_std_inc year, gen(age_std_inc_new) epolate


bysort panel: replace location_id = location_id[_n-1] if location_id==.

replace age_std_inc=age_std_inc_new if age_std_inc==.

merge 1:1 location_id year_id using "FILEPATH/child_tb_prop.dta", keepusing(tb_prop) nogen

gen ln_tb_inc=ln(age_std_inc)

cap log close

log using "FILEPATH/child_tb_lri_regress_GBD2019_rerun.smcl", replace
	regress tb_prop ln_tb_inc
cap log close


