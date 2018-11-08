** Description: Fitting a logarithmic trend line to data from clinical studies


clear all 
set more off

// compute age_standardized incidence for both sexes combined

use "FILEPATH", clear

drop if inlist(age_group_id,22,27)

keep if measure_id==6

// merge on population
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3) nogen

gen cases=mean*population

collapse (sum) cases population, by (location_id year_id age_group_id) fast

gen sex_id=3

gen mean=cases/population

// merge on age weights
merge m:1 age_group_id using "FILEPATH", keep(3)nogen

// compute age-standardized rates
gen age_std_inc=mean*weight

collapse (sum) age_std_inc, by (location_id year_id) fast

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

// merge on input data
merge 1:1 location_id year_id using "FILEPATH", keepusing(TB_prop) nogen

gen ln_tb_inc=ln(age_std_inc)

cap log close

log using "FILEPATH", replace

regress TB_prop ln_tb_inc

cap log close

predict pred_prop
predict se_prop, stdp


