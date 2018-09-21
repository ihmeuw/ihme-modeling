
** Description: compute XDR proportions

// get region names

use "FILEPATH", clear

keep ihme_loc_id region_name super_region_name

rename ihme_loc_id iso3

tempfile iso3

save `iso3', replace

// get MDR raw data
insheet using "FILEPATH", comma names clear

replace country="Czech Republic" if country=="Czechia"

replace country="Curaçao" if country=="CuraÁao"

// merge on region names
merge 1:1 iso3 using `iso3', keep(3)nogen

drop if mdr_dst==. & xdr==.

collapse (sum) mdr_dst xdr, by (super_region_name)

gen XDR_prop=xdr/mdr_dst

save "FILEPATH", replace


// convert XDR proportions to rates

// get SR level TB deaths
use "FILEPATH", clear

// merge on population
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen

// compute deaths for both sexes combined
collapse (sum) *_death population, by(location_id year_id age_group_id) fast

gen all_tb_death=tb_death+hiv_dr_sen_death+ hiv_mdr_death+ hiv_xdr_death

gen all_mdr_death=mdr_death+ xdr_death+ hiv_mdr_death+ hiv_xdr_death

// merge on location names
merge m:1 location_id using "FILEPATH", keepusing(location_name) keep(3)nogen

rename location_name super_region_name

// merge on XDR proportions
merge m:1 super_region_name using "FILEPATH", keep(3) nogen

gen all_xdr_death=xdr_death+hiv_xdr_death

gen xdr_prop_back_calc=all_xdr_death/all_mdr_death

gen xdr_rate=all_xdr_death/population

save "FILEPATH", replace


// extrapolation

keep if year_id==2016

keep year_id location_id super_region_name xdr_rate

expand 2, gen(new)

replace year_id=1992 if new==1

drop new

replace xdr_rate=0 if year==1992

// Prepare to project back 

expand 2 if year==1992, gen(exp)
replace year=1980 if exp==1
drop exp

// interpolate/extrapolate

egen panel = group(location_id super_region_name)
tsset panel year
tsfill, full

bysort panel: ipolate xdr_rate year, gen(xdr_rate_new) epolate

bysort panel: replace location_id = location_id[_n-1] if location_id==.

bysort panel: replace super_region_name = super_region_name[_n-1] if super_region_name==""

save "FILEPATH", replace


// convert rates back to proportions

// bring in XDR death rates before extrapolation
use "FILEPATH", clear
// merge on extrapolated XDR death rates
merge 1:1 location_id year_id using "FILEPATH", keepusing(xdr_rate_new) keep(3)nogen 
// compute new XDR deaths
gen all_xdr_death_new=xdr_rate_new*population
// compute new XDR proportions
gen xdr_prop_new=all_xdr_death_new/all_mdr_death

save "FILEPATH", replace

keep super_region_name year_id xdr_prop_new

rename xdr_prop_new XDR_prop

save "FILEPATH", replace

