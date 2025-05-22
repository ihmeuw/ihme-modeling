/*******************************************************
** Created: 7 June 2016
 
******************************************************/

clear all 
set more off
set maxvar 30000
version 13.0
 
if c(os) == "Windows" {
	local j FILEPATH
 
}
if c(os) == "Unix" {
	local j FILEPATH
} 

// arguments
local acause `1'
  
local data_dir FILEPATH  
local out_dir  FILEPATH

// functions
adopath + FILEPATH
run FILEPATH
run FILEPATH
*************************************************************

import delimited FILEPATH, clear
keep if acause == "`acause'" & (standard_grouping == "mild_prop" | standard_grouping == "modsev_prop")

levelsof modelable_entity_id, local(me_id_list)
levelsof bundle_id, local(bundle_id_list)
levelsof ref_me_id, local(re_me_id)

 
if "`acause'" == "neonatal_preterm" {
	local me_id_list 1560 1561 1562 1565 1566 1567
	local bundle_id_list 83 84 85 86 87 88
	local ref_me_id 1560
}
 
if "`acause'" == "neonatal_enceph" {
	local me_id_list 1581 1584
	local bundle_id_list 90 91
	local ref_me_id 1584 
}

 
local dummy_me_id_list: list me_id_list - ref_me_id

local me_id_count: word count `me_id_list'
 

use FILEPATH, clear
 
 
keep location_id region_name location_name ihme_loc_id sex year q_nn_med
 

gen year_round = floor(year)

drop year

rename year_round year

drop if year <1970

keep region_name location_id location_name ihme_loc_id sex year q_nn_med

gen NMR = q_nn_med * 1000
gen ln_NMR=log(NMR)
keep if sex == "both"
replace sex = "Both" if sex == "both"

 
tempfile NMR
save `NMR', replace
 
 
expand `me_id_count'
bysort location_id year: gen id = _n
local x 1
gen modelable_entity_id = . 
foreach me_id of local dummy_me_id_list {
	di "Creating dummy for me_id `me_id'"
	gen dummy_`me_id' = 0
	replace dummy_`me_id' = 1 if id == `x'
	replace modelable_entity_id = `me_id' if id == `x'
	local x = `x' + 1
}

replace modelable_entity_id = `ref_me_id' if modelable_entity_id == . 

di "saving template"
tempfile template_`acause'
save `template_`acause'', replace 

di "getting data"
clear
tempfile data_`acause'
save `data_`acause'', emptyok
 

foreach bundle_id of local bundle_id_list{
	di "Acause is `acause' and Bundle_id is `bundle_id'"

	get_epi_data, bundle_id(`bundle_id') clear
	capture confirm variable modelable_entity_id
	 
    gen modelable_entity_id = 0


    preserve
    import delimited FILEPATH, clear
    keep if bundle_id == `bundle_id'
    local current_me = modelable_entity[1]
    restore

    replace modelable_entity_id = `current_me'


	append using `data_`acause'', force
	save `data_`acause'', replace 

}

drop if is_outlier == 1
 
gen year = floor((year_start+year_end)/2)
save `data_`acause'', replace


merge m:1 modelable_entity_id location_id year sex using `template_`acause'', force


rename mean proportion 
gen log_proportion = log(proportion)


regress log_proportion ln_NMR dummy*
predict est_proportion
predict proportion_se, stdp
keep location_id sex year est_proportion proportion_se modelable_entity_id
save `data_`acause'', replace


foreach me_id of local me_id_list {

	import delimited FILEPATH, clear
	keep if modelable_entity_id == `me_id'
	local grouping = grouping

	use `data_`acause'', clear

	keep if modelable_entity_id == `me_id'
	drop modelable_entity_id

	forvalues x = 0/999 {
		gen draw_`x' = rnormal(est_proportion, proportion_se)
		replace draw_`x' = exp(draw_`x')
	}
	drop est_proportion proportion_se
	drop sex
	expand 2, generate(sex)
	replace sex = 2 if sex == 0

 
	collapse (mean) draw*, by(location_id year sex) 


	export delimited "FILEPATH`acause'_`grouping'_draws.csv", replace
	save "FILEPATH/`acause'_`grouping'_draws.dta", replace
}


