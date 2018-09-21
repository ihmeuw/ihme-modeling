
clear all 
set more off
set maxvar 30000
version 13.0

// priming the working environment
if c(os) == "Windows" {
	local j /*FILEPATH*/
	// Load the PDF appending application
}
if c(os) == "Unix" {
	local j /*FILEPATH*/
} 

// arguments
local acause `1'

di in red "`acause' is acause is `acause'"

	// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"

// directories
local data_dir /*FILEPATH*/
local out_dir /*FILEPATH*/

// functions
adopath + /*FILEPATH*/
run /*FILEPATH*/
run /*FILEPATH*/
*************************************************************

import delimited /*FILEPATH*/, clear
keep if acause == "`acause'" & (standard_grouping == "mild_prop" | standard_grouping == "modsev_prop")

levelsof modelable_entity_id, local(me_id_list)
levelsof bundle_id, local(bundle_id_list)
levelsof ref_me_id, local(re_me_id)


di "getting severity proprotion me_ids"
// me_id 1560 is reference
if "`acause'" == "neonatal_preterm" {
	local me_id_list 1560 1561 1562 1565 1566 1567
	local bundle_id_list 83 84 85 86 87 88
	local ref_me_id 1560
}
// me_id 1584 is reference
if "`acause'" == "neonatal_enceph" {
	local me_id_list 1581 1584
	local bundle_id_list 90 91
	local ref_me_id 1584 
}

di "generating dummy local"
local dummy_me_id_list: list me_id_list - ref_me_id

local me_id_count: word count `me_id_list'

di in red "`dummy_me_id_list'"
di in red "`me_id_count'"

di "first, make an empty template which we will fill with predictions."
di "getting ln_NMR"
use /*FILEPATH*/, clear
gen year_round = floor(year)
drop year
rename year_round year
drop if year <1970

keep region_name location_name ihme_loc_id sex year q_nn_med
gen NMR = q_nn_med * 1000
gen ln_NMR=log(NMR)
keep if sex == "both"
replace sex = "Both" if sex == "both"

di "merging on location_id"
tempfile NMR
save `NMR', replace

//bring in converter file
import delimited /*FILEPATH*/, clear
tempfile converter
save `converter', replace

di "getting location metadata"
get_location_metadata, location_set_id(9) gbd_round_id(4) clear
drop ihme_loc_id
merge 1:1 location_id using `converter'
keep if _merge == 3
drop _merge
merge 1:m ihme_loc_id using `NMR', force
keep if _merge == 3
drop _merge

di "generating dummy vars and me_id var"
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
	if !_rc {
              di in red "modelable_entity_id already exists as column in dataset. We drop this for now."
              drop modelable_entity_id

               }
    else {
               di in red "modelable_entity does not exist as column in dataset, create the column"
                       
               }

    gen modelable_entity_id = 0

    //  This code here creates a new column for bundle_id
    preserve
    import delimited /*FILEPATH*/, clear
    keep if bundle_id == `bundle_id'
    local current_me = modelable_entity[1]
    restore

    replace modelable_entity_id = `current_me'


	append using `data_`acause'', force
	save `data_`acause'', replace 

}

drop if is_outlier == 1

di "formatting regression data for merge"
gen year = floor((year_start+year_end)/2)
save `data_`acause'', replace

di "merging data to template"
merge m:1 modelable_entity_id location_id year sex using `template_`acause'', force

di "final formatting"
rename mean proportion 
gen log_proportion = log(proportion)

di "regressing"
regress log_proportion ln_NMR dummy*
predict est_proportion
predict proportion_se, stdp
keep location_id sex year est_proportion proportion_se modelable_entity_id
save `data_`acause'', replace

di "generate draws and save"
foreach me_id of local me_id_list {
	di "getting data for saving"
	import delimited /*FILEPATH*/, clear
	keep if modelable_entity_id == `me_id'
	local grouping = grouping

	use `data_`acause'', clear
	di "Me_id is `me_id'"
	keep if modelable_entity_id == `me_id'
	drop modelable_entity_id
	di "generating draws for `me_id'"
	forvalues x = 0/999 {
		gen draw_`x' = rnormal(est_proportion, proportion_se)
		replace draw_`x' = exp(draw_`x')
	}
	drop est_proportion proportion_se
	drop sex
	expand 2, generate(sex)
	replace sex = 2 if sex == 0

	di "collapse to one est per me_id-location-year-sex"
	collapse (mean) draw*, by(location_id year sex) 

	di "saving"
	export delimited /*FILEPATH*/, replace
	save /*FILEPATH*/, replace
}


