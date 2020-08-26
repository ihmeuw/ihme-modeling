*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix FILEPATH
		set odbcmgr ADDRESS
		}
	else if c(os) == "Windows" {
		// Compatibility paths for Windows STATA GUI development
		global prefix FILEPATH
		local 2 FILEPATH
		local 3 "--draws"
		local 4 FILEPATH
		local 5 "--interms"
		local 6 FILEPATH
		local 7 "--logs"
		local 8 FILEPATH
		}

	// define locals from jobmon task
	local params_dir 		`2'
	local draws_dir 		`4'
	local interms_dir		`6'
	local logs_dir 			`8'

	cap log using "`logs_dir'/FILEPATH", replace
	if !_rc local close_log 1
	else local close_log 0

	di "`params_dir'"
	di "`draws_dir'"
	di "`interms_dir'"
	di "`logs_dir'"
	
	// Load shared functions
	adopath + FILEPATH
*** ======================= MAIN EXECUTION ======================= ***

*-----------------Merge the following bundles------------------------*
*Case data
*Population at risk
*Screening coverage


**************Step 1. Generate dataset to run analysis****************
tempfile master
*Step A. Create file "Data2Model2019" ********************

*pull in case totals from Bundle ID ADDRESS1
insheet using "`params_dir'/FILEPATH", clear

rename year_end year_id
drop if value_detail==""

*ensure 1 row per country (applies to UGA only)
sort location_id year_id value_detail
order location_id year_id location_name value_detail cases, last
gen lag_species = cases[_n-1]
gen new_total=cases
replace new_total=cases+lag_species if (location_id==190 & year_id>1977)
replace new_total=cases+lag_species if location_id==190 & year_id==1977 & value_detail=="reported_rhodesiense"
drop if location_id==190 & value_detail=="reported_gambiense"
save "`interms_dir'/FILEPATH", replace

insheet using "`params_dir'/FILEPATH", clear
generate year_id=year_end

save "`interms_dir'/FILEPATH", replace
use "`interms_dir'/FILEPATH"


*pull in population at risk estimates from Bundle ID ADDRESS2
merge 1:1 location_id year_id using "`interms_dir'/FILEPATH", force

*not all years have population at risk estimates, so apply 2% growth from prior year
*adding 2018
expand 2 if year_id==2017, gen(new)
replace year_id=2018 if new==1
drop new

*adding 2019
expand 2 if year_id==2018, gen(new)
replace year_id=2019 if new==1

sort location_id year_id
gen lag1 = value_ppl_risk[_n-1]
*2016 population at risk is a 2% increase from 2015 estimate
replace value_ppl_risk=1.02*(lag1) if year_id==2016
gen lag2 = value_ppl_risk[_n-1]
*2017 population at risk is a 2% increase from 2016 estimate
replace value_ppl_risk=1.02*(lag2) if year_id==2017
gen lag3 = value_ppl_risk[_n-1]
*2018 population at risk is a 2% increase from 2016 estimate
replace value_ppl_risk=1.02*(lag3) if year_id==2018
gen lag4 = value_ppl_risk[_n-1]
*2019 population at risk is a 2% increase from 2018 estimate
replace value_ppl_risk=1.02*(lag4) if year_id==2019
drop lag4 lag3 lag2 lag1 

*no data reported yet for 2018 and 2019, so drop data
replace cases=. if year_id==2018
replace cases=. if year_id==2019

drop _merge

*merge in screening coverage
save "`interms_dir'/FILEPATH", replace
insheet using "`params_dir'/FILEPATH", clear
gen year_id=year_end
save "`interms_dir'/FILEPATH", replace
use "`interms_dir'/FILEPATH"
merge 1:1 location_id year_id using "`interms_dir'/FILEPATH", force

*drop obs with screening coverage but no case data
drop if _merge==2
drop measure is_outlier source_type nid seq bundle_id underlying_nid  
drop age_start year_start age_end sample_size representative_name sex field_citation_value
drop _merge

generate reported_tgb=cases if value_detail=="reported_gambiense" & location_id!=190
generate reported_tgr=cases if value_detail=="reported_rhodesiense" & location_id!=190

replace reported_tgb=lag_species  if location_id==190
replace reported_tgr=cases if location_id==190

replace reported_tgb=0 if reported_tgb==. & year_id!=2018 & year_id!=2019
replace reported_tgr=0 if reported_tgr==. & year_id!=2018 & year_id!=2019

generate total_reported=reported_tgb+reported_tgr

generate incidence_risk=total_reported/value_ppl_risk
generate ln_inc_risk=ln(incidence_risk)

drop value_detail mean upper lower cases new_total lag_species new 

expand 2 if location_id==184 & year_id==2015, gen(MOZ)
expand 2 if location_id==217 & year_id==2015, gen(SLE)

replace year_id=2016 if (MOZ==1 | SLE==1)

drop MOZ SLE

expand 2 if location_id==184 & year_id==2016, gen(MOZ)
expand 2 if location_id==217 & year_id==2016, gen(SLE)

replace year_id=2017 if (MOZ==1 | SLE==1)

drop MOZ SLE

expand 2 if location_id==184 & year_id==2017, gen(MOZ)
expand 2 if location_id==217 & year_id==2017, gen(SLE)

replace year_id=2018 if (MOZ==1 | SLE==1)

drop MOZ SLE

expand 2 if location_id==184 & year_id==2018, gen(MOZ)
expand 2 if location_id==217 & year_id==2018, gen(SLE)

replace year_id=2019 if (MOZ==1 | SLE==1)

drop MOZ SLE


*replacing NGA with Delta State per new subnationals in GBD 2019 (all NGA cases occur in Delta state)
replace location_id=25327 if location_id==214
replace location_name="Delta" if location_id==25327

**variables we need:
*cause_id
*ihme_loc_id
*year_id
*location_name
*ppl_risk
*total_reported
*who_estimate_risk--not required for the model
*ppl_screened
*WHO_estimate_risk
*reported_tgb
*reported_tgr
*incidence_risk
*ln_inc_risk
*coverage
*ln_coverage
*location_id
*parent_id
*region_id
*region_name

*this file data2Model will be fed into main analysis. 

save `master'

get_location_metadata, location_set_id(35) clear
keep location_id region_name region_id ihme_loc_id parent_id

merge 1:m location_id using `master', assert(1 3) keep(3) nogenerate

save "`interms_dir'FILEPATH", replace

*** ======================= CLOSE LOG ======================= ***
if `close_log' log close

exit, STATA clear
