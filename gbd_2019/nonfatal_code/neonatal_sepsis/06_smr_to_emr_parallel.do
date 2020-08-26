******************************
** Purpose: Create cause and location-specific excess mortality data for DisMod to use to calculate long-term outcomes. 
** The EMR data is created by pulling in the location-specific all-cause mortality rate (mx) and calculating emr = mx * (smr - 1)
** 05_smr_to_emr.do submits this script in parallel for every location 
*********************************
** Inputs:
** - qsub args: location_id, in_dir, out_dir
** - mortality rate, using get_life_table shared function
** Outputs:
** - Location-specific EMR, based on mortality rate: "FILEPATH/`location_id'_emr.csv"
*********************************

clear all
set graphics off
set more off
set maxvar 32000
ssc install estout, replace 

** QSUB arguments
local location_id `1'
local in_dir `2'
local out_dir `3'

** Test arguments
*local location_id 44737
*ocal in_dir "FILEPATH"
*local out_dir "FILEPATH"

** Locals
local acause_list "neonatal_sepsis" 
*local acause_list " "neonatal_enceph" "
local j "FILEPATH"

** Functions
adopath + "FILEPATH"

** Display qsub arguments
di in red "QSUB arguments: location_id = `location_id', in_dir = `in_dir', out_dir = `out_dir'"


**********************
di "Pull in location-specific mortality rate (mx) with HIV and with shock"
get_life_table, with_shock(1) with_hiv(1) location_id(`location_id') life_table_parameter_id(1) gbd_round_id(6) decomp_step("step4") clear

** returns location_id, age_group_id, year_id, sex_id, mean, life_table_id, run_id
** only mean mortality rate is returned, no draws, so replicate 1000 means as "draws"
expand 1000
bysort age_group_id sex_id year_id mean: gen draw = -1 + _n
rename mean mx

** SMR values from literature data are divided into two age categories: "young" and "old"
** Mortality rates are grouped by GBD age groups. Assign GBD age groups under 20 years (age_group_id = 9) as "young" and GBD age group over 20 years as "old"
gen age = ""
replace age = "young" if age_group_id < 9 | age_group_id == 28
replace age = "old" if age == ""

keep if year_id == 1990 | year_id == 1995 | year_id == 2000 | year_id == 2005 | year_id == 2010 | year_id == 2015 | year_id == 2017 | year_id == 2019
keep age age_group_id location_id year_id sex_id draw mx 

** Reshape from long to wide
reshape wide mx, i(age age_group_id location_id year_id sex_id) j(draw) 
rename mx* mort_draw_*

tempfile mort
save `mort'

**********************
di "Pull in SMR data (agnostic for location/year/sex, specific for age categories young/old"
import delimited "`in_dir'/smr_vals.csv", clear

tempfile smr_vals
save `smr_vals'

foreach acause of local acause_list {

	use `smr_vals', clear

	keep if acause == "`acause'"

	forvalues x = 0/999 {
		if mod(`x', 100)==0{
			di in red "Working on SMR draw `x'"
		}
		gen smr_draw_`x' = rnormal(mean, std)
	}

	** Format SMR data and merge with mx results
	rename sex sex_id
	gen age = ""
	replace age = "young" if age_start == 0
	replace age = "old" if age_start == 20 
	
	merge 1:m sex_id age using `mort', keep(3)
	drop _merge

	di "Calculating EMR draws"
	forvalues x = 0/999 {
		gen emr_draw_`x' = mort_draw_`x'*(smr_draw_`x' - 1)
	}

	di "Recalculating mean, lower and upper confidence bounds from draws to be compatible with DisMod"
	drop orig_mean mean upper lower smr* mort*
	egen mean = rowmean(emr_draw*)
	fastpctile emr_draw*, pct(2.5 97.5) names(lower upper)
	keep location_id sex_id age_group_id year_id mean upper lower 

	di "Save"
	export delimited "`out_dir'/`location_id'_emr.csv", replace 

}
