 

clear all
set graphics off
set more off
set maxvar 32000

 
	if c(os) == "Windows" {
		local j FILEPATH
 
		quietly do FILEPATH
	}
	if c(os) == "Unix" {
		local j FILEPATH
		ssc install estout, replace 
		ssc install metan, replace
	} 
 
 
local location_id `1'
local in_dir `2'
local out_dir `3'
 
adopath + FILEPATH
 
local acause_list " "neonatal_preterm" "
 
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"

 
get_life_table, with_shock(1) with_hiv(1) location_id(`location_id') life_table_parameter_id(1) gbd_round_id(6) decomp_step("step4") clear

 
collapse (min) mean, by (age_group_id location_id year_id sex_id life_table_parameter_id run_id)
expand 1000
bysort age_group_id sex_id year_id mean: gen draw = -1 + _n
rename mean mx


tempfile mort
save `mort'

 
gen age = ""
replace age = "young" if age_group_id < 9 | age_group_id == 28
replace age = "old" if age == ""
 
keep age age_group_id location_id year_id sex_id draw mx 

keep if year_id == 1990 | year_id == 1995 | year_id == 2000 | year_id == 2005 | year_id == 2010 | year_id == 2017 | year_id == 2019

reshape wide mx, i(age age_group_id location_id year_id sex_id) j(draw) 

rename mx* mort_draw_*

save `mort', replace 

 
import delimited "`in_dir'/smr_vals.csv", clear
tempfile smr_vals
save `smr_vals'

foreach acause of local acause_list {

	use `smr_vals', clear

	di "Acause is `acause'"
	keep if acause == "`acause'"

	di "generating SMR draws"
	forvalues x = 0/999 {
		if mod(`x', 100)==0{
			di in red "working on draw `x'"
		}
		gen smr_draw_`x' = rnormal(mean, std)
	}
 
	rename sex sex_id
	gen age = ""
	replace age = "young" if age_start == 0
	replace age = "old" if age_start == 20 
	
	merge 1:m sex_id age using `mort', keep(3)
	drop _merge

	di "generating EMR draws"
	forvalues x = 0/999 {
		gen emr_draw_`x' = mort_draw_`x'*(smr_draw_`x' - 1)
	}

 
	drop orig_mean mean upper lower smr* mort*
	egen mean = rowmean(emr_draw*)
	fastpctile emr_draw*, pct(2.5 97.5) names(lower upper)
	keep location_id sex_id age_group_id year_id mean upper lower 

	di "save"
	export delimited "FILEPATH/`location_id'_emr.csv", replace 

}
