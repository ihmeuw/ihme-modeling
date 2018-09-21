** Purpose: Create Telangana observations out of SCD & MCCD data, pulling some of the Andhra PrUSER sample size into Telangana

local source = source in 1
assert inlist("`source'", "India_SCD_states_rural", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10")

** set id for Andhra prUSER
if inlist("`source'", "India_SCD_states_rural") local ap_id 43908
else if inlist("`source'", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10") local ap_id 43872

** set id for Telangana
if inlist("`source'", "India_SCD_states_rural") local tg_id 43938
else if inlist("`source'", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10") local tg_id 43902

tempfile data
save `data', replace

** save andhra prUSER data in original
di "`ap_id'"
di "`tg_id'"
keep if location_id==`ap_id'
tempfile ap
save `ap', replace
** save deaths before; we shouldn't change the number of deaths at all
** replace this for accuracy without changing it in final output
replace deaths_rd = cf_rd*sample_size
su deaths_rd
local deaths_before = `r(sum)'

** call andhra prUSER telangana and append together; now cause fractions have been copied and we need to split the sample_size
replace location_id = `tg_id'
append using `ap'
tempfile both
save `both', replace

** to split sample_size, bring in the envelope
do "/home/j/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
drop pop
** keep only AP & Telangana
keep if inlist(location_id, `ap_id', `tg_id')
** Fix ages for merging with recode stage data
replace age = 91 if age==0
replace age = 93 if age==0.01
replace age = 94 if age==0.1
tempfile env
save `env', replace
** Now, we want both AP location_id & TG location_id to have an env for AP, an env for TG, and an env for both. reshape& merge to accomplish
reshape wide env, i(year iso3 sex age) j(location_id)
gen envboth = env`ap_id'+env`tg_id'
merge 1:m year iso3 sex age using `env', assert(3) nogen
drop env
save `env', replace

** merge the envelope with the ap+tg data
merge 1:m year iso3 sex age location_id using `both', assert(1 3) keep(3) nogen

replace sample_size = sample_size*(env`ap_id'/envboth) if location_id==`ap_id'
replace sample_size = sample_size*(env`tg_id'/envboth) if location_id==`tg_id'

** recalculate deaths numbers
drop deaths* env`tg_id' env`ap_id' envboth
foreach stage in raw corr rd {
	gen deaths_`stage' = cf_`stage'*sample_size
}
** now make sure that deaths weren't changed
su deaths_rd
assert abs(`r(sum)'-`deaths_before')<.001
** make sure that we have same number of ap & tg in the end
count if location_id==`ap_id'
local ap_count=`r(N)'
assert `ap_count'>0
count if location_id==`tg_id'
assert `r(N)'>0 & `r(N)'==`ap_count'
tempfile ap_tg
save `ap_tg', replace

** use the old data, drop the andhra prUSER, add in the ap+tg data
use `data'
drop if location_id==`ap_id'
append using `ap_tg'

** DONE
