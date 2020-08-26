
*** BOILERPLATE ***
set more off, perm
clear

if c(os) == "Unix" {
    local  "ADDRESS"
    set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
    local  "ADDRESS"
}


*** LOAD SHARED FUNCTIONS ***
adopath + FILEPATH	
run FILEPATH
run FILEPATH

create_connection_string, database(ADDRESS)
local shared = r(conn_string)


*** PULL LIST OF MODELLED AGE GROUPS AND CONVERT FROM SPACE- TO COMMA-DELIMITED VECTOR ***
get_demographics, gbd_team(cod) clear
local ages =subinstr(`r(age_group_ids)', " ", ",", .)


*** PULL START AND END YEARS FOR ALL MODELLED AGE GROUPS ***
odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (ages)") `shared' clear
    /* it is likely that "(ages)" in above line should be "(`ages')" instead */ 


*** CREATE AGE-MID POINT VARIABLE AND EXPAND TO CREATE SEX-SPECIFIC DATA ROWS ***
egen age_mid = rowmean(age_start age_end)

expand 2, gen(sex_id)
replace sex_id = sex_id + 1

gen effective_sample_size = 1

tempfile ages
save `ages', replace  


*** IMPORT AGE-SPECIFIC INCIDENCE DATA, MERGE TO AGE-GROUP DATASET CREATED ABOVE ***
import excel using "FILEPATH", clear firstrow case(preserve)

generate sex_id = (sex=="Male") + (sex=="Female")*2 + (sex=="Both")*3
keep if age_start>0 | age_end<99 | sex_id<3
egen age_mid = rowmean(age_start age_end)
replace cases = effective_sample_size * mean if missing(cases)
egen group = group(location_id year_start year_end)
keep cases effective_sample_size sex_id age_mid group

append using `ages'

gen sexC = (-1 * (sex_id==1)) + ((sex_id==2))
replace group = 999 if missing(group)


*** MODEL AGE/SEX PATTERN ***	
capture drop ageS* 
capture drop sp

mkspline ageS = age_mid, cubic knots(0.01 1 10 40)

menbreg cases sexC ageS*, exp(effective_sample_size) || group:
predict ageSexCurve, fixedonly	
replace ageSexCurve = 0 if age_group_id==2

* Check the age pattern visually *
*scatter ageSexCurve age_mid if group==999, by(sex_id)



*** CLEAN UP AND SAVE AGE DISTRIBUTION DATASET ***	
preserve
keep if group==999
keep ageSexCurve age_group_id sex_id

save "FILEPATH", replace
