*** ======================= BOILERPLATE ======================= ***
    clear all
set more off
set maxvar 32000
if c(os) == "Unix" {
    global prefix FILEPATH
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
* Compatibility paths for Windows STATA GUI development
    global prefix FILEPATH
    local 2 FILEPATH
    local 3 "--draws"
    local 4 FILEPATH
    local 5 "--interms"
    local 6 FILEPATH
    local 7 "--logs"
    local 8 FILEPATH
}

*define locals from jobmon task
local params_dir         `2'
local draws_dir          `4'
local interms_dir        `6'
local logs_dir           `8'

cap log using "`logs_dir'/FILEPATH", replace
if !_rc local close_log 1
else local close_log 0

di "`params_dir'"
di "`draws_dir'"
di "`interms_dir'"
di "`logs_dir'"

*Load shared functions
adopath + FILEPATH
*** ======================= MAIN EXECUTION ======================= ***

local rootDir "`draws_dir'"
capture mkdir `rootDir'/FILEPATH
capture mkdir `rootDir'/FILEPATH
capture mkdir `rootDir'/FILEPATH
capture mkdir `rootDir'/FILEPATH
capture mkdir `rootDir'/FILEPATH

numlist "ADDRESS ADDRESS/ADDRESS"
local meidList `r(numlist)'

get_demographics, gbd_team(epi) clear
local locations `r(location_id)'
local ages `r(age_group_id)'
local years `r(year_id)'
local firstYear = `=word("`years'", 1)'
local currentYear = `=word("`years'", -1)'

clear
set obs `=wordcount("`ages'")'
local i = 1
generate age_group_id = .
foreach age of local ages {
    replace age_group_id = `age' in `i'
    local ++i
}

expand `=`currentYear' - `firstYear' + 1'
bysort age_group_id: gen year_id = `firstYear' + _n - 1
keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')

expand 2, gen(sex_id)
replace sex_id = sex_id + 1

expand 2, gen(measure_id)
replace measure_id = measure_id + 5

forvalues i = 0/999 {
    quietly generate draw_`i' = 0
}

save `rootDir'/FILEPATH, replace


* GET NONFATAL ESTIMATES *
use "`interms_dir'/FILEPATH", clear
levelsof location_id, local(hatLocs) clean

local nonhatLocs: list locations - hatLocs

use `rootDir'/FILEPATH, clear

generate location_id = .
generate modelable_entity_id = .

foreach meid of local meidList {
    replace modelable_entity_id = `meid'
    foreach location in `nonhatLocs' {
        replace location_id = `location'
        export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/`meid'/`location'.csv,  replace
    }
}

use "`interms_dir'/FILEPATH", clear
keep location_id year_id sex_id age_group_id measure_id total_* sleep_* disf_*
drop if missing(age_group_id)
keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')

local meid_disf_g ADDRESS 
local meid_sleep_g ADDRESS 

local meid_disf_r ADDRESS 
local meid_sleep_r ADDRESS 

local count 1
tempfile appendTemp


foreach stub in total sleep disf {
    foreach species in r g {
        preserve
        keep location_id year_id sex_id age_group_id measure_id `stub'_`species'_*

        generate outcome = "`stub'"
        generate species = "`species'"
        if "`stub'"!="total" generate modelable_entity_id = `meid_`stub'_`species''

        rename `stub'_`species'_* draw_*

        if `count'>1 append using `appendTemp'
        save `appendTemp', replace
        local ++count
        restore
    }
}

use `appendTemp', clear

local nameADDRESS disf_g
local nameADDRESS sleep_g

local nameADDRESS disf_r
local nameADDRESS sleep_r

local nameADDRESS total
local nameADDRESS disf
local nameADDRESS sleep


fastcollapse draw_* if outcome=="total", by(location_id year_id sex_id age_group_id measure_id outcome) type(sum) append
replace modelable_entity_id = ADDRESS if missing(species) 
drop if missing(modelable_entity_id)

replace draw_737=0 if missing(draw_737)
replace draw_632=0 if missing(draw_632)

foreach location of local hatLocs {
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/FILEPATH/`location'.csv if modelable_entity_id==ADDRESS & location_id==`location',  replace    }


foreach location of local hatLocs {
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using  `rootDir'/FILEPATH/`location'.csv if modelable_entity_id==ADDRESS & location_id==`location',  replace
}

foreach location of local hatLocs {
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/FILEPATH/`location'.csv if modelable_entity_id==ADDRESS & location_id==`location',  replace
}

foreach location of local hatLocs {
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/FILEPATH/`location'.csv if modelable_entity_id==ADDRESS & location_id==`location',  replace
}

foreach location of local hatLocs {
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/FILEPATH/`location'.csv if modelable_entity_id==ADDRESS & location_id==`location',  replace
}

local mark_best True

*** ======================= CLOSE LOG ======================= ***
if `close_log' log close

exit, STATA clear