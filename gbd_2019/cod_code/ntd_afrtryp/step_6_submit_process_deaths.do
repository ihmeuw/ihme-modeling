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

local rootDir "`draws_dir'"
capture mkdir `rootDir'
capture mkdir `rootDir'/FILEPATH
capture mkdir `rootDir'/FILEPATH


get_demographics, gbd_team(cod) clear
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

expand 2, gen(sex_id)
replace sex_id = sex_id + 1

forvalues i = 0/999 {
    quietly generate draw_`i' = 0
}

generate cause_id = ADDRESS
generate location_id = .

save `rootDir'/FILEPATH, replace


* GET DEATH ESTIMATES *
use  "`interms_dir'/FILEPATH", clear
levelsof location_id, local(hatLocs) clean

local nonhatLocs: list locations - hatLocs

macro dir

use `rootDir'/FILEPATH, clear

local i 1
foreach location in `nonhatLocs' {
    replace location_id = `location'
    export delimited using `rootDir'/FILEPATH/`location'.csv, replace
    di "iteration `i'"
    local ++i
}


*change fiilepath and file name 
use  "`interms_dir'/FILEPATH", clear
keep location_id year_id sex_id age_group_id deaths_*
rename deaths_* draw_*

capture generate cause_id = ADDRESS
replace cause_id = ADDRESS

foreach location of local hatLocs {
    preserve
    keep if location_id==`location'
    quietly export delimited using `rootDir'/FILEPATH/`location'.csv,  replace
    restore
    drop if location_id == `location'
}


*** ======================= CLOSE LOG ======================= ***
if `close_log' log close

exit, STATA clear