// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Date: 27 January 2017
// Purpose: Limit countries, parallel file
// do "FILEPATH/01_country_limit.do"

// PREP STATA
    clear all
    set more off
    set maxvar 32000
    if c(os) == "Unix" {
        global prefix "FILEPATH"
        set odbcmgr unixodbc
    }
    else if c(os) == "Windows" {
        global prefix "FILEPATH"
    }

// Program directory
    local prog_dir "`1'"

// Temp directory
    local tmp_dir "`2'"

// Sequela_id
    local parent_me `3'

// Limit applied equela_id
    local limit_me `4'

// Model
    local model `5'

// location
    local location `6'

// keep?
    local keep_id `7'

// other file paths
    local code_dir "FILEPATH"
    local in_dir "FILEPATH"

// set adopath
    adopath + "FILEPATH"

// years
    local years 1990 1995 2000 2005 2010 2017
// sexes
    local sexes 1 2

** ** TEST **
** local username "USERNAME"
** local prog_dir "FILEPATH"
** local tmp_dir "FILEPATH"
** capture mkdir "`tmp_dir'"
** local grouping "clonorchiasis"
** local parent_me 1525
** local limit_me 3078
** local model 94801
** local location 10

    cap log using "FILEPATH/`location'.smcl", replace
    if !_rc local close 1
    else local close 0

// ****************************************************************************
// Use get_draws
    run "FILEPATH/get_draws.ado"

    if `keep_id' != 1 {
        di "`parent_me' `location'"
        get_draws, gbd_id_type(modelable_entity_id) gbd_id(`parent_me') source(ADDRESS) measure_id(5 6) location_id(`location') year_id(`years') sex_id(`sexes') clear
        foreach var of varlist draw* {
            quietly replace `var' = 0
        }
        quietly replace modelable_entity_id = `limit_me'
        quietly replace model_version_id = `model'
        quietly outsheet using "FILEPATH/`location'.csv", comma names replace
    }
    else if `keep_id' == 1 {
        di "`parent_me' `location'"
        get_draws, gbd_id_type(modelable_entity_id) gbd_id(`parent_me') source(ADDRESS) measure_id(5 6) location_id(`location') year_id(`years') sex_id(`sexes') clear
        quietly replace modelable_entity_id = `limit_me'
        quietly replace model_version_id = `model'
        quietly outsheet using "FILEPATH/`location'.csv", comma names replace
    }


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// write check here
    file open finished using "FILEPATH/finished_`location'.txt", replace write
    file close finished

// close logs
    if `close' log close
    clear
