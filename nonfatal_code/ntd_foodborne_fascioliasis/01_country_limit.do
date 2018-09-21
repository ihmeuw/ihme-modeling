// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: USERNAME
// Purpose: Limit countries

// PREP STATA
    clear
    set more off
    set maxvar 3200
    if c(os) == "Unix" {
        global prefix "FILEPATH"
        set odbcmgr unixodbc
        set mem 2g
    }
    else if c(os) == "Windows" {
        global prefix "FILEPATH"
        set mem 2g
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

// other file paths
    local code_dir "`prog_dir'/01_code"
    local in_dir "`prog_dir'/02_inputs"
    
// set adopath
    adopath + "FILEPATH"
// set the shell file
    local shell_file "FILEPATH/stata_shell.sh"

// years
    local years 1990 1995 2000 2005 2010 2016
// sexes
    local sexes 1 2

    
// ****************************************************************************
// Use get_draws
    run "FILEPATH/get_draws.ado"
    
// Make sequela-specific director
    capture mkdir "`tmp_dir'/`limit_me'"
    capture mkdir "`tmp_dir'/`limit_me'/00_logs"
    capture mkdir "`tmp_dir'/`limit_me'/01_country_limit"
    capture mkdir "`tmp_dir'/`limit_me'/01_country_limit/checks"

    
// Load country list
    get_location_metadata, location_set_id(35) clear
    keep if is_estimate == 1 & most_detailed == 1
    tempfile locdf
    save `locdf'
    insheet using "`in_dir'/fbt_sequela_country_list.csv", comma names clear
    di "`parent_me'"
    keep me_id_`parent_me'
    rename me_id_`parent_me' location_name
    drop if location_name == ""
    duplicates drop
    ** Merge on location_name codes
    if `parent_me' == 1527 {
        merge 1:m location_name using `locdf', keep(1 3) keepusing(location_id) nogen
        replace location_id = 35467 if location_id == .
    }
    else {
        merge 1:m location_name using `locdf', assert(2 3) keep(3) keepusing(location_id) nogen
 
    }
    keep location_id
    gen keep_location_id = 1
    tempfile location_id_list
    save `location_id_list', replace
    
// Get final country list
    use `locdf', clear
    merge 1:1 location_id using `location_id_list', assert(1 3) nogen
    replace keep_location_id = 0 if keep_location_id == .
    levelsof location_id, local(locations)
        local a = 0
    foreach location of local locations {
        quietly levelsof keep_location_id if location_id == `location', local(keep_id) c
        if `keep_id' == 1 di "Keeping `location' data"
        if `keep_id' == 0 di "Removing `location' data"

        di "submitting job `a': `parent_me' `location'"
        ! qsub -P proj_custom_models -N "loc_`location'_country_exclusions" -pe multi_slot 4 -l mem_free=8 "`shell_file'" "`code_dir'/01_country_limit_parallel.do" "`prog_dir' `tmp_dir' `parent_me' `limit_me' `model' `location' `keep_id'"
        local ++ a
        sleep 100
    }

    local b = 0
    while `b' == 0 {
        local checks : dir "`tmp_dir'/`limit_me'/01_country_limit/checks" files "finished_*.txt", respectcase
        local count : word count `checks'
        di "checking `c(current_time)': `count' of `a' jobs finished"
        if (`count' == `a') continue, break
        else sleep 60000
    }
    
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// write check here
    file open finished using "`tmp_dir'/checks/country_limit/finished_`parent_me'.txt", replace write
    file close finished
