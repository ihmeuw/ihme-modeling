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
    local params_dir        `2'
    local draws_dir         `4'
    local interms_dir       `6'
    local logs_dir          `8'

    cap log using "`logs_dir'FILEPATH", replace
    if !_rc local close_log 1
    else local close_log 0

    di "`params_dir'"
    di "`draws_dir'"
    di "`interms_dir'"
    di "`logs_dir'"

    // Load shared functions
    adopath + FILEPATH

*** ======================= MAIN EXECUTION ======================= ***
    tempfile ages

    create_connection_string, database(ADDRESS)
    local shared = r(conn_string)


*** PULL LIST OF MODELLED AGE GROUPS AND CONVERT FROM SPACE- TO COMMA-DELIMITED VECTOR ***  
    get_demographics, gbd_team(cod) clear
    local age_groups `=subinstr("`r(age_group_id)'", " ", ",", .)'

*** PULL START AND END YEARS FOR ALL MODELLED AGE GROUPS ***    
    ADDRESS load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`age_groups')") `shared' clear
    save `ages'

*** PULL IN AGE-SPECIFIC DATA ***   
    insheet using "`params_dir'/FILEPATH", clear
    drop if age_start == 0 & inlist(age_end, 99, 100) 


*** PREP FOR MODEL ***  
    append using `ages'

    egen age_mid = rowmean(age_start age_end)
    replace cases = mean * effective_sample_size
    replace effective_sample_size = 1 if missing(effective_sample_size)

    capture drop ageS*
    capture drop ageCurve
    mkspline ageS = age_mid, cubic knots(1 5 10 60) displayknots



*** MODEL AND PREDICT AGE/SEX PATTERN ***   
    mepoisson cases ageS*, exp(effective_sample_size) || location_id:   
    generate ageSexCurve = _b[_cons] + (ageS1 * _b[ageS1]) + (ageS2 * _b[ageS2]) + (ageS3 * _b[ageS3])  
    predict ageSexCurveSe, stdp

save "`interms_dir'/FILEPATH", replace

    keep if missing(seq)
    keep age_group_id ageSexCurve*
    expand 2, gen(sex_id)
    replace sex_id = sex_id + 1



*** SAVE ***
    save "`interms_dir'/FILEPATH", replace


*** ======================= CLOSE LOG ======================= ***
    if `close_log' log close

    exit, STATA clear
