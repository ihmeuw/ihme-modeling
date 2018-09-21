** ********************************************************************************************************
** Author:NAME 
** ********************************************************************************************************

set more off


// Source
    global source "`1'"

// Date
    global timestamp "`2'"
    
// Establish directories
    // J:drive
    if c(os) == "Windows" {
        global j ""
        global h ""
    }
    if c(os) == "" {
        global j ""
        set odbcmgr unixodbc
        local cwd = c(pwd)
        cd "~"
        global h = c(pwd)
        cd `cwd'
    }

    // Set up fast collapse
    run "PATH"

    global repo_dir "PATH"

// Working directories
    local log_dir "PATH"
    capture mkdir `log_dir'
    global source_dir "PATH"
    local out_dir "PATH"
    capture mkdir "PATH"
    capture mkdir "PATH"
    capture mkdir "PATH"
    capture mkdir "PATH"
    global map_dir "PATH"

// Log our efforts
    log using "`log_dir'/01_map_${source}_${timestamp}", replace
    
// Fetch country information
    do "PATH"
    create_connection_string
    local conn_string `r(conn_string)'
    odbc load, `conn_string' exec("SELECT location_type, ihme_loc_id, developed, region_id FROM shared.location_hierarchy_history WHERE location_set_version_id = 153 and location_type in('admin0','admin1','nonsovereign','urbanicity', 'other') ORDER BY sort_order") clear

    ** make iso3s the same as they are in the database
    gen iso3 = substr(ihme_loc_id, 1, 3)
    ** remake dev_status into the format we use in RDP
    ** there are sometimes missing developed values in location_hierarchy_history and we don't want to use these; will make isid iso3 fail
    drop if developed==""
    tostring developed, replace
    replace developed = "D" + developed if substr(developed, 1, 1)!="G"
    rename developed dev_status
    rename region_id region
    drop location_type ihme_loc_id
    duplicates drop
    isid iso3
    tempfile geo
    save `geo', replace
// Load in formatted data
    use "PATH", clear

// Drop useless years, except for ICD7A because we only use this source for age-sex weights, and historical Australia data
    drop if year<1970 & "$source"!="_ICD7A" & !strmatch("$source","_Australia*") & "$source"!="ICD8A" & "$source" != "ICD7A"

// Map six minor territories to their location id
    // These fall into Six minor territories, Rural
    replace location_id = 44539 if inlist(location_id, 43907,43912,43914,43915,43925,43933)

    // These fall into Six minor territories, Urban
    replace location_id = 44540 if inlist(location_id, 43871,43876,43878,43879,43889,43897)

// Map geographical location
    merge m:1 iso3 using `geo', keep(1 3)
    count if _m==1
    if `r(N)'>0 {
        noisily display in red "These ISO3s are dropped because they are not used in the GBD estimation process"
        noisily tab iso3 if _m==1 /* & !inlist(iso3, "ASM", "GUM", "VIR") */
    }
    drop if _m==1 & !inlist(iso3, "ASM", "GUM", "VIR", "MNP")
    drop _m

// Map to the cause list
    rename cause cause_code
    capture drop cause_name
    capture tostring cause_code, replace
    ** drop the decimal point in the ICD data
    if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
        replace cause_code = subinstr(cause_code, ".", "", .)
        replace cause_code = subinstr(cause_code, ",", "", .)
        replace cause_code = subinstr(cause_code, " ", "", .)
    }
    
    ** If the dataset has source_detail (i.e. it's a lit review), then use source_label to uniquely identify the sub-maps
    capture merge m:1 cause_code source_label using "$map_dir/map_${source}.dta", keep(1 3)


    if _rc!=0 {
        merge m:1 cause_code using "$map_dir/map_${source}.dta",  keepusing(yll_cause cause_name) keep(1 3)
    }

    do "PATH" yll_cause cause_name cause_code

    
    ** Trim unmatched 5-digit to 4-digit if _m==1 in ICD10 and ICD9
    if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
        replace cause_code = substr(cause_code, 1, 4) if _m==1
        drop _m
        merge m:1 cause_code using "$map_dir/map_${source}.dta", update keepusing(yll_cause cause_name) keep(1 3 4 5) 
    }
    
    ** Trim unmatched 4-digit to 3-digit if _m==1 in ICD10 and ICD9
    if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
        replace cause_code = substr(cause_code, 1, 3) if _m==1
        drop _m
        merge m:1 cause_code using "$map_dir/map_${source}.dta", update keepusing(yll_cause cause_name) keep(1 3 4 5)
    }

    replace _merge = 3 if _auto_mapped == 1
    drop _auto_mapped

    ** Clean up cc_code
    ** no longer need to do any replace with engine room, just assert cc code is ok
    assert !inlist(yll_cause, "CC Code", "CC_Code", "CC_CODE", "cc code", "CC_code", "CC code")
    
        
    ** Drop Stillbirths & sub-totals
    drop if yll_cause == "_sb"
    drop if yll_cause == "sub_total"
    
    replace yll_cause = cause_code if inlist(cause_code, "inj_war_war", "inj_war_execution", "inj_war_terrorism", "diarrhea", "meningitis_meningo")


    ** ASSERT THAT ALL CAUSES MAPPED TO ARE VALID

    ** get list of valid acauses
    preserve
        do "PATH"
        keep acause yld_only
        rename acause yll_cause
        duplicates drop
        tempfile valid_acauses
        save `valid_acauses'
    restore
    merge m:1 yll_cause using `valid_acauses', gen(is_valid_acause_ind) keep(1 3)

    ** if it is master only, then it wasn't in the list of valid acauses
    capture assert is_valid_acause_ind != 1
    if _rc {
        di in red "FOUND THESE ACAUSES THAT ARE OUTSIDE OF CAUSE HIERARCHY OR YlD_ONLY"
        levelsof yll_cause if is_valid_acause_ind == 1, clean
        WeRequirePerfection
    }
    count if yld_only == 1
    if `r(N)' > 0 {
        di in red "FOUND THESE ACAUSES THAT ARE YLD ONLY"
        levelsof yll_cause if yld_only == 1, clean
        preserve
            ** save a report for mohsen of yld only causes
            keep if yld_only == 1
            fastcollapse deaths*, by(iso3 source source_label list frmat im_frmat year cause_code yll_cause) type(sum)
            local explore_dir "PATH"
            capture mkdir `explore_dir'
            save "`explore_dir'/yld_only_causes.dta", replace
        restore
    }
    drop is_valid_acause_ind yld_only

// Save
    label drop _all
    rename cause_code cause
    rename yll_cause acause

    tempfile at_test
    save `at_test', replace
    collapse (sum) deaths*, by(iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name acause dev_status region) fast
    compress
    save "`out_dir'/01_mapped.dta", replace
    save "`out_dir'/_archive/01_mapped_${timestamp}.dta", replace
    capture log close

    
    
