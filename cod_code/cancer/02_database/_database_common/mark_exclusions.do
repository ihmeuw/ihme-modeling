*****************************************
** Description: Marks datasets to be excluded from the calling process
** Input(s)/Output(s): USER (directly adjusts active data)
** How To Use: called by */02_database scripts

*****************************************
** **************************************************************************
** Define Function(s)
** **************************************************************************
capture program drop check_merge
program define check_merge
    // checks for merge error as a way to let the user update the exclusion table without slowing the estimation process
    count if _merge == 2
    if r(N) {
        noisily di "ERROR: not all exception entries merged"
        pause on
        pause
    }
    drop if _merge == 2
    drop _merge

end

** **************************************************************************
** Mark data if they qualify as "excluded"
** **************************************************************************
// accept args
    args data_type_of_exclusion

// save copy of current dataset
    tempfile inc_data
    save `inc_data'

// create map of exclusions based on prep status and data_type
    import delimited using "$registry_database_storage/data_set_table.csv", varnames(1) clear
    gen exclude = 1 if !inlist(data_type, 1, 2, 3)
    replace exclude = 1 if prep_status_id != 5
    gen exclusion_reason = "Dataset is marked with a prep-status that excludes it from the database. See prep_status on the data_set table."
    keep data_set_name exclude exclusion_reason
    tempfile prep_status_exclusion
    save `prep_status_exclusion', replace

    use `inc_data', clear
    merge m:1 data_set_name using `prep_status_exclusion', keep(1 3) update replace nogen
    tostring country_id year, replace
    save `inc_data', replace

// load database-specific exclusions
    import delimited using "$registry_database_storage/database_exclusion_table.csv", varnames(1) clear
    keep if excluded_from == "$database_name"
    if "`data_type_of_exclusion'" == "2" drop if substr(lower(data_set_name), -4, .) == "_mor"
    if "`data_type_of_exclusion'" == "3" drop if substr(lower(data_set_name), -4, .) == "_inc"
    keep data_set_name acause registry_id country_id year exclusion_reason

// Validate that database-specific exclusions are correctly entered. This process should be moved to a different script.
    count if registry_id != "0.0.1" & country_id != 1
    assert !r(N)
    // check for completeness and remove additional spaces where possible
    foreach v of varlist _all {
        capture confrim string variable `v'
        if _rc tostring `v', replace
        replace `v' = trim(itrim(`v'))
        capture count if `v' == ""
        if r(N) {
            noisily di "ERROR: some `v' entries on the exclusion table are misisng. Cancelling processes."
            BREAK
        }
    }
    count if year != "1" & (data_set_name == "unassigned_all_datasets" & registry_id == "0.0.1")
    if r(N){
        noisily di "ERROR: a full year of data may only be excluded for a specified dataset or registry."
        BREAK
    }
    count if registry_id != "0.0.1" & country_id != "1"
    if r(N){
        noisily di "ERROR: registry_id and country_id may not be specified at the same time"
        BREAK
    }


    replace data_set_name = "" if data_set_name == "unassigned_all_datasets"
    replace acause = "" if acause == "_neo"
    replace registry_id = "" if registry_id == "0.0.1"
    replace country_id = "" if country_id == "1"
    replace year = "" if year == "1"
    gen exclude = 1
    tempfile exceptions
    save `exceptions'
    local numExceptions = _N
    foreach row of numlist 1/`numExceptions' {
        use `exceptions', clear
        keep if _n == `row'
        local merge_vars = ""
        foreach v of varlist _all {
            if inlist("`v'", "exclude", "exclusion_reason") continue
            count if `v' == ""
            if r(N) drop `v'
            else local merge_vars = "`merge_vars' `v'"
        }
        tempfile current_exceptions
        save `current_exceptions'
        use `inc_data', clear
        merge m:1 `merge_vars' using `current_exceptions', update replace
        check_merge
        save `inc_data', replace
    }


// Save drop reasons and drop
    destring country_id year, replace
    replace exclude = 0 if exclude == .
    rename (exclude exclusion_reason) (to_drop dropReason)
    record_and_drop "Exclusions"

** *****
** END
** ******
