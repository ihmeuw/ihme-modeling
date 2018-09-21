*****************************************
** Description:  Loads nLx estimates from the mortality team, then calculates
**      lambda values used to adjust for background mortality when converting
**      relative to absolute survival
*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** SET MACROS
** ****************************************************************
// Define relevant directories
    local output_file = "$scalars_folder/lambda_values.dta"

** ****************************************************************
** GET GBD RESOURCES
** ****************************************************************
// Get location data
    use "$parameters_folder/locations.dta", clear
    keep location_id location_type
    tempfile location_map
    save `location_map', replace

// load previous years lambda values, used to replace any missing data
    use "$nonfatal_model_storage/GBD2015/_scalars/lambda_values.dta", clear
    gen age_group_id = age/5+5
    replace age_group_id = 5 if age == 1
    replace age_group_id = 4 if age == .1
    replace age_group_id = 3 if age ==.01
    replace age_group_id = 2 if age == 0
    drop age
    rename lambda old_lambda

    preserve
        keep if age_group_id == 21
        tempfile old
        save `old'
    restore
    replace age_group_id = 30 if age_group_id == 21
    append using `old'
    replace age_group_id = 31 if age_group_id == 21
    append using `old'
    replace age_group_id = 32 if age_group_id == 21
    append using `old'
    replace age_group_id = 235 if age_group_id == 21

    tempfile ensure_all
    save `ensure_all', replace

** **************************************************************************
** RUN PROGRAM
** **************************************************************************
// Get most recent version of life table from mortality team
    run "$shared_functions_folder/stata/get_ids.ado"
    get_ids, table("life_table_parameter") clear
    levelsof life_table_parameter_id if parameter_name == "nLx", clean local(ltp_id)

    run "$shared_functions_folder/stata/get_life_table.ado"
    get_life_table, location_set_id(9) life_table_parameter_id(`ltp_id') clear
    rename (year_id mean) (year nLx)

// Keep only relevant data
    keep location_id sex year age_group_id nLx

// Calculate lambda values
    // Rename and sort
        rename sex_id sex
        gsort  +location_id +sex +year -age_group_id

    // Calculate lambda
        bysort location_id sex year: gen lambda = (ln(nLx/nLx[_n+1]))
        replace lambda = lambda/5 if age_group_id > 5
        replace lambda = lambda/4 if age_group_id == 5
        replace lambda = lambda/1 if age_group_id == 1
        drop nLx

    // Expand age group for under 1
        preserve
            keep if age_group_id == 28
            tempfile youngest
            save `youngest'
        restore
        foreach under_1 of numlist 2/4 {
            replace age_group_id = `under_1' if age_group_id == 28
            append using `youngest'
        }
        drop if age_group_id == 28

        preserve
            keep if age_group_id == 5
            tempfile young
            save `young'
        restore
        replace age_group_id = 1 if age_group_id == 5
        append using `young'

    // add any missing data
        merge 1:1 location_id year sex age_group_id using `ensure_all'
        replace lambda = old_lambda if _merge == 2
        drop old_lambda _merge

    // use age group '95-99' as '95+'
        count if age_group_id == 235
        if r(N)    drop if age_group_id == 33
        else replace age_group_id = 235 if age_group_id == 33

// Save
    compress
    save "`output_file'", replace

** **********
** END
** **********
