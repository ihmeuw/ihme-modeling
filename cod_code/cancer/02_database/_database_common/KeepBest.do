*****************************************
** Description: Drop redundant or unmatched cancer data according to defined rules.
**      Keeps only data designated as "priority". If there is no "priority" data,
**      then uses all data for that registry_year.
**      Next eliminates redundancies in order of reliability
** Input(s): managed by */02_database code
** Output(s): compiled, single-datapoint dataset in a temporary folder to be used by the parent script
** Notes: - A uid (registry-year-sex-cause ID) must be defined prior to running
**        - Replaces all blank data_set_names with a combined version of data_set_name_INC
**              and data_set_name_MOR
** How To Use: called by parent script

*****************************************
// Alert user
    noisily di "Running KeepBest for $database_name database"

// Accept Arguments
    args section

// Get data type
    levelsof dataType, clean local(dataType)

// set keepBest temp folder
    local keep_best_temp_folder = "$temp_folder/_keep_best"
    make_directory_tree, path("`keep_best_temp_folder'")

** **************************************************************************
** Define check_data_sets
**        Purpose: counts the total number of datasets by uid that are not marked as 'to_drop'
** **************************************************************************
// //
    capture program drop check_data_sets
    program check_data_sets
        capture drop num_data_sets
        sort uid
        bysort uid: egen num_data_sets = total(data_set_check) if to_drop == 0

    end

** **************************************************************************
** Get Additional Resources and Add required information
** **************************************************************************
// // get list of preferred data, then create map of gbd_iteration and explicit data_set exclusions
    // save copy of current data
        tempfile no_gbd_iteration
        save `no_gbd_iteration', replace

    // create gbd_iteration maps
        import delimited using "$registry_database_storage/data_set_table.csv", varnames(1) clear
        tempfile gbd_iteration_info
        save `gbd_iteration_info', replace
        rename (data_set_name gbd_iteration) (data_set_name_INC gbd_iteration_INC)
        tempfile gbd_it_inc
        save `gbd_it_inc'
        use `gbd_iteration_info', clear
        rename (data_set_name gbd_iteration) (data_set_name_MOR gbd_iteration_MOR)
        tempfile gbd_it_mor
        save `gbd_it_mor'

    // restore current data and merge with gbd_iteration information
        use `no_gbd_iteration', clear
        if inlist(`dataType', 2, 3) merge m:1 data_set_name using `gbd_iteration_info', keep(3) assert(2 3) nogen
        else {
            merge m:1 data_set_name_INC using `gbd_it_inc', keep(3) assert(2 3) nogen
            merge m:1 data_set_name_MOR using `gbd_it_mor', keep(3) assert(2 3) nogen
            egen gbd_iteration = rowmean(gbd_iteration*)
        }

** **************************************************************************
** Part 1: Prepare Data
** **************************************************************************
// // For the mi_datasets database Drop non-matched data (Combined data_sets will contain "&" symbol).
    if  "$database_name" == "mi_ratio" {
        gen to_drop = 0
        gen dropReason = ""
        replace to_drop = 1 if regexm(data_set_name, " & ")
        replace dropReason = "Unmatched datasets excluded from mi_ratio database"  if to_drop == 1
        record_and_drop "unmatched_mi"
    }

// Set variables
    sort registry_id year sex
    egen uid = concat(registry_id year sex), punct("_")
    gen to_drop = 0
    gen dropReason = ""
    capture levelsof acause, clean local(acauses)

// Save versions of all of the data
    save "`keep_best_temp_folder'/before_keep_best.dta", replace
    save "`keep_best_temp_folder'/all_data_`section'.dta", replace

** **************************************************************************
** Part 2: Keep Best
** **************************************************************************
// // Iterate through causes to keep the best data by cause
quietly foreach cause in `acauses' {
    // keep only data for the current cause
        noisily di "Keeping best data for `cause', `section'..."
        keep if acause == "`cause'"

    // Check data_set_names
        capture drop data_set_check
        sort uid data_set_name
        bysort uid data_set_name: gen data_set_check = _n == 1
        replace data_set_check = 0 if data_set_check != 1
        check_data_sets

    // keep data from the same source/data_set if present. (runs whether or not `drop_matched_data' is set to 1)
        local dropReason = "dataset has matching data from the same data_set_name (`cause')"
        bysort uid: gen has_non_combined = 1 if !regexm(data_set_name, " & ")
        replace to_drop = 1 if regexm(data_set_name, " & ") & has_non_combined == 1 & num_data_sets > 1
        replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
        drop has_non_combined
        check_data_sets

    //  Keep Preferred datasets if preferred data is present (excellent data). NOTE:  preferred data must be in order of preference
        if "$database_name" == "mi_ratio" local preferred_datasets = "USA_SEER SWE_NCR_1990_2010 NORDCAN aut_2007_2008_inc EUREG_GBD2016 BRA_SPCR_2011"
        else if inlist("$database_name", "cod_mortality", "nonfatal_skin") local preferred_datasets = "usa_seer_1973_2008_inc USA_SEER SWE_NCR_1990_2010 IND_PBCR_2009_2011 BRA_SPCR_2011 JPN_NationalCIS_1958_2013"
        if regexm("$database_name", "nonfatal") local preferred_datasets = "ci5_period_ix_inc ci5_period_i_viii_with_skin_inc"
        foreach preferred_data in `preferred_datasets' {
            local dropReason = "dataset has `preferred_data' data for same registry (`cause')"
            gen preferred = 1 if substr(upper(data_set_name), 1, length("`preferred_data'")) == upper("`preferred_data'") // preferred = 1 if a preferred dataset
            replace preferred = 0 if preferred != 1
            bysort uid: egen has_preferred = total(preferred) if num_data_sets > 1
            replace to_drop = 1  if preferred != 1 & has_preferred > 0 & num_data_sets > 1
            replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
            drop preferred has_preferred
            check_data_sets
        }

    // Drop Deferred Datasets
        local deferred_datasets = "COD_VR USA_SEER_threeYearGrouped_1973_2012"
        foreach deferred in `deferred_datasets' {
            local dropReason = "dataset is among the least favored sets of data and other data are available (`cause')"
            gen preferred = 0 if substr(upper(data_set_name), 1, length("`deferred'")) == upper("`deferred'") // preferred = 0 if a least-favored dataset
            replace preferred = 1 if preferred != 0
            bysort uid: egen has_preferred = total(preferred) if num_data_sets > 1
            replace to_drop = 1  if preferred == 0 & has_preferred > 0 & num_data_sets > 1
            replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
            drop preferred has_preferred
            check_data_sets
        }

    // Keep the most recently formatted dataset(s) (Presumably newer data is only added because it is better. Assumes that new data which is not as good as old data will be dropped during formatting)
        local dropReason = "dataset has more recently formatted data for the same registry (`cause')"
        bysort uid: egen max_gbd = max(gbd_iteration)
        replace to_drop = 1 if gbd_iteration != max_gbd & num_data_sets > 1
        replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
        drop max_gbd
        check_data_sets

    // // Handle CI5 Data
        if "$database_name" == "mi_ratio" {
            // Drop CI5 Data
                local ci5_data_set_names = "ci5_1995_1997_inc ci5_period_i_ ci5_period_ix CI5_X_2003_2007 ci5_plus"
                foreach ci5_data_set_name in `ci5_data_set_names' {
                    local dropReason = "dataset has non-`ci5_data_set_name' data for the same registry (`cause')"
                    replace to_drop = 1 if regexm(data_set_name, "`ci5_data_set_name'") & num_data_sets > 1
                    replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
                    check_data_sets
                }
        }
        else if inlist("$database_name", "cod_mortality", "nonfatal_skin", "nonfatal"){
            // Keep CI5 Data
                local ci5_data_set_names = "ci5_plus CI5_X_2003_2007 ci5_period_i_ ci5_period_ix ci5_1995_1997_inc"
                foreach ci5_data_set_name in `ci5_data_set_names' {
                    local dropReason = "dataset has `ci5_data_set_name' data for the same registry"
                    replace to_drop = 1 if !regexm(data_set_name, "`ci5_data_set_name'") & num_data_sets > 1
                    replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
                    check_data_sets
                }
        }

    // Keep data with smallest year span (lowest priority, since it isn't necessarily linked with data quality)
        local dropReason = "dropped in favor of data with smaller year span from same registry (`cause')"
        bysort uid: egen min_span = min(year_span)
        replace to_drop = 1  if year_span != min_span & num_data_sets > 1
        replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
        drop min_span
        check_data_sets

    // Keep preferred data_set_names in case of exceptions
        if "$database_name" == "mi_ratio" local preferred_in_exception = "SVN_2008_2009 tto_1995_2006_inc"
        else if "$database_name" == "cod_mortality" local preferred_in_exception = "NORDCAN EUREG_GBD2016 aut_2007_2008_inc tto_1995_2006_inc"
        foreach preferred_data in `preferred_in_exception' {
            local dropReason = "dataset has `preferred_data' data for same registry (`cause')"
            gen preferred = 1 if regexm(upper(data_set_name), upper("`preferred_data'"))
            replace preferred = 0 if preferred != 1
            bysort uid: egen has_preferred = total(preferred) if num_data_sets > 1
            replace to_drop = 1  if preferred != 1 & has_preferred > 0 & num_data_sets > 1
            replace dropReason = "`dropReason'" if to_drop == 1 & dropReason == ""
            drop preferred has_preferred
            check_data_sets
        }

    // Verify that some data still remains
        noisily di "     Verifying dropped data..."
        check_data_sets
        count if num_data_sets > 1 & num_data_sets != .
        if r(N) != 0 {
            pause on
            noisily di "Error: Some data not dropped for some uids in `cause'!"
            pause
            pause off
        }
        count if num_data_sets == 0
        if r(N) > 0 {
            pause on
            noisily di "Error: All data dropped for some uids in `cause'!"
            pause
            pause off
        }
        count if (to_drop == 1 & dropReason == "") | (to_drop == 0 & dropReason != "")
        if r(N) > 0 {
            pause on
            noisily di "Error: Drop label error in in `cause'!"
            pause
            pause off
        }

    // Drop extraneous variables
        drop data_set_check num_data_sets

    // Record drop reasons, then drop
        count if to_drop == 1
        if r(N) > 0 record_and_drop "`cause' KeepBest"

    // Save tempfile, then append changes to original data
        noisily di "     Appending..."
        save "`keep_best_temp_folder'/`cause'_`section'.dta", replace
        use "`keep_best_temp_folder'/all_data_`section'.dta", clear
        drop if acause == "`cause'"
        append using "`keep_best_temp_folder'/`cause'_`section'.dta"
        save "`keep_best_temp_folder'/all_data_`section'.dta", replace
}

    // Drop extraneous variables
        drop uid gbd_iteration
        capture drop to_drop dropReason

    save "$temp_folder/best_data_`section'.dta", replace

** ******
** End
** ******

