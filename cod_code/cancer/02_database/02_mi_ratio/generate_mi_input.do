*****************************************
** Description: Loads compiled incidence and mortality data, merges them,
**      then selects a single datapoint for each location_id-year-sex-age-cancer
** Input(s): USER
** Output(s): a formatted dta file with today's date
** How To Use: simply run script

*****************************************
// Clear workspace and set to run all selected code without pausing
    clear all
    set more off

// set database name and gbd_iteration
    global database_name = "mi_ratio"
    global gbd_iteration = 2016

** **************************************************************************
** Set macros and load additional resources
** **************************************************************************
// Load and run set_common function based on the operating system
  global database_name = "mi_ratio"
    run "FILEPATH" "database" // loads globals

// Set folders
    local output_folder = "$mi_ratio_database_storage"
    global temp_folder = "$mi_ratio_database_workspace"
    local nid_combo_map = "${temp_folder}/combo_map.dta"
    global dropped_data_file = "$mi_ratio_dropped_data"
    capture rm "`dataset_combination_map'"

** ****************************************************************
** Define add_merge_name function
**         Purpose: generates merge_name variable to enable merges. merge_name is equal to the data_set name from the first character to the year designation.
**        Notes: generates merge_name variable
** ****************************************************************
capture program drop add_merge_name
program add_merge_name
    // generate merge_name
        gen locationOfYear = strpos(data_set_name, "_1")
        replace locationOfYear = strpos(data_set_name, "_2") if  (locationOfYear == 0) | (strpos(data_set_name, "_2") < strpos(data_set_name, "_1") & strpos(data_set_name, "_2") != 0)
        gen merge_name = substr(data_set_name, 1, locationOfYear - 1)
        replace merge_name = data_set_name if merge_name == ""
        drop locationOfYear

    // Temporary exception for GBD2016. Adjust name of usa_seer_1973_2013_inc/_mor to avoid conflict Remove after GBD2016.
        replace merge_name = "usa_seer_update" if regexm(data_set_name, "usa_seer_1973_2013")

    // check for duplicates
        duplicates tag merge_name registry_id sex acause year*, gen(duplicate)
        count if duplicate != 0
        if r(N) > 0 {
            noisily di "Error in merge process."
            noisily di "Please ensure that all data from the same data_set shares a common naming schema"
            noisily di "and that there is no data redundancy from a single data_set (by registry_id, sex, acause, and year)."
            BREAK
        }
        drop duplicate

end

** ****************************************************************
** Part 1: Import Data and Drop Exclusions
** ****************************************************************
// // Get INC data and Reformat to enable merge
    use "$mi_datasets_database_storage/GBD${gbd_iteration}/compiled_07_finalized_inc.dta", clear
    keep cases* pop* data_set* location_id registry_id acause sex year*
    collapse(sum) cases* (mean) pop*, by(data_set* location_id registry acause sex year*) fast

    // add required variables
    add_merge_name
    load_location_info
    gen_year_data

    // // Drop exceptions
    do "$root_database_code/_database_common/mark_exclusions.do" 2

    // Rename to enable merge
    rename data_set_name data_set_name_INC
    gen inc_data = 1
    tempfile incData
    save `incData', replace

// // Get MOR data
    use "$mi_datasets_database_storage/GBD${gbd_iteration}/compiled_07_finalized_mor.dta", clear
    keep deaths* pop* data_set* location_id registry_id acause sex year*
    collapse(sum) deaths* (mean) pop*, by(data_set* location_id registry_id acause sex year*) fast

    // add required variables
    add_merge_name
    load_location_info
    gen_year_data

    // // Drop exceptions
    do "$root_database_code/_database_common/mark_exclusions.do" 3

    rename data_set_name data_set_name_MOR
    rename pop* mor_pop*
    gen mor_data = 1

** ****************************************************************
** Part 2: Merge Data. Refine data and add variables relevant to MI models
** ****************************************************************
// Merge data from the same data_set
    // Note: cases1/deaths1 with missing values are dropped during standardize_format
    merge 1:1 merge_name acause location_id registry_id sex year* using `incData'
    rename merge_name data_set_name

// Keep population from the incidence data where available
    foreach p of varlist pop* {
        replace `p' = mor_`p' if `p' ==.
    }
    drop mor_pop*

// Add dataType Variable
    gen dataType = 1 if inc_data == 1 & mor_data == 1
    replace dataType = 2 if inc_data == 1 & mor_data != 1
    replace dataType = 3 if inc_data != 1 & mor_data == 1

// Adjust data_set identifiers
    replace data_set_name = data_set_name_INC if dataType == 2
    replace data_set_name = data_set_name_MOR if dataType == 3

// Keep relevant variables
    keep data_set* location_id registry_id year* sex acause dataType cases* deaths* pop*
    save_database_data, output_file("01_merged.dta")

** ****************************************************************************
** PART 3: Load Data and Apply Restrictions
** *****************************************************************************
// Load Merged Registry Data
    use "$mi_ratio_database_storage/01_merged.dta", clear

// Apply Restrictions
    // apply cause and sex restrictions
        apply_cause_sex_restrictions

    // Apply location restrictions and add location information
        load_location_info, apply_restricitons("true")

    // Apply year restrictions
        apply_year_restrictions

    // Apply age restrictions
        apply_age_restrictions

    // Apply registry restrictions
        apply_registry_restrictions

//  Drop Within-data_set Redundancy
    drop_dataset_redundancy

** ****************************************************************************
** PART 4: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
** *****************************************************************************
// // Keep National Data where possible
    KeepNational
    compress
    save "$temp_folder/first_MI_drop_tempfile.dta", replace

** ****************************************************************
** PART 5: Merge Incidence-only and Mortality-only Data by registry_id-year where possible
** ****************************************************************
// // Separate INC-only: check for redundancy and rename identifiers
    // Keep INC-only
        use "$temp_folder/first_MI_drop_tempfile.dta", clear
        keep if dataType == 2
        drop *MOR deaths*

    // Drop registry_id-cause-years that are missing all data
        gen to_drop = 0
        replace to_drop = 1 if cases1 == .
        count if to_drop == 1
        if r(N) {
            gen dropReason = "incidence dataset contained no incidence for this location-sex-year-cause " if to_drop == 1
            record_and_drop "Part 4"
        }
        else drop to_drop

    // Save file for use with leukemia data in Part 6 (below)
        preserve
            keep if regexm(acause, "neo_leukemia_")
            capture count
            if r(N) > 0    {
                save "$temp_folder/leukemia_data.dta", replace
            }
        restore

    // Run KeepBest
        callKeepBest "INC_only"

    // Alert user of remaining data overlaps
        AlertIfOverlap

    // Add NIDs
        addNIDs, data_type(2) merge_variable("data_set_name_INC") section_id("INC only")
        rename NID NID_inc
        AlertIfOverlap

    // Finalize INC-only
        drop dataType data_set_name
        capture drop data_set_id
        save "$temp_folder/INC_only.dta", replace

// // MOR-only: check for redundancy and rename identifiers
    // Keep Mor-only
        use "$temp_folder/first_MI_drop_tempfile.dta", clear
        keep if dataType == 3
        drop *INC cases* pop*

    // Drop registry_id-cause-years that are missing all data
        gen to_drop = 0
        replace to_drop = 1 if deaths1 == .
        count if to_drop == 1
        if r(N) {
            gen dropReason = "mortality dataset contained no information for this location-sex-year-cause " if to_drop == 1
            record_and_drop "Part 4"
        }
        else drop to_drop

    // Run KeepBest
        callKeepBest "MOR_only"

    // Alert user of remaining data overlaps
        AlertIfOverlap

    // Add NIDs
        addNIDs, data_type(3) merge_variable("data_set_name_MOR") section_id("MOR only")
        rename NID NID_mor
        AlertIfOverlap

    // Finalize MOR-only
        drop dataType data_set_name
        capture drop data_set_id
        save "$temp_folder/MORonly.dta", replace

// // Merge INC-only/MOR-only and keep only matching registries
    // merge
        merge 1:1 registry_id sex year* acause using "$temp_folder/INC_only.dta"

    // Record drop reason, then drop un-merged data
        gen to_drop = 0
        replace to_drop = 1 if _merge != 3
        drop _merge
        capture count if to_drop == 1
        if r(N) {
            noisily di "Removing Unmatched Data..."
            gen dropReason = "registry-year does not have matching inc/mor data" if to_drop == 1
            gen dataType = .
            record_and_drop "Part 4" "$database_name"
        }
        else drop to_drop

// // save combined data
    // generate required variables
        capture drop dataType
        gen dataType = 1
        egen data_set_name = concat(data_set_name*), punct(" & ")
        gen data_set_id = 9.2

    // alert user if data_set information is missing
        count if data_set_name_INC == "" | data_set_name_MOR == ""
        if r(N) {
            pause on
            di "ERROR: some data_set information was lost during the INC_only/MOR_only merge"
            pause
            pause off
        }

    // save
        compress
        save "$temp_folder/second_MI_drop_tempfile.dta", replace

** ****************************************************************
** PART 6: Append Merged Incidence-only and Mortality-only Data where possible
** ****************************************************************
// Append Merged INC/MOR Data to data of which INC/MOR are from the same data_set
    use "$temp_folder/first_MI_drop_tempfile.dta", clear
    keep if dataType == 1

    // Add NIDs
    addNIDs, data_type(2) merge_variable("data_set_name_INC") section_id("merged INC")
    rename NID NID_inc
    addNIDs, data_type(3) merge_variable("data_set_name_MOR") section_id("merged MOR")
    rename NID NID_mor

    // attach to the rest of the data
    append using "$temp_folder/second_MI_drop_tempfile.dta"
    compress
    save "$temp_folder/third_MI_drop_tempfile.dta", replace

// Run KeepNational and KeepBest on the recombined data to eliminate redundancies
    callKeepBest "INC_MOR"

// Save
    compress
    save "$temp_folder/fourth_MI_drop_tempfile.dta", replace

** ****************************************************************
** Part 7: Remove Conflicts, Check for Remaining Redundancy, Finalize and Save
** ****************************************************************
// Save
    keep country_id location_id data_set* registry* year sex acause NID* cases* deaths* pop*
    order location_id year sex acause NID* cases* deaths* pop*
    save_database_data, output_file("02_selected_MI_data.dta")

// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts

// Check again for Redundancy
    AlertIfOverlap

// Drop Irrelevant/Empty Columns
    capture drop pop_year
    foreach n of numlist 3/6 23/26 91/94{
        capture drop cases`n'
        capture drop deaths`n'
        capture drop pop`n'
    }

// Combine NIDs so that there is a single entry for each location_id year sex an acause
    #delim ;
    combine_inputs, use_mean_pop(0) combined_variables("NID_inc NID_mor")
        adjustment_section("mi_prep") metric_variables("cases* deaths* pop*")
        uid_variables("location_id year sex acause") dataset_combination_map("`nid_combo_map'");
    #delim cr
    gen NID = NID_inc
    replace NID = "284465" if NID_inc != NID_mor
    keep location_id year sex acause NID cases* deaths* pop*
    collapse (sum) cases* deaths* pop*, by(location_id year sex acause NID)

// Reshape, Calculate MI, and Drop Data with NULL MI Ratio.
    reshape long cases deaths pop, i(location_id acause sex year NID) j(age)

// Add age_group_id
    drop if age == 1
    gen age_group_id = age - 1
    drop age

// Remove labels
    foreach var of varlist _all {
        capture _strip_labels `var'
    }
// Save
    export delimited "$mi_ratio_database_storage/03_MI_ratio_model_input_$today.dta"

** ****************************************************************
** End drop_andCheck.do
** ****************************************************************
