** *****************************************************************************
** Description: Loads compiled incidence data, then selects
**           a single datapoint for each registry-year_id-sex-age-cancer
** Input(s): N/A
** Output(s): a formatted dta file
** How To Use: simply run script
** Contributors: USERNAME
** *****************************************************************************
// Clear memory and set memory and variable limits
    clear all
    set more off

// Load and run set_common function based on the operating system
    if "$h" == "" & c(os) == "Windows" global h = "H:"
    else if "$h" == "" global h = "/homes/" + c(username)
    global staging_name = "cod_mortality"
    run "FILEPATH" "staging"
    global staging_common_code = "$root_staging_code/_staging_common"
    run "$staging_common_code/set_staging_common.ado"
    run "$staging_common_code/load_common_staging_functions.ado"
    global gbd_iteration = "$current_gbd_round"
    global database_name = "cod_mortality"
    global dropped_data_file = "$cod_mortality_dropped_data"

** *****************************************************************************
** Set Macros
** *****************************************************************************
// Set macros
    better_remove, path("$cod_mortality_dropped_data")
	global temp_folder = "$cod_mortality_staging_workspace/$today"
    make_directory_tree, path("$temp_folder")


// Get map of dataset_names
    do "FILEPATH" "dataset"
    keep dataset_id dataset_name
    drop if dataset_id < 10
    tempfile dataset_id_map
    save `dataset_id_map'

// get current max cod_mortality_version_id
    local inc_version = `0'

** ****************************************************************************
** PART 1: Load Data and Apply Restrictions. Drop Known Outliers
** *****************************************************************************
// Get Data
    use "$mi_datasets_staging_storage/appended_p7_inc_version`inc_version'.dta", clear

// drop unwanted causes
    keep dataset_id nid underlying_nid registry_index year_start year_end sex age acause pop cases
    capture drop cases1
    capture drop pop1

// Ensure presence of valid NID values
    capture destring nid underlying_nid, replace 
    replace nid = 0 if nid == .
    replace nid = underlying_nid if underlying_nid != .
    rename nid NID

// Generate 'year_id' as the average year and calculate year span
    gen_year_data

// Apply Restrictions
// Apply location restrictions and add location information
        load_location_info
    // apply cause and sex restrictions
        apply_cause_sex_restrictions
    // Apply year restrictions
        apply_year_restrictions
    // Apply age restrictions
        apply_age_restrictions
    // Apply registry restrictions. 
        apply_registry_restrictions
    
    // set dataType to indicate "incidence only" (used in KeepBest)
        gen dataType = 2

** *****************************************************************************
** PART 2:  Drop missing or suspicious data, and known outliers
**             Drop within-dataset Redundancy
** *****************************************************************************
// Drop if all entries are missing. Also drop if data are suspicious 
    merge m:1 dataset_id using `dataset_id_map', keep(1 3) assert(2 3) nogen
    egen cases1 = rowtotal(cases*), missing

    gen to_drop = 1 if cases1 == .  
    gen dropReason = "no data" if to_drop == 1
    replace dropReason = "CI5 data that were extracted improperly" if (cases1 == 0 & substr(lower(dataset_name), 1, 3) == "ci5")
    noisily di "Removing data-years with no actual data"
    record_and_drop "preliminary drop and check"
    drop cases1

// Drop exceptions
    do "${root_staging_code}/_staging_common/mark_exclusions.do"

// Drop within-dataset redundancies
    drop_dataset_redundancy

// Save
    save "$temp_folder/02_first_save.dta", replace

** *****************************************************************************
** PART 3: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
**         Then Compare Registry-Years and Drop Remaining Redundant data
**         Then Remove remaining conflicts
** *****************************************************************************
// Keep only national data where possible
    KeepNational
    compress
    save "$temp_folder/02_second_save.dta", replace


// Mark and Drop Other Redundancies: run KeepBest
    !python "FILEPATH" "$temp_folder/02_second_save.dta" "cod_mortality"
    import delimited using "$temp_folder/keepbest_output_selected_inc.csv", clear 
 
// re-attach dataset_id and location information for last part of script 
    merge m:1 dataset_name using `dataset_id_map', assert(2 3) keep(3) nogen
    load_location_info

** *****************************************************************************
** Part 4: Check for Remaining Conflicts, and Save
** *****************************************************************************
// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts
    di "Checking for overlaps..."
    AlertIfOverlap
    save "$temp_folder/02_fourth_save.dta", replace

// Split India data
    do "$root_staging_code/_staging_common/india_assign_rural_urban.do" "cases* pop*"
    compress
    save "$temp_folder/02_fifth_save.dta", replace 

// Format for pipeline and save output
    capture rename sex sex_id
    gen NID = 0
    gen underlying_nid = 0 
    keep dataset_id registry_index year_start year_end sex acause pop cases* age NID underlying_nid
    save_staging_data, output_file("01_selected_incidence_data.dta")

** *****************************************************************************
** END
** *****************************************************************************

 exit, STATA clear
 