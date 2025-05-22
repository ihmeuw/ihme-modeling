** *****************************************************************************
** Description: Loads compiled incidence data, then selects
**           a single datapoint for each registry-year_id-sex-age-cancer
** Input(s): N/A
** Output(s): a formatted dta file
** How To Use: simply run script
** Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME
** *****************************************************************************
// Clear memory and set memory and variable limits
    clear all
    set more off
    pause on
// Load and run set_common function based on the operating system
    if "$h" == "" & c(os) == "Windows" global h = "H:"
    else if "$h" == "" global h = "/homes/" + c(username)
    global staging_name = "cod_mortality"
    global path_to_file = "$FILEPATH"
    run "$path_to_file" "staging"
    global staging_common_code = "$root_staging_code/_staging_common"
    run "$FILEPATH"
    run "$FILEPATH"
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
    do "${cancer_repo}/_database/load_database_table" "dataset"
    keep dataset_id dataset_name
    drop if dataset_id < 10
    tempfile dataset_id_map
    save `dataset_id_map'

// get parameter -- staged_incidence_version_id
    local inc_version = `0'

** ****************************************************************************
** PART 1: Load Data and Apply Restrictions. Drop Known Outliers.
** *****************************************************************************
// Get Data
    use "$FILEPATH", clear
    export delimited using "$FILEPATH", replace

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
    export delimited using "$FILEPATH", replace

// Apply Restrictions
    display "About to apply restrictions'
// Apply location restrictions and add location information
        display "About to load_location_info"
        load_location_info
        export delimited using "$FILEPATH", replace
    // apply cause and sex restrictions
        display "About to apply_cause_sex_restrictions"
        apply_cause_sex_restrictions
        export delimited using "$FILEPATH", replace
    // Apply year restrictions
        display "About to apply_year_restrictions"
        apply_year_restrictions
        export delimited using "$FILEPATH", replace
    // Apply age restrictions
        display "About to apply_age_restrictions"
        apply_age_restrictions
        export delimited using "$FILEPATH", replace
    // Apply registry restrictions. 
        display "About to apply_registry_restrictions"
        apply_registry_restrictions
        export delimited using "$FILEPATH", replace
    
    // save data here...
    export delimited using "$FILEPATH", replace

    // set dataType to indicate "incidence only" (used in KeepBest)
        gen dataType = 2

** *****************************************************************************
** PART 2:  Drop missing or suspicious data, and known outliers
**             Drop within-dataset Redundancy
** *****************************************************************************
// Drop if all entries are missing.
    export delimited using "$FILEPATH", replace

    gen to_drop = 1 if cases1 == .  // | (cases1 == 0 & substr(lower(dataset_name), 1, 3) == "ci5")
    gen dropReason = "no data" if to_drop == 1
    replace dropReason = "CI5 data that were likely extracted improperly" if (cases1 == 0 & substr(lower(dataset_name), 1, 3) == "ci5")
    noisily di "Removing data-years with no actual data"
    record_and_drop "preliminary drop and check"
    drop cases1

    save "$FILEPATH", replace

// Drop exceptions
    do "$FILEPATH"
    export delimited using "$FILEPATH", replace

// Drop within-dataset redundancies
    drop_dataset_redundancy
    export delimited using "$FILEPATH", replace

// Save
    save "$FILEPATH", replace

** *****************************************************************************
** PART 3: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
**         Then Compare Registry-Years and Drop Remaining Redundant data
**         Then Remove remaining conflicts
** *****************************************************************************
// Keep only national data where possible
    KeepNational
    compress
    export delimited using "$FILEPATH", replace
    save "$FILEPATH", replace

// See if $temp_folder gets changed in the below Python file
    di "temp_folder is: "
    macro list temp_folder

// Mark and Drop Other Redundancies: run KeepBest
    !python "$FILEPATH" "cod_mortality" "" "CR" "all_ages"

// See if $temp_folder got changed
    di "temp_folder is: "
    macro list temp_folder

    di "In 01_drop_andCheck.do, in Keep National Data, about to load:"
    macro list temp_folder
    di "keepbest_output_selected_inc_CR.csv"
    import delimited using "$FILEPATH", clear
    export delimited using "$FILEPATH", replace

// add the location data onto the data //

    preserve
        do "${cancer_repo}/_database/load_database_table" "registry"
        keep registry_index location_id country_id for_processing_only
        tempfile registry_table
        save `registry_table', replace
    restore

    drop country_id location_id for_processing_only

//  now we merge on what we need from the registry table
    merge m:1 registry_index using `registry_table', assert(2 3) keep(3) nogen

//  this variable exists in the data, but has values other than 0; we need the values all to be 0
    replace underlying_nid = 0

//  this variable exists but hasn't been calculated
    replace year_span = year_end - year_start + 1

//  add some other variables that the pipeline needs
    gen NID = 0
    gen dataType = 2
    gen subMod = 1
    replace subMod = 0 if location_id == country_id

** *****************************************************************************
** Part 4: Check for Remaining Conflicts, and Save
** *****************************************************************************
// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts
    di "Checking for overlaps..."
    AlertIfOverlap
    save "$FILEPATH", replace

// Split India data
    do "$FILEPATH" "cases* pop*"
    save "$FILEPATH", replace
    export delimited "$FILEPATH", replace
    compress
    save "$FILEPATH", replace

// Format for pipeline and save output
    capture rename sex sex_id
    capture confirm variable NID
    if _rc != 0 {
        gen NID = 0
    }
    capture confirm variable underlying_nid
    if _rc != 0 {
        gen underlying_nid = 0
    }
    keep dataset_id registry_index year_start year_end sex acause pop cases* age NID underlying_nid

    save_staging_data, output_file("01_selected_incidence_data.dta")

** *****************************************************************************
** END
** *****************************************************************************

exit, STATA clear

