*****************************************
** Description: Loads compiled incidence data, then selects
**           a single datapoint for each registry-year-sex-age-cancer
** Input(s): USER
** Output(s): a formatted dta file
** How To Use: simply run script

*****************************************
// Clear memory and set memory and variable limits
    clear all
    set more off

// Load and run set_common function based on the operating system
  global database_name = "cod_mortality"
    run "FILEPATH" "database"  // loads globals and functions
    global gbd_iteration = 2016

** **************************************************************************
** Set Macros
** **************************************************************************
// Set macros
    global dropped_data_file = "$cod_mortality_dropped_data"
    better_remove, path("$dropped_data_file")
    global temp_folder = "$cod_mortality_database_workspace"

// Get map of data_set_names
    import delimited using "$registry_database_storage/data_set_table.csv", varnames(1) clear
    keep data_set_id data_set_name
    drop if data_set_id < 10
    tempfile data_set_id_map
    save `data_set_id_map'

// map of NIDs to merge with selected incidence later
    import delimited using "$registry_database_storage/nqry_table.csv", varnames(1) clear
    rename nid NID
    drop if data_type == 3
    drop data_type
    keep NID underlying_nid year_start    year_end data_set_name registry_id
    rename (year_start year_end) (nid_year_start nid_year_end)
    tempfile NID_map
    save `NID_map'

// sdi_quintile map
    import delimited using "FILEPATH", varnames(1) clear
    replace sdi_quintile = "1" if sdi_quintile == "Low SDI"
    replace sdi_quintile = "2" if sdi_quintile == "Low-middle SDI"
    replace sdi_quintile = "3" if sdi_quintile == "Middle SDI"
    replace sdi_quintile = "4" if sdi_quintile == "High-middle SDI"
    replace sdi_quintile = "5" if sdi_quintile == "High SDI"
    destring location_id sdi_quintile, replace
    keep location_id sdi_quintile
    tempfile sdi_quintile_map
    save `sdi_quintile_map'

** ****************************************************************************
** PART 1: Load Data and Apply Restrictions. Drop Known Outliers
** *****************************************************************************
// Get Data
    use  "$mi_datasets_database_storage/GBD${gbd_iteration}/compiled_07_finalized_inc.dta", clear
    merge m:1 data_set_name using `data_set_id_map', keep(1 3) assert(2 3) nogen

// Generate 'year' as the average year and calculate year span
    gen_year_data

// Apply Restrictions
    // apply cause and sex restrictions
        apply_cause_sex_restrictions

    // Apply location restrictions and add location information
        load_location_info

    // Apply year restrictions
        apply_year_restrictions

    // Apply age restrictions
        apply_age_restrictions

    // Apply registry restrictions
        apply_registry_restrictions

    // set dataType to indicate "incidence only" (used in KeepBest)
        gen dataType = 2

** ****************************************************************************
** PART 2:  Drop missing or suspicious data, and known outliers
**             Drop within-data_set Redundancy
** *****************************************************************************
// Drop if all entries are missing. Also drop if data are suspicious
    drop cases1
    egen cases1 = rowtotal(cases*), missing
    gen to_drop = 1 if cases1 == . | (cases1 == 0 & substr(lower(data_set_name), 1, 3) == "ci5")
    gen dropReason = "no data" if to_drop == 1
    replace dropReason = "CI5 data that were likely extracted improperly" if (cases1 == 0 & substr(lower(data_set_name), 1, 3) == "ci5")
    noisily di "Removing data-years with no actual data"
    record_and_drop "preliminary drop and check"

// // Drop exceptions
    do "$root_database_code/_database_common/mark_exclusions.do"

// Drop within-dataset redundancies
    drop_dataset_redundancy

** ****************************************************************************
** PART 3: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
**         Then Compare Registry-Years and Drop Remaining Redundant data
**         Then Remove remaining conflicts
** *****************************************************************************
// Keep only national data where possible
    KeepNational
    compress
    save "$temp_folder/02_first_save.dta", replace

// // Mark and Drop Other Redundancies: Create UID and run KeepBest
    callKeepBest "CoD_Incidence"
    compress
    save "$temp_folder/02_second_save.dta", replace

// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts
    AlertIfOverlap
    compress
    save "$temp_folder/02_third_save.dta", replace

** ****************************************************************
** Part 4: Add NIDs, Check for Remaining Redundancy, and Save
** ****************************************************************
// data check that all selected incidence would be included after joinby
    pause on
    joinby registry_id data_set_name using `NID_map', unmatched(master)
    capture count if _merge == 1
    if r(N) {
        noisily di "some incidence datapoints do not exist on the nqry table"
        pause
    }
    gen to_drop = 0
    gen dropReason = ""
    bysort registry_id data_set_name: gen within_years = 1 if year_start >= nid_year_start & year_end <= nid_year_end
    replace to_drop = 1 if within_years != 1 & _merge == 3
    replace dropReason = "datapoint does not exist in selected incidence" if to_drop == 1 & dropReason == ""

// save before dropping any data (data will have dropped data marked")
    save "$temp_folder/NID_dropped_inc.dta", replace

// ensure that at least one datapoint will still exist for each uid
    gen exists = 1
    bysort location_id registry_id year sex acause: egen possible_data = total(exists)
    bysort location_id registry_id year sex acause: egen dropped_rows = total(to_drop)
    count if possible_data == dropped_rows
    if r(N) != 0 {
        preserve
            keep if possible_data == dropped_rows & _merge == 3
            drop NID underlying_nid nid_year* to_drop dropReason within_years dropped_rows possible_data _merge
            duplicates drop
            gen NID = .
            gen underlying_nid =.
            gen to_drop =0
            tempfile no_good_match
            save `no_good_match'
            capture count
            noisily di "`r(N)' datapoints are on the nqry table but could not be matched to an NID"
            pause
        restore
        drop if possible_data == dropped_rows  & _merge == 3
        append using `no_good_match'
    }
    count if to_drop == 1 & _merge == 1 // extra test to ensure that no bugs were introduced
    assert !r(N)
    drop if to_drop == 1
    replace NID = underlying_nid if underlying_nid != .
    count if NID == .
    if r(N) {
        noisily di "ALERT: `r(N)' datapoints are still missing NIDs!"
        pause
    }
    capture tostring NID, replace
    capture tostring underlying_nid, replace
    AlertIfOverlap

// map sdi_quintiles to selected incidence data
    merge m:1 location_id using `sdi_quintile_map', assert(2 3) keep(3) nogen

// data check after
     keep location_id country_id data_set* registry* year_start year_end year year_span sex acause cases* pop* sdi_quintile NID
    save_database_data, output_file("01_selected_incidence_data.dta")
    pause off

** *******
** END
** *******
