// Purpose:    Connects icd and text-based diagnoses with gbd causes

** **************************************************************************
** CONFIGURATION  (AUTORUN)
**         Define J drive location. Sets application preferences (memory allocation, variable limits). Set standard folders based on the arguments
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(3) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)

// set location at which to save missing codes if found
    local missing_codes = "`error_folder'/`data_set_name'_MISSING_CODES_`data_type'_$today.dta"
    capture remove "`missing_codes'"

** **************************************************************************
** Get Additional Resources
** **************************************************************************
// Get cause map
    use "${gbd_cause_map_`data_type'}", clear
    gen map_priority = 3
    replace map_priority = 2 if (substr(coding_system, 1, 7) == "CUSTOM_") & ((length(coding_system) == 10) | (!regexm(substr(coding_system, 12, .), "_")))
    replace map_priority = 1 if (substr(coding_system, 1, 7) == "CUSTOM_") & regexm(substr(coding_system, 12, .), "_")
    sort map_priority
    levelsof coding_system if substr(coding_system, 1, 7) == "CUSTOM_", clean local(list_of_custom_maps)
    levelsof coding_system, clean local(list_of_coding_systems)
    tempfile cause_map
    save `cause_map', replace

// Get age-sex restrictions
    use "$cause_restrictions", clear
    keep acause male female
    keep if substr(acause, 1, 4) == "neo_"
    rename acause gbd_cause
    tempfile sex_restrictions
    save `sex_restrictions', replace

// get location information
    import delimited using "$registry_id_table" , clear
    keep registry_id location_id
    tempfile location_ids
    save `location_ids', replace
    import delimited using "$common_cancer_data/modeled_locations.csv", clear
    merge 1:m location_id using `location_ids', keep(3) nogen
    keep registry_id ihme_loc_id
    tempfile location_info
    save `location_info', replace


** **************************************************************************
** Map Data
**        Connect each cause/cause_name with it's corresponding GBD cause
** **************************************************************************
    ** ************************
    ** Prepare Data
    ** ************************
    // GET DATA
        use "`data_folder'/$step_2_output", clear

    // Ensure correct data format for cause and cause name (type may have been converted if all values were empty upon save)
        capture tostring cause, replace
        capture tostring cause_name, replace

    // add data_set_name
        capture gen data_set_name = "`data_set_name'"

    // add location_info
        merge m:1 registry_id using `location_info', keep(1 3) assert(2 3) nogen

    // regenerate metric totals (some metric totals were dropped during subtotal disaggregation)
        capture drop `metric'1
        egen `metric'1 = rowtotal(`metric'*)

    // keep only data of interest
        keep data_set_name registry_id ihme_loc_id year_start year_end frmat im_frmat sex coding_system cause cause_name `metric'*

    // Check coding systems
        replace coding_system = trim(coding_system)
        gen ok_coding_system = 0
        foreach cod_sys in `list_of_coding_systems'{
            replace ok_coding_system = 1 if coding_system == "`cod_sys'"
        }
        count if ok_coding_system == 0
        if r(N) {
            display in red "ERROR: There are non-standard coding systems.  Coding systems must be one of: "
            BREAK
        }
        drop ok_coding_system

    ** ************************
    ** Merge with Map
    ** ************************
        // // Merge with special custom maps.
            foreach special_map_name in `list_of_custom_maps' {
                // determine the map type and set item to check. if there are only three letters after "CUSTOM_" or only no underscore after "CUSTOM_XXX_" then the map is ihme_loc_id specific
                    if length("`special_map_name'") == 10 | !regexm(substr("`special_map_name'", 12, .), "_") local check_var = "ihme_loc_id"
                    else local check_var = "data_set_name"
                    local check_for = substr("`special_map_name'", 8, .)

                // check for data that matches the map type. if it does, exit the loop
                    levelsof `check_var', clean local(to_check)
                    local has_special = 0
                    foreach tick in `to_check' {
                        if "`tick'" == "`check_for'" {
                            local list_of_maps = trim("`list_of_maps' `special_map_name'")
                            local has_special = 1
                            exit
                        }
                    }
            }

            // if dataset contains a special map type...
            if `has_special' {
                // create a unique CUSTOM map specific to the dataset
                    local first_loop = 1
                    preserve
                        foreach special_map in `list_of_maps' CUSTOM {
                            use `cause_map',clear
                            keep if coding_system == "`special_map'"
                            gen custom_coding_system = coding_system
                            replace coding_system = "CUSTOM"
                            if `first_loop' {
                                local first_loop = 0
                                gen keep = 1
                            }
                            else {
                                append using "`temp_folder'/dataset_custom_map.dta"
                                duplicates tag cause cause_name, gen(tag)
                                drop if tag != 0 & keep != 1
                                drop tag
                                replace keep = 1
                            }
                            if "`special_map'" == "CUSTOM" drop keep
                            save "`temp_folder'/dataset_custom_map.dta", replace
                        }
                        use `cause_map', clear
                        drop if substr(coding_system, 1, 6) == "CUSTOM"
                        gen lowest_priority = 1
                        append using "`temp_folder'/dataset_custom_map.dta"
                        duplicates tag cause cause_name, gen(tag)
                        drop if lowest_priority == 1 & tag != 0
                        drop tag lowest_priority
                        sort coding_system cause cause_name
                        save "`temp_folder'/final_map.dta", replace
                    restore

                // merge the results with the rest of the data
                    merge m:1 cause cause_name coding_system using "`temp_folder'/final_map.dta", keep(1 3 4) nogen

            }
            else merge m:1 cause cause_name coding_system using `cause_map', keep(1 3) nogen

    ** ************************
    ** Verify Mapping
    ** ************************
    // Check for unmapped codes
        count if gbd_cause == ""
        if r(N) {
            keep cause cause_name coding_system gbd_cause acause*
            keep if gbd_cause == ""
            duplicates drop
            save "`missing_codes'", replace
            display in red "ALERT: Not all codes matched to GBD causes. Please add the unmatched codes to the cause map before proceeding."
            display in red "See the following file: `missing_codes'"
            BREAK
        }

    ** ************************
    ** Expand any remaining ICD10 code ranges.
    ** ************************
    // Expand any remaining ICD10 code ranges if they are mapped as garbage codes and have no additional causes.
        count if regexm(cause, "-") & coding_system == "ICD10" & gbd_cause == "_gc" & acause1 == "_gc"
        if r(N) {
            // Disaggregate ranges
            preserve
                keep if regexm(cause, "-") & coding_system == "ICD10" & gbd_cause == "_gc" & acause1 == "_gc"
                duplicates drop
                save "`temp_folder'/03_to_be_disaggregated.dta", replace
                capture saveold "`temp_folder'/03_to_be_disaggregated.dta", replace
                // Run python script disaggregate code ranges and re-map any
                    !python "$code_prefix/01_inputs/01_registry/03_mapping/disaggregate_codes.py" "`data_set_group'" "`data_folder'" "`data_type'"

                    // format outputs of python script
                    use "`temp_folder'/03_causes_disaggregated_`data_type'.dta", clear
                    keep orig_cause subcauses
                    duplicates drop
                    rename (orig_cause subcauses) (cause acause)
                    split acause, p(",")
                    drop acause
                    gen gbd_cause = "_gc"
                    tempfile range_causes
                    save `range_causes', replace
            restore
            merge m:1 cause gbd_cause using `range_causes', keep(1 3 4 5) update replace nogen
        }

        // handle comma-separated causes
        count if regexm(cause, ",")  & !regexm(cause, "-") & coding_system == "ICD10" & gbd_cause == "_gc" & acause1 == "_gc"
        if r(N) {
            preserve
                keep if regexm(cause, ",")  & !regexm(cause, "-") & coding_system == "ICD10" & gbd_cause == "_gc" & acause1 == "_gc"
                keep cause
                duplicates drop
                gen acause = cause
                split acause, p(",")
                drop acause
                gen gbd_cause = "_gc"
                tempfile comma_separated_causes
                save `comma_separated_causes', replace
            restore
            merge m:1 cause gbd_cause using `comma_separated_causes', keep(1 3 4 5) update replace nogen
        }

    ** ************************
    ** Finalize
    ** ************************
    // Drop subtotals and codes that cannot be disaggregated
        drop if gbd_cause == "sub_total" | gbd_cause == "_none"

    // Replace special coding system (replace ICCC mark with "CUSTOM")
        replace coding_system = "CUSTOM" if !inlist(coding_system, "ICD10", "ICD9_detail")

    // Apply sex restrictions
        merge m:1 gbd_cause using `sex_restrictions', keep(1 3)
        drop if _merge == 3 & sex == 1 & male == 0
        drop if _merge == 3 & sex == 2 & female == 0
        drop male female _merge

    // Collapse everything
        collapse (sum) `metric'*, by(registry_id year_start year_end frmat im sex coding_system cause cause_name gbd_cause acause*) fast

    // restore data_set_name
        capture gen data_set_name = "`data_set_name'"

    // SAVE
        save_prep_step, process(3)
        capture log close

** **************************************************************************
** END mapping.do
** **************************************************************************
