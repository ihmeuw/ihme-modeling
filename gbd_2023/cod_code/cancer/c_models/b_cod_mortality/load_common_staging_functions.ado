** *****************************************************************************
** Description: Loads functions used by staging scripts
** Input(s)/Output(s): see individual functions
** How To Use: called by staging scripts
** Contributors: INDIVIDUAL_NAME
** *****************************************************************************
// Load Function to Ensure Population Consistency
    capture program drop ensure_pop_consistency
    do "${cancer_repo}/stata_utils/data_consistency_functions.do"
pause on
** *****************************************************************************
** Define save_staging_data
**         Description: saves data as current version and archive copy
** *****************************************************************************
capture program drop save_staging_data
program define save_staging_data
    syntax, output_file(string)

    //compress and save. do not save if dropped_data
        compress
        if "`output_file'" != "$dropped_data_file" {
            if "$staging_name" == "mi_ratio" {
                noisily saveold "${${staging_name}_staging_storage}/`output_file'", replace
            }
            else {
                display "Writing to ------------------------------------------------------------:"
                display "${${staging_name}_storage}/`output_file'"
                noisily saveold "${${staging_name}_storage}/`output_file'", replace
            }
        }

    // save archive copy
        if "`output_file'" == "$dropped_data_file" local archive_file = "$dropped_data_file"      
        else local archive_file = subinstr("${${staging_name}_staging_workspace}/_archive/`output_file'", ".dta", "_$today", 1)

        // make_directory_tree, path("`archive_file'")
        saveold "`archive_file'", replace
end
** *****************************************************************************
**  Define record_and_drop
**         Description: saves a list of the data dropped at the indicated section. then drops
** *****************************************************************************
capture program drop record_and_drop
program define record_and_drop
    //
//   quietly {
        // accept arguments
            args section

        // verify that to_drop has been created
            foreach v of varlist to_drop dropReason {
                capture confirm variable `v'
                if _rc {
                    noisily di in red "ERROR: variable 'v' must be defined to run record_and_drop"
                    BREAK
                }
            }

        // save all data in memory
            tempfile beforeDrop
            save `beforeDrop', replace

        // keep only the data that will be dropped
            noisily di "     Recording drop reason..."
            drop if to_drop != 1
            capture count
            if r(N) > 0 {
                keep location_id dataset* registry* year* sex acause dropReason
                duplicates drop

                // create list and save
                gen section = "`section'"
                capture confirm file "$dropped_data_file"
                if !_rc append using "$dropped_data_file", force
                save_staging_data, output_file("$dropped_data_file")
            }

        // restore input data and drop
            use `beforeDrop', clear
            drop if to_drop == 1
            drop to_drop dropReason
//   }
end

** *****************************************************************************
** Define add_representative_status
**         Description:
** *****************************************************************************
capture program drop add_representative_status
program define add_representative_status
    // save current version of data
        tempfile no_represent
        save `no_represent', replace

    // Load representative status
        do "${cancer_repo}/DATABASE" "registry"
        //use "FILEPATH", clear 
       keep registry_index representative_of_location_id location_id
        rename registry_index registry_index
        duplicates drop registry_index, force
        tempfile rep_info
        save `rep_info', replace

    // re-load current version of data and add representative status
        use `no_represent', clear
        merge m:1 location_id registry_index using `rep_info', keep(1 3) nogen
end

** *****************************************************************************
** Define add_coverage_status
** *****************************************************************************

capture program drop add_coverage_status
program define add_coverage_status
    // ensure that status is added on non-combined data
        capture confirm variable dataset_name
        if !_rc {
            capture count if regexm(dataset_name, "&" )
            if r(N) {
                noisily di "ERROR: add_coverage_status must be run before combining data sets"
                BREAK
            }
        }

    // generate coverage status
        capture gen coverage_of_location_id = 1 if substr(registry_index, -2, .) == ".1"
        replace coverage_of_location_id = 0 if coverage_of_location_id != 1
end

** *****************************************************************************
** Define gen_year_data to genearate a year_id variable as the average year
**         Description: Generates year as the average year and calculates span. Rounds fractional years DOWN
** *****************************************************************************
capture program drop gen_year_data
program define gen_year_data
    capture gen year_span = year_end - year_start + 1
    capture gen year_id = floor((year_start + year_end)/2)
end

** *****************************************************************************
** Define apply_year_restrictions
** *****************************************************************************
capture program drop apply_year_restrictions
program define apply_year_restrictions
    // ensure that year_id variable is present
        gen_year_data

    // apply year restrictions
        gen to_drop = 0
        replace to_drop = 1 if year_id < 1970
        gen dropReason = "year_id is less than minimum threshold (1970)" if to_drop == 1
        record_and_drop "preliminary drop and check"

end

** *****************************************************************************
** Define apply_age_restrictions
**         Description: drops age groups that contain no data or should not be modeled
** *****************************************************************************
capture program drop apply_age_restrictions
program define apply_age_restrictions
    tempfile all_ages
    save `all_ages', replace

    // generate age restrictions map
        import delimited using "$FILEPATH", clear
        if inlist("$staging_name", "cod_mortality", "mi_ratio") {
            keep acause yll_age_start yll_age_end
            rename (yll_age_start yll_age_end) (age_start age_end)
            drop if age_start == .
        }
        else if substr("$staging_name", 1, 8) == "nonfatal" {
            keep acause yld_age_start yld_age_end
            rename (yld_age_start yld_age_end) (age_start age_end)
        }
        foreach a of varlist age* {
            replace `a' = (`a'/5) + 6 if `a' > 0
            replace `a' = 2 if `a' == 0
        }
        tempfile age_restrictions
        save `age_restrictions', replace

    // restore data and merge with map
        use `all_ages', clear
        merge m:1 acause using `age_restrictions', keep(1 3) assert(2 3) nogen

    // apply age_group restrictions
        foreach n of numlist 3/6 91/94 {
            capture drop pop`n'
            capture drop deaths`n'
            capture drop cases`n'
        }
        if "$staging_name" == "cod_mortality" | regexm("$staging_name", "nonfatal") {
            foreach n of numlist 2 7/22 {
                capture replace deaths`n' = . if `n' < age_start | `n' > age_end
                capture replace cases`n' = . if `n' < age_start | `n' > age_end
            }
        }
        drop age_start age_end

end

** *****************************************************************************
** Define apply_cause_sex_restrictions
**             Description: applies cause and sex restrictions to the staging
** *****************************************************************************
capture program drop apply_cause_sex_restrictions
program define apply_cause_sex_restrictions
    // save current version of data
        tempfile all_cause_sex
        capture rename sex sex_id
        save `all_cause_sex', replace

    // Load cause restrictions
        import delimited using "$FILEPATH", clear
        if "$staging_name" == "mi_ratio" keep if mi_model == 1
        if "$staging_name" == "cod_mortality" keep if cod_model == 1
        keep acause male female
        rename (male female) (sex_male sex_female)
        reshape long sex, i(acause) j(gender) string
        drop if sex == 0
        replace sex = 1 if gender == "_male"
        replace sex = 2 if gender == "_female"
        rename sex sex_id
        drop gender
        tempfile cause_restrictions
        save `cause_restrictions', replace

    // re-load current version of data and apply cause and sex restrinctions
        use "`all_cause_sex'", clear
        merge m:1 acause sex_id using `cause_restrictions', keep(1 3)

    // mark and drop data
        gen to_drop = 0
        replace to_drop = 1 if _merge == 1
        gen dropReason = "data do not meet cause and sex restriction criteria" if to_drop == 1
        drop _merge
        record_and_drop "preliminary drop and check"

end

** *****************************************************************************
** Define apply_registry_restrictions
**             Description: applies registry restrictions, preventing use of un-modeled registries in the model
** *****************************************************************************
capture program drop apply_registry_restrictions
program define apply_registry_restrictions
        args section

    // save current version of data
        tempfile a
        save `a', replace
        do "$DATABASE" "registry"
        //use "FILEPATH", clear
       keep registry_index for_processing_only
        duplicates drop
        tempfile b
        save `b', replace

        use `a', clear
        merge m:1 registry_index using `b', keep(3) nogen
    // create drop variables
        gen to_drop = 0
        gen dropReason = ""

    // drop data from subnationally_modeled that could not be fit into a subnational location
        replace to_drop = 1 if subMod == 1 & location_id == country_id & substr(registry_index, -2, .) != ".1"
        replace to_drop = 0 if inlist(dataset_id, 805) & "`section'" == "mirs" //want to use national data from SEER when modeling MIRs only 
        replace dropReason = "data are from subnationally modeled location but does not fit criteria for subnational locations" if to_drop == 1

    // mark and drop data
        record_and_drop "preliminary drop and check"

end

** *****************************************************************************
** Define load_location_info
**        Description: adds location information used in the progams defined below.
** *****************************************************************************
capture program drop load_location_info
program define load_location_info
    // run quietly
    display "Inside load_location_info <------------------------------------"
    noisily {

        // save current version of data
            tempfile not_locations
            save `not_locations', replace

        // get the registry table in memory, keep certain columns, deduplicate, and save as 'locations'
        do "$DATABASE" "registry"
        //use "FILEPATH", clear
       keep  registry_id registry_index location_id country_id
        duplicates drop
        tempfile locations
        save `locations', replace
        // reload the current version of the data
        use `not_locations', clear
        // merge the current version of the data with the "locations" aka registry data
        merge m:1 registry_index using `locations', keep(3) assert(2 3)
        drop _merge
        capture drop subMod
        gen subMod = 0
        foreach num of numlist 6 11 16 51 62 67 72 86 90 93 95 102 130 135 142 163 165 179 180 196 214 {
            replace subMod = 1 if country_id == `num'
        }
    }
end


** *****************************************************************************
** Define drop_dataset_redundancy
**        Description: drops redundancies within a data set-registry
** *****************************************************************************
capture program drop drop_dataset_redundancy
program define drop_dataset_redundancy
    // Keep within-dataset redundancies with only the smallest year-span
    noisily di "Removing Within-dataset Redundancy"
    quietly {
        // Find redundancies
            sort dataType dataset* location_id sex registry_index acause year_id
            egen uid = concat(dataType dataset* location_id sex registry_index acause year_id), punct("_")
            duplicates tag uid, gen(duplicate)
            bysort uid: egen smallestSpan = min(year_span)

        // Mark non-best data
            gen to_drop = 0 if duplicate == 0
            replace to_drop = 0 if year_span == smallestSpan & duplicate > 0
            replace to_drop = 1 if year_span != smallestSpan & duplicate > 0
            gen dropReason = "data in same dataset has smaller year span" if to_drop == 1

        // Drop data
            record_and_drop "drop within dataset redundancy"
            drop uid duplicate smallestSpan
    }
end

** *****************************************************************************
** Define KeepNational
**         Description: Keep data with national coverage if present and the country is not modeled subnationally
** *****************************************************************************
capture program drop KeepNational
program define KeepNational
    // // Keep National Data where possible
    quietly {

        // ensure that required information is present
            if "$subnationally_modeled" == "" load_location_info
            else{
                foreach v of varlist country_id location_type subMod {
                    capture confirm variable `v'
                    if _rc load_location_info
                }
            }

        // Generate uid
            sort country_id location_id dataset* registry_index year_id sex acause dataType
            egen uid = concat(country_id year_id sex acause dataType), punct("_")
            sort uid
            gen to_drop = 0
            gen dropReason = ""

        // Mark 'to_drop' registry-years for which national data is present, excluding registries from countries that are modeled subnationally
            capture confirm variable coverage_of_location_id
            if _rc add_coverage_status
            rename coverage_of_location_id national_coverage
            replace national_coverage = 0 if location_id != country_id | subMod == 1 // ignore subnationally_modeled data
            bysort uid: egen has_national = total(national_coverage)
            replace to_drop = 1 if has_national > 0 & national_coverage != 1 & subMod != 1
            replace dropReason = "National data are present that supercede this registry" if to_drop == 1

        // record drop reason and drop data marked 'to_drop'
            capture count if to_drop == 1
            if r(N) > 0 {
                noisily di "Keeping National Data where Possible..."
                replace dropReason = "National data are present for the same location datapoint and country is not modeled subnationally" if to_drop == 1
                record_and_drop "Keep National"
            }
            drop uid has_national national_coverage
    }
end

** *****************************************************************************
** Define callKeepBest: calls the KeepBest script.
** *****************************************************************************
capture program drop callKeepBest
program define callKeepBest
    // set location of KeepBest Script
        local keepBest_script = "${staging_common_code}/KeepBest.do"

    // accept arguments
        args section

    // ensure that singel 'year_id' variable exists
        gen_year_data

    // Run KeepBest
        do `keepBest_script' "`section'"
        use "$temp_folder/best_data_`section'.dta", clear

end

** *****************************************************************************
** Define AlertIfOverlap Function
**         Purpoes: Checks for redundancy and pauses the script if redundancies are present
** *****************************************************************************
capture program drop AlertIfOverlap
program define AlertIfOverlap
    if "$staging_name" == "mi_ratio" { 
        duplicates tag sex location_id registry_index acause year_id age, gen(overlap)
        gsort -overlap +sex +registry_index +age +acause +year_id +dataset_id
    }
    else { 
        duplicates tag sex location_id registry_index acause year_id age, gen(overlap)
        gsort -overlap +sex +registry_index +age +acause +year_id +dataset_id 
    } 

    // Alert user if tags remain
    capture inspect(overlap)
    if r(N_pos) > 0 {
        display in red "Alert: Redundant registry_index-Years are Present"
        di r(N_pos)
        pause on
        pause
        pause off
    }
    drop overlap

end

** *****************************************************************************
** Define AlertIfAllDropped
**         Description: Alerts user if all data is dropped for the given id .
** *****************************************************************************
capture program drop AlertIfAllDropped
program define AlertIfAllDropped
    args id section uid

    capture count if groupid == `id' & to_drop == 0
    if r(N) == 0 {
        pause on
        di in red "All registry_indexies dropped for `uid' (id `id') during `section'"
        pause
        pause off
    }

end

** *****************************************************************************
** Define removeConflicts
**        Description: Removes redundancy due to overlapping coverage
** *****************************************************************************
capture program drop removeConflicts
program define removeConflicts
    // save copy of current data
        tempfile pre_map
        save `pre_map', replace

    // ensure that required information is present
        if "$subnationally_modeled" == "" load_location_info

    // Create map of which registries are contained in other registries
        do "$DATABASE" "registry"
        //use "FILEPATH", clear 
       preserve
            keep registry_index coverage_of_location_id
            rename registry_index registry_index
            duplicates drop registry_index, force
            tempfile coverage_map
            save `coverage_map', replace
        restore
            keep if parent_registry != ""
            keep registry_index parent_registry_index
            duplicates drop registry_index, force
            levelsof parent_registry_index, clean local(parents)
            tempfile parent_registry_map
            save `parent_registry_map', replace

    // Ensure presence of critical variables
        use `pre_map', clear
        capture gen to_drop = 0
        capture gen dropReason = ""
        // "Raise an error if there are any items in memory that aren't matches with using"
        merge m:1 registry_index using `coverage_map', keep(3) assert(2 3) nogen

    // Drop data for registries that are superceeded by data from another registry with full coverage of the same coverage_id (location_id)
        bysort location_id year_id sex acause age: egen has_location_coverage = total(coverage_of_location_id)
        replace to_drop = 1 if has_location_coverage != 0 & coverage_of_location_id != 1
        replace dropReason = "Data superceeded by registry with complete coverage of same location_id" if to_drop == 1 & dropReason == ""
        drop has_location_coverage

    // Drop data for subnationally modeled locations if data are not assigned to a subnational location_id
        foreach s in $subnationally_modeled {
            replace to_drop = 1 if country_id == `s' & location_id == 0
        }
        replace dropReason = "Subnational data could not be assigned to subnational location" if to_drop == 1 & dropReason == ""

    // Remove registries if the same data is captured by another registry with greater coverage
    di "removing child registries..."
    quietly  foreach p in `parents' {
            preserve
                use `parent_registry_map', clear
                keep if parent_registry_index == "`p'"
                levelsof registry_index, clean local(registry_child)
           restore
           gen is_parent = 1 if registry_index == "`p'"
            gen is_child = 0
            foreach r in `registry_child' {
                replace is_child = 1 if registry_index == "`r'"
            }
            bysort location_id year_id sex acause age: egen has_parent  = total(is_parent)
            replace to_drop = 1 if has_parent != 0 & is_child == 1 & is_parent != 1
            replace dropReason = "Same registry data are contained in registry_index `p'" if to_drop == 1 & dropReason == ""
            drop is_parent is_child has_parent
        }

    // Drop data, recording the drop reason
        record_and_drop "Remove Conflicts"

end


** *****************************************************************************
** Define combine_entries
**        Description: saves a list of the datasets and registries that are combined to make a single datapoint by unique id (uid) supplied (e.g. location-year_id-sex)
** *****************************************************************************
capture program drop combine_inputs
program define combine_inputs
    syntax, adjustment_section(string) combined_variables(string) nid_var(string) metric_variables(string) dataset_combination_map(string) [use_mean_pop(string) uid_variables(string)]

    // Alert User
        noisily di "Checking for rows to combine..."

    quietly {
        // check for presence of a map
            if "`uid_variables'" == "" local uid_variables = "location_id year_id acause"
            local adjustment_section = subinstr("`adjustment_section'", " ", "_", .)

        // Create a list of the variables to maintain on collapse
            local input_variables = ""
            foreach v of varlist _all {
                if substr("`v'", 1, 5) == "cases" | substr("`v'", 1,3) == "pop" | substr("`v'",1,6) == "deaths" continue
                if inlist("`v'", "registry_index", "dataset_id", "NID") continue
                if regexm("`metric_variables'", "`v'") continue
                local input_variables = "`input_variables' `v'"
            }


        // Ensure that at least one combined variable was correctly requested
            if !regexm("`combined_variables'", "dataset_id") & !regexm("`combined_variables'", "registry_index") & !regexm("`combined_variables'", "NID") {
                noisily di in red "ERROR: combined_variables incorrectly specified"
                BREAK
            }

        // Store a list of the
            local uncombined_variables = ""
            foreach var in registry_index dataset_id `nid_var' {
                if !regexm("`combined_variables'", "`var'") local uncombined_variables = "`uncombined_variables' `var'"
            }

            // Mark combined data
            local marker_vars = "`uid_variables'"
            if "`adjustment_section'" != "finalization" local marker_vars = trim(subinstr("`uid_variables'", "acause", "", 1)) // acause cannot be included since CoD requires dataset information to be the same across causes for a given location-year_id-sex
            egen uid = concat(`marker_vars'), punct(", ")
            bysort uid registry_index: gen new_input = _n == 1
            replace new_input = 0 if new_input == .
            bysort uid: egen total_inputs = total(new_input)  // mark data as 'to_combine' if there is more than one dataset
            gen combine_registry = 1 if total_inputs > 1
            drop new_input total_inputs
            bysort uid dataset_id: gen new_input = _n == 1
            replace new_input = 0 if new_input == .
            bysort uid: egen total_inputs = total(new_input)  // mark data as 'to_combine' if there is more than one dataset
            gen combine_dataset = 1 if total_inputs > 1
            drop new_input total_inputs
            bysort uid `nid_var': gen new_input = _n == 1
            replace new_input = 0 if new_input == .
            bysort uid: egen total_inputs = total(new_input)  // mark data as 'to_combine' if there is more than one dataset
            gen combine_NID = 1 if total_inputs > 1
            drop new_input total_inputs

        // Save copy of the data as it exists before
            tempfile before_combination
            save `before_combination', replace

        // // Exit if there are no data to combine.
            count if combine_registry == 1 | combine_dataset == 1 | combine_NID == 1
            if !r(N) {
                use `before_combination' , clear
                drop uid combine_registry combine_dataset combine_NID
                capture drop new_input total_inputs
                exit
            }

    }
    // // If script still running, save a record of the combined registries
        // Alert User
            noisily di "Combining Data..."

        // subset
            keep if combine_registry == 1 | combine_dataset == 1 | combine_NID == 1
        quietly {
            // ensure that variables are in string format
                foreach v of varlist registry_index {
                    capture tostring `v', replace
                    replace `v' = "" if `v' == "."
                }
            // keep only unique entries for those uids/fields that need to be combined
                keep `marker_vars' `combined_variables'  combine_registry combine_dataset combine_NID
                if regexm("`combined_variables'", "registry_index") {
                    bysort `marker_vars' registry_index: gen new_registry_index = 1 if _n == 1 & combine_registry == 1
                    replace registry_index = "" if new_registry_index != 1
                }
                if regexm("`combined_variables'", "dataset_id") {
                    bysort `marker_vars' dataset_id: gen new_dataset_id = 1 if _n == 1 & combine_dataset == 1
                    local comb_vs = trim("`comb_vs' dataset_")
                }
                if regexm("`combined_variables'", "NID") {
                    foreach n in `combined_variables' {
                        bysort `marker_vars' `nid_var': gen new_`n' = 1 if _n == 1 & combine_NID == 1
                }
                gen has_data = 0
                if regexm("`combined_variables'", "dataset_") {
                    foreach v in "dataset_ NID" {
                        replace has_data = 1 if "`v'" != ""
                    }
                }
                foreach v of varlist `combined_variables' {
                    replace has_data = 1 if `v' != ""
                    drop new_`v'
                }
                drop if has_data == 0
                bysort `marker_vars': gen id_num = _n
                reshape wide `combined_variables', i(`marker_vars' combine_registry combine_dataset combine_NID) j(id_num)
            // combine field entries
            local reshape_vars = ""
            if regexm("`combined_variables'", "registry_index") {
                gen reg_combo = ""
                foreach r of varlist registry_index* {
                    replace reg_combo = trim(reg_combo + " + " + `r') if `r' != "" & combine_registry == 1
                }
                replace reg_combo = substr(reg_combo, 3, .) if substr(reg_combo, 1, 2) == "+ "
                gen registries_`adjustment_section' = reg_combo
                drop registry_index* reg_combo
                local reshape_vars = trim("`reshape_vars' registries_")
            }
            if regexm("`combined_variables'", "dataset_id") {
                gen ds_combo = ""
                foreach d of varlist dataset_* {
                    replace ds_combo = trim(ds_combo + " + " + `d') if `d' != ""
                }
                replace ds_combo = substr(ds_combo, 3, .) if substr(ds_combo, 1, 2) == "+ "
                gen datasets_`adjustment_section' = ds_combo
                drop dataset_* ds_combo
            }
            if regexm("`combined_variables'", "NID") {
                rename NID* nid*
                gen NID_combo = ""
                foreach n of varlist nid* {
                    replace NID_combo = trim(NID_combo + " + " + "`n'") if "`n'" != "" & combine_NID == 1
                }
                replace NID_combo = substr(NID_combo, 3, .) if substr(NID_combo, 1, 2) == "+ "
                gen NIDs_`adjustment_section' = NID_combo
                drop nid* NID_combo
                local reshape_vars = trim("`reshape_vars' NIDs_")
            }
            // merge combined fields with existing map (if present) and save
            drop combine*
            reshape long `reshape_vars',  i(`marker_vars') j(adjustment_section) string
            if "`adjustment_section'" == "finalization" {
                capture gen acause = ""
                replace acause = "multiple causes (required for CoD - must be one site per uid)"
            }
            capture confirm file "`dataset_combination_map'"
            if "`adjustment_section'" != "prep" & !_rc append using "`dataset_combination_map'"
            save "`dataset_combination_map'", replace
        }
    // add generic entries for combined uid-fields, then combine the data
        use `before_combination', clear
        replace registry_index = "combined_registries" if combine_registry == 1 & regexm("`combined_variables'", "registry_index")
        gen dataset_id_tmp = string(dataset_id)
        drop dataset_id
        rename dataset_id_tmp dataset_id
        tostring dataset_id, replace
        replace dataset_id = "combined_datasets" if combine_dataset == 1 & regexm("`combined_variables'", "dataset_id")
        replace `nid_var' = "284465" if combine_NID == 1 & regexm("`combined_variables'", "NID") // merged NID

    // drop extraneous data and collapse
        keep `input_variables' `metric_variables' registry_index dataset_id `nid_var'

    // collapse (population and cases added together)
        /*
        if `use_mean_pop' {
            local metrics_no_pop = trim(subinstr("`metric_variables'", "pop*", "", .))
            collapse (sum) `metrics_no_pop' (mean) pop*, by(`input_variables' registry_index dataset_id `nid_var')
        }*/
        collapse (sum) `metric_variables', by(`input_variables' registry_index dataset_id `nid_var') fast
        // verify that there is only one entry per combined variable
        local to_check = trim("`uid_variables' `uncombined_variables'")
        check_for_redundancy, uid_variables("`to_check'") variables_to_check("`combined_variables'")

    //  ensure population consistency
        //ensure_pop_consistency, uid_variables("location_id year_id sex registry_index")
        }
end

** *****************************************************************************
** END
** *****************************************************************************
