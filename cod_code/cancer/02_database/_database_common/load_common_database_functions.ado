*****************************************
** Description: Loads functions used by */02_database scripts
** Input(s)/Output(s): see individual functions
** How To Use: called by */02_database scripts

*****************************************
// Load Function to Ensure Population Consistency
    capture program drop ensure_pop_consistency
    do "$common_cancer_code/data_consistency_functions.do"

** ****************************************************************
** Define save_database_data
**         Purpose: saves data as current version and archive copy
** ****************************************************************
capture program drop save_database_data
program define save_database_data
    syntax, output_file(string)

    //compress and save. do not save if dropped_data
        compress
        if "`output_file'" != "dropped_data" {
            noisily saveold "${${database_name}_database_storage}/`output_file'", replace
        }

    // save archive copy
        if "`output_file'" == "dropped_data" local archive_file = "$dropped_data_file"
        else local archive_file = subinstr("${${database_name}_database_workspace}/_archive/`output_file'", ".dta", "_$today.dta", 1)
        make_directory_tree, path("`archive_file'")

        saveold "`archive_file'", replace
end

** ****************************************************************
**  Define record_and_drop
**         Purpose: saves a list of the data dropped at the indicated section. then drops
** ****************************************************************
capture program drop record_and_drop
program define record_and_drop
    //
    quietly {
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
                keep location_id data_set* registry* year* sex acause dropReason
                duplicates drop

                // create list and save
                gen section = "`section'"
                capture confirm file "$dropped_data_file"
                if !_rc    append using "$dropped_data_file", force
                save_database_data, output_file("dropped_data")
            }

        // restore input data and drop
            use `beforeDrop', clear
            drop if to_drop == 1
            drop to_drop dropReason
    }
end

** ****************************************************************
** Define add_representative_status
**         Purpose:
** ****************************************************************
capture program drop add_representative_status
program define add_representative_status
    add_coverage_status
    rename full_coverage representative
end

** ****************************************************************
** Define
**         Purpose:
** ****************************************************************
capture program drop add_coverage_status
program define add_coverage_status
    // ensure that status is added on non-combined data
        capture confirm variable data_set_name
        if !_rc {
            capture count if regexm(data_set_name, "&" )
            if r(N) {
                noisily di "ERROR: add_coverage_status must be run before combining data sets"
                BREAK
            }
        }

    // generate coverage status
        capture gen full_coverage = 1 if substr(registry_id, -2, .) == ".1"
        replace full_coverage = 0 if full_coverage != 1
end

** ****************************************************************
** Define gen_year_data to genearate a year variable as the average year
**         Purpose: Generates year as the average year and calculates span. Rounds fractional years DOWN
** ****************************************************************
capture program drop gen_year_data
program define gen_year_data
    capture gen year_span = year_end - year_start + 1
    capture gen year = floor((year_start + year_end)/2)
end

** ****************************************************************
** Define apply_year_restrictions
**         Purpose:
** ****************************************************************
capture program drop apply_year_restrictions
program define apply_year_restrictions
    // ensure that year variable is present
        gen_year_data

    // apply year restrictions
        gen to_drop = 0
        replace to_drop = 1 if year < 1970
        gen dropReason = "year is less than minimum threshold (1970)" if to_drop == 1
        record_and_drop "preliminary drop and check"

end

** ****************************************************************
** Define apply_age_restrictions
**         Purpose: drops age groups that contain no data or should not be modeled
** ****************************************************************
capture program drop apply_age_restrictions
program define apply_age_restrictions
    tempfile all_ages
    save `all_ages', replace

    // generate age restrictions map
        import delimited using "$common_cancer_data/causes.csv", clear
        if inlist("$database_name", "cod_mortality", "mi_ratio") {
            keep acause yll_age_start yll_age_end
            rename (yll_age_start yll_age_end) (age_start age_end)
            drop if age_start == .
        }
        else if substr("$database_name", 1, 8) == "nonfatal" {
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
        if "$database_name" == "cod_mortality" | regexm("$database_name", "nonfatal") {
            foreach n of numlist 2 7/22 {
                capture replace deaths`n' = . if `n' < age_start | `n' > age_end
                capture replace cases`n' = . if `n' < age_start | `n' > age_end
            }
        }
        drop age_start age_end

end

** ***************************************************************************
** Define apply_cause_sex_restrictions
**             Purpose: applies cause and sex restrictions to the database
** ****************************************************************************
capture program drop apply_cause_sex_restrictions
program define apply_cause_sex_restrictions
    // save current version of data
        tempfile all_cause_sex
        save `all_cause_sex', replace

    // Load cause restrictions
        import delimited using "$common_cancer_data/causes.csv", clear
        if "$database_name" == "mi_ratio" keep if mi_model == 1
        if "$database_name" == "cod_mortality" keep if cod_model == 1
        keep acause male female
        rename (male female) (sex_male sex_female)
        reshape long sex, i(acause) j(gender) string
        drop if sex == 0
        replace sex = 1 if gender == "_male"
        replace sex = 2 if gender == "_female"
        drop gender
        tempfile cause_restrictions
        save `cause_restrictions', replace

    // re-load current version of data and apply cause and sex restrinctions
        use "`all_cause_sex'", clear
        merge m:1 acause sex using `cause_restrictions', keep(1 3)

    // mark and drop data
        gen to_drop = 0
        replace to_drop = 1 if _merge == 1
        gen dropReason = "data do not meet cause and sex restriction criteria" if to_drop == 1
        drop _merge
        record_and_drop "preliminary drop and check"

end

** ***************************************************************************
** Define apply_registry_restrictions
**             Purpose: applies registry restrictions, preventing use of un-modeled registries in the model
** ****************************************************************************
capture program drop apply_registry_restrictions
program define apply_registry_restrictions
    // create drop variables
        gen to_drop = 0
        gen dropReason = ""

    // mark to_drop registries that should not be modeled
        replace to_drop = 1 if substr(registry_id, 1, 2) == "0." & model == 0
        replace dropReason = "registry is from a non-modeled location" if to_drop == 1

    // drop data from subnationally_modeled that could not be fit into a subnational location
        replace to_drop = 1 if subMod == 1 & location_id == country_id & substr(registry_id, -2, .) != ".1"
        replace dropReason = "data are from subnationally modeled location but does not fit criteria for subnational locations" if to_drop == 1

    // mark and drop data
        record_and_drop "preliminary drop and check"

end

** ****************************************************************************
** Define load_location_info
**        Purpose: adds location information used in the progams defined below
** *****************************************************************************
capture program drop load_location_info
program define load_location_info
    syntax, [apply_restricitons(string)]

    // run quietly
        quietly{

        // save current version of data
            tempfile not_locations
            save `not_locations', replace

        // create map of location information and set global defining subnational locations
            import delimited using "$common_cancer_data/modeled_locations.csv", clear
            keep if !inlist(location_type, "global", "superregion", "region")

            // create list of subnationally modeled countries
            gen subnationally_modeled = 0
            replace subnationally_modeled = 1 if location_type != "admin0" & parent_type !="region"
            levelsof country_id if subnationally_modeled == 1, clean local(subnationally_modeled)
            global subnationally_modeled : list uniq subnationally_modeled

            // verify that map is complete
            foreach v of varlist location_id country_id location_type developed {
                count if inlist("`v'", "." , "")
                if r(N) {
                    noisily di in red "ERROR: Error when creating location map. Some `v' information is missing from modeled_locations.csv"
                    BREAK
                }
            }

            // save map
            keep location_id country_id location_type ihme_loc_id developed is_estimate
            rename is_estimate model
            tempfile location_info
            save `location_info', replace

        // reload current version of data and add location information
            use `not_locations', clear
            if "`apply_restrictions'" == "true" local thats_nice = "yes it is. don't do anything else differently for now."
            foreach i in country_id location_type developed ihme_loc_id is_estimate {
                capture drop `i'
            }
            merge m:1 location_id using `location_info', keep(3) assert(2 3) nogen

        // // Mark sub-nationally modeled subnational data
            capture drop subMod
            gen subMod = 0
            foreach sub_mod in $subnationally_modeled {
                noisily di "Marking country `sub_mod' as subnational"
                replace subMod = 1 if country_id == `sub_mod'
            }

        }
end


** ****************************************************************************
** Define drop_dataset_redundancy
**        Purpose: drops redundancies within a data set-registry based on current assumptions
** *****************************************************************************
capture program drop drop_dataset_redundancy
program define drop_dataset_redundancy
    // Keep within-dataset redundancies with only the smallest year-span
    noisily di "Removing Within-data_set Redundancy"
    quietly {
        // Find redundancies
            sort dataType data_set* location_id sex registry_id acause year
            egen uid = concat(dataType data_set* location_id sex registry_id acause year), punct("_")
            duplicates tag uid, gen(duplicate)
            bysort uid: egen smallestSpan = min(year_span)

        // Mark non-best data
            gen to_drop = 0 if duplicate == 0
            replace to_drop = 0 if year_span == smallestSpan & duplicate > 0
            replace to_drop = 1 if year_span != smallestSpan & duplicate > 0
            gen dropReason = "data in same data_set has smaller year span" if to_drop == 1

        // Drop data
            record_and_drop "drop within data_set redundancy"
            drop uid duplicate smallestSpan
    }
end

** ****************************************************************************
** Define KeepNational
**         Purpose: Keep data with national coverage if present and the country is not modeled subnationally
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
            sort country_id location_id data_set* registry_id year sex acause dataType
            egen uid = concat(country_id year sex acause dataType), punct("_")
            sort uid
            gen to_drop = 0
            gen dropReason = ""

        // Mark 'to_drop' registry-years for which national data is present, excluding registries from countries that are modeled subnationally
            capture confirm variable full_coverage
            if _rc add_coverage_status
            rename full_coverage national_coverage
            replace national_coverage = 0 if location_id != country_id |subMod == 1 // ignore subnationally_modeled data
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

** ****************************************************************
** Define callKeepBest: calls the KeepBest script.
**        Purpose: call the KeepBest script
** ****************************************************************
capture program drop callKeepBest
program define callKeepBest
    // set location of KeepBest Script
        local keepBest_script = "${database_common_code}/KeepBest.do"

    // accept arguments
        args section

    // ensure that singel 'year' variable exists
        gen_year_data

    // Run KeepBest
        do `keepBest_script' "`section'"
        use "$temp_folder/best_data_`section'.dta", clear

end

** ****************************************************************
** Define AlertIfOverlap Function
**         Purpoes: Checks for redundancy and pauses the script if redundancies are present
** ****************************************************************
capture program drop AlertIfOverlap
program define AlertIfOverlap
    duplicates tag sex location_id registry_id acause year, gen(overlap)
    gsort -overlap +sex +location_id +registry_id +acause +year

    // Alert user if tags remain
    capture inspect(overlap)
    if r(N_pos) > 0 {
        sort overlap sex location_id acause year registry_id data_set*
        display in red "Alert: Redundant registry_id-Years are Present"
        pause on
        pause
        pause off
    }
    drop overlap

end

** ****************************************************************
** Define AlertIfAllDropped
**         Purpose: Alerts user if all data is dropped for the given id .
** ****************************************************************
capture program drop AlertIfAllDropped
program define AlertIfAllDropped
    args id section uid

    capture count if groupid == `id' & to_drop == 0
    if r(N) == 0 {
        pause on
        di in red "All registry_idies dropped for `uid' (id `id') during `section'"
        pause
        pause off
    }

end

** ****************************************************************
** Define removeConflicts
**        Purpose: Removes redundancy due to overlapping coverage
** ****************************************************************
capture program drop removeConflicts
program define removeConflicts
    // save copy of current data
        tempfile pre_map
        save `pre_map', replace

    // ensure that required information is present
        if "$subnationally_modeled" == "" load_location_info

    // Create map of which registries are contained in other registries
        import delimited using "$registry_database_storage/registry_table.csv", varnames(1) clear
        preserve
            keep registry_id full_coverage
            tempfile coverage_map
            save `coverage_map', replace
        restore
        keep if parent_registry != ""
        keep registry_id parent_registry
        local overcast_list_length = _N
        tempfile overcast_map
        save `overcast_map', replace

    // Ensure presence of critical variables
        use `pre_map', clear
        capture gen to_drop = 0
        capture gen dropReason = ""
        merge m:1 registry_id using `coverage_map', keep(3) assert(2 3) nogen

    // Drop data for registries that are superceeded by data from another registry with full coverage of the same coverage_id (location_id)
        bysort location_id year sex acause: egen has_location_coverage = total(full_coverage)
        replace to_drop = 1 if has_location_coverage != 0 & full_coverage != 1
        replace dropReason = "Data superceeded by registry with complete coverage of same location_id" if to_drop == 1 & dropReason == ""
        drop has_location_coverage

    // Drop data for subnationally modeled locations if data are not assigned to a subnational location_id
        foreach s in $subnationally_modeled {
            replace to_drop = 1 if country_id == `s' & location_id == 0
        }
        replace dropReason = "Subnational data could not be assigned to subnational location" if to_drop == 1 & dropReason == ""

    // Remove registries if the same data is captured by another registry with greater coverage
        tempfile short_list
        quietly foreach row of numlist 1/`overcast_list_length' {
            preserve
                use `overcast_map', clear
                keep if _n == `row'
                levelsof parent_registry, clean local(registry_parent)
                if "`registry_parent'" == "" continue
                save `short_list', replace
            restore
            merge m:1 registry_id using `short_list', keep(1 3) nogen
            gen overcasting = 1 if registry_id == "`registry_parent'"
            replace overcasting = 0 if overcasting != 1
            bysort location_id year sex acause: egen has_overcast = total(overcasting)
            replace to_drop = 1 if has_overcast != 0 & parent_registry == "`registry_parent'" & overcasting != 1
            replace dropReason = "Same registry data are contained in registry_id `registry_parent'" if to_drop == 1 & dropReason == ""
            drop parent_registry overcasting has_overcast
        }

    // Drop data, recording the drop reason
        record_and_drop "Remove Conflicts"

end

** ****************************************************************
** Define addNIDs
**        Purpose: adds source NIDs to the selected data
** ****************************************************************
capture program drop addNIDs
program define addNIDs
    syntax, [data_type(string) merge_variable(string) section_id(string)]

    if "`merge_variable'" == "" local merge_variable = "data_set_name"

    // load NID map
    preserve
        import delimited using "$nqry_table", varnames(1) clear
        rename nid NID
        if "`data_type'"== "2" keep if inlist(data_type, 1, 2)
        if "`data_type'" == "3" keep if inlist(data_type, 1, 3)
        drop data_type
        keep NID underlying_nid year_start year_end data_set_name registry_id
        rename data_set_name `merge_variable'
        rename (year_start year_end) (nid_year_start nid_year_end)
        tempfile NID_map
        save `NID_map'
    restore
    pause on

    //
    joinby registry_id `merge_variable' using `NID_map', unmatched(master)
    capture count if _merge == 1
    if r(N) {
        noisily di "`section_id': `r(N)' datapoints do not exist on the nqry table"
        //pause
    }
    gen to_drop = 0
    gen dropReason = ""
    bysort registry_id data_set_name: gen within_years = 1 if year_start >= nid_year_start & year_end <= nid_year_end
    replace to_drop = 1 if within_years != 1 & _merge == 3
    replace dropReason = "`section_id': datapoint does not exist in selected data type" if to_drop == 1 & dropReason == ""
    duplicates tag sex location_id registry_id acause year to_drop, gen(redundant)
    bysort location_id registry_id year sex acause to_drop: egen has_underlying = total(underlying_nid)
    replace to_drop = 1 if redundant == 1 & underlying_nid == . & has_underlying != 0
    replace dropReason = "`section_id': nid has range that is redundant with an underlying nid" if to_drop == 1 & dropReason == ""

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
            noisily di "`section_id': `r(N)' datapoints are on the nqry table but could not be matched to an NID"
            //pause
        restore
        drop if possible_data == dropped_rows  & _merge == 3
        append using `no_good_match'
    }
    count if to_drop == 1 & _merge == 1 // extra test to ensure that no bugs were introduced
    assert !r(N)
    drop if to_drop == 1
    drop _merge to_drop dropReason nid_year* within_years redundant has_underlying exists possible_data dropped_rows
    count if NID == .
    if r(N) {
        noisily di "ALERT: `section_id': `r(N)' datapoints are still missing NIDs!"
        //pause
    }
    capture tostring NID, replace
    replace NID = "284465" if NID == ""
    duplicates drop
    pause off

end

** ****************************************************************
** Define combine_entries
**        Purpose: saves a list of the data_sets and registries that are combined to make a single datapoint by unique id (uid) supplied (e.g. location-year-sex)
** ****************************************************************
capture program drop combine_inputs
program define combine_inputs
    syntax, adjustment_section(string) combined_variables(string) metric_variables(string) [use_mean_pop(string) uid_variables(string)]

    // Alert User
        noisily di "Checking for rows to combine..."

    // //
    quietly {
        // check for presence of a map
            local dataset_combination_map = "$cod_mortality_dataset_combo_map"
            local map_variables = "location_id year sex acause"
            local adjustment_section = subinstr("`adjustment_section'", " ", "_", .)

        // Create a list of the variables to maintain on collapse
            local input_variables = ""
            foreach v of varlist _all {
                if substr("`v'", 1, 5) == "cases" | substr("`v'", 1,3) == "pop" continue
                if inlist("`v'", "registry_id", "data_set_id", "NID") continue
                if regexm("`metric_variables'", "`v'") continue
                local input_variables = "`input_variables' `v'"
            }

        // Set the uid_variables, if not set
            if "`uid_variables'" == "" local uid_variables = "location_id year sex"

        // Ensure that at least one combined variable was correctly requested
            if !regexm("`combined_variables'", "data_set_id") & !regexm("`combined_variables'", "registry_id") & !regexm("`combined_variables'", "NID") {
                noisily di in red "ERROR: combined_variables incorrectly specified"
                BREAK
            }

        // Store a list of the
            local uncombined_variables = ""
            foreach var in registry_id data_set_id NID {
                if !regexm("`combined_variables'", "`var'") local uncombined_variables = "`uncombined_variables' `var'"
            }

        // Mark combined data
            egen uid = concat(`uid_variables'), punct(", ")
            bysort uid `combined_variables': gen new_input = _n == 1
            replace new_input = 0 if new_input == .
            bysort uid: egen total_inputs = total(new_input)  // mark data as 'to_combine' if there is more than one dataset
            gen to_combine = 1 if total_inputs > 1
            foreach v of varlist `combined_variables' {
                capture tostring `v', replace
            }

        // Save copy of the data as it exists before
            tempfile before_combination
            save `before_combination', replace

        // // Exit if there are no data to combine.
            count if to_combine == 1
            if !r(N) {
                use `before_combination' , clear
                drop uid new_input total_inputs to_combine
                exit
            }
    }

    // // If script still running, save a record of the combined registries
        // Alert User
            noisily di "Combining Registries..."

        // subset
            keep if to_combine == 1

        quietly {
            // determine which variables are present
                keep `map_variables' `combined_variables'
                if regexm("`combined_variables'", "registry_id") {
                    rename registry_id registry_
                    local comb_vs = "registry_"
                }
                if regexm("`combined_variables'", "data_set_id") {
                    rename data_set_id data_set_
                    local comb_vs = trim("`comb_vs' data_set_")
                }
                if regexm("`combined_variables'", "NID") {
                    rename NID nid_
                    local comb_vs = trim("`comb_vs' nid_")
                }

            // reshape
                bysort `map_variables': gen id_num = _n
                reshape wide `comb_vs', i(`map_variables') j(id_num)

            // create combined entries
            if regexm("`combined_variables'", "registry_id") {
                gen reg_combo = ""
                foreach r of varlist registry_* {
                    replace reg_combo = trim(reg_combo + " + " + `r') if `r' != ""
                }

                replace reg_combo = substr(reg_combo, 3, .) if substr(reg_combo, 1, 2) == "+ "
                gen registries_`adjustment_section' = reg_combo
                drop registry_* reg_combo
            }
            if regexm("`combined_variables'", "data_set_id") {
                gen ds_combo = ""
                foreach d of varlist data_set_* {
                    replace ds_combo = trim(ds_combo + " + " + `d') if `d' != ""
                }
                replace ds_combo = substr(ds_combo, 3, .) if substr(ds_combo, 1, 2) == "+ "
                gen datasets_`adjustment_section' = ds_combo
                drop data_set_* ds_combo
            }
            if regexm("`combined_variables'", "NID") {
                gen nid_combo = ""
                foreach n of varlist nid_* {
                    replace nid_combo = trim(nid_combo + " + " + `n') if `n' != ""
                }
                replace nid_combo = substr(nid_combo, 3, .) if substr(nid_combo, 1, 2) == "+ "
                gen NIDs_`adjustment_section' = nid_combo
                drop nid_* nid_combo
            }

            // merge with map and save
            if "`adjustment_section'" != "prep" merge 1:1 `map_variables' using "`dataset_combination_map'", nogen
            save "`dataset_combination_map'", replace
        }

    // combine registries
        use `before_combination', clear
        replace registry_id = "combined_registries" if to_combine == 1 & regexm("`combined_variables'", "registry_id")
        replace data_set_id = "combined_data_sets" if to_combine == 1 & regexm("`combined_variables'", "data_set_id")
        replace NID = "284465" if to_combine == 1 & regexm("`combined_variables'", "NID") // merged NID

    // drop extraneous data and collapse
        keep `input_variables' `metric_variables' registry_id data_set_id NID

    // collapse (population and cases added together)
        if `use_mean_pop' {
            local metrics_no_pop = trim(subinstr("`metric_variables'", "pop*", "", .))
            collapse (sum) `metrics_no_pop' (mean) pop*, by(`input_variables' registry_id data_set_id NID)
        }
        else {
            collapse (sum) `metric_variables', by(`input_variables' registry_id data_set_id NID) fast
        }

    // verify that there is only one entry per combined variable
        local to_check = trim("`uid_variables' `uncombined_variables'")
        check_for_redundancy, uid_variables("`to_check'") variables_to_check("`combined_variables'")

    //  ensure population consistency
        ensure_pop_consistency, uid_variables("location_id year sex registry_id")

end

** ***************
** END
** ***************
