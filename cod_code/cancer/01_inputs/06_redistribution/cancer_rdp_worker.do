
// Purpose:        Formats for redistribution, runs redistribution, and then reformats it back to CoD format

** **************************************************************************
** ANALYSIS CONFIGURATION
** **************************************************************************
// Clear memory and set memory and variable limits
        clear all
        set more off

// accept arguments
    args data_set_group data_set_name data_type split_group
    if "$troubleshoot_mode" != "1" global troubleshoot_mode = 0

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(6) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'") split_group("`split_group'")

// set folders
    local rdp_temp_folder = r(temp_folder)
    local rdp_parameters = r(rdp_parameters)
    local location_hierarchy  = "$location_hierarchy_file"
    local rdp_cause_map_data_type = "${rdp_cause_map_`data_type'}"
    local actual_rdp_script = "$code_for_this_prep_step/redistribution_cancer.py"
    local temp_folder "`rdp_temp_folder'/split_`split_group'"
    make_directory_tree, path("`temp_folder'")

// Inputs and outputs
    local input_folder "`rdp_temp_folder'/_input_data"
    local non_decimal_cause_map = "`temp_folder'/non_decimal_cause_map.dta"
    local pre_rdp_data = "`temp_folder'/pre_rdp_data.dta"
    local rdp_script_input = "`temp_folder'/rdp_script_input.dta"
    local rdp_script_output = "`temp_folder'/rdp_script_output.dta"
    local final_output = "`temp_folder'/final_post_rdp_split_`split_group'_data.dta"

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// Set source label tag to 0 (we don't have this in cancer but do in CoD)
    local source_label_tag = 0

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
// enable pause feature if $troubleshoot_mode
    if $troubleshoot_mode pause on

// Get data
    import delimited "`input_folder'/split_`split_group'.csv", varnames(1) case(preserve) clear

// Reformat string variables to remove leading apostrophe
    foreach var of varlist * {
        capture replace `var' = subinstr(`var',"'","",1)
    }

// Collapse data down
    collapse (sum) cases*, by(location_id registry_id year_start year_end sex coding_system acause split_group) fast

// Save a before so we can merge things on later
    // Rename
        foreach i of numlist 1/26 {
            capture gen cases`i' = 0
            rename cases`i' orig_cases`i'
        }

    // Save
        save "`pre_rdp_data'", replace

    // Rename back
        foreach i of numlist 1/26 {
            rename orig_cases`i' cases`i'
        }

    // calculate the pre-rdp total for later comparison
        capture summ(cases1)
        local pre_rdp_total = r(sum)

// Reshape age wide
    egen uid = group(location_id registry_id year_start year_end sex coding_system acause), missing
    reshape long cases, i(uid) j(gbd_age)
    drop uid

// // Verify that split group needs redistribution
    local noGarbage = 0

    // Verify that the split group contains data that are not zeros. If non-zero data exists, drop causes that have 0 cases.
        count if cases == 0 | cases == .
        if r(N) != _N drop if cases == 0 | cases == .
        else local noGarbage = 1

    // Verify that the split group contains un-mapped codes by testing if any data begins with a capital letter or a number
        count if substr(acause, 1, 1) == upper(substr(acause, 1, 1))  | real(substr(acause, 1, 1)) != .
        if r(N) == 0 local noGarbage = 1

// Convert GBD age to age groups
    gen age = .
    replace age = 0 if gbd_age == 2
    replace age = 1 if gbd_age == 3
    replace age = (gbd_age - 6) * 5 if (gbd_age - 6) * 5 >= 5 & (gbd_age - 6) * 5 <= 80
    drop if age == .
    drop gbd_age

// Merge on location hierarchy metadata
    merge m:1 location_id using "`location_hierarchy'", keep(1 3) keepusing(location_id dev_status super_region region country subnational_level1 subnational_level2) assert(2 3) nogen

// Rename variables
    rename acause cause
    rename cases freq

// create a map to revert causes from non-decimal form. then remove decimals from causes to enable rdp
    preserve
        keep cause
        duplicates drop
        gen rdp_cause = subinstr(cause, ".", "", .)
        save "`non_decimal_cause_map'", replace
    restore
    replace cause = subinstr(cause, ".", "", .)

// Save intermediate file for RDP
    saveold "`rdp_script_input'", replace
    if `noGarbage' {
        keep registry_id year_start year_end sex coding_system split_group age cause freq
        saveold "`rdp_script_output'", replace
    }

// Get code_version
    levelsof(coding_system), local(code_version) clean
    assert    word("`code_version'", 2) == ""

// Prep resources for redistribution (and afterwards)
    // Get source label if needed
        if `source_label_tag' == 1 {
            levelsof(source_label), local(source_label) clean
        }

    // Get packagesets_ids
        use "${packagesets_`code_version'}", clear
        if `source_label_tag' == 1 {
            keep if source_label == "`source_label'"
        }
        count
        if `r(N)' == 0 {
            display in red "THERE AREN'T ANY SOURCE LABELS TAGGED `source_label' IN ${packagesets_`code_version'}"
            display in red "Failing redistribution now... bye-bye"
            BREAK
        }
        else if `r(N)' > 1 {
            display in red "THERE TOO MANY CODE SYSTEMS IN ${packagesets_`code_version'} `source_label'"
            display in red "Failing redistribution now... bye-bye"
            BREAK
        }
        levelsof(package_set_id), local(package_set_id) clean

// Run redistribution (no magic tables for cancer)
    local magic_table = 0
    if !`noGarbage' {
        !python "`actual_rdp_script'" "`temp_folder'" "`rdp_parameters'" "`package_set_id'" "`split_group'" "`magic_table'"
    }

// Get redistributed data
    use "`rdp_script_output'", clear
    compress

// rename freq
    rename freq cases

// Replace lingering ZZZ with CC code
    replace cause = "cc_code" if cause == "ZZZ"

// Convert age groups to GBD age
    tostring(age), replace format("%12.2f") force
    destring(age), replace
    gen gbd_age = .
    replace gbd_age = (age/5) + 6 if age * 5 >= 5 & age <= 80
    replace gbd_age = 2 if age == 0
    replace gbd_age = 3 if age == 1
    drop age
    rename gbd_age age

// reformat acause
    // convert back to cancer cause
        rename cause rdp_cause
        capture merge m:1 rdp_cause using "`non_decimal_cause_map'", keep(1 3) assert(1 3)
        if _rc {
            di "ERROR, not all ICD codes converted back to decimal causes"
            if $troubleshoot_mode pause
            else BREAK
        }
        replace cause = rdp_cause if _merge == 1
        drop rdp_cause _merge

        //    merge with cause map
            replace cause = trim(itrim(cause))
            merge m:1 coding_system cause using "`rdp_cause_map_data_type'", keep(1 3)

        // verify merge
            capture rm "`temp_folder'/bad_codes.dta"
            gen bad_code = 1 if _merge == 1 & cause != "cc_code"
            count if bad_code == 1

        // if some codes don't merge, save them to a list
            if r(N) {
                preserve
                    display in red "The following causes are not in the cause map:"
                    levelsof cause if bad_code == 1
                    keep if bad_code == 1
                    gen data_set_name = "`data_set_name'"
                    gen data_type = "`data_type'"
                    keep cause coding_system split_group
                    duplicates drop
                    save "`temp_folder'/bad_codes.dta", replace
                restore
            }
            drop bad_code

        // break if invalid entries exist
            save "`temp_folder'/mapped_rdp_result.dta", replace
            gen acause = gbd_cause if _merge == 3 & gbd_cause != "_gc"
            replace acause = cause if (_merge == 1 | gbd_cause == "_gc")
            replace cause = "" if _merge == 3 & acause != ""
            count if acause == "" | acause == "_gc"
            if r(N) > 0 {
                di "ERROR, not all data mapped to causes"
                if $troubleshoot_mode pause
                else BREAK
            }
            rename _merge causeMap_merge
            drop gbd_cause

    // Collapse to merge data
        collapse (sum) cases, by(acause registry_id year_start year_end sex coding_system split_group age) fast

    // Reshape
        egen uid = group(acause registry_id year_start year_end sex coding_system split_group), missing
        reshape wide cases, i(uid) j(age)
        drop uid

    // Merge with original data source
        merge 1:1 registry_id year_start year_end sex coding_system acause split_group using "`pre_rdp_data'"
        rename _merge beforeafter

    // Verify that metrics are within acceptable range
        preserve
            // calculate totals
                summ (orig_cases1)
                if round(r(sum)) != round(`pre_rdp_total') {
                    display "Error in `data_set_name' (`data_type', split `split_group'): Total number of 'original' events post-rdp does not equal number of 'original' events pre-rdp (`pre_rdp_total' before, `r(sum)' after')"
                    if $troubleshoot_mode pause
                    else BREAK
                }

            // check the dataset total
                egen cases1 = rowtotal(cases*)
                summ(cases1)
                local post_total = r(sum)
                local delta = round(`post_total') - round(`pre_rdp_total')
                if abs(`delta') > 0.005 * `pre_rdp_total'  {
                    noisily di in red "Error in `data_set_name' (`data_type', split `split_group'): Total cases before rdp does not equal total after. `pre_rdp_total' events before, `post_total' after. A difference of `delta' events"
                    if $troubleshoot_mode pause
                    else BREAK
                }
                summ(cases1) if substr(acause, 1, 4) == "neo_"
                local post_total_neo_ = r(sum)
                summ (orig_cases1) if substr(acause, 1, 4) == "neo_"
                local pre_rdp_total_neo_ = r(sum)
                if (`pre_rdp_total_neo_') > round(`post_total_neo_') + 1 {
                    noisily di in red "Error in `data_set_name' (`data_type', split `split_group'): Total mapped cases before rdp is less than total after. `pre_rdp_total_neo_' events before, `post_total_neo_' after."
                    if $troubleshoot_mode pause
                    else BREAK
                }
        restore

    // Reformat cases variables
        // Make sure all cases variables exist
            foreach i of numlist 2/26 {
                capture gen cases`i' = 0
                replace cases`i' = 0 if cases`i' == .
            }
        // Recalculate aggregates
            aorder
            egen double cases1 = rowtotal(cases2-cases26)


    // Fill in 0s where needed
        foreach var of varlist cases* orig_cases* {
            replace `var' = 0 if `var' == .
        }

    // Get max before after
        egen temp = max(beforeafter), by(sex coding_system acause registry_id year_start year_end)
        replace beforeafter = temp

    // Collapse to acause level
        collapse(sum) *cases*, by(sex coding_system acause registry_id year_start year_end beforeafter) fast
        gen split_group = `split_group'
        order registry_id year_start year_end sex coding_system acause cases* orig_cases* beforeafter split_group

    // Save
        capture _strip_labels*
        compress
        save "`final_output'", replace

    if $troubleshoot_mode pause off
    capture log close

** **************************************************************************
**  END cancer_rdp_worker.do
** **************************************************************************
