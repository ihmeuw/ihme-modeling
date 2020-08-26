** *****************************************************************************
** Description: Loads compiled incidence and mortality data, merges them,
**      then selects a single datapoint for each location_id-year_id-sex-age-cancer
** Input(s): N/A
** Output(s): a formatted dta file with today's date
** How To Use: run script
** *****************************************************************************
// Clear workspace and set to run all selected code without pausing
    clear all
    set more off

// set staging name and gbd_iteration
    global staging_name = "mi_ratio"
    global gbd_iteration = 2019

    args vers
** *****************************************************************************
** Set macros and load additional resources
** *****************************************************************************
// Load and run set_common function based on the operating system
    if "$h" == "" & c(os) == "Windows" global h = "H:"
    else if "$h" == "" global h = "/homes/" + c(username)
    global staging_name = "mi_ratio"
    run "$h/cancer_estimation/stata_utils/set_common_roots.do" "staging" 
    global staging_common_code = "$root_staging_code/_staging_common"
    run "$staging_common_code/set_staging_common.ado"
    run "$staging_common_code/load_common_staging_functions.ado"

// Set folders
    local output_folder = "$mi_ratio_staging_storage"
    global temp_folder = "$mi_ratio_staging_workspace/$today"
    local nid_combo_map = "${temp_folder}/combo_map.dta"
    global dropped_data_file = "$mi_ratio_dropped_data"
    capture rm "`nid_combo_map'"
    make_directory_tree, path("$temp_folder")
    di "TEMPORARY FOLDER: $temp_folder"

// dataset_name map
    do "${cancer_repo}/_database/load_database_table" "dataset"
    keep dataset_id dataset_name
    tempfile dataset_name_map
    save `dataset_name_map'

** *****************************************************************************
** Define add_merge_name function
**         Description: generates merge_name variable to enable merges. merge_name is equal to the dataset name from the first character to the year designation.
**        Notes: generates merge_name variable
** *****************************************************************************
capture program drop add_merge_name
program add_merge_name
    // generate merge_name
        gen locationOfYear = strpos(dataset_name, "_1")
        replace locationOfYear = strpos(dataset_name, "_2") if  (locationOfYear == 0) | (strpos(dataset_name, "_2") < strpos(dataset_name, "_1") & strpos(dataset_name, "_2") != 0)
        gen merge_name = substr(dataset_name, 1, locationOfYear - 1)
        replace merge_name = dataset_name if merge_name == ""
        drop locationOfYear

    // check for duplicates
        duplicates tag merge_name registry_index sex_id acause year*, gen(duplicate)
        count if duplicate != 0
        if r(N) > 0 {
            noisily di "Error in merge process."
            noisily di "Please ensure that all data from the same dataset shares a common naming schema"
            noisily di "and that there is no data redundancy from a single dataset (by registry_index, sex, acause, and year_id)."
            BREAK
        }
        drop duplicate

end
** *****************************************************************************
** Part 1: Import Data and Drop Exclusions
** *****************************************************************************
// // Get INC data and Reformat to enable merge
    import delimited "$mi_datasets_staging_storage/GBD${gbd_iteration}/appended_p7_inc_version`vers'.csv", clear

    // merge dataset_name
    merge m:1 dataset_id using `dataset_name_map', assert(2 3) keep(3) nogen

    // drop unwanted causes and reshape wide
    rename sex sex_id
    rename nid NID_inc
    keep registry* year_start year_end sex_id acause dataset_id dataset_name age cases pop NID
    keep if regexm(acause, "neo_")
    reshape wide cases pop, i(registry_index year_start year_end sex_id acause dataset_id dataset_name NID) j(age)

    // create location_id and country_id column
    gen tmp_var = registry_index
    replace tmp_var = subinstr(tmp_var, ".", " ", .)
    gen location_id = word(tmp_var,2)
    gen country_id = word(tmp_var,1)
    replace location_id = word(tmp_var,1) if location_id == "0"
    drop tmp_var
    destring location_id, replace
    keep cases* pop* dataset* location_id registry* acause sex_id year* NID
    collapse(sum) cases* (mean) pop*, by(dataset* location_id registry* acause sex_id year* NID) fast

    // add required variables
    add_merge_name
    load_location_info
    gen_year_data
    rename year_span year_span_INC

    // // Drop exceptions
    do "$root_staging_code/_staging_common/mark_exclusions.do" 2

    // Rename to enable merge
    rename dataset_name dataset_name_INC
    rename pop* inc_pop*
    gen inc_data = 1
    tempfile incData
    save `incData', replace
    
// // Get MOR data
    import delimited "$mi_datasets_staging_storage/GBD${gbd_iteration}/appended_p7_mor_version`vers'.csv", clear

    // merge dataset_name
    merge m:1 dataset_id using `dataset_name_map', assert(2 3) keep(3) nogen

    // drop unwanted causes and reshape wide
    rename sex sex_id
    rename nid NID_mor
    keep registry* year_start year_end sex_id acause dataset_id dataset_name age deaths pop NID
    keep if regexm(acause, "neo_")
    reshape wide deaths pop, i(registry_index year_start year_end sex_id acause dataset_id dataset_name NID) j(age)

    // create location_id and country_id column
    gen tmp_var = registry_index
    replace tmp_var = subinstr(tmp_var, ".", " ", .)
    gen location_id = word(tmp_var,2)
    gen country_id = word(tmp_var,1)
    replace location_id = word(tmp_var,1) if location_id == "0"
    drop tmp_var
    destring location_id, replace
    keep deaths* pop* dataset* location_id registry* acause sex_id year* NID
    collapse(sum) deaths* (mean) pop*, by(dataset* location_id registry* acause sex_id year* NID) fast

    // add required variables
    add_merge_name
    load_location_info
    gen_year_data
    rename year_span year_span_MOR

    // // Drop exceptions
    do "$root_staging_code/_staging_common/mark_exclusions.do" 3
    rename dataset_name dataset_name_MOR
    rename pop* mor_pop*
    gen mor_data = 1

// Merge data from the same dataset
    merge 1:1 merge_name acause location_id registry_index sex year_start year_end using `incData'
    rename merge_name dataset_name
    save "$temp_folder/merged.dta", replace

** *****************************************************************************
** Part 2: Refine data and add variables relevant to MI models
** *****************************************************************************
use "$temp_folder/merged.dta", replace

// Use matched population to fill missing population values
    capture drop inc_pop1 
    capture drop mor_pop1
    egen tot_pop_inc = rowtotal(inc_pop*), missing
    egen tot_pop_mor = rowtotal(mor_pop*), missing
    foreach i of varlist inc_pop* {
        local m = subinstr("`i'", "inc", "mor", .)
        replace `i' = `m' if inlist(tot_pop_inc, ., 0) & _merge == 3 
        replace `m' = `i' if inlist(tot_pop_mor, ., 0) & _merge == 3 
    }
    drop tot_pop*

// Generate pop_total variables for later coverage-area comparison
    egen pop_total_INC = rowtotal(inc_pop*), missing
    egen pop_total_MOR = rowtotal(mor_pop*), missing
    drop mor_pop* inc_pop*

// Add dataType Variable
    gen dataType = 1 if inc_data == 1 & mor_data == 1
    replace dataType = 2 if inc_data == 1 & mor_data != 1
    replace dataType = 3 if inc_data != 1 & mor_data == 1

// Adjust dataset identifiers
    replace dataset_name = dataset_name_INC if dataType == 2
    replace dataset_name = dataset_name_MOR if dataType == 3

// Keep relevant variables
    keep dataset* location_id registry_index registry_id year_start year_end sex_id acause dataType cases* deaths* pop* NID*
    save "$temp_folder/refined.dta", replace

** *****************************************************************************
** PART 3: Load Data and Apply Restrictions
** *****************************************************************************
// Load Merged Registry Data
    use "$temp_folder/refined.dta", clear

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

//  Drop Within-dataset Redundancy
    drop_dataset_redundancy

** *****************************************************************************
** PART 4: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
** *****************************************************************************
// // Keep National Data where possible
    KeepNational
    compress
    save "$temp_folder/first_MI_drop_tempfile.dta", replace

** *****************************************************************************
** PART 5: Merge Incidence-only and Mortality-only Data by registry_index-year where possible
** *****************************************************************************
// // Separate INC-only: check for redundancy and rename identifiers
    // Keep INC-only
        use "$temp_folder/first_MI_drop_tempfile.dta", clear

        ** NOTE: temporary fix until KeepBest works correctly
        drop if dataset_id == 224 // duplicate issue for EST data
        egen cases1 = rowtotal(cases*)
        keep if dataType == 2
        drop *MOR deaths* 
        rename year_span year_span_INC

    // adjust data by year span to reflect a single year. Do not need to adjust 
    //      population because it should reflect the mean population over the period
    foreach v of varlist cases* {
        replace `v' =  `v'/year_span_INC
    }
    replace year_span_INC = 1

    // Drop registry_index-cause-years that are missing all data
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

    // Finalize INC-only
        drop dataType dataset_name
        capture drop dataset_id
        save "$temp_folder/INC_only.dta", replace

// // MOR-only: check for redundancy and rename identifiers
    // Keep Mor-only
        use "$temp_folder/first_MI_drop_tempfile.dta", clear
        //egen deaths1 = rowtotal(deaths*)
        keep if dataType == 3
        drop *INC cases*
        rename year_span year_span_MOR

    // adjust data by year span to reflect a single year. Do not need to adjust 
    //      population because it should reflect the mean population over the period
        foreach v of varlist deaths* {
            replace `v' =  `v'/year_span_MOR
        }
        replace year_span_MOR = 1

    // Drop registry_index-cause-years that are missing all data
        gen to_drop = 0
        egen deaths1 = rowtotal(deaths*)
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

    // Finalize MOR-only
        drop dataType dataset_name
        capture drop dataset_id
        save "$temp_folder/MORonly.dta", replace

// // Merge INC-only/MOR-only and keep only matching registries
    use "$temp_folder/MORonly.dta", clear
    // merge
        split(registry_index), gen(loc) parse(.)
        replace registry_index = loc1 + "." + loc2 + ".1" if loc3 == "0"  & dataset_name_MOR  =="CoD_VR_ICD10"
        duplicates tag registry_index sex_id year_id acause, gen(dups)
        drop if dups == 1 & dataset_name_MOR =="CoD_VR_ICD10"
        merge 1:1 registry_index sex_id year_id* acause using "$temp_folder/INC_only.dta"

    // Record drop reason, then drop un-merged data
        gen to_drop = 0
        replace to_drop = 1 if _merge != 3
        drop _merge
        capture count if to_drop == 1
        if r(N) {
            noisily di "Removing Unmatched Data..."
            gen dropReason = "registry-year_id does not have matching inc/mor data" if to_drop == 1
            gen dataType = .
            record_and_drop "Part 4: matching" "$staging_name"
        }
        else drop to_drop
    
    // Drop merged data where the year_spans don't match
        gen to_drop = 0
        replace to_drop = 1 if year_span_INC != year_span_MOR
        capture count if to_drop == 1
        if r(N) {
            gen dropReason = "matched data have different coverage periods" if to_drop == 1
            gen dataType = .
            record_and_drop "Part 4: coverage" "$staging_name"
        }
        else drop to_drop

    // Drop pairs for which the coverage population is too dissimilar 
        gen pop_err = 0
        gen test_pop = (pop_total_INC/pop_total_MOR)
        replace test_pop  = 1 - test_pop if test_pop <= 1
        replace test_pop = test_pop - 1 if test_pop > 1
        replace pop_err = 1 if (test_pop > 0.10 & test_pop != .) | inlist(pop_total_INC, ., 0) | inlist(pop_total_MOR, ., 0)

// // save combined data
    // generate required variables
        capture drop dataType
        gen dataType = 1
        egen dataset_name = concat(dataset_name*), punct(" & ")
        gen dataset_id = 3 // id for unassigned_combined_datasets
        gen NID = NID_inc if NID_inc == NID_mor 
        replace NID = 284465 if NID_inc != NID_mor 

    // alert user if dataset information is missing
        count if dataset_name_INC == "" | dataset_name_MOR == ""
        if r(N) {
            pause on
            di "ERROR: some dataset information was lost during the INC_only/MOR_only merge"
            pause
            pause off
        }

    // save
        compress
        save "$temp_folder/second_MI_drop_tempfile.dta", replace
** *****************************************************************************
** PART 6: Append Merged Incidence-only and Mortality-only Data where possible
** *****************************************************************************

// Append Merged INC/MOR Data to data of which INC/MOR are from the same dataset
    use "$temp_folder/first_MI_drop_tempfile.dta", clear
    keep if dataType == 1

    // attach to the rest of the data
    append using "$temp_folder/second_MI_drop_tempfile.dta"
    compress

    save "$temp_folder/third_MI_drop_tempfile.dta", replace

// Run KeepNational and KeepBest on the recombined data to eliminate redundancies
    callKeepBest "INC_MOR"

// Save a copy of the final data selections with full information
    keep country_id location_id dataset* registry* year_id sex_id acause NID* cases* deaths* pop*
    order location_id year_id sex_id acause NID* cases* deaths* pop*
    save_staging_data, output_file("selected_MI_data_$today.dta")

** *****************************************************************************
** Part 7: Remove Conflicts, Check for Remaining Redundancy, Finalize and Save
** *****************************************************************************
// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts

// Check again for Redundancy
    AlertIfOverlap

// Combine NIDs so that there is a single entry for each location_id year_id sex_id an acause
    capture tostring NID, replace
    #delim ;
    combine_inputs, use_mean_pop(0) combined_variables("NID") nid_var("NID")
        adjustment_section("mi_prep") metric_variables("cases* deaths* pop*")
        uid_variables("location_id year_id sex_id acause") dataset_combination_map("`nid_combo_map'");
    #delim cr
    capture destring NID, replace
    replace NID = 284465 if NID_inc != NID_mor | NID == .
    keep location_id year_id sex_id acause NID cases* deaths* pop* NID
    collapse (sum) cases* deaths* pop*, by(location_id year_id sex_id acause NID)

// Reshape, Calculate MI, and Drop Data with NULL MI Ratio.
    drop pop*
    reshape long cases deaths, i(location_id acause sex_id year_id NID) j(age)

// Add age_group_id
    drop if age == 1
    gen age_group_id = age - 1
    replace age_group_id = 235 if age == 25
    replace age_group_id = 32 if age == 24
    replace age_group_id = 31 if age == 23
    replace age_group_id = 30 if age == 22
    replace age_group_id = 20 if age == 21
    drop age

// Remove labels
    foreach var of varlist _all {
        capture _strip_labels `var'
    }
// Save
    export delimited "$mi_ratio_staging_storage/MI_ratio_model_input_$today.csv", replace

** *****************************************************************************
** End drop_andCheck.do
** *****************************************************************************