** *****************************************************************************
** Description: Loads compiled incidence and mortality data, merges them,
**      then selects a single datapoint for each location_id-year_id-sex-age-cancer
** Arguments: 
**    - vers (int) : refers to staged_incidence_version_id 
**    - type (str) : either "VR" or "CR"
** NOTE: REDACTED
** Input(s): N/A
** Output(s): a formatted dta file with today's date
** How To Use: simply run script
** Contributors: REDACTED
** *****************************************************************************
// Clear workspace and set to run all selected code without pausing
    clear all
    set more off

// set staging name and gbd_iteration
    global staging_name = "mi_ratio"
    global gbd_iteration = "$current_gbd_round"

    args vers type
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
    // import delimited using "$mi_datasets_staging_storage/appended_p7_inc_version`vers'.csv", clear
    use "$mi_datasets_staging_storage/appended_p7_inc_version`vers'.dta", clear
    if "`type'" == "VR" {
        // only keep data that has complete coverage so that we are matching correctly with VR data
        preserve
            do "${cancer_repo}/_database/load_database_table" "registry"
            drop if registry_collection_type == 3
            keep registry_index coverage_of_location_id
            tempfile reg_map
            save `reg_map'
        restore
        merge m:1 registry_index using `reg_map', keep(3) nogen
        keep if coverage_of_location_id == 1
        drop coverage_of_location_id
    }
    replace registry_index = "163.4867.6" if registry_index == "163.4687.6" //incorrect location_id 

    // merge dataset_name
    merge m:1 dataset_id using `dataset_name_map', assert(2 3) keep(3) nogen
    drop if dataset_id == 357 | dataset_id == 370 | dataset_id == 143 // dup datasets, and ds-id 143 didn't go through finalization
    drop if dataset_id == 475 // FRA_Q483_I has unknown age range in backup db 
    drop if dataset_id == 882 // FRA_Q864_I has unknown age range in current db 
    // drop unwanted causes and reshape wide
    capture rename sex sex_id
    rename nid NID_inc
    keep registry* year_start year_end sex_id acause dataset_id dataset_name age cases pop NID
    keep if regexm(acause, "neo_")
    duplicates drop registry_index year_start year_end sex_id acause dataset_id dataset_name NID_inc age, force
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
    save "$temp_folder/incData_`type'.dta", replace
    
// // Get MOR data
    if "`type'" == "VR" {
        // import delimited "FILEPATH.csv", clear
        use "$mi_datasets_staging_storage/VR_complete_with_aggregations_version`vers'.dta", clear
    }
    else {
        // import delimited "FILEPATH.csv", clear
        use "$mi_datasets_staging_storage/appended_p7_mor_version`vers'.dta", clear
        replace registry_index = "163.4867.6" if registry_index == "163.4687.6" //incorrect location_id 
    }

    // merge dataset_name
    merge m:1 dataset_id using `dataset_name_map', assert(2 3) keep(3) nogen

    // drop unwanted causes and reshape wide
    capture rename sex sex_id
    rename nid NID_mor
    keep registry* year_start year_end sex_id acause dataset_id dataset_name age deaths pop NID
    keep if regexm(acause, "neo_")
    duplicates drop registry_index year_start year_end sex_id acause dataset_id dataset_name NID_mor age, force
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
    if "`type'" == "VR" {
        save "$temp_folder/morDataVR.dta", replace
    }
    else {
        save "$temp_folder/morDataCR.dta", replace
    }

// Merge data from the same dataset
    ** NOTE: cases1/deaths1 with missing values are dropped during standardize_format.do after all cases/deaths values are verified (as of 5/15/2015). If data appears to be missing from a dataset after the merge, verify missing entries in the formatted file
    if "`type'" == "VR" {
        merge 1:m acause location_id sex year_start year_end using `incData'
    }
    else {
        merge 1:1 merge_name acause location_id registry_index sex year_start year_end using `incData'
    }
    rename merge_name dataset_name
    
    save "$temp_folder/merged_`type'.dta", replace


** *****************************************************************************
** Part 2: Refine data and add variables relevant to MI models
** *****************************************************************************
    use "$temp_folder/merged_`type'.dta", clear

// Use matched population to fill missing population values
    capture drop inc_pop1 
    capture drop mor_pop1
    egen tot_pop_inc = rowtotal(inc_pop*), missing
    egen tot_pop_mor = rowtotal(mor_pop*), missing
    foreach i of varlist inc_pop* {
        local m = subinstr("`i'", "inc", "mor", .)
        local m = subinstr("`i'", "population", "pop", .)
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
    save "$temp_folder/refined_`type'.dta", replace

** *****************************************************************************
** PART 3: Load Data and Apply Restrictions
** *****************************************************************************

// Load Merged Registry Data
    use "$temp_folder/refined_`type'.dta", clear


// Apply Restrictions
    // apply cause and sex restrictions
        apply_cause_sex_restrictions

    // Apply location restrictions and add location information
        load_location_info, apply_restrictions("true")

    // Apply year restrictions
        apply_year_restrictions

    // Apply age restrictions
        apply_age_restrictions

    // Apply registry restrictions
        apply_registry_restrictions "mirs"

//  Drop Within-dataset Redundancy
    drop_dataset_redundancy

** *****************************************************************************
** PART 4: Keep National Data
**              Keep subnational data if present. Then keep remaining national data if present. (see KeepNational above)
** *****************************************************************************
// // Keep National Data where possible
    KeepNational
    compress
    save "$temp_folder/first_MI_drop_tempfile_`type'.dta", replace

** *****************************************************************************
** PART 5: Merge Incidence-only and Mortality-only Data by registry_index-year where possible
** *****************************************************************************

// // Separate INC-only: check for redundancy and rename identifiers
    // Keep INC-only
        use "$temp_folder/first_MI_drop_tempfile_`type'.dta", clear
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
                save "$temp_folder/leukemia_data_`type'.dta", replace
            }
        restore

    // preserve population values
    preserve 
        keep pop_total_INC registry_index year* sex_id acause dataset_id dataset_name* 
        save "$temp_folder/inc_only_pop_`type'.dta", replace

    restore
    // Run KeepBest
        reshape long cases, i(registry_index year* sex_id acause dataset_id dataset_name*) j(age)
        gen deaths = .
        save "$temp_folder/inc_only_keepbest_input_`type'.dta", replace   

        !python "${cancer_repo}/b_staging/_staging_common/KeepBest.py" "$temp_folder/inc_only_keepbest_input_`type'.dta" "mi_ratio" "''" "`type'"
     
        import delimited using "$temp_folder/keepbest_output_mirs_`type'.csv", clear 
        
        drop deaths  
        drop if cases == . 

        // merge dataset and location+info 
        load_location_info
        merge m:1 dataset_name using `dataset_name_map', assert(2 3) keep(3) nogen
        //reshape wide cases,  i(registry* year* sex_id acause dataset_id dataset_name*) j(age)

    // merge back population 
        merge m:1 registry_index year_id sex_id acause dataset_name using "$temp_folder/inc_only_pop_`type'.dta", assert(2 3) keep(3) nogen 

    // Alert user of remaining data overlaps
        AlertIfOverlap

    // Finalize INC-only
       capture drop dataType dataset_name dataset_id
       save "$temp_folder/INC_only_`type'.dta", replace

// // MOR-only: check for redundancy and rename identifiers
    // Keep Mor-only
        use "$temp_folder/first_MI_drop_tempfile_`type'.dta", clear

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

    // preserve population values 
    preserve 
        keep pop_total_MOR registry_index year* sex_id acause dataset_id dataset_name* 
        save "$temp_folder/mor_only_pop_`type'.dta", replace
    restore

    // Run KeepBest
        reshape long deaths, i(registry_index year* sex_id acause dataset_id dataset_name*) j(age)
        gen cases = .
        save "$temp_folder/mor_only_keepbest_input_`type'.dta", replace 
        !python "${cancer_repo}/b_staging/_staging_common/KeepBest.py" "$temp_folder/mor_only_keepbest_input_`type'.dta" "mi_ratio" "MOR" "`type'"
        import delimited using "$temp_folder/keepbest_output_mirs_MOR_`type'.csv", clear 
        drop if deaths == .
        drop cases

        // merge dataset and location+info 
        load_location_info
        merge m:1 dataset_name using `dataset_name_map', assert(2 3) keep(3) nogen
        //reshape wide deaths,  i(registry_index year* sex_id acause dataset_id dataset_name*) j(age)

    // merge back population 
        merge m:1 registry_index year_id sex_id acause dataset_name using "$temp_folder/mor_only_pop_`type'.dta", assert(2 3) keep(3) nogen 

    // Alert user of remaining data overlaps
        AlertIfOverlap

    // Finalize MOR-only
        capture drop dataType dataset_name dataset_id
        save "$temp_folder/MORonly_`type'.dta", replace


// // Merge INC-only/MOR-only and keep only matching registries
    use "$temp_folder/MORonly_`type'.dta", clear

    // merge
        split(registry_index), gen(loc) parse(.)
        replace registry_index = loc1 + "." + loc2 + ".1" if loc3 == "0"  & dataset_name_MOR  =="CoD_VR_ICD10"
        duplicates tag registry_index sex_id year_id acause age, gen(dups)
        drop if dups == 1 & dataset_name_MOR =="CoD_VR_ICD10"
        merge 1:1 registry_index sex_id year_id* acause age using "$temp_folder/INC_only_`type'.dta"
 
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
        capture drop dataset_name
        egen dataset_name = concat(dataset_name*), punct(" & ")
        capture drop dataset_id 
        gen dataset_id = 3 
        gen NID = 284465

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
        save "$temp_folder/second_MI_drop_tempfile_`type'.dta", replace

** *****************************************************************************
** PART 6: Append Merged Incidence-only and Mortality-only Data where possible
** *****************************************************************************

// Append Merged INC/MOR Data to data of which INC/MOR are from the same dataset
    use "$temp_folder/first_MI_drop_tempfile_`type'.dta", clear
    keep if dataType == 1
    
    duplicates drop registry_index year* sex_id acause dataset_name*, force 
    reshape long cases deaths, i(registry_index year* sex_id acause dataset_name*) j(age)

    // divide by year_span to get metric data by year
    foreach v of varlist deaths cases {
        replace `v' =  `v'/year_span
    }
    // attach to the rest of the data
    if "`type'" == "VR" {
        append using "$temp_folder/second_MI_drop_tempfile_VR.dta"
        replace dataset_name = dataset_name_INC if dataset_id == 10 
    }
    else {
        append using "$temp_folder/second_MI_drop_tempfile_CR.dta"
    }

    compress
    save "$temp_folder/third_MI_drop_tempfile_`type'.dta", replace


// preserve population values 
    preserve 
        keep pop* registry_index year* sex_id acause dataset_id dataset_name* age
        save "$temp_folder/pop_inc_mor_`type'.dta", replace 
    restore

// Run KeepNational and KeepBest on the recombined data to eliminate redundancies
    !python "${cancer_repo}/b_staging/_staging_common/KeepBest.py" "$temp_folder/third_MI_drop_tempfile_`type'.dta" "mi_ratio" "INC_MOR" "`type'"
    
    import delimited using  "$temp_folder/keepbest_output_mirs_INC_MOR_`type'.csv", clear 

    // reshape wide deaths cases, i(registry_index year_id sex_id acause dataset_id dataset_name*) j(age)

// merge population values back
//    merge m:1 registry_index year_id sex_id acause dataset_name* age using "$temp_folder/pop_inc_mor.dta", keep(1 3) nogen


    // merge dataset and location+info 
    load_location_info
    //merge m:1 dataset_name using `dataset_name_map', assert(2 3) keep(3) nogen
// Save a copy of the final data selections with full information
    keep country_id location_id dataset* registry* year_id sex_id nid* *acause cases* deaths* age //pop*
    order location_id year_id sex_id acause cases* deaths* age //pop*
    save_staging_data, output_file("selected_MI_data_`type'_$today.dta")

rename nid* NID* 
// keep national US
drop if country_id == 102 & location_id != 102 & inlist(acause, "neo_bone", "neo_neuro", "neo_tissue_sarcoma", "neo_liver_hbl")
drop if location_id == 102 & ~inlist(acause, "neo_bone", "neo_neuro", "neo_tissue_sarcoma", "neo_liver_hbl")

** *****************************************************************************
** Part 7: Remove Conflicts, Check for Remaining Redundancy, Finalize and Save
** *****************************************************************************
// Remove registry conflicts, redundant data for a location-year caused by overlapping registry coverage
    removeConflicts

    if "`type'" == "VR" {
    	duplicates tag sex location_id registry_index acause year_id, gen(overlap)

    	tempfile pre_collapse
    	save `pre_collapse'

    	keep if overlap == 1
    	//egen pop_comparison = max(pop_total_INC), by(location_id year_id sex_id acause)
    	//keep if pop_total_INC == pop_comparison

    	replace overlap = 0
    	append using `pre_collapse'
    	drop if overlap == 1
    	drop overlap
    }
// Check again for Redundancy
    AlertIfOverlap

// Combine NIDs so that there is a single entry for each location_id year_id sex_id an acause
    replace NID = 284465 
    capture tostring NID, replace
    #delim ;
    combine_inputs, combined_variables("NID") nid_var("NID")
        adjustment_section("mi_prep") metric_variables("cases* deaths*")
        uid_variables("location_id year_id sex_id acause age") dataset_combination_map("`nid_combo_map'");
    #delim cr
    capture destring NID, replace
    replace NID = 284465 if NID_inc != NID_mor | NID == .
    keep location_id year_id sex_id acause NID cases* deaths* age 
    collapse (sum) cases* deaths*, by(age location_id year_id sex_id acause NID)

// Reshape, Calculate MI, and Drop Data with NULL MI Ratio.
    //capture drop pop*
    //reshape long cases deaths, i(location_id acause sex_id year_id NID) j(age)

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

// Combine sexes for GBD2023 new causes
// STATA's inlist function is limited to 10 items, creating a new column 'is_new_cause' and assigning a 1 is the workaround to this limitation
    preserve
        gen is_new_cause = 0
        replace is_new_cause = 1 if inlist(acause, "neo_bone","neo_eye","neo_eye_other","neo_eye_rb","neo_liver_hbl","neo_lymphoma_burkitt","neo_lymphoma_other","neo_neuro","neo_tissue_sarcoma")
        replace is_new_cause = 1 if inlist(acause, "neo_ben_brain")

        keep if is_new_cause == 1
        drop is_new_cause

        replace sex_id = 3
        collapse (sum) cases deaths, by(location_id acause sex_id year_id NID age_group_id)
        tempfile both_sex
        save `both_sex'
    restore
    drop if inlist(acause, "neo_bone","neo_eye","neo_eye_other","neo_eye_rb","neo_liver_hbl","neo_lymphoma_burkitt","neo_lymphoma_other","neo_neuro","neo_tissue_sarcoma")
    drop if inlist(acause, "neo_ben_brain")
    append using `both_sex'
    
// Save
    export delimited "$mi_ratio_staging_storage/MI_ratio_model_input_`type'_$today.csv", replace
** *****************************************************************************
** End drop_andCheck.do
** *****************************************************************************
