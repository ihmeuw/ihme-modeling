
// Purpose:    Drop redundant or unmatched cancer data according to defined rules. Alert user of errors.
// Process:
//          Part 1: Add population where possible. Drop rows that are still missing population
//          Part 2: Combine data that should not be projected
//          Part 3: Project data that does not need to be combined
//          Part 4: Combine and then project data to match the location_id population
//          Part 5: Compile the results of Part 2- Part 4

** **************************************************************************
** Configuration
**         Sets application preferences (memory allocation, variable limits)
** **************************************************************************
// Clear memory and set memory and variable limits
    clear all
    set more off
    set maxvar 10000

// Load and run set_common function based on the operating system
    if "$h" == "" & c(os) == "Windows" global h = <filepath>
    else if "$h" == "" global h = <filepath>
    global staging_name = "cod_mortality"
    run "$h/dev_branch/cancer_estimation/_common/set_common_roots.do" "staging"   

** **************************************************************************
** Set Macros
** **************************************************************************
// Set save locations and data maps as defined by set_staging_common
    local input_folder = "$cod_mortality_staging_storage"
    local temp_folder = "$cod_mortality_staging_workspace"
    local subroutines = "$cod_mortality_staging_code/subroutines"
    global dropped_data_file = "$cod_mortality_dropped_data"
    local common_pop_data = "$common_cancer_data/populations.dta"
    make_directory_tree, path("$dropped_data_file")

// set local macro to designate the metrics of interest
    local metric_variables = "cases* pop*"

** ****************************************************************************
** Get Additional Resources
** *****************************************************************************
// Load use_ihme_pop status
    do "${code_prefix}/_database/load_database_table.do" "dataset"
    keep dataset_id can_use_ihme_pop
    tempfile pop_status
    save `pop_status', replace

// load registry_name
    do "${code_prefix}/_database/load_database_table.do" "registry"
    keep registry_index registry_name
    duplicates drop registry_index, force
    tempfile reg_name
    save `reg_name', replace

// load population data
    use "`common_pop_data'", clear
    convert_pop_for_prep  // loaded by set_common
    rename (year_id sex_id population) (year sex pop)
    reshape wide pop, i(location_id year sex) j(age)
    tempfile pop_data
    save `pop_data', replace

// sdi_quintile map
    import delimited using "$common_cancer_data/sdi.csv", varnames(1) clear
    keep location_id sdi_quintile
    destring location_id sdi_quintile, replace
    drop if sdi_quintile == .
    duplicates drop
    tempfile sdi_quintile_map
    save `sdi_quintile_map'
** ****************************************************************************
**  Prepare Input Data
**      add required variables, create dataset_combination_map, split data where
**          necessary
** *****************************************************************************
// Get Data
    use "`input_folder'/01_selected_incidence_data.dta", clear
    merge m:1 registry_index using `reg_name', keep(3) nogen

// Keep only relevant data
    keep dataset_id dataset_name location_id registry_index registry_name NID year* sex acause cases* pop*

// Add relevant information
// Generate 'year' as the average year and calculate year span
    gen_year_data

// add use_ihme_pop status
    merge m:1 dataset_id using `pop_status', keep(1 3) nogen

// ensure presence of all necessary location information (country_id, 
//      subMod, development status, location_type)
    load_location_info

// add coverage status
    capture confirm variable 
    if _rc add_coverage_status

// ensure presence of  sdi_quintile
    capture drop sdi_quintile
    merge m:1 location_id using `sdi_quintile_map', nogen assert(2 3) keep(3) nogen
    // hot-fix for russia and norway sdi's
    replace sdi_quintile = 4 if country_id == 62 //Russia
    replace sdi_quintile = 5 if country_id == 90 //Norway

// Drop all-ages incidence data (all-ages population data are used in 
//      determining dataset requirements and are therefore not dropped)
    drop cases1

// Ensure proper format of dataset_id
    capture confirm numeric variable dataset_id
    if !_rc {
        rename dataset_id old_data_id
        gen dataset_id = string(old_data_id)
        drop old_data_id
    }
    drop dataset_name

// Estimate the number of cases for the median year ( total_cases/num_years )
    foreach n of numlist 2 7/22 {
        replace cases`n' = cases`n'/year_span
    }

// Run script to split India data into Urban & Rural, if not split
    do "$root_staging_code/_staging_common/india_assign_rural_urban.do" "cases* pop*"

// keep only those variables relevant to the rest of the script
    keep location_id year sex acause country_id registry_index dataset_id NID sdi_quintile coverage_of_location_id subMod can_use_ihme_pop cases* pop*

** *****************************************************************************
** Part 1: Verify and Correct population
**    Add population where missing and allowed
**    Combine registries across data sets (required, and will replace missing 
**      population with present population from another dataset)
**    Finally, drop any uids that are missing population data after updates have 
**      been made
** *****************************************************************************
// add population where missing and allowed (national data from High SDI 
//      quintile or if exception)
    gen get_pop = 1 if inlist(pop1, ., 0) & ((coverage_of_location_id == 1 & sdi_quintile == 5) | can_use_ihme_pop == 1)
    capture count if get_pop == 1
    if r(N) {
        preserve
            keep if get_pop == 1
            keep location_id year sex
            duplicates drop
            merge 1:1 location_id year sex using `pop_data', keep(1 3) assert(2 3) nogen
            keep location_id year sex pop*
            tempfile addtl_pop
            save `addtl_pop', replace
        restore
        preserve
            keep if get_pop == 1
            drop pop*
            merge m:1 location_id year sex using `addtl_pop', keep(1 3) nogen
            tempfile has_added_pop
            save `has_added_pop', replace
        restore
        drop if get_pop == 1
        append using `has_added_pop'
    }
    drop get_pop

// Replace missing population info with "NA" entry
    foreach n of numlist 2 7/22 {
        replace pop`n' = . if pop`n' == 0
    }
    drop pop1
    egen pop1 = rowtotal(pop*), missing
    drop can_use_ihme_pop

// Collapse to combine registries from the same registry_index regardless of 
//      dataset. Preserve dataset information by changing the dataset name of 
//      datasets that will be combined
    capture rename year year_id
    combine_inputs, use_mean_pop(1) combined_variables("dataset_id NID") adjustment_section("prep") metric_variables("`metric_variables'") dataset_combination_map("$cod_mortality_dataset_combo_map")

// Drop data from non-High SDI quintiles that are still missing population
    drop if inlist(pop1,.,0) & sdi_quintile != 5
    drop if inlist(pop1,.,0) & coverage_of_location_id != 1

** ****************************************************************
** Part 1b: Mark data by the type of adjustment it requires
**
** ****************************************************************
// Project existing national data, or coverage_of_location_id subnational data, 
//      from High SDI locations
    gen projected_only = 1 if country_id == location_id & coverage_of_location_id == 1  & sdi_quintile == 5
        replace projected_only = 1 if coverage_of_location_id == 1 & subMod == 1 & sdi_quintile == 5

// Combine and project non-national data, from High SDI qunitile or 
//      Subnationally Modeled country, with population.
//      Exclude Hong Kong (to prevent it from combining with China)
    gen combined_projected = 1 if country_id != location_id & coverage_of_location_id != 1 & sdi_quintile == 5

// Combine and project non-national data from High SDI, subnational locations to
//   the subnational location_ids
    gen combined_projected_subnat = 1 if country_id != location_id & coverage_of_location_id != 1 & subMod == 1  & sdi_quintile == 5

// Combine-only any remaining data
    gen combined_only = 1 if projected_only == . & combined_projected == . & combined_projected_subnat == .

// save the prepped data
    save "`temp_folder'/prepped_input.dta", replace


** ****************************************************************
** Part 2: Project national estimates, multiplying rate by IHME population
**      also project sub-national estimates for High SDI subnational locations
**      (for eligible locations)
** ****************************************************************
// Re-load data, then keep only the relevant data
    use "`temp_folder'/prepped_input.dta", clear
    keep if projected_only == 1
    drop combined_projected* combined_only

// Ensure that all location-year has only one unique registry and dataset combination
    check_for_redundancy, uid_variables("location_id year sex") variables_to_check("dataset_id registry_index NID") optional_message("during Part 2")

// check for duplicates
    check_for_duplicates, uid_variables("location_id year sex acause") optional_message("during Part 2")

// calculate rate
    foreach n of numlist 2 7/22 {
        gen double rate`n' = cases`n'/pop`n'
    }
    rename pop* registry_pop*

// merge with IHME estimate for coverage_of_location_id population, then 
//      recalculate cases based on the rate and coverage_of_location_id population
    merge m:1 location_id year sex using `pop_data', keep(1 3) nogen
    foreach n of numlist 2 7/22 {
        gen recalculated_cases`n' = rate`n'*pop`n'
    }
    rename (cases* recalculated_cases*) (orig_cases* cases*)

// save
    save "`temp_folder'/projected_only.dta", replace

** ****************************************************************
** Part 3: Combine non-national data, then project to create national estimate
**      (for eligible locations)
** ****************************************************************
// Re-load data, then keep only the relevant data
    use "`temp_folder'/prepped_input.dta", clear
    keep if combined_projected == 1
    drop projected_only combined_projected_subnat combined_only

// Update location_id to equal the country_id, enabling merge and combine_inputs
//       functions
    drop location_id
    rename country_id location_id
    replace coverage_of_location_id = 1

// Collapse to combine registries from the same country_id regardless of 
//      dataset. Preserve dataset information by changing the dataset name of 
//      datasets that will be combined
    combine_inputs, use_mean_pop(0) combined_variables("registry_index dataset_id NID") adjustment_section("combined_projected") metric_variables("`metric_variables'") dataset_combination_map("$cod_mortality_dataset_combo_map")

// Ensure that all location-year has only one unique registry and dataset 
//      combination
    check_for_redundancy, uid_variables("location_id year sex") variables_to_check("dataset_id registry_index NID") optional_message("during Part 3")

// check for duplicates
    check_for_duplicates, uid_variables("location_id year sex acause") optional_message("during Part 3")

// calculate rate
    foreach n of numlist 2 7/22 {
        gen double rate`n' = cases`n'/pop`n'
    }
    rename pop* registry_pop*

// merge with IHME estimate for coverage_of_location_id population, then 
//      recalculate cases based on the rate and coverage_of_location_id population
    merge m:1 location_id year sex using `pop_data', keep(1 3) nogen
    foreach n of numlist 2 7/22 {
        gen recalculated_cases`n' = rate`n'*pop`n'
    }
    rename (cases* recalculated_cases*) (orig_cases* cases*)

// drop irrelevant variables and save
    save "`temp_folder'/combined_projected.dta", replace

** ****************************************************************
** Part 4: Combine sub-national data, then project to create estimate for 
**      sub-national location (for eligible locations)
** ****************************************************************
// Re-load data, then keep only the relevant data
    use "`temp_folder'/prepped_input.dta", clear
    keep if combined_projected_subnat == 1
    drop projected_only combined_projected combined_only

// Update the coverage
    replace coverage_of_location_id = 1

// Collapse to combine registries from the same country_id regardless of 
//      dataset. Preserve dataset information by changing the dataset name
//     of datasets that will be combined
    combine_inputs, use_mean_pop(0) combined_variables("registry_index dataset_id NID")  adjustment_section("combd_projd_subnat") metric_variables("`metric_variables'") dataset_combination_map("$cod_mortality_dataset_combo_map")

// Ensure that all location-year has only one unique registry and dataset combination
    check_for_redundancy, uid_variables("location_id year sex") variables_to_check("dataset_id registry_index NID") optional_message("during Part 4")

// check for duplicates
    check_for_duplicates, uid_variables("location_id year sex acause") optional_message("during Part 4")

// calculate rate
    foreach n of numlist 2 7/22 {
        gen double rate`n' = cases`n'/pop`n'
    }
    rename pop* registry_pop*

// merge with IHME estimate for coverage_of_location_id population, then 
//      recalculate cases based on the rate and coverage_of_location_id population
    merge m:1 location_id year sex using `pop_data', keep(1 3) nogen
    foreach n of numlist 2 7/22 {
        gen recalculated_cases`n' = rate`n'*pop`n'
    }
    rename (cases* recalculated_cases*) (orig_cases* cases*)

// drop irrelevant variables and save
    save "`temp_folder'/combined_projected_subnational.dta", replace

** ****************************************************************
** Part 5: Combine non-national data that has population
**      (for eligible locations)
** ****************************************************************
// Re-load data, then keep only the relevant data
    use "`temp_folder'/prepped_input.dta", clear
    keep if combined_only == 1
    drop combined_projected* projected_only

// drop any data ajusted above (national data, High SDI quintile data) except 
//      subnationally modeled data that are not High-income
    combine_inputs, use_mean_pop(0) combined_variables("registry_index dataset_id NID") adjustment_section("combined_only") metric_variables("`metric_variables'") dataset_combination_map("$cod_mortality_dataset_combo_map")

// Ensure that all location-year has only one unique registry and dataset combination
    check_for_redundancy, uid_variables("location_id year sex") variables_to_check("dataset_id registry_index NID") optional_message("during Part 5")

// reshape, then calculate rate to ensure that all datapoints have the same population
    drop pop1
    keep cases* pop* location_id year sex acause registry_index dataset_id NID
    reshape long cases pop, i(location_id year sex acause registry_index dataset_id NID) j(age)
    bysort location_id year age sex: egen double avg_pop = mean(pop)
    gen corrected = 1 if !inrange(pop, avg_pop-1, avg_pop +1) & (avg_pop != . & pop!= .)
    rename (pop cases) (orig_pop orig_cases)
    gen rate = orig_cases/orig_pop
    gen cases = rate*avg_pop
    rename avg_pop pop
    drop rate orig_*
    reshape wide cases pop, i(location_id year sex acause registry_index dataset_id NID) j(age)
    gen combined_only = 1

// check for duplicates
    check_for_duplicates, uid_variables("location_id year sex acause") optional_message("during Part 5")

// save
    save "`temp_folder'/combined_only.dta", replace

** ****************************************************************
** Part 6: Compile the adjusted data
** ****************************************************************
// Compile data
    use "`temp_folder'/projected_only.dta", clear
    append using "`temp_folder'/combined_projected.dta"
    append using "`temp_folder'/combined_projected_subnational.dta"
    append using "`temp_folder'/combined_only.dta"

// // drop estimated data if raw data are present (removes redundancy)
    // determine presence of combined data
    gen priority = 1 if projected_only == 1
    replace priority = 2 if combined_projected == 1 & priority == .
    replace priority = 3 if combined_projected_subnat == 1 & priority == .
    replace priority = 4 if priority == .
    bysort location_id year sex acause: egen highest_priority = min(priority)

    // generate to_drop variable and mark data to be dropped
    gen to_drop = 0
    replace to_drop = 1 if priority != highest_priority
    gen dropReason = "data superceded by more accurate estimates" if to_drop == 1
    record_and_drop "03_combined_incidence_data"

// recalculate all-ages numbers
    egen cases1 = rowtotal(cases*), missing
    capture drop pop1
    egen pop1 = rowtotal(pop*), missing

// Ensure that all location-year-sex-cause is unique
    check_for_duplicates, uid_variables("location_id year sex acause") optional_message("during Part 6")

// Ensure that all location-year has only one unique registry and dataset combination
    check_for_redundancy, uid_variables("location_id year sex acause") variables_to_check("dataset_id registry_index NID") optional_message("during Part 5")

// Ensure that registry and dataset entries are the same for each unique 
//      location_id and year
    #delim ;
    keep location_id year sex acause registry_index dataset_id NID cases* 
        pop* combined_only projected_only combined_projected*;
    combine_inputs, use_mean_pop(0) 
        combined_variables("registry_index dataset_id NID") 
        adjustment_section("finalization") 
        metric_variables("`metric_variables' combined_only projected_only combined_projected combined_projected_subnat") 
        uid_variables("location_id year") 
        dataset_combination_map("$cod_mortality_dataset_combo_map");
    check_for_redundancy, uid_variables("location_id year") 
        variables_to_check("dataset_id registry_index NID") 
        optional_message("during Part 6");
    #delim cr

// missing population should NOT exist
    count if pop1 == . | pop1 == 0
    if r(N) {
        pause on
        pause MISSING POPULATION EXISTS WHEN IT SHOULDN'T. PLEASE RESOLVE. 
    }

// save
    order location_id year sex acause NID dataset_id registry_index  cases* pop*
    compress
    save_staging_data, output_file("02_combined_incidence.dta")

** ************************************
** END
** ************************************