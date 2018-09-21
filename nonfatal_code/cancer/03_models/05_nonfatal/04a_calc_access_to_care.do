*****************************************
** Description: Generates age-standardized mi_ratios, then uses them to generate
**          the access-to-care scalar
** **************************************************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** Define Functions
** ****************************************************************
capture program drop standardize_age
program define standardize_age
    // accept arguments
        syntax , data_var(string)

    // end function if all_age data exists
        capture drop if age_group_id == 27

    // keep age-specific data and compile to asr
        preserve
            keep if age_group_id == 1 | inrange(age_group_id, 6, 20) | inrange(age_group_id, 30, 32) | age_group_id == 235
            merge m:1 age_group_id using "$age_weights", keep(1 3) assert(2 3) nogen
            merge m:1 location_id year age_group_id sex using "$population_data", keep(1 3) assert(2 3) nogen
            foreach var of varlist `data_var'* {
                    qui replace `var' = `var' * weight / pop
            }
            replace age_group_id = 27
            fastcollapse `data_var'*, type(sum) by(location_id year sex acause age_group_id)
            tempfile asr_data
            save `asr_data', replace
        restore

    // append asr data to the rest of the data
        append using `asr_data'

end

** ****************************************************************
** SET MACROS FOR CODE
** ****************************************************************
// Accept arguments
    args acause resubmission
    if "`resubmission'" == "" local resubmission = 0

// output_folder
    local output_folder = "$atc_folder/`acause'"
    capture make_directory_tree, path("`output_folder'")

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// Get countries
    use "$parameters_folder/locations.dta", clear
    tempfile locations
    save `locations', replace
    capture levelsof(location_id) if model == 1, local(local_ids) clean

// load functions to summarize and finalize the data
    run "$nonfatal_model_code/subroutines/load_common_nonfatal_functions.ado"

** **************************************************************************
** Part 1: Get Deaths and Incidence to Calculate Age Standardized MI
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }

// Get deaths
    clear
    tempfile death_draws
    noisily di "Appending death draws..."
    foreach local_id of local local_ids {
        local death_file = "$mortality_folder/`acause'/`local_id'.dta"
        append using "`death_file'"
    }
    duplicates drop
    capture rename (year_id sex_id) (year sex)
    capture rename draw_* death_*
    capture drop acause
    capture gen acause = "`acause'"
    standardize_age, data_var("death_")
    drop acause
    keep if age_group_id == 27
    save `death_draws', replace

// Get incidence
    clear
    tempfile incidence_draws
    noisily di "Appending incidence draws... "
    foreach local_id of local local_ids {
        local incidence_file = "$incidence_folder/`acause'/`local_id'.dta"
        append using "`incidence_file'"
    }
    duplicates drop
    capture rename draw_* incidence_*
    capture drop acause
    capture gen acause = "`acause'"
    standardize_age, data_var("incidence_")
    keep if age_group_id == 27
    merge 1:1 location_id year sex age_group_id using `death_draws', assert(3) nogen


** **************************************************************************
** Part 2: Calculate Age-Standardized MI, then generate access to care
** **************************************************************************
quietly {
    noisily di "Calculating access to care..."
        forvalues i = 0/999 {
            // alert user
                if mod(`i', 100) == 0 noisily display "    draw `i'..."

            // Calculate age-standardized mi ratio
                 gen double mi_`i' = death_`i' / incidence_`i'

            // Get minimum and maximum mi for each draw
                // NOTE: the bleow is a faster method of performing the following two operations : egen double max_mi_`i' = max(mi_`i'); egen double min_mi_`i' = min(mi_`i')
                mata a = .
                mata st_view(a, ., "mi_`i'")
                mata st_store(1,st_addvar("double","max_mi_`i'"), colmax(a))
                replace max_mi_`i' = max_mi_`i'[1]
                mata st_store(1,st_addvar("double","min_mi_`i'"), colmin(a))
                replace min_mi_`i' = min_mi_`i'[1]

            // Calculate access to care
                gen double access_to_care_`i' = (1 - (mi_`i' - min_mi_`i')/(max_mi_`i' - min_mi_`i'))

            // Clean up
                drop max_mi_`i' min_mi_`i' mi_`i' death_`i' incidence_`i'

    }
}

** **************************************************************************
** Part 3: Format and Save
** **************************************************************************
// Drop irrelevant variables and sort
    keep location_id year sex acause access_to_care*
    order location_id year sex acause access_to_care*
    sort location_id year sex

// Save full atc file
    compress
    save "`output_folder'/access_to_care_all.dta", replace

// Save individual atc file for each location
    capture levelsof (location_id), clean local(atc_local_ids)
    quietly {
        noisily di "Saving access to care..."
        preserve
        foreach local_id in `atc_local_ids' {
            noisily display "    `local_id'"
            keep if location_id == `local_id'
            save "`output_folder'/access_to_care_draws_`local_id'.dta", replace
            restore, preserve
        }
    }

// Save summary
    use "`output_folder'/access_to_care_all.dta", clear
    generate_summary_file, output_file("`output_folder'/access_to_care_summary.dta") data_var("access_to_care")

** *************
** END
** *************
