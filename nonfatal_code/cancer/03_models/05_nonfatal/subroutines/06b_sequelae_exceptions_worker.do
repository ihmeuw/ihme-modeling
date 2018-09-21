*****************************************
** Description: Handles exceptions in sequelae calculation

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** SET MACROS FOR CODE
** ****************************************************************
// accept arguments
    args sequela resubmission
    if "`resubmission'" == "" local resubmission = 1

// set input and output locations, set sequela-specific parameters
    local draws_file = "$modeled_procedures_folder/1724/1724_draws.dta"
    local sequela_data = "$sequelae_adjustment_folder/`sequela'_data.dta"
    local description = "`sequela' prevalence"
    local output_folder = "$sequelae_adjustment_folder/`sequela'"
    local output_file = "`output_folder'/sequela_`sequela'_uploaded.dta"

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/upload"
    make_directory_tree, path("`log_folder'")

// Start Logs
    capture log close _all
    log using "`log_folder'/upload_`acause'.txt", text replace

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local incidence_measure = incidence_measure_id[1]
    local prevalence_measure = prevalence_measure_id[1]

// Get list of location ids
    use "$parameters_folder/locations.dta", clear
    capture levelsof(location_id) if model == 1, local(modeled_locations) clean

// set adjustment_value based on lit review
    local fraction_1725 = 0.55
    local fraction_1726 = 0.18

// Ensure presence of output folder
    make_directory_tree, path("`output_folder'")
    if !`resubmission' {
     ** better_remove, path("`output_folder'") recursive(1)
    }

// Load timestamp, get_draws function and save_results function
    run $generate_timestamp
    run $save_results

** ****************************************************************
**
** ****************************************************************
// Format sequela draws if not present
capture confirm file "`sequela_data'"
if !`resubmission' | _rc {
    // load prostatectomy_draws
        use "`draws_file'", clear

    // multiply by adjustment value
        quietly foreach i of numlist 0/999 {
            replace draw_`i' = draw_`i'*`fraction_`sequela''
        }

    // save copy of full dataset
        compress
        save "`sequela_data'", replace
}
else use "`sequela_data'", clear

// save input for save_results
    quietly foreach local_id in `modeled_locations'{
        local location_file = "`output_folder'/`prevalence_measure'_`local_id'.csv"
        noisily di "Saving output for `local_id'..."
        outsheet if location_id == `local_id' using "`location_file'", comma replace
    }

// upload and verify upload
    save_results, modelable_entity_id(`sequela')  description("`description'") in_dir("$sequelae_adjustment_folder/`sequela'") metrics(`prevalence_measure') mark_best(yes) file_pattern("{measure_id}_{location_id}.csv")
    do $check_save_results "$timestamp" "`sequela'" "`description'" "`output_file'"

** ****************************************************************
**
** ****************************************************************
