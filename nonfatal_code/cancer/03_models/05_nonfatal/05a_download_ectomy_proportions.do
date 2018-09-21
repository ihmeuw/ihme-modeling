*****************************************
** Description: Downloads and saves results from ectomy proportion models
*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** SET MACROS FOR CODE
** ****************************************************************
// Accept arguments
    args pr_id
    if "`resubmission'" == "" local resubmission = 0

// Ensure Presence of subfolders
    local output_folder "$procedure_proportion_folder/download_`pr_id'"
    make_directory_tree, path("`output_folder'")

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/get_modeled_proportions_`pr_id'"
    capture mkdir "`log_folder'"

// Start Logs
    capture log close _all
    capture log using "`log_folder'/`pr_id'.log", text replace

** ****************************************************************
** GET GBD RESOURCES
** ****************************************************************
// Get countries
    use "$parameters_folder/locations.dta", clear
    tempfile locations
    save `locations', replace
    capture levelsof(location_id) if model == 1, local(local_ids) clean

// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local incidence_measure = incidence_measure_id[1]
    local prevalence_measure = prevalence_measure_id[1]

// Import function to retrieve epi estimates
    run "$get_draws"

** **************************************************************************
** Get Proportion Data
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }

// Get draws
    get_draws, gbd_id_field(modelable_entity_id) source(dismod) measure_ids(18) gbd_id(`pr_id') clear

// Keep only what we need
    keep location_id year_id sex_id age_group_id model_version_id draw_*
    order location_id year_id sex_id age_group_id
    sort location_id year_id sex_id age_group_id

// Save
    compress
    save "`output_folder'/modeled_proportions_`pr_id'.dta", replace

// close log
    capture log close

** **************************************************************************
** END
** **************************************************************************
