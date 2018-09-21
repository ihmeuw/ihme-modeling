
*****************************************
** Description: Launch scripts to subtract sequela incidence if necessary, which
**          prevents double-counting by the central model
*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** SET MACROS
** ****************************************************************
// Accept or dUSERt arguments
    args acause resubmission
    if "`resubmission'" == "" local resubmission = 0

// If not present, create troubleshooting global to enable submission of sub-jobs. This variable will be equal to 1 if running just_check
    if "$troubleshooting" == "" global troubleshooting = 0

// Ensure Presence of subfolders
    local output_folder "$adjusted_incidence_folder/`acause'"
    make_directory_tree, path("`output_folder'")

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/adjust_incidence_`acause'"
    capture mkdir "`log_folder'"

// Start Logs
    capture log using "`log_folder'/_master.log", text replace

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// Get list of location ids
    use "$parameters_folder/locations.dta", clear
    capture levelsof(location_id) if model == 1, local(modeled_locations) clean
    capture levelsof(location_id) if location_id != 1, clean local(all_locations)
    keep location_id location_type
    tempfile location_info
    save `location_info', replace

// get modelable entity id if data need to be adjusted for procedure-due remission
    use "$parameters_folder/causes.dta", clear
    levelsof procedure_rate_id if acause == "`acause'" & to_adjust == 1, clean local(p_id)

** **************************************************************************
** Submit Jobs, Check For Completion, and Save
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }


// Submit jobs
    if !$troubleshooting & !`resubmission' {
        local submission_cause = substr("`acause'", 5, .)
        foreach local_id in `modeled_locations' {
            $qsub -pe multi_slot 2 -l mem_free=4g -N "aiW_`submission_cause'_`local_id'" "$stata_shell" "$adjust_incidence_worker" "`acause' `local_id'"
        }
    }

// Check for completion
    noisily di "Finding outputs... "
    foreach local_id in `modeled_locations' {
        local checkfile = "`output_folder'/`local_id'.dta"
        check_for_output, locate_file("`checkfile'") timeout(0) failScript("$adjust_incidence_worker") scriptArguments("`acause' `local_id' 1")
    }

// Save verification file
    clear
    set obs 1
    gen adjustments = "complete"
    save "`output_folder'/incidence_adjusted.dta", replace

// close log
    capture log close

** *************
** END
** *************
