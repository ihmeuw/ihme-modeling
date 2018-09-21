*****************************************
** Description: Launches scripts to calculate incremental survival
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
    local output_folder "$survival_folder/`acause'"
    make_directory_tree, path("`output_folder'")

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/survival_`acause'"
    capture mkdir "`log_folder'"

// Start Logs
    capture log close _all
    capture log using "`log_folder'/_master.log", text replace

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// Get list of location ids
    use "$parameters_folder/locations.dta", clear
    capture levelsof(location_id) if model == 1, local(modeled_locations) clean
    keep location_id location_type
    tempfile locations
    save `locations', replace

** **************************************************************************
** Submit Jobs, Check For Completion, and Save
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission'  {
        better_remove, path("`output_folder'") recursive(1)
    }


// Submit jobs
if !$troubleshooting & "$run_dont_submit"!="1"{
    local submission_cause = substr("`acause'", 5, .)
    foreach local_id in `modeled_locations' {
            $qsub -pe multi_slot 4 -l mem_free=8g -N "srvW_`submission_cause'_`local_id'" "$stata_shell" "$survival_worker" "`acause' `local_id'"
    }
}

// Check for completion, compile and save
    clear
    noisily di "Finding and appending outputs... "
    foreach local_id in `modeled_locations' {
        local checkfile = "`output_folder'/`local_id'.dta"
        check_for_output, locate_file("`checkfile'") timeout(.25) failScript("$survival_worker") scriptArguments("`acause' `local_id'")
    }
    clear
    set obs 1
    gen finished = "finished"
    save "`output_folder'/survival_summary.dta", replace

// close log
    capture log close

** *************
** END
** *************
