
*****************************************
** Description: Launches scripts to calculate prevalence for the desired causes, sexes,
**          and locations in conjunction with cancer nonfatal modeling
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
    local output_folder "$prevalence_folder/`acause'"
    make_directory_tree, path("`output_folder'")

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/prevalence_`acause'"
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
        $qsub -pe multi_slot 3 -l mem_free=6g -N "pvW_`submission_cause'_`local_id'" "$stata_shell" "$prevalence_worker" "`acause' `local_id'"
    }
}

// Check for completion
    noisily di "Finding outputs... "
    foreach local_id in `modeled_locations' {
        local checkfile = "`output_folder'/prevalence_summary_`local_id'.dta"
        check_for_output, locate_file("`checkfile'") timeout(1) failScript("$prevalence_worker") scriptArguments("`acause' `local_id'")
    }

// Compile summaries
    clear
    set obs 1
    gen finished = "finished"
    save "`output_folder'/prevalence_summary.dta", replace

// close log
    capture log close

** *************
** END
** *************
