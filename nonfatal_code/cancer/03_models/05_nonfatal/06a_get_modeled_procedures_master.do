*****************************************
** Description: Retrieve final "ectomy" procedure rates from the epi database
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
    args rate_id resubmission
    if "`resubmission'" == "" local resubmission = 0

// If not present, create troubleshooting global to enable submission of sub-jobs. This variable will be equal to 1 if running just_check
    if "$troubleshooting" == "" global troubleshooting = 0

// set list of data_types
    local data_types = "prevalence incidence"
    local output_folder = "$modeled_procedures_folder"

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/get_mod_proc_`data_type'_`rate_id'"
    capture mkdir "`log_folder'"

// Start Logs
    capture log close _all
    capture log using "`log_folder'/`rate_id'.log", text replace

** **************************************************************************
** Submit Jobs, Check For Completion, and Save
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }

// Submit jobs
if !$troubleshooting {
    foreach data_type in `data_types' {
        noisily di "`data_type'"
        $qsub -pe multi_slot 3 -l mem_free=6g -N "gmpW_`rate_id'_`data_type'" "$stata_shell" "$modeled_procedures_worker" "`rate_id' `data_type'"
    }
}

// Check for completion
    noisily di "Finding outputs... "
    foreach data_type in `data_types' {
        local checkfile = "$modeled_procedures_folder/`rate_id'/modeled_`data_type'_`rate_id'.dta"
        check_for_output, locate_file("`checkfile'") timeout(30) failScript("$modeled_procedures_worker") scriptArguments("`rate_id' `data_type' 1")
    }

// Save verification file
    clear
    set obs 1
    gen modeled_procedures = "obtained"
    save "$modeled_procedures_folder/`rate_id'_obtained.dta", replace

// close log
    capture log close

** *************
** END
** *************
