*****************************************
** Description: Calculates incidence and prevalence of "ectomy" procedures,
**          then uploads those calculations to Epi/Dismod

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
    args pr_id resubmission
    if "`resubmission'" == "" local resubmission = 0

// If not present, create troubleshooting global to enable submission of sub-jobs. This variable will be equal to 1 if running just_check
    if "$troubleshooting" == "" global troubleshooting = 0

// Set procedure rate data location
    local procedure_rate_data = "$procedure_proportion_folder/procedure_proportion_`pr_id'.dta"
    local upload_description = "cancer procedure proportions"

// Create output folder
    local data_folder = "$procedure_proportion_folder"
    local output_file = "`data_folder'/`pr_id'_uploaded.dta"
    local output_folder =  "`data_folder'/upload_`pr_id'"
    make_directory_tree, path("`output_folder'")

** ****************************************************************
** Get Additional Resources
** ****************************************************************
// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local proportion_measure = proportion_measure_id[1]

// Get acause and associated sexes
    use "$parameters_folder/causes.dta", clear
    levelsof acause if procedure_proportion_id == `pr_id', clean local(cause)

// Get locations
    use location_id using "`procedure_rate_data'", clear
    duplicates drop
    levelsof location_id, clean local(locations)

** **************************************************************************
** Part 1:
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }

// calculate the ectomy proportion for each modelable entity
    di "Creating upload..."
    if !$troubleshooting {
        foreach local_id in `locations' {
            $qsub -pe multi_slot 2 -l mem_free=4g -N "epW_`pr_id'_`local_id'" "$stata_shell" "$ectomy_prop_worker" "`pr_id' `local_id'"
        }
    }

// Verify Completion
    foreach local_id in `locations' {
        local checkfile = "`output_folder'/`proportion_measure'_ectomy_model_input_`local_id'.csv"
        check_for_output, locate_file("`checkfile'") timeout(.5) failScript("$ectomy_prop_worker") scriptArguments("`pr_id' `local_id'")
    }

** **************************************************************************
** Part 2: Upload
** **************************************************************************
// Load timestamp and save_results function
    do $generate_timestamp
    run $save_results

// Upload
    save_results, modelable_entity_id(`pr_id') description("`upload_description'") in_dir("`output_folder'") metrics(`proportion_measure') file_pattern("{measure_id}_ectomy_prop_input_{location_id}.csv")

// Verify save_results
    do $check_save_results $timestamp `rate_id' `upload_description' `output_file'

// close log
    capture log close
** **************************************************************************
** END
** **************************************************************************
