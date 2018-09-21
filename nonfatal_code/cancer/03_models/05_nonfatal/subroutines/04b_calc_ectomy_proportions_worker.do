*****************************************
** Description: Calculates incidence and prevalence of "ectomy" procedures

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
    args pr_id local_id
    if "`pr_id'" == "" local pr_id 3129

// Get acause and associated sexes
    use "$parameters_folder/causes.dta", clear
    levelsof acause if procedure_proportion_id == `pr_id', clean local(cause)

// Set save location of input data
    local procedure_rate_data = "$procedure_proportion_folder/procedure_proportion_`pr_id'.dta"

// Create output folder
    local output_folder =  "$procedure_proportion_folder/upload_`pr_id'"
    make_directory_tree, path("`output_folder'")

// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local proportion_measure = proportion_measure_id[1]

** **************************************************************************
** Part 1:
** **************************************************************************
// calculate the ectomy proportion for the modelable entity

    // get data for the location and cause of interest
    clear
    local input_file = "$incidence_folder/`cause'/`local_id'.dta"
    noisily di "Appending `input_file'"
    append using  "`input_file'"

    // Keep relevant data
    keep if !inlist(age_group_id, 22, 27)
    tempfile data
    save `data', replace

    // merge with incidence or prevalence
    merge 1:m location_id year sex age using `procedure_rate_data', keep(3) nogen
    rename draw_* proportion_*

    // calculate percentage
    foreach i of numlist 0/999 {
        gen draw_`i' = proportion_`i'/incidence_`i'
    }
    drop proportion_* incidence_*

    // format for upload
    rename (year sex) (year_id sex_id)

    // save
    keep location_id year sex age draw_*
    compress
    outsheet using "`output_folder'/`proportion_measure'_ectomy_prop_input_`local_id'.csv", comma replace


** **************************************************************************
** END
** **************************************************************************
