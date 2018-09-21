*****************************************
** Description: Subtracts sequela incidence if necessary to prevent double-counting

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** Set Macros and Directories
** ****************************************************************
// Accept or dUSERt arguments
    args acause local_id

//  folders
    local input_data = "$incidence_folder/`acause'/`local_id'.dta"
    local output_data = "$adjusted_incidence_folder/`acause'/`local_id'.dta"

// standby draws script if draws data are missing
    if !regexm("`acause'", "neo_nmsc") local incidence_draws_script = "$incidence_worker"
    else local incidence_draws_script = "$incidence_worker_nmsc"

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// get modelable entity id if data need to be adjusted for procedure-due remission
    use "$parameters_folder/causes.dta", clear
    levelsof procedure_rate_id if acause == "`acause'" & to_adjust == 1, clean local(p_id)

// load functions to summarize and finalize the data. Send argument to load adjustment-specific functions
    run "$nonfatal_model_code/subroutines/load_common_nonfatal_functions.ado" "adjustment"

** **************************************************************************
** Part 3: Adjust Incidence if Necessary, otherwise copy and paste data to indicate completion
** **************************************************************************
// ensure that input data are present
    check_for_output, locate_file("`input_data'") failScript("`incidence_draws_script'") scriptArguments("`acause' `local_id'")

// If no adjustment is needed, copy data to indicate completion. If adjustment is required, adjust remission to remove remission status due to cancer treatment ("sequelae adjustment")
    if "`p_id'" == "" {
        copy "`input_data'" "`output_data'", replace
    }
    else if "`p_id'" != ""{
        // load relevant data
            use "`input_data'", clear
            keep if !inlist(age, 22, 27)

        // subtract sequelae to prevent double-counting in central model
            adjust_for_sequelae, procedure_id("`p_id'") varname("draw_") data_type("incidence") acause("`acause'")

        // Keep and sort relevant data
            keep location_id year sex age acause draw_*
            order location_id year sex age acause
            sort location_id year sex age acause

        // Save draws
            compress
            save "`output_data'", replace
    }

// close log
    capture log close

** **************************************************************************
** END
** **************************************************************************
