*****************************************
** Description: Uses incidence draws and modeled "ectomy" procedure proportions to
**          estimate raw procedure rates, then uploads those rates to Epi/Dismod

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"
    run "$nonfatal_code_folder/subroutines/format_for_epi_upload.ado"
    run "$shared_functions_folder/stata/fastcollapse.ado"
    run "$shared_functions_folder/stata/fastpctile.ado"
    run "$shared_functions_folder/stata/fastrowmean.ado"

capture program drop format_survival
program define format_survival
    syntax, cause(string) location_id(string)

    noisily di "   `local_id'"
    capture restore, not
    preserve
        local input_file = "$survival_folder/`cause'/`local_id'.dta"
        use "`input_file'", clear
        keep if survival_month == 60
        keep location_id year sex age_group_id survival_abs* incidence*
        drop if inlist(age_group_id, 22, 27)
        compress
        save "`temp_folder'/`local_id'.dta", replace
    restore

end

** ****************************************************************
** SET MACROS FOR CODE
** ****************************************************************
// accept arguments
    args rate_id resubmission
    if "`resubmission'" == "" local resubmission = 1

//
    local stoma_adjustment = 0.58

// Get procedure rate id and acause
    use "$parameters_folder/causes.dta", clear
    levelsof acause if procedure_rate_id == `rate_id', clean local(cause)
    levelsof procedure_proportion_id if procedure_rate_id == `rate_id', clean local(pr_id)

// Set data folder and save location of rate data
    local output_folder =  "$procedure_rate_folder/upload_`rate_id'"
    make_directory_tree, path("`output_folder'")
    local modeled_proportions = "$procedure_proportion_folder/download_`pr_id'/modeled_proportions_`pr_id'.dta"

// set temp files
    local temp_folder = "`output_folder'/_temp"
    make_directory_tree, path("`temp_folder'")
    local appended_surv = "`temp_folder'/appended_survival_data.dta"
    local final_estimates = "`temp_folder'/final_estimates.dta"

// get locations
    use "$parameters_folder/locations.dta", clear
    capture levelsof location_id if model == 1, clean local(local_ids)

// modelable entity name
    use "$parameters_folder/modelable_entity_ids.dta", clear
    levelsof modelable_entity_name if modelable_entity_id == `rate_id', clean local(modelable_entity_name)
    noisily di "creating rate input for `modelable_entity_name'"

// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local proportion_measure = proportion_measure_id[1]
    local incidence_measure = incidence_measure_id[1]

** **************************************************************************
** calculate the number of procedures
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' {
        better_remove, path("`output_folder'") recursive(1)
    }

// get data for the location and cause of interest
    clear
    di "Checking for `appended_surv'..."
    capture confirm file "`appended_surv'"
    if _rc | !`resubmission' {
        noisily di "Formatting inputs for `rate_id'..."
        if !`resubmission' {
            quietly foreach local_id of local local_ids {
                format_survival, cause(`cause') location(`local_id')
            }
        }
        clear
        noisily di "Appending inputs for `rate_id'..."
        foreach local_id of local local_ids {
            noisily di "   `local_id'"
            local input_file = "`temp_folder'/`local_id'.dta"
            capture confirm file "`input_file'"
            if _rc format_survival, cause(`cause') location(`local_id')
            append using "`input_file'"
        }
        save "`appended_surv'", replace
    }

// format data (keep year and sex variables to enable merge with population data)
    clear
    di "Checking for `final_estimates'..."
    capture confirm file "`final_estimates'"
    if _rc | !`resubmission' {
        use "`appended_surv'", clear
        gen year_id = year
        gen sex_id = sex

        merge m:1 location_id year_id sex_id age_group_id using `modeled_proportions', keep(3)
        count if _merge != 3
        assert r(N) == 0
        drop _merge
        rename draw_* proportion_*
        drop model_version_id

        foreach i of numlist 0/999 {
            gen procedures_`i' = proportion_`i'*incidence_`i'*survival_abs_`i'
            if `rate_id' == 1731 replace procedures_`i' = procedures_`i' * `stoma_adjustment'
        }
        preserve
            keep location_id year sex age *_0
            save "`temp_folder'/test_values.dta", replace
        restore
        drop proportion_* survival_abs_*

        // Convert to rate space
            merge m:1 location_id year sex age_group_id using "$population_data", keep(1 3) assert(2 3) nogen
            display "Converting procedures from counts to rates"
            foreach var of varlist procedures_* {
                capture replace `var' = `var' / pop
            }

    // Calculate summary statistics
        fastrowmean procedures_*, mean_var_name(mean)
        fastrowmean incidence_*, mean_var_name(cases)
        fastpctile procedures_*, pct(2.5 97.5) names(lower upper)
        drop procedures_* incidence_*
        save "`final_estimates'", replace
    }
    use "`final_estimates'", clear

** **************************************************************************
** Format data for epi uploader
** **************************************************************************
// add variable entries
    capture drop modelable_entity_id
    gen modelable_entity_id=`rate_id'
    gen modelable_entity_name="`modelable_entity_name'"
    gen case_name= "cancer ectomy incidence rate"
    gen extractor = c(username)
    gen underlying_nid=.
    gen nid=257604
    gen representative_name ="Nationally and subnationally representative"
    gen urbanicity_type="Unknown"
    gen site_memo = ""
    gen uncertainty_type=.
    gen uncertainty_type_value=95
    gen ihme_loc_id=.

// format year
    rename year_id year_start
    gen year_end=year_start+4

// run function to finish formatting
    format_for_epi_upload, additional_variables("location_name page_num ihme_loc_id table_num")

// save
    compress
    outsheet using "`output_folder'/`incidence_measure'_ectomy_rate_input.csv", comma replace

// save alert
    clear
    set obs 1
    gen complete = "complete"
    save "`output_folder'/`rate_id'_input_complete.dta", replace
    export excel "$nonfatal_data_folder/_ectomy_proportions/03_procedure_rates/`rate_id'.xlsx", replace

** **************************************************************************
** END
** **************************************************************************
