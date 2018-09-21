*****************************************
** Description: Calculate survival
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

// folders
    local access_to_care = "$atc_folder/`acause'/access_to_care_draws_`local_id'.dta"
    local lambda_values = "$scalars_folder/lambda_values.dta"
    local survival_curves = "$scalars_folder/survival_curves.dta"
    local total_incidence = "$incidence_folder/`acause'/`local_id'.dta"
    local output_folder = "$survival_folder/`acause'"

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// maximum survival months
    use "$parameters_folder/constants.dta"
    local max_survival_months = max_survival_months[1]

// load functions to summarize and finalize the data
    run "$nonfatal_model_code/subroutines/load_common_nonfatal_functions.ado"

** **************************************************************************
** Combine incidence and access to care to calculate incremental survival rate
** **************************************************************************
// subset lambda values
    use "`lambda_values'" if location_id == `local_id', clear
    capture count
    assert r(N)
    tempfile lambda_subset
    save `lambda_subset', replace

// // Get incidence
    // Get data
        capture confirm file "`total_incidence'"
        if _rc noisily di "Missing incidence file for `location_id'"
        use "`total_incidence'", clear
        capture rename draw_* incidence_*

    // Keep relevant data
        keep if !inlist(age_group_id, 22, 27)

// Merge with access to care variable
    capture drop acause
    merge m:1 location_id year sex using "`access_to_care'", keep(1 3) assert(2 3) nogen

// // Transform access to care into survival
    // merge with survival curves
        display "Merging with survival curves..."
            // Specially Handle Exceptions
            if regexm("`acause'", "neo_liver_")  replace acause = "neo_liver"
            if regexm("`acause'", "neo_leukemia_other") replace acause = "neo_leukemia_ll_chronic"
            if "`acause'" == "neo_nmsc" replace acause = "neo_nmsc_scc"

            // Merge
            joinby acause sex using "`survival_curves'"

            // Ensure that acause is coorectly entered to revert Specially Handled Causes
            replace acause = "`acause'"

    // Only keep survival if less than or equal to our set maximum survival months
        keep if survival_month <= `max_survival_months'

    // Calculate relative survival percentage ---reflects where each survival should sit within the range of survival curves we've defined for this cause/sex combo, according to how good its MI ratio is
        quietly forvalues i = 0/999 {
            gen double survival_relative_`i' = (access_to_care_`i' * (survival_best - survival_worst)) + survival_worst
        }

    // merge with lambda values
        display "Merging with lambda values"
        merge m:1 location_id year sex age using `lambda_subset', keep(1 3) assert(2 3) nogen

    // Calculate absolute survival ("transform relative survival into absolute survival to control for background mortality")
        display "Calculating absolute survival"
        quietly forvalues i = 0/999 {
            gen double survival_abs_`i' = survival_relative_`i' * exp(lambda * survival_year)
            // NOTE: for some values the absolute survival is outside of the probabalistic range [0,1]. Until this is addressed cap values to the probabalistic range
            replace survival_abs_`i' = 0 if survival_abs_`i' == .
            replace survival_abs_`i' = 1 if survival_abs_`i' > 1 | survival_abs_`i' == .
        }

// Calculate incremental survival rate
    sort location_id sex age year survival_years
    di "Calculating incremental survival..."
    quietly forvalues i = 0/999 {
        display "        draw_`i'"
        bysort location_id sex age year: gen double mortality_incremental_`i' = survival_abs_`i' - survival_abs_`i'[_n+1]
        // NOTE: For whatever reason, we have absolute survival greater than 1 and incremental mortality < 0, will need to investigate the cause further.
		replace mortality_incremental_`i' = 0 if mortality_incremental_`i' < 0 | mortality_incremental_`i' == .
        ** replace mortality_incremental_`i' = 1 if mortality_incremental_`i' > 1
        // fill in the most-years survived lines with remaining survivors
        bysort location_id sex age year: egen total_die_`i' = total(mortality_incremental_`i')
        replace mortality_incremental_`i' = (1 - total_die_`i') if survival_month == `max_survival_months'
    }

// Save Draws
    drop survival_relative_* access_to_care* total_die*
    compress
    capture rm "`output_folder'/`local_id'.dta"
    save "`output_folder'/`local_id'.dta", replace


** **************************************************************************
** END
** **************************************************************************

