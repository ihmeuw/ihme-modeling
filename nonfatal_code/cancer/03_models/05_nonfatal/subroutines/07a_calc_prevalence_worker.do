*****************************************
** Description: Calculates prevalence
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
    noisily di "`acause' `local_id'"

//  folders
    local sequela_durations =  "$scalars_folder/sequela_durations.dta"
    local survival_folder = "$survival_folder/`acause'"
    local incremental_prevalence_folder = "$incremental_prevalence_folder/`acause'"
    local output_folder = "$prevalence_folder/`acause'"

// Ensure presence of output folder and survival folder
    make_directory_tree, path("`output_folder'")
    make_directory_tree, path("`incremental_prevalence_folder'")

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// load functions to summarize and finalize the data. Send argument to load adjustment-specific functions
    run "$nonfatal_model_code/subroutines/load_common_nonfatal_functions.ado" "adjustment"

// maximum survival months
    use "$parameters_folder/constants.dta"
    local max_survival_months = max_survival_months[1]

// get modelable entity id if data need to be adjusted for procedure-due remission
    use "$parameters_folder/causes.dta", clear
    levelsof procedure_rate_id if acause == "`acause'", clean local(p_id)

** **************************************************************************
** Part 1: Adjust Sequela Durations
** **************************************************************************
// Get Survival data
    local survival_file = "`survival_folder'/`local_id'.dta"
    capture confirm file "`survival_file'"
    if _rc do "$survival_worker" "`acause'" `local_id'
    use "`survival_file'", clear
    drop survival_abs_*

// Calculate number of people who die each year by multiplying incremental mortality rate by incidence
    display "Calculating deaths for using incidence and incremental mortality..."
    quietly forvalues i = 0/999 {
        gen double deaths_`i' = incidence_`i' * mortality_incremental_`i'
        replace deaths_`i' = 0 if deaths_`i' < 0 | deaths_`i' == .
    }

// Calculate sequela duration
    // merge with sequela duration
        display "Merging with sequela durations..."
            // Specially Handle Exceptions
                if acause == "neo_leukemia_other" replace acause = "neo_leukemia_ll_chronic"
                if regexm("`acause'", "neo_liver_")  replace acause = "neo_liver"
                if "`acause'" == "neo_nmsc_scc" replace acause = "neo_nmsc"

            // Merge
                joinby acause using `sequela_durations'

            // Revert Specially Handled Causes
                if regexm("`acause'", "neo_leukemia_") | regexm("`acause'", "neo_liver_") | "`acause'" == "neo_nmsc_scc" {
                    replace acause = "`acause'"
                }

        // generate copy of input sequela_duration for later use
            gen raw_sequela_duration = sequela_duration

        // test for errors
            duplicates tag location_id year sex acause age survival_month stage, gen(tag)
            count if tag != 0
            if r(N) {
                di "duplicates"
                BREAK
            }
            drop tag

// Adjust sequela duration based on survival
    // First adjust incremental sequela duration (including remission) to equal the total months from diagnosis (at midyear) to death (at end year). This is the total amount of time someone may experience any of the sequela (from diagnosis to death, separated out by the amount of time they are living with cancer)
        gen incremental_duration = survival_month + 6

    // Now iteratively adjust times of each sequela so time lived with cancer is equal to the sum of all sequela durations
        // First zero out disseminated and terminal for cases we have decided survived, either everything after 5 year survival or something later
            sort location_id year sex acause age survival_month stage
            replace sequela_duration = 0 if stage == "terminal" & survival_month == `max_survival_months'
            replace sequela_duration = 0 if stage == "disseminated" & survival_month == `max_survival_months'

        // The most flexible is remission: Adjust remission time to equal the difference between incremental_duration sequela duration and the duration of each of the other sequela
            bysort location_id year sex acause age survival_month: egen non_remission_calc = total(sequela_duration) if stage != "in_remission"
            bysort location_id year sex acause age survival_month: egen non_remission = mean(non_remission_calc)
            replace sequela_duration = incremental_duration - non_remission if stage == "in_remission"
            replace sequela_duration = 0 if stage == "in_remission" & sequela_duration < 0

        // The next most flexible time is primary diagnosis and treatment
            bysort location_id year sex acause age survival_month: egen non_primary_calc = total(sequela_duration) if stage != "primary"
            bysort location_id year sex acause age survival_month: egen non_primary = mean(non_primary_calc)
            replace sequela_duration = incremental_duration - non_primary if stage == "primary"
            replace sequela_duration = 0 if stage == "primary" & sequela_duration < 0

        // Finally we can adjust the metastatic time if the totals still do not add up
            bysort location_id year sex acause age survival_month: egen non_disseminated_calc = total(sequela_duration) if stage != "disseminated"
            bysort location_id year sex acause age survival_month: egen non_disseminated = mean(non_disseminated_calc)
            replace sequela_duration = incremental_duration - non_disseminated if stage == "disseminated"
            replace sequela_duration = 0 if stage == "disseminated" & sequela_duration < 0

        // test for errors
            duplicates tag location_id year sex acause age survival_month stage, gen(tag)
            count if tag != 0
            if r(N) {
                di "duplicates"
                BREAK
            }
            drop tag
            bysort location_id year sex acause age survival_month: egen check_total = total(sequela_duration)

            count if incremental_duration != check_total | sequela_duration < 0 | sequela_duration == .
            if r(N) {
                noisily di "Error during calculation."
                BREAK
            }


** **************************************************************************
** Part 2:  Calculate Incremental Prevalence
** **************************************************************************
// Drop unnecessary vars
    noisily di "Dropping variables that are no longer needed..."
    keep location_id year sex acause age stage sequela_duration survival_month raw_sequela_duration incremental_duration deaths*

//  Create prevalence (in people-months) of each sequela by multiplying sequela duration by the number of people (deaths) in that death-cohort
    noisily di "Calculating people-months of each sequela, by survival-duration cohort and unique ID..."
    quietly forvalues i = 0/999 {
        gen ppl_months_`i' = deaths_`i' * sequela_duration
    }

// Save a copy of prevalence at this stage (used to calculate incidence by stage and useful in troubleshooting)
    save "`incremental_prevalence_folder'/incremental_prevalence_`local_id'", replace

** **************************************************************************
** Part 3: Calculate Final Prevalence and Save
** **************************************************************************
// collapse sequela durations to get absolute duration (in months)
    noisily di _n "Collapsing people-months together, no longer differentiating by years-survived, to get total people-months of each sequela by unique ID"
    fastcollapse ppl_months_* deaths*, type(sum) by(location_id year sex acause age stage)

// Calculate absolute sequela duration (in years) by dividing by 12  -- is this cases per year? hope so.
    noisily di "Converting people-months to prevalence (people-years)"
    quietly forvalues i = 0/999 {
        gen prevalence_`i' = ppl_months_`i'/12
    }

// Adjust remission to remove remission status due to cancer treatment ("sequelae adjustment"), if necessary
    if "`p_id'" != "" {
        noisily di "adjusting due to ectomies"
        adjust_for_sequelae, procedure_id("`p_id'") varname("prevalence_") data_type("prevalence") acause("`acause'")
    }

// check for errors by converting to rate space (all numbers should be inrange(prevalence_rate, 0, 1))
    preserve
        do $convert_andCheck_rates "prevalence_"
    restore

// // run funcitons to finalize draws and create summary file
    finalize_draws, output_file("`output_folder'/prevalence_draws_`local_id'.dta") data_var("prevalence") additional_vars("stage")
    generate_summary_file, output_file("`output_folder'/prevalence_summary_`local_id'.dta") data_var("prevalence") additional_vars("stage")

// close log
    capture log close

** **************************************************************************
** END
** **************************************************************************
