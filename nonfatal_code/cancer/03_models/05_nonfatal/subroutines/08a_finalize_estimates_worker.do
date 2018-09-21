*****************************************
** Description: Reformats final incidence and prevalence estimates, then converts
**           to rate space (#events/population) for upload into Epi

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** Accept Arguments or Set DUSERts
** ****************************************************************
    args acause local_id types_short

** ****************************************************************
**
** ****************************************************************
// output folder
    local output_folder = "$finalize_estimates_folder/`acause'"

// Set data types to finalize. Sent as a combination of first letters because stata cannot accept spaces in a single argument
    if regexm("`types_short'", "p") local types = "prevalence"
    if regexm("`types_short'" , "i") local types = itrim("`types' incidence")

** ****************************************************************
** GET RESOURCES
** ****************************************************************
// determine if p_id is present, indicating that incidence data need to be adjusted to avoid double-counting procedures
    use "$parameters_folder/causes.dta", clear
    levelsof procedure_rate_id if acause == "`acause'" & to_adjust == 1, clean local(p_id)
    if "`p_id'" != "" {
        run "$nonfatal_model_code/subroutines/load_common_nonfatal_functions.ado" "adjustment"
    }

// get list of modelable entity ids
    use "$parameters_folder/modelable_entity_ids.dta", clear
    keep if inlist(stage, "primary", "in_remission", "disseminated", "terminal")
    levelsof modelable_entity_id if acause == "`acause'", clean local(me_ids)
    tempfile m_ids
    save `m_ids', replace

// get measure_ids
    // get measure_ids
    use "$parameters_folder/constants.dta", clear
    local incidence_measure = incidence_measure_id[1]
    local prevalence_measure = prevalence_measure_id[1]

** **************************************************************************
** FINALIZE BY TYPE
** **************************************************************************
foreach type in `types' {
    noisily di "`type'"
    // Import Data
            if "`type'" == "incidence"  {
                local input_file = "$adjusted_incidence_folder/`acause'/`local_id'.dta"
                check_for_output, locate_file("`input_file'") timeout(0) failScript("$adjust_incidence_worker") scriptArguments("`acause' `local_id' 1")
                use "`input_file'", clear
                capture _strip_labels *
            }
            if "`type'" == "prevalence" {
                local input_file = "$prevalence_folder/`acause'/prevalence_draws_`local_id'.dta"
                check_for_output, locate_file("`input_file'") timeout(0) failScript("$prevalence_worker") scriptArguments("`acause' `local_id' 1")
                use "`input_file'", clear
                capture rename prevalence_* draw_*
            }

            capture drop acause
            gen acause = "`acause'"
            capture _strip_labels *
            capture gen sex_id = sex
            gen year_id = year

    // // change to rate space
        run $convert_andCheck_rates "draw_"

    // Add modelable_entity_id
        if "`type'" == "incidence" {
            gen stage = "primary"
            merge m:1 acause stage using `m_ids', keep(1 3) assert(2 3) nogen
        }
        if "`type'" == "prevalence" {
            drop if stage == "unadjusted_remission"
            merge m:1 acause stage using `m_ids', keep(1 3) assert(2 3) nogen
        }

    // Reep relevant variables
        keep modelable_entity_id location_id year_id sex_id age_group_id draw_*
        order modelable_entity_id, first

    // list modelable entities in final file
        levelsof modelable_entity_id, clean local(present_me_ids)

    // Save output file for each modelabe entity id
        foreach m in `present_me_ids' {
            // crash if any data were dropped
            capture count if modelable_entity_id == `m'
            if !r(N) BREAK

            // save
            noisily di "     Saving `type' model `m' for `acause' location `local_id'..."
            cap mkdir "`output_folder'/`m'"
            local output_file = "`output_folder'/`m'/``type'_measure'_`local_id'.csv"
            capture rm "`output_file'"
            outsheet if modelable_entity_id == `m' using "`output_file'", comma replace
        }
}

// close log
capture log close

** ************
** END
** ************
