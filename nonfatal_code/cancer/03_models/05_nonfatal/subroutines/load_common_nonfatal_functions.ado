*****************************************
** Description: Defines common summary and finalization functions
*****************************************
** **************************
** Set Boolean to prevent functions from reloading when not necessary
** **************************
if "$common_functions_set" != "" {
    noisily di "Common Functions are Set :)"
    exit
}

** **************************
** Accept argument to define calculate_inc_from_mi if sent
** **************************
// accept arguments
    args extra_functions

** **************************
** load IHME written mata functions which speed up collapse, pctile, and rowmean
** **************************
// load functions
    run "$shared_functions_folder/stata/fastcollapse.ado"
    run "$shared_functions_folder/stata/fastpctile.ado"
    run "$shared_functions_folder/stata/fastrowmean.ado"

** ****************************************************************
** Generate all-ages estimate: generate "age = 22"/"all ages" data for the given variable type

** ****************************************************************
capture program drop all_age
program define all_age
    // accept arguments
        syntax , [ageFormat(string)] relevant_vars(string) data_var(string)

    // end function if all_age data exists
        capture count if age_group_id == 22
        if r(N) exit

    // keep age-specific data and compile to all-ages
        preserve
            keep if inrange(age_group_id, 2, 20) | inrange(age_group_id, 30, 32) | age_group_id == 235
            replace age_group_id = 22
            noisily di "`relevant_vars' data `data_var'"
            fastcollapse `data_var'*, type(sum) by(`relevant_vars')
            tempfile all_age_data
            save `all_age_data', replace
        restore

    // append all-ages data to the rest of the data
        append using `all_age_data'

    // Keep relevant data
        keep `relevant_vars' `data_var'*
end

** ****************************************************************
** Generate age-standardized rate: generate "age_group_id = 27"/"asr" data for the given variable type

** ****************************************************************
capture program drop asr
program define asr
    // accept arguments
        syntax ,  [ageFormat(string)] relevant_vars(string) data_var(string)

    // end function if all_age data exists
        capture count if age_group_id == 27
        if r(N) exit

    // keep age-specific data and compile to asr
        preserve
            keep if inrange(age_group_id, 2, 20) | inrange(age_group_id, 30, 32) | age_group_id == 235
            merge m:1 age_group_id using "$age_weights", keep(1 3) assert(2 3) nogen
            merge m:1 location_id year age_group_id sex using "$population_data", keep(1 3) assert(2 3) nogen
            foreach var of varlist `data_var'* {
                    qui replace `var' = `var' * weight / pop
            }
            replace age_group_id = 27
            fastcollapse `data_var'*, type(sum) by(`relevant_vars')
            tempfile asr_data
            save `asr_data', replace
        restore

    // append asr data to the rest of the data
        append using `asr_data'

end

** ****************************************************************
** Define program to finalize draws
** ****************************************************************
capture program drop finalize_draws
program define finalize_draws
    // accept arguments
        syntax, output_file(string) data_var(string) [additional_vars(string)]

    // Generate all-ages data
        all_age, relevant_vars("location_id year sex age_group_id acause `additional_vars'") data_var("`data_var'_")

    // Generate Age-Standardized Rate
        asr, relevant_vars("location_id year sex age_group_id acause `additional_vars'") data_var("`data_var'_")

    // Verify that there are no duplicates
        duplicates drop
        duplicates tag location_id year sex acause age_group_id `additional_vars', gen(tag)
        count if tag != 0
        if r(N) {
            di "ERROR: duplicates found during finalization."
            BREAK
        }
        drop tag

    // Reformat and save draw-level data
        keep location_id year sex age_group_id acause `additional_vars' `data_var'*
        order location_id year sex age_group_id acause `additional_vars' `data_var'*
        sort location_id year sex age_group_id
        compress
        save "`output_file'", replace
end

** ****************************************************************
** Define program to finalize incidence draws
** ***************************************************************
capture program drop generate_summary_file
program define generate_summary_file

    // accept arguments
        syntax, output_file(string) data_var(string) [additional_vars(string) percentiles(string)]

    // set percentiles
        if "`percentiles'" != "" noisily display "Calculating summary statistics with the following lower and upper percentiles: `percentiles'"
        else {
            local percentiles = "2.5 97.5"
            noisily display "Calculating summary statistics with dUSERt lower and upper percentiles: `percentiles'"
        }

    // Calculate summary statistics
        fastrowmean `data_var'*, mean_var_name(mean_`data_var')
        fastpctile `data_var'*, pct(`percentiles') names(lower_`data_var' upper_`data_var')

    // Reformat
        capture confirm var age_group_id
        if !_rc local if_ages = "age_group_id"
        else local if_ages = ""
        keep location_id year sex `if_ages' acause `additional_vars' mean_`data_var' lower_`data_var' upper_`data_var'
        order location_id year sex `if_ages' acause `additional_vars' mean_`data_var' lower_`data_var' upper_`data_var'
        sort location_id year sex `if_ages'

    // Save summary
        compress
        capture rm "`output_file'"
        save "`output_file'", replace
end



** ****************************************************************
** Define program to adjust incidence and prevalence based on sequelae
** ****************************************************************
if "`extra_functions'" == "adjustment" {
    // Generate all-ages estimate
    capture program drop adjust_for_sequelae
    program define adjust_for_sequelae
        syntax ,  procedure_id(string) varname(string) data_type(string) acause(string)

        // Save copy of original estimates
            tempfile all_stages
            save `all_stages', replace

        // Load procedures
            local proportion_file "$modeled_procedures_folder/`procedure_id'/modeled_`data_type'_`procedure_id'.dta"

        // Separate remission data and merge with proportions
            keep if !inlist(age_group_id, 22, 27)
            if "`data_type'" == "prevalence" keep if stage == "in_remission"
            merge 1:1 location_id year age_group_id sex using `proportion_file', keep(1 3) nogen

        // Subtract procedures from total
            forvalues i = 0/999 {
                replace `varname'`i' = `varname'`i' - procedures_`i'
            }
            tempfile adjusted_remission
            save `adjusted_remission', replace

        // // If adjusting prevalence, recombine with original dataset
            if "`data_type'" == "prevalence" {
                use `all_stages', clear
                replace stage = "unadjusted_remission" if stage == "in_remission"
                append using `adjusted_remission'
            }

        // Alert user that adjustment has completed
            noisily di "Data Adjusted for Sequelae"

    end
}

** **************************
** Set Boolean to prevent functions from reloading when not necessary
** **************************
global common_functions_set = 1

** **************************
** END
** **************************
