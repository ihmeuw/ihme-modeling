// Purpose:        File that submits redistribution jobs to cluster

** ****************************************************************
** DEFINE LOCALS
** ****************************************************************
    noisily di "Running rdp master on `data_set_name'!"
    set more off

// accept arguments
    args data_set_group data_set_name data_type keep_old_outputs

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(6) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// Folders
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)

// Error file
    local skipped_rdp_file = "`error_folder'/`data_set_name'_`data_type'_not_redistributed_${today}.dta"

// worker scripts
    local rdp_worker_script "$code_for_this_prep_step/cancer_rdp_worker.do"

// Redistributed files
    local sg_rdp_file = "`temp_folder'/split_[sg]/final_post_rdp_split_[sg]_data.dta"

// Load Function to Check for Cluster Job Submission Outputs and Run the Script Directly if Output
    capture program drop check_for_output
    run "$common_cancer_code/check_for_output.ado"

// ensure that temp folder and subfolders are present
    capture mkdir "`temp_folder'/_input_data"

** ****************************************************************
** Verify that RDP needs to be run
** ****************************************************************
// Get data
    use "$pre_rdp_file", clear

// Determine if there are any garbage codes. If there are, proceed running redistribution, if not, just resave the data set
    count if substr(acause, 1, 4) != "neo_"
    if r(N) == 0 {
        save_prep_step, process(6)
        exit, clear
    }

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
if $troubleshoot_mode pause on

// Merge with location hierarchy
    capture confirm file "$location_hierarchy_file"
    if _rc di "Missing map data. Run .../redistribution/get_rdp_resources.do"
    merge m:1 location_id using "$location_hierarchy_file", keep(1 3) keepusing(location_id dev_status super_region region country subnational_level1 subnational_level2)

// Set aside registry data that are not in the location hierarchy. Mark these data as not-redistributed
    preserve
        keep if _merge == 1
        drop _merge
        gen not_redistributed = 1
        order not_redistributed, first
        if r(N) save "`skipped_rdp_file'", replace
        capture count if substr(registry_id, 1, 2) != "0."
        ** assert !r(N)
    restore
    drop if _merge == 1
    drop _merge

// save a copy of the input acauses (will be used to drop any acause solely produced by rdp and not part of the input dataset)
    preserve
        keep registry_id location_id year* sex acause
        duplicates drop
        save "`temp_folder'/input_acause_list.dta", replace
    restore

// save total events for later verification
    capture summ(cases1)
    local pre_rdp_total = r(sum)

// Generate groups to split
    egen split_group = group(location_id registry_id year* coding_system), missing

// Save a temporary copy
    save "`temp_folder'/_input_data/_split_group_data.dta", replace

// Reformat
    // Keep only the variables that are needed (will keep location_id so that we can merge the hierarchy info back on in the worker jobs)
        keep location_id registry_id year_start year_end sex coding_system acause cases* split_group
        order location_id registry_id year_start year_end sex coding_system acause cases* split_group

    // Add apostrophe to all string variables
        foreach var of varlist * {
            capture replace `var' = "'" + `var'
        }

// Split groups and submit jobs
    summ split_group
    local split_min = `r(min)'
    local split_max = `r(max)'
    if `split_max' > 1 di "Submitting scripts..."
    forvalues sg = `split_min'/`split_max' {
        // remove old outputs if requested
        if !`keep_old_outputs' {
            local output_file = subinstr("`sg_rdp_file'", "[sg]", "`sg'", .)
            capture rm("`output_file'")
        }

        // Save split group
            outsheet using "`temp_folder'/_input_data/split_`sg'.csv" if split_group == `sg', comma names replace

        // Determine if there is any garbage in the group
            capture count if split_group == `sg' & (!regexm(acause, "[a-zA-Z]") | inlist(substr(acause, 1, 2), "'C", "'D"))  // count the number of entries that are not mapped to a gbd cause
            local garbage_quant = r(N)

        // submit jobs if necessary
        if `garbage_quant' & inlist(($resubmit_mode + $troubleshoot_mode), 0 , 1, .) {

            // Determine memory requirement
                local memory = `garbage_quant'/4    // approximate required memory usage
                if `memory' < 6 local memory = 6
                local slots = ceil(`memory'/2)+1
                local mem = `slots'*2

            // Submit job
                $qsub -pe multi_slot `slots' -l mem_free=`mem'g -N "rdpW_`data_set_name'_`data_type'_`sg'" "$stata_shell" "`rdp_worker_script'" "`data_set_group' `data_set_name' `data_type' `sg' $troubleshoot_mode"
                noisily display "Submitted split `sg' (of `split_max') using `slots' slots"
        }
    }


// Check for completion & get files collapsed by acause
    clear
    local first_file = 1
    if !`keep_old_outputs' capture rm file "`temp_folder'/temp_rdp.dta"

    // wait for buffer
    local buffer = (`split_max'/300)*60000
    if inlist(($resubmit_mode + $troubleshoot_mode), 0 , 1, .) sleep `buffer'

    // check for outputs
    forvalues sg = `split_min'/`split_max' {
        local checkfile = subinstr("`sg_rdp_file'", "[sg]", "`sg'", .)
        check_for_output, locate_file("`checkfile'") timeout(0) failScript("`rdp_worker_script'") scriptArguments("`data_set_group' `data_set_name' `data_type' `sg' $troubleshoot_mode")
        noisily display "Appending split `sg' of `split_max'"
        append using "`checkfile'"
        quietly save "`temp_folder'/temp_rdp.dta", replace
    }

// verify that all worker outputs are added and none were duplicated
    duplicates drop
    bysort split_group: gen nvals = _n == 1
    count if nvals == 1
    if r(N) < `split_max' {
        if     $troubleshoot_mode pause
        else BREAK
    }
    drop nvals

// Verify that metrics are within acceptable range
    preserve
        // verify totals
            summ (orig_cases1)
            if round(r(sum)) != round(`pre_rdp_total') {
                display "Error in `data_set_name' (`data_type', split `split_group'): Total number of 'original' events post-rdp does not equal number of 'original' events pre-rdp (`pre_rdp_total' before, `r(sum)' after')"
                if $troubleshoot_mode pause
                else BREAK
            }

            local pre_rdp_total = r(sum)

        // check the dataset total
            summ(cases1)
            local post_total = r(sum)
            local delta = round(`post_total') - round(`pre_rdp_total')
            if abs(`delta') > 0.0005 * round(`pre_rdp_total')  {
                noisily di in red "Error in `data_set_name' (`data_type'): Total cases before rdp does not equal total after. `pre_rdp_total' events before, `post_total' after. A difference of `delta' events"
                if $troubleshoot_mode pause
                else BREAK
            }
            summ(cases1) if substr(acause, 1, 4) == "neo_"
            local post_total_neo_ = r(sum)
            summ (orig_cases1) if substr(acause, 1, 4) == "neo_"
            local pre_rdp_total_neo_ = r(sum)
            if round(`pre_rdp_total_neo_') > round(`post_total_neo_') {
                noisily di in red "Error in `data_set_name' (`data_type'): Total mapped cases before rdp is less than total after. `pre_rdp_total_neo_' events before, `post_total_neo_' after."
                if $troubleshoot_mode pause
                else BREAK
            }
    restore

// keep only those causes appearing in the original dataseet
    merge m:1 registry_id year* sex acause using "`temp_folder'/input_acause_list.dta", keep(1 3)
    gen cause = acause if _merge == 1 | substr(acause, 1, 4) != "neo_"
    replace acause = "rdp_remnant" if _merge == 1 |  substr(acause, 1, 4) != "neo_"
    drop _merge

// replace data that were not redistributed
    capture confirm file "`skipped_rdp_file'"
    if !_rc append using "`skipped_rdp_file'"

// restore data_set_name
    capture gen data_set_name = "`data_set_name'"

// convert back to original data_type if necessary
    if "`data_type'" == "mor" {
        rename *cases* *deaths*
    }

// Save redistributed file
    save_prep_step, process(6)

// compile a list of codes that did not merge
    clear
    forvalues sg = `split_min'/`split_max' {
        capture append using "`temp_folder'/split_`sg'/bad_codes.dta"
    }
    if _N save "`error_folder'/`data_set_name'_`data_type'_bad_codes_${today}.dta", replace

//
    if $troubleshoot_mode pause off
    capture log close

** **************************************************************************
**  END cancer_rdp_master.do
** **************************************************************************
