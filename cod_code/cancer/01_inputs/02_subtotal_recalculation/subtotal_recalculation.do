
// Purpose:    Standardizing cause codes & mapping

** **************************************************************************
** CONFIGURATION  (AUTORUN)
**         Define J drive location. Sets application preferences (memory allocation, variable limits). Set standard folders based on the arguments
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type resubmit

// Load and run set_common function based on the operating system
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(2) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)
    local code_folder = r(code_folder)

// set location of subroutines
    local subroutines_folder = "$code_for_this_prep_step"
    local remove_subtotals_script = "`subroutines_folder'/remove_subtotals.py"
    local code_components_script = "`subroutines_folder'/code_components.py"

** *******************************************************************
** Part 1: Check for Data Exception
**     Sources for which there are known rounding errors (See Part 6)
** ********************************************************************
// Define Source Exceptions
    local data_set_exceptions = [LIST OF EXCEPTIONS]


// Set exception variable to 1 if dataset is known to acceptably contain subtotals that don't add correctly (i.e. are recalculated to negative values)
    local data_exception = 0
    foreach s of local data_set_exceptions {
        if "`data_set_name'" == "`s'" local data_exception = 1
    }

** *********************************************************************
** Part 2: Load Data. Drop "All" cause data and Check for Aggregate Codes
**
** *********************************************************************
    // GET DATA
        use "`data_folder'/$step_1_output", clear

    // Collapse after standardization
        collapse (sum) `metric'*, by(registry_id year_start year_end frmat im_frmat sex coding_system cause cause_name) fast

    // Drop "All" codes
        gen all = 1 if regexm(cause,  "C00-C43,C45-C97") | regexm(cause, "C00-43,C45-97") | regexm(cause, "C00-C80") | regexm(cause, "C00-C94") | regexm(cause, "C00-C96") | regexm(cause, "C00-C97")
        drop if all == 1
        drop all

    // Generate uniqid, unique to each registry_id, year, and sex
        egen uniqid=group(registry_id year_start year_end sex frmat im_frmat), missing
        sort cause cause_name uniqid

    // save a version of the file before the check
        tempfile preCheck
        save `preCheck', replace

    // Keep just the cause and the unique id
        keep cause uniqid coding_system
        duplicates drop

    // Determine if Aggregates Exist (denoted by comma or dash in cause variable)
        local aggregate_code_exist = 0
        count if regexm(cause,",")==1 | regexm(cause,"-")==1 & inlist(coding_system, "ICD10")
        if `r(N)' > 0 local aggregate_code_exist = 1


** *********************************************************************
** Part 3a: If No Aggregate Codes Exist, Finalize and End Script
** *********************************************************************
    if !`aggregate_code_exist'{

        // Use presplit data
        use `preCheck', clear

        // add variables to indicate that no recalculation was needed
            gen orig_cause = cause
            gen codes_removed = ""

        // Keep only variables of interest
            keep registry_id year_start year_end sex frmat im_frmat coding_system cause cause_name orig_cause codes_removed `metric'*
            order coding_system cause cause_name orig_cause codes_removed `metric'* , last

        // Save
            save_prep_step, process(2)

        // Close Log
            capture log close

        // Exit Current *.do file
            noisily di "No recalculation needed. Script complete."
             exit, clear
    }

** *********************************************************************
** Part 3b: If Aggregate Codes Exist (if script is still running),
**            remove old outputs and save a copy of the data in its current state
** *********************************************************************
    // remove any files present in the temp_folder
    if !`resubmit'{
        noisily di "Removing previous temporary files..."
        better_remove, path(`temp_folder') recursive("true") extension(".dta")
    }

    // Create Additional Temp Folders
        capture mkdir "`temp_folder'/inputs"
        capture mkdir "`temp_folder'/outputs"

    //
        noisily di "Saving inputs..."

    // separately save copies of data by whether they can be disaggregated
        use `preCheck', clear
        drop `metric'1
        rename `metric'* metric*
        save `preCheck', replace

    // data that cannot be disaggregated
        keep if coding_system != "ICD10"
        count
        if r(N) > 0 {
            local has_non_icd10 = 1
            capture rm "`temp_folder'/02_coding_system_not_split.dta"
            save "`temp_folder'/02_coding_system_not_split.dta", replace
        }
        else local has_non_icd10 = 0

    // data that can be disaggregated
        use `preCheck', clear
        keep if coding_system == "ICD10"
        tempfile presplit
        save `presplit', replace

    // find the largest uid, and set the lowest
        capture levelsof(uniqid) if coding_system == "ICD10", local(uids) clean
        summ uniqid
        local max_uid = `r(max)'
        local min_uid = `r(min)'

    // Save raw cause input
        rename cause orig
        drop metric*
        compress
        tempfile code_components_raw
        save `code_components_raw', replace
        save "`temp_folder'/02_code_components_raw.dta", replace
        capture saveold "`temp_folder'/02_code_components_raw.dta", replace

** *********************************************************************
** Part 4: Determine Possible Components of Aggregate Codes
**        (if the script has not ended, then aggregate must codes exist).
** *********************************************************************
    // Run python script to separate all codes into their component codes (example: C01-02 becomes C01 and C02)
        !python "`code_components_script'" "`data_type'" "`temp_folder'"

    // Check for completed code separation (code_components_split.dta) and
        clear
        // Check for file
            local numAttempts = 1
            local checkfile "`temp_folder'/02_code_components_split.dta"
            capture confirm file "`checkfile'"
            if _rc == 0 {
                noisily display "code components FOUND!"
            }
            else {
                noisily di in red "Could not find completed code components file"
                BREAK
            }
        use "`temp_folder'/02_code_components_split.dta"
        compress
        save "`temp_folder'/02_code_components_split.dta", replace
        capture saveold "`temp_folder'/02_code_components_split.dta", replace

    // Re-attach uniquid and verify that no data were lost by merging separated code components with list of original codes
        joinby orig coding_system using `code_components_raw', unmatched(both) nolabel
        count if _merge != 3
        if `r(N)' > 0 {
            noisily display in red "ERROR: Unable to merge raw code components with split code components. Check code_components_split.dta for problems."
            BREAK
        }
        drop _merge

    // Save List of Original Codes for
        keep cause orig uniqid
        duplicates drop
        save "`temp_folder'/unique_ids_and_codes.dta", replace
        capture saveold "`temp_folder'/unique_ids_and_codes.dta", replace

** ***********************************************************************
** Part 5: Submit and then compile results of recalculation ("remove_subtotals")
** ***********************************************************************
    // For each uid, save input data then submit recalculation script. Paralellize the process if there are many uids (faster)
        // Prepare input data to save for each uide
        use `presplit', clear
        keep uniqid metric* cause

        save "`temp_folder'/recalculation_input.dta", replace

        // List of uniqids that exist and has codes that need to be disaggregated
        levelsof uniqid

        // if there are many uids, parallelize the script submission (faster)
        if `max_uid' > 500 & c(os) != "Windows" & !`resubmit'{
            foreach uid in `r(levels)' {
                $qsub -pe multi_slot 4 -l mem_free=8g -N "SD_`data_set_name'_`data_type'_`uid'" "$py_shell" "`remove_subtotals_script'" "`uid' `data_type' `temp_folder' `data_exception' `error_folder'"
            }
        }
        else if !`resubmit' {
            !python "`remove_subtotals_script'" "all" `data_type' `temp_folder' `data_exception' "`error_folder'"
        }

    // Append all cause hierarchies together. Check every 15 seconds for completed step. If a long wait has passed, attempt to run the program again
        clear
        sleep 15000
        local waitSeconds = 15
        if `resubmit' local waitSeconds = .1
        foreach uid in `r(levels)' {
            local numAttempts = 0
            local maxAttempts = 12
            // Check for file
                local checkfile "`temp_folder'/outputs/uid_`uid'_output.dta"
                capture confirm file "`checkfile'"
                while _rc {
                        if `numAttempts' == `maxAttempts' {
                            noisily di in red "Could not find completed cause hierarchy `uid' file"
                            BREAK
                        }
                        if `numAttempts' == `maxAttempts' - 2 {
                            !python "`remove_subtotals_script'" `uid' `data_type' `temp_folder' `data_exception' `error_folder'
                            sleep 2000
                        }
                        else {
                            noisily display "`checkfile' not found, checking again in `waitSeconds' seconds"
                            local sleepTime = 1000 * `waitSeconds'
                            sleep `sleepTime'
                        }
                        local numAttempts = `numAttempts' + 1
                        capture confirm file "`checkfile'"
                }
                if !_rc    noisily di "uid `uid' found"
                append using "`checkfile'"
        }

    // Save the compiled outputs (note: this file includes the original metric data along with the disaggregated data, "orig_metric_values")
        compress
        save "`temp_folder'/temp_recalculation_output.dta", replace
        capture saveold "`temp_folder'/temp_recalculation_output.dta", replace

** ***********************************************************************
** Part 6: Format results of recalculation
** ***********************************************************************
    // get uid data
        use `presplit', clear
        drop metric*
        tempfile uid_values
        save `uid_values', replace

    // merge recalculation output with uid data
        use "`temp_folder'/temp_recalculation_output.dta", clear
        replace age = subinstr(age, "metric", "", .)
        drop orig_metric
        reshape wide metric, i(cause codes_removed codes_remaining uniqid) j(age) string
        merge m:1 uniqid cause using `uid_values', keep(1 3) assert(2 3) nogen
        rename (cause codes_remaining) (orig_cause cause)
        save "`temp_folder'/final_recalculation_output.dta", replace

    //    replace blank causes with the original aggregate (will be handled in cause recalculation and rdp). drop causes that were zeroed in subtotal recalculation
        egen metric1 = rowtotal(metric*)
        drop if metric1 == 0
        replace cause = orig_cause if cause == "" & metric1 != 0
        drop metric1

** *********************************************************************
** Finalize: Keep Variables of Interest, Save New and Archive Old
** *********************************************************************
    // Append data from coding systems that are not split
        if `has_non_icd10' == 1 append using "`temp_folder'/02_coding_system_not_split.dta"

    // rename metric data
        rename metric* `metric'*

    // Keep only variables of interest
        keep registry_id year_start year_end sex frmat im_frmat coding_system cause cause_name orig_cause codes_removed `metric'*
        order registry_id year_start year_end sex frmat im_frmat coding_system cause cause_name orig_cause codes_removed `metric'*
        sort registry_id year_start year_end sex frmat im_frmat coding_system cause cause_name orig_cause codes_removed `metric'*

    // restore data_set_name
        capture gen data_set_name = "`data_set_name'"

    // Save
        save_prep_step, process(2)

    // Close Log
        capture log close

** *********************************************************************
** END subtotal_recalculation.do
** *********************************************************************

