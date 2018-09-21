
// Purpose:    Disaggregates observations for which multiple acauses (alternate causes) are assigned (generally only garbage codes). Redistributes the aggregate number of cases/deaths among those separated observations.

** **************************************************************************
** CONFIGURATION
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(5) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)

// set maps
    local acause_rates = "${age_sex_weights_`data_type'}"
    local gbd_cancer_map = "${gbd_cause_map_`data_type'}"
    local nmsc_subtype_proportions = "$parameters_folder/scc_bcc_proportions.dta"
    local hiv_proportions = [FILEPATH]

// set scripts
    local merge_with_pop = "$registry_input_code/_registry_common/merge_with_population.do"

** ****************************************************************
** Get Additional Resources
** ****************************************************************
// Add GBD codes as ICD codes in the cause map
    use `gbd_cancer_map', clear
    keep if coding_system == "GBD"
    replace cause = cause_name
    replace coding_system = "ICD10"
    tempfile GBD_codes
    save `GBD_codes', replace
    replace coding_system = "ICD9_detail"
    append using `GBD_codes'
    save `GBD_codes', replace
    use `gbd_cancer_map', clear
    keep if regexm(coding_system, "ICD")
    append using `GBD_codes'
    tempfile cancer_map
    save `cancer_map', replace

// Get cause map. Note: all alternate causes should be gbd_causes or ICD10 codes
    use `cancer_map', clear
    keep cause coding_system gbd_cause acause1 acause2
    tempfile cause_map
    save `cause_map', replace

// Get cause weights
    use `acause_rates', clear
    if "`data_type'" == "inc" rename inc_rate* rate*
    else rename death_rate* rate*
    rename acause wgt_cause
    tempfile cause_rates
    save `cause_rates', replace

// create map of possible wgt_cause names
    use `cause_rates', clear
    keep sex wgt_cause
    duplicates drop
    tempfile possible_weights
    save `possible_weights', replace

// Generate wgt_cause_map
    use `cancer_map', clear
    keep cause gbd_cause coding_system
    rename (cause gbd_cause) (wgt_cause mapped_cause)
    tempfile wgt_cause_map
    save `wgt_cause_map', replace

// Get Kaposi sarcoma proportions
    use `hiv_proportions', clear
    keep if p_reg == 1 // global proportions
    keep if regexm(cause, "C46")
    rename (target prop3) (gbd_cause prop2)
    replace cause = substr(cause, 1, 3) + "." + substr(cause, 4, .) if strlen(cause) >3
    gen start_year_range = substr(year_range, 1, 4)
    gen end_year_range = substr(year_range, -4, .)
    destring start end, replace
    keep sex cause gbd_cause start end prop*
    duplicates drop
    tempfile kaposi_proportions
    save `kaposi_proportions', replace

** **************************************************************************
** Prepare Data
** **************************************************************************
// Get Data
    use "`data_folder'/$step_4_output", clear

// Change coding system
    replace coding_system = "ICD10" if coding_system != "ICD9_detail"

// Alert if unmapped causes exist
    count if gbd_cause == "" | acause1 == ""
    if r(N) > 0 {
        di "ERROR: Missing causes present in the dataset"
        BREAK
    }

// Calculate total metrics for later comparison
    capture drop `metric'_total
    capture drop `metric'1
    egen `metric'1 = rowtotal(`metric'*)
    capture sum(`metric'1)
    local pre_disagg_total = r(sum)

// Preserve unique identifiers (UIDs) and metric data
    // create dummy variable to enable later merge
    gen obs = _n
    preserve
        keep registry_id year_start year_end sex coding_system cause cause_name obs
        gen orig_cause = cause + cause_name
        drop cause*
        tempfile UIDs
        save `UIDs', replace
    restore
    // preserve metric data
    preserve
        keep obs `metric'*
        rename `metric'1 obs_total
        tempfile metric_data
        save `metric_data', replace
    restore

// reshape data to determine the number of alternate causes. tag data with multiple acauses
    keep obs registry_id year* sex cause acause* coding_system
    reshape long acause, i(obs sex registry_id year* coding_system) j(acause_num)
    capture _strip_labels*
    gen wgt_cause = acause
    replace wgt_cause = cause if acause == ""
    drop if acause == ""

// Merge with wgt_cause_map to enable merge with cause_weights
    merge m:1 wgt_cause coding_system using `wgt_cause_map', keep(1 3)
    replace wgt_cause = mapped_cause if _merge == 3
    replace wgt_cause = "average_cancer" if substr(wgt_cause, 1, 4) != "neo_"
    replace wgt_cause = "neo_leukemia" if regexm(acause, "leukemia")
    replace wgt_cause = "neo_nmsc" if regexm(acause, "neo_nmsc")
    drop if mapped_cause == "_none"
    drop mapped_cause _merge

// Merge names of possible weights with the dataset. Rename any wgt_cause entries that failed to merge as the  "average_cancer"
    merge m:1 sex wgt_cause using `possible_weights', keep(1 3)
    replace wgt_cause = "average_cancer" if _merge == 1 & wgt_cause != "_none"
    drop _merge

// drop data that are mapped to "_none"
    drop if wgt_cause == "_none"

// Save a copy of the data for later use
    tempfile prepared_for_merge
    save `prepared_for_merge', replace

** **************************************************************************
** Create Cause Weights
**         merge data with rates before merging with population data (will multiply by pop by rate to create weights)
** **************************************************************************
// re-load data
    use `prepared_for_merge', clear

// merge with rates
    merge m:1 sex wgt_cause using `cause_rates', keep(1 3) assert(2 3) nogen

// merge with population data
    do "`merge_with_pop'" "`data_folder'"

// make weights
    foreach n of numlist 2 7/22 {
        gen wgt`n' = rate`n'*pop`n'
        replace wgt`n' = 0 if wgt`n' == .
    }

// keep only relevant information
    keep obs wgt*
    duplicates drop

// save
    tempfile cause_wgts
    save `cause_wgts', replace

** ***************************************************************************
** Disaggregate
**         use weights to distribute aggregate totals among the alternate causes for each observation
** ***************************************************************************
// merge with weights
    use `prepared_for_merge', clear
    sort obs
    merge m:1 obs using `metric_data', assert(3) nogen
    duplicates tag obs, gen(need_split)
    replace need_split = 1 if need_split > 0
    merge m:1 obs wgt_cause using `cause_wgts', keep(1 3) nogen
    foreach i of numlist 2 7/22 {
        gen orig_`metric'`i' = `metric'`i'
        egen wgt_tot`i' = total(wgt`i'), by(obs)
        replace wgt`i' = 1 if need_split == 1 & wgt_tot`i' == 0
        egen wgt_scaled`i' = pc(wgt`i'), by(obs) prop
        replace `metric'`i' = `metric'`i' * wgt_scaled`i' if need_split == 1
    }

// verify calculations
    egen double row_total = rowtotal(`metric'*)
    bysort obs: egen new_obs_total = total(row_total)
    gen diff = new_obs_total - obs_total
    count if abs(diff) > 1
    if r(N){
        noisily di "ERROR: New observation total does not equal original at full disaggregation step"
        aorder
        else BREAK
    }

// drop extra variables
    drop wgt* need_split row_total new_obs_total orig_`metric'* diff

// // Specially Handle Kaposi Sarcoma Data
    // merge with kaposi sarcoma weights. this will create multiple copies of each obs (location, year, sex, cause...)
        joinby sex cause using `kaposi_proportions', unmatched(master)
    // keep the obs copies with data years that match the proportion years (if no data matches the proportion years, keep one copy and do nothing)
        gen year = floor((year_start+ year_end)/2)
        bysort obs: gen any_match = 1 if _merge == 3 & inrange(year, start, end)
        bysort obs: egen has_match = total(any_match) if _merge == 3
        bysort obs: gen single_entry = _n == 1 if _merge == 3 & has_match == 0
        drop if _merge == 3 & (has_match > 0 & !inrange(year, start, end) ) | (has_match == 0 & single_entry == 0)
        foreach i of numlist 2 7/22 {
            gen orig_`metric'`i' = `metric'`i'
            replace prop`i' = . if has_match == 0
            replace `metric'`i' = `metric'`i' * prop`i' if _merge == 3 & has_match > 0
        }
    // replace redistributed causes
    replace acause = gbd_cause if _merge == 3 & has_match > 0
    replace acause = "C46" if substr(acause, 1, 3) == "C46" & strlen(acause) <= 6

    // verify calculations
    egen double row_total = rowtotal(`metric'*)
    bysort obs: egen new_obs_total = total(row_total)
    gen diff = new_obs_total - obs_total
    count if abs(diff) > 1
    if r(N){
        noisily di "ERROR: New observation total does not equal original at Kaposi disaggregation step"
        aorder
        else BREAK
    }

    // drop extra variables
    drop _merge prop* any_match has_match gbd_cause row_total new_obs_total orig_`metric'* diff

// If incidence data only, specially handle NMSC garbage (C44/173 data)
    if "`data_type'" == "inc" {
        // adjust bcc_scc map
        preserve
            use "`nmsc_subtype_proportions'", clear
            rename (acause cause) (mapped_cause acause)
            tempfile sb_props
            save `sb_props', replace
        restore

        // merge with bcc_scc map
            joinby sex acause using `sb_props', unmatched(master)
        // multiply by proportions
            foreach i of numlist 2 7/22 {
                gen orig_`metric'`i' = `metric'`i'
                replace `metric'`i' = `metric'`i' * prop`i' if _merge ==3
            }

        // verify calculations
            egen double row_total = rowtotal(`metric'*)
            bysort obs: egen new_obs_total = total(row_total)
            gen diff = new_obs_total - obs_total
            count if abs(diff) > 1
            if r(N){
                noisily di "ERROR: New observation total does not equal original at NMSC disaggregation step"
                aorder
                else BREAK
            }

        // replace redistributed causes
            replace acause = mapped_cause if _merge == 3

        // drop extra variables
            drop mapped_cause _merge prop* row_total new_obs_total orig_`metric'* diff
    }

** ***************************************************************************
** Clean-up, Check for Errors, and Save
**
** ***************************************************************************
// // Map disaggregated causes to gbd causes. Preserve any ICD codes that are associated with garbage codes
    // Preserve any ICD codes that are associated with garbage codes.
        rename acause disag_cause
        replace cause = disag_cause if disag_cause != "_gc" // replaces disaggregat

    // prepare for merge with map
        replace cause = "zzz" if cause == "ZZZ"
        replace coding_system = "ICD10" if inlist(substr(cause, 1, 1), "C", "D")

    // merge with cause map
        capture merge m:1 cause coding_system using `cause_map', keep(1 3) assert(2 3)
        if _rc {
            di "ERROR: Not all causes could be mapped"
            BREAK
        }

    // Verify that newly mapped codes are mapped to only one cause
    capture count if trim(acause2) != "" & | (acause1 != gbd_cause & gbd_cause != "_gc")
    if r(N) > 0 {
        keep if acause2 != "" & | acause1 != gbd_cause
        di "ERROR: Some disaggregated causes are mapped to more than one code. Please correct the map for `data_type'."
        di "(Note: only two alternate causes are currently shown. For all alternate causes, see the full map.)"
        BREAK
    }

    // rename gbd_cause and remove irrelevant variables
    rename gbd_cause acause
    drop _merge acause1 acause2

// Drop extraneous information and Merge with UIDs
    keep obs acause cause disag_cause `metric'*
    merge m:1 obs using `UIDs', assert(3) nogen

// Recalculate totals
    capture drop `metric'1
    egen `metric'1 = rowtotal(`metric'*)

// Check for calculation errors. A difference within 0.0005% is acceptable since it likely results from a decimal storage error
    capture sum(`metric'1)
    local delta = r(sum) - `pre_disagg_total'
    if `delta' > 0.00001 * `pre_disagg_total' {
        noisily di in red "ERROR: Total `metric' before disaggregation does not equal total after (difference = `delta' `metric')."
        BREAK
    }

// Check for missing cause information
    capture count if cause == "" | acause == ""
    if r(N) > 0 {
        noisily di in red "ERROR: Cannot continue with missing cause information. Error in cause_disaggregation suspected."
        BREAK
    }

// Collapse to combine rows of the same uid
    collapse (sum) `metric'*, by(registry_id year_start year_end sex coding_system acause cause orig_cause disag_cause) fast

// restore data_set_name
    capture gen data_set_name = "`data_set_name'"

// SAVE
    order registry_id sex year_start year_end acause cause disag_cause orig_cause registry `metric'* coding_system
    save_prep_step, process(5)
    capture log close


** **************************************************************************
** END cause_disaggregation.do
** **************************************************************************
