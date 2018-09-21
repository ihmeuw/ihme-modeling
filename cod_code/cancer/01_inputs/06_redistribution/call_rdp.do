
// Purpose:        Creates file that can be submitted to redistribution, then submits that file

** **************************************************************************
** CONFIGURATION  (AUTORUN)
**         Define J drive location. Sets application preferences (memory allocation, variable limits). Set standard folders based on the arguments
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type keep_old_outputs

// Create Arguments if Running Manually
    if "`keep_old_outputs'" == "" local keep_old_outputs = 0

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(6) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)

// set scripts
    local cancer_rdp_master = "$code_for_this_prep_step/cancer_rdp_master.do"

** **************************************************************************
** Prepare Data for RDP
** **************************************************************************
    // delete previous outputs if requested
        if !`keep_old_outputs' better_remove, path("`temp_folder'") recursive("true")

    // Get cause restricitons
        use "$cause_restrictions", clear
        keep if substr(acause, 1, 4) == "neo_"
        keep acause *_age_* male female
        tempfile cause_restrictions
        save `cause_restrictions', replace

    // Get garbage code remap (for ICD9 coding systems with codes that are mapped to ICD10)
        import delimited using "$garbage_remap", clear varnames(1) case(preserve)
        keep if inlist(data_type, "both", "`data_type'")
        keep ICD10 ICD9_detail
        rename (ICD10 ICD9_detail) (acause new_cause)
        gen coding_system = "ICD9_detail"
        tempfile garbage_recode
        save `garbage_recode', replace

    // get location_ids
        import delimited using "$registry_id_table", clear
        keep registry_id location_id
        tempfile location_id_map
        save `location_id_map', replace

    ** ************************
    ** format file for redistribution
    ** ************************
        // GET DATA
            use "`data_folder'/$step_5_output", clear

        // Keep only relevant data
            keep `metric'* registry_id year_start year_end sex coding_system acause cause
            drop if regexm(acause, "hiv")

        // add location information
            merge m:1 registry_id using `location_id_map', keep(1 3) assert(2 3) nogen

        // Replace garbage codes with causes
            replace acause = cause if acause == "_gc"

        // Set all coding systems to ICD10 and ICD9_detail, since these are the only ones processed by RDP
            gen has_9 = 1 if coding_system == "ICD9_detail"
            bysort registry_id sex year*: egen uid_has_9 = total(has_9)
            replace coding_system = "ICD9_detail" if uid_has_9 > 0
            replace coding_system = "ICD10" if coding_system != "ICD9_detail"
            drop *has_9

            // Recode ICD9_detail coding systems that contain ICD10 codes
            count if coding_system == "ICD9_detail"
            if r(N) {
                merge m:1 coding_system acause using `garbage_recode', keep(1 3) nogen
                replace acause = new_cause if new_cause != ""
            }

            // Collapse
            collapse (sum) `metric'*, by(registry_id location_id year_start year_end sex coding_system acause) fast

        // Rename deaths to cases so that it can go through cancer redistribution without a lot of fuss
            if "`data_type'" == "mor" {
                quietly foreach var of varlist deaths* {
                    local nn = subinstr("`var'","deaths","cases",.)
                    rename `var' `nn'
                }
            }

        // Apply restrictions
            // Reshape everything long
                egen obs = group(registry_id location_id year* sex coding_system), missing
                preserve
                    keep obs registry_id location_id year_start year_end
                    duplicates drop
                    tempfile UIDs
                    save `UIDs', replace
                restore
                keep obs sex coding_system acause cases*
                foreach i of numlist 1/26 {
                    capture gen cases`i' = 0
                    replace cases`i' = 0 if cases`i' == .
                }
                aorder
                drop cases1 cases4-cases6 cases26
                reshape long cases@, i(obs sex coding_system acause) j(gbd_age)
                gen age = (gbd_age - 6)*5
                replace age = 0 if gbd_age == 2
                replace age = 1 if gbd_age == 3

            // Merge with restrictions
                merge m:1 acause using `cause_restrictions', keep(1 3) nogen
                quietly foreach var of varlist *_age_start {
                    replace `var' = 0 if `var' == .
                }
                quietly foreach var of varlist *_age_end {
                    replace `var' = 99 if `var' == . | `var' == 80
                }
                quietly foreach var of varlist male female {
                    replace `var' = 1 if `var' == .
                }
                drop if sex == 1 & male == 0
                drop if sex == 2 & female == 0

            // Apply restrictions
                if "`data_type'" == "inc" local yl = "yld"
                else if "`data_type'" == "mor" local yl = "yll"
                replace acause = "ZZZ" if (sex == 1 & male == 0) | (sex == 2 & female == 0)
                replace acause = "ZZZ" if age < `yl'_age_start & age > `yl'_age_end
                replace acause = "C80" if coding_system == "ICD10" & (acause == "ZZZ" | acause == "195")
                replace acause = "195" if coding_system == "ICD9_detail" & (acause == "ZZZ" | acause == "C80")
                collapse (sum) cases*, by(obs sex coding_system acause gbd_age) fast

            // Reshape wide
                capture drop age male female *_age_start *_age_end
                reshape wide cases, i(obs sex coding_system acause) j(gbd_age)
                egen cases1 = rowtotal(cases*)
                foreach i of numlist 1/26 {
                    capture gen cases`i' = 0
                    replace cases`i' = 0 if cases`i' == .
                }
                merge m:1 obs using `UIDs', keep(1 3) assert(3) nogen
                drop obs

        // save copy for troubleshooting
            compress
            aorder
            save "$pre_rdp_file", replace

** ****************************************************************
**  Run RDP
** ****************************************************************
    // Submit job
        do "`cancer_rdp_master'" "`data_set_group'" "`data_set_name'" "`data_type'" `keep_old_outputs'

** **************************************************************************
**  END call_rdp.do
** **************************************************************************
