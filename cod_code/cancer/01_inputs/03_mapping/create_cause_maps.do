
// Purpose:    Create cause maps for use in mapping and redistribution steps of cancer prep

** **************************************************************************
** CONFIGURATION
** **************************************************************************
    // Clear memory and set STATA to run without pausing
        clear all
        set more off
        set maxvar 32000

    // Load and run set_common function based on the operating system
        do "FILEPATH" "registry_input"   // loads globals and set_common function
        set_common, process(3) data_set_group("_parameters") data_set_name("mapping")

    // set folders
        local data_folder = r(data_folder)
        local temp_folder = r(temp_folder)
        local archive_folder = r(archive_folder)

    // set subroutine
        local format_map = "$registry_input_code/03_mapping/format_map.do"

    // set additional filepaths
        local CoD_ICD10_map = "$CoD_ICD10_map"
        local CoD_ICD9_detail_map = "$CoD_ICD9_detail_map"
        local full_CoD_map = "`data_folder'/reference_maps/CoD_icd_to_cause_map.dta"

** *************************************************************************
** Get CoD maps
** *************************************************************************
    // // ICD10 & ICD9_detail
        // Get ICD10 YLD causes for incidence
            use "`CoD_ICD10_map'", clear
            keep if inlist(substr(cause_code, 1, 1), "C", "D")
            keep cause_code cause_name yll_cause yld_cause

        // Add decimal
            replace cause_code = substr(cause_code, 1, 3) + "." + substr(cause_code, 4, .) if strlen(cause_code) > 3

        // Append and save
            gen coding_system = "ICD10"
            duplicates drop
            compress
            tempfile icd_10_cause_map
            save `icd_10_cause_map', replace

        // Get ICD9_detail YLD causes for incidence
            use "`CoD_ICD9_detail_map'", clear
            drop if inlist(substr(lower(cause_code), 1, 1), "v", "e", "a", "z", "c", "u")
            keep cause_code cause_name yll_cause yld_cause

        // Add decimal
            replace cause_code = substr(cause_code, 1, 3) + "." + substr(cause_code, 4, .) if strlen(cause_code) > 3

        // drop non-cancer data
            gen test = real(cause_code)
            drop if test < 140 | test >= 240
            drop test

        // append and save
            gen coding_system = "ICD9_detail"
            append using `icd_10_cause_map'
            duplicates drop
            compress
            tempfile icd_cause_map
            save `icd_cause_map', replace

    // Drop irrelevant data and remove unnecessary characters
        foreach var of varlist _all {
            tostring `var', replace
        }
        capture _strip_labels *

    // Ensure that there are no empty entries
        replace yld_cause = yll_cause if yld_cause == "" & yll_cause != ""
        replace yll_cause = yld_cause if yll_cause == "" & yld_cause != ""

    // save
        save "`full_CoD_map'", replace

** **************************************************************************
** Get data and keep relevant
** **************************************************************************
    // Get file and save archived copy
        import delimited using "$parameters_folder/custom_cancer_map.csv", clear varnames(1)

    // Check that all data is present.
        drop if coding_system == "" & cause_name == "" & gbd_cause == "" & cause == ""
        replace gbd_cause = trim(gbd_cause)
        replace gbd_cause = "" if gbd_cause == "."
        count if gbd_cause == "" | (cause == "" & cause_name == "")
        assert !r(N)
        count if coding_system == ""
        assert !r(N)

    // Drop irrelevant data and remove unnecessary characters
        foreach var of varlist _all {
            if "`var'" != "data_type" tostring `var', replace
        }
        keep coding_system cause cause_name gbd_cause* additional_cause* data_type
        capture _strip_labels *

    // mark data as priority for use in removing duplicates
        gen remap = 1

    // save incidence-only map
        preserve
            keep if inlist(data_type, 1, 2)
            drop data_type
            save "`temp_folder'/formatted_custom_cancer_map_inc.dta", replace
        restore

    // save mortality-only map
        keep if inlist(data_type, 1, 3)
        drop data_type
        save "`temp_folder'/formatted_custom_cancer_map_mor.dta", replace

** *************************************************************************
** Create Cause Maps for each data type
** *************************************************************************
foreach data_type in inc mor {
    ** *************************************************************************
    ** Get CoD maps
    ** *************************************************************************
        // Determine yll/yld cause type
        if "`data_type'" == "mor" local cause_type = "yll"
        else local cause_type = "yld"

        // Get ICD YLL causes for mortality
            use "`full_CoD_map'", clear
            keep coding_system cause_code `cause_type'_cause*
            rename (cause_code `cause_type'_cause*) (cause gbd_cause*)
            gen additional_cause1 = gbd_cause if gbd_cause != "_gc"

        // Add cancer-specific mapping
            append using "`temp_folder'/formatted_custom_cancer_map_`data_type'.dta"
            replace additional_cause1 = gbd_cause if additional_cause1 == "" & gbd_cause != "_gc"
            replace additional_cause1 = cause if additional_cause1 == "" & gbd_cause == "_gc" & regexm(coding_system, "ICD")

        // re-categorize benign causes (if someone dies, the cancer wasn't benign)
            if "`data_type'" == "mor" {
                foreach v of varlist gbd_cause additional_cause* {
                    replace `v' = subinstr(`v', "_benign", "", .)
                }
            }

    ** *************************************************************************
    ** Format and Finalize Mortality Map for 03_mapping
    ** *************************************************************************
        // format entries
            do "`format_map'"

        // Save
            save_prep_step, process(3) parameter_file("${gbd_cause_map_`data_type'}")
            capture log close

    ** *************************************************************************
    ** Re-format and Finalize Map for 06_redistribution
    ** *************************************************************************
        // Keep only ICD coding systems
            keep if substr(coding_system, 1, 3) ==  "ICD"
            gen existing = 1

        // Load remaining ICD codes (codes that may be generated during redistribution)
            append using "`full_CoD_map'"
            keep existing coding_system cause gbd_cause

        // Remove duplicates by prioritizing the cancer map
            duplicates tag coding_system cause, gen(dup)
            drop if dup != 0 & existing != 1
            drop dup

        // add copies of codes with no decimal
            tempfile appended_map
            save `appended_map', replace
            drop if !regexm(cause, "\.")
            replace cause = subinstr(cause, ".", "", 1)
            drop if regexm(cause, "\.") & coding_system == "ICD9_detail"
            replace cause = subinstr(cause, ".", "", .)
            gen dec_removed = 1
            append using `appended_map'
            duplicates tag coding_system cause, gen(dup)
            drop if dup != 0 & dec_removed != 1
            drop dup dec_removed
            replace cause = trim(itrim(cause))

        // Ensure that there are no remaining duplicates
            duplicates tag coding_system cause, gen(dup)
            count if dup !=0
            assert !r(N)
            drop dup

        // Save
            keep coding_system cause gbd_cause
            order coding_system cause gbd_cause
            sort coding_system cause gbd_cause
            save_prep_step, process(6) parameter_file("${rdp_cause_map_`data_type'}")
            capture log close

}

** **************************
** END
** **************************
