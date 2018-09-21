
// Purpose:    Handles formatting for the creation of cancer maps

** **************************************************************************
** Standardize: NOTE - this section should be identical to
**    'Part 3: Standardize Causes' in standardize_format.do
** **************************************************************************
    // rename acauses
        rename additional_cause* acause*

    // drop invalid ICD10 codes
        drop if coding_system == "ICD10" & length(cause) < 3

    // standardize ICD10 causes
        capture program drop format_ICD10_codes
        local standardize_causes = "$standardize_causes_script"
        do `standardize_causes'
        format %24s cause_name

    // save a copy
        compress
        tempfile causes_standardized
        save `causes_standardized', replace

** **************************************************************************
** Format data
** **************************************************************************
// // Rename variables and drop unnecessary variables
    // drop subtotals 
        replace gbd_cause = "sub_total" if gbd_cause == "sub  total" | gbd_cause == "sub total"
        replace gbd_cause = "sub_total" if cause_name == "All"

    // drop unused variables
        drop gbd_cause_name

    // replace blank acause1 (acause1 is used to map entries to the age weights file in age/sex splitting, hence it must be equal to gbd_cause unless it is a garbage code with special handling)
        foreach var of varlist _all {
            if "`var'" == "remap" continue
            di "`var'"
            replace `var' = trim(`var')
            replace `var' = "" if `var' == "."
        }

    // Correct cause names
        foreach v of varlist gbd_cause acause* {
            replace `v' = "neo_leukemia_ll_acute" if `v' == "neo_leukemia_all"
            replace `v' = "neo_leukemia_ll_chronic" if `v' == "neo_leukemia_cll"
            replace `v' = "neo_leukemia_ml_acute" if `v' == "neo_leukemia_aml"
            replace `v' = "neo_leukemia_ml_chronic" if `v' == "neo_leukemia_cml"
            replace `v' = subinstr(`v', "_cancer", "", .)
            replace `v' = trim(itrim(`v'))
        }

// // Drop remaining duplicates
    // drop obvious duplicates
        duplicates drop

    // drop remaining duplicates with the CoD maps
        duplicates tag coding_system cause cause_name, gen(dup)
        bysort coding_system cause cause_name: egen has_remap = total(remap)
        drop if dup != 0 & has_remap != 0 & remap != 1
        drop dup *remap

    // drop remaining duplicates within map
        duplicates tag coding_system cause cause_name, gen(remaining_dup)
        count if remaining_dup != 0
        if r(N) > 0 {
            gsort -remaining_dup +coding_system +cause +cause_name +gbd_cause
            pause on
            di "Please remove conflicting entries before proceeding"
            pause
            pause off
        }
        drop remaining_dup

// // Reshape to remove blank acauseX (ensures that there are no empty variables between gbd_cause and any additional causes
    // tag any additional causes and keep only those causes
        reshape long acause@, i(coding_system cause cause_name gbd_cause) j(acause_num)

    // remove erroneous entries like RDP packages
        replace acause =  "" if regexm(acause,"RDP")

    // drop duplicates that occurr within the same uid (eliminate non-unique additional causes)
        sort coding_system cause cause_name gbd_cause acause acause_num
        egen uid = concat(coding_system cause cause_name gbd_cause acause), p("_")
        drop if uid[_n] == uid[_n-1]
        drop uid

    // eliminate blank entries when there are additional causes
        egen uid = concat(coding_system cause cause_name gbd_cause), p("_")
        bysort uid: egen num_acause = sum(acause_num)
        drop if num_acause > 1 & acause == ""
        drop uid num_acause

    // generate new acause numbers and reshape
        drop acause_num
        egen acause_num = seq(), by(coding_system cause cause_name gbd_cause)
        reshape wide acause, i(coding_system cause cause_name gbd_cause) j(acause_num)

    // if acause1 is still missing replace it with the gbd_cause.
        replace acause1 = gbd_cause if acause1 == "" & gbd_cause != "_gc"
        replace acause1 = cause if inlist(acause1, "", "_gc") & gbd_cause == "_gc" & regexm(coding_system, "ICD")
        drop if cause == "" & cause_name == ""

    // add missing entries for GBD coding_system
        preserve
            keep gbd_cause
            duplicates drop
            drop if gbd_cause == "_gc"
            gen coding_system = "GBD"
            gen cause_name = gbd_cause
            gen acause1 = gbd_cause
            tempfile gbd_causes
            save `gbd_causes', replace
        restore
        append using `gbd_causes'
        duplicates drop

** **************************************************************************
** Verify Formatting
** **************************************************************************
    duplicates tag cause cause_name coding_system, gen(duplicated)
    count if duplicated != 0
    if r(N){
        noisily di "ERROR: duplicates found after fomrtting"
        pause on
        pause

    }

** *******
** END
** *******
