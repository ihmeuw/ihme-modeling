// Purpose:    Create mortality rates that will be used to generate weights in age age/sex splitting and acause disaggregation

** *****************************************************************************
** **************************************************************************
** Configure and Set Folders
** **************************************************************************
// Clear memory and set memory and variable limits
    clear all
    set more off

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(3) data_set_group("_parameters") data_set_name("mapping") data_type("mor") h("`h'")

// Input folder
    local input_data [filepath]

** ********************
** Get additional resources
** ********************
// Load map of cancer causes
    import delimited using "$common_cancer_data/causes.csv"
    keep if mi_model == 1
    keep acause
    tempfile cancer_causes
    save `cancer_causes', replace

** ********************
** Format CoD data to create cancer rates
** ********************
// import cod data
    use "`input_data'", clear
    merge m:1 acause using `cancer_causes', keep(3) nogen

// verify that restrictions are correctly entered
    foreach v of varlist yll_age_end yll_age_start male female {
        capture count if `v' == .
        if r(N){
            noisily di "ERROR: `v' has missing values. These must be replaced before continuing"
            BREAK
        }
    }

// combine to global-level data
    fastcollapse pop_* deaths_*, by(sex acause yll_age_start yll_age_end male female) type(sum)

// add "average cancer" cause
    preserve
        fastcollapse pop_* deaths_*, by(sex) type(mean)
        gen yll_age_start = 0
        gen yll_age_end = 100
        gen male = 1
        gen female = 1
        gen acause = "average_cancer"
        tempfile average_cancer
        save `average_cancer', replace
    restore
    append using `average_cancer'

// Make weights for sex = 3
    preserve
        collapse (sum) pop_* deaths_* female male (mean) yll_age_start yll_age_end, by(acause)
        replace female = 1 if female > 1
        replace male = 1 if male > 1
        gen sex = 3
        tempfile sex3
        save `sex3', replace
    restore
    append using `sex3'

// make age long to facilitate combination of age groups
    reshape long deaths_ pop_, i(sex acause yll_age_start yll_age_end male female) j(age)

// update age groups to agree with cancer team age groups
    drop if inlist(age, 91, 92, 93, 94)
    replace age = 80 if age > 80


// combine like- data
    fastcollapse pop deaths, by(sex acause yll_age_start yll_age_end male female age) type(sum)

// create rates
    generate rate = deaths/pop
    replace rate = 0 if rate == . | rate<0
    drop deaths pop

// // Drop data that is unusable per restrictions
    // create marker for restriction violations
        gen is_restriction_violation = 0

    // mark restricted sexes
        replace is_restriction_violation = 1 if sex == 1 & male == 0
        replace is_restriction_violation = 1 if sex == 2 & female == 0

    // mark restricted ages.
        replace is_restriction_violation = 1 if age < yll_age_start
        replace is_restriction_violation = 1 if age > yll_age_end

    // replace rates of restriction violoations with 0
        replace rate = 0 if is_restriction_violation == 1

    // ensure that non-zero rates are present for every entry that is not a restriction violation
        capture count if inlist(rate, 0 , .) & is_restriction_violation == 0
        assert !r(N)

    // remove marker for restriction violations
        drop is_restriction_violation

// format age for cancer prep
    replace age = (age/5) + 6 if age > 1
    replace age = 2 if age == 1

// keep relevant information and reshape for use in cancer prep
    keep sex acause rate age
    rename rate death_rate
    reshape wide death_rate, i(acause sex) j(age)

// sort and save
    order sex acause
    sort acause sex
    save_prep_step, process(3) parameter_file("$age_sex_weights_mor")

** ****
** END
** ****
