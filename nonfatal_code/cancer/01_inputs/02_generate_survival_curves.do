*****************************************
** Description: Loads formatted survival curves, then generates 5-year survival estimates
*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "[FILEPATH]" "nonfatal_model"

** ****************************************************************
** SET MACROS
** ****************************************************************
// Set output files
    local output_file = "$scalars_folder/survival_curves.dta"
    local long_term_copy = "$long_term_copy_scalars/survival_curves.dta"

// Directory of extracted survival data from SEER
    local survival_input_dir "[PATH]"

// SEER & SURVCAN 5-year survival for 1950 and latest
    local survival_5yr_historical_data "`survival_input_dir'/data/intermediate/seer_1950_SurvCan_worst_5_year.dta"

// Leukemia subtype data
    local gbd2015_update = "`survival_input_dir'/data/intermediate/gbd2015_update.dta"

// Yearly survival pattern
    local survival_yearly_data "`survival_input_dir'/data/intermediate/seer_yearly_1_30_years.dta"

** **************************************************************************
** RUN PROGRAM
** **************************************************************************
** ***************************
** Get data
** ***************************
// format GBD2015 update for worst case
    use `gbd2015_update', clear
    rename (survival_best survived_years year) (survival years_survived year_diagnosed)
    sort year_diagnosed
    keep if year_diagnosed == year_diagnosed[1]
    replace year_diagnosed = 1950
    tempfile update_worst
    save `update_worst', replace

// // Get 1950s data (worst case). add update
    use "`survival_5yr_historical_data'", clear
    rename survival_worst survival
    gen years_survived = 5
    gen year_diagnosed = 1950
    // add update
    drop if inlist(acause, "neo_meso", "neo_gallbladder") | regexm(acause, "neo_leukemia_")
    append using `update_worst'
    // save
    tempfile survival_1950
    save `survival_1950', replace

// Load SEER survival and add update
    use "`survival_yearly_data'", clear
    drop if inlist(acause, "neo_meso", "neo_gallbladder") | regexm(acause, "neo_leukemia_")
    append using `gbd2015_update'

// format, then append worst case survival
    rename (survival_best survived_years year) (survival years_survived year_diagnosed)
    append using `survival_1950'
    keep acause sex year_diag years_survived survival
    sort acause sex year_diag years_surv

** ***************************
** Format Data and Run Regressions
** ***************************
// Square up the data so every possible year-survived has a point for 1950, even if missing
    preserve
        keep if year_diagnosed == 1950
        append using `update_worst'
        keep acause sex year_diagnosed
        duplicates drop
        expand 30
        bysort acause sex year_diagnosed: gen years_survived=_n
        tempfile all_years
        save `all_years', replace
    restore
    merge 1:1 acause sex year_diagnosed years_survived using `all_years', nogen
    sort acause sex year_diag years_surv

// Create a zero-years for everything
    preserve
        keep acause sex year_diagnosed
        duplicates drop
        gen years_survived = 0
        gen survival = 100
        tempfile at_diagnosis
        save `at_diagnosis', replace
    restore
    append using `at_diagnosis'
    sort acause sex year_diag years_surv

// Drop 21-30 years-survived for causes where they are all blank
    drop if years_survived > 20 & acause != "_neo" & acause != "neo_breast" & acause != "neo_colorectal" & acause != "neo_lung" & acause != "neo_prostate"

// Make survival into a fraction
    replace survival = survival / 100

// Generate id for each cause-sex combination
    gen id = acause + "_M" if sex == 1
    replace id = acause + "_F" if sex == 2
    levelsof(id), local(ids) clean

// GLM survival by cause sex combo
    foreach i of local ids {
        preserve
            display "Running regression for `i'"
            keep if id == "`i'"
            glm survival i.year_diagnosed i.years_survived##i.years_survived, family(binomial)
            ** glm survival year_diagnosed i.years_survived##i.years_survived, family(binomial)
            predict predicted_survival, mu
            tempfile `i'
            save ``i'', replace
        restore
    }

// Append together
    clear
    foreach i of local ids {
        append using ``i''
    }
    drop id

// Save
    save "`survival_input_dir'/data/intermediate/glm.dta", replace

// Save again with the 1950- 2010 dichotomy
    keep if year_diag==1950 | year_diag==2010
    gen fallacious_assumption="best" if year_diag==2010
    replace fallacious="worst" if year_diag==1950
    drop year_diag survival
    rename (predicted years_survived) (survival_ survived_years)
    reshape wide survival, i(acause sex survived_years) j(fallacious) string

// Fix all the year-zeros
    replace survival_best = 1 if survived_years==0
    replace survival_worst = 1 if survived_years==0

// Save
    compress
    save "`survival_input_dir'/data/final/glm_survival_curves.dta", replace

** ******************************************
** GBD 2010 survival curves
** ******************************************
// Use gallbladder cancer from GBD 2010 (missing from current survival data)
    preserve
        clear
        gen sex = .
        tempfile gallbladder_data
        save `gallbladder_data', replace
        foreach sex_letter in M F {
            insheet using "[FILEPATH]", comma clear
            keep if year == 2010
            gen acause = "neo_gallbladder"
            gen survived_years = months / 12
            rename best survival_best
            rename worst survival_worst
            if "`sex_letter'" == "M" gen sex = 1
            if "`sex_letter'" == "F" gen sex = 2
            keep acause sex survived_years survival_best survival_worst
            order acause sex survived_years survival_best survival_worst
            append using `gallbladder_data'
            tempfile gallbladder_data
            save `gallbladder_data', replace
        }
    restore
    drop if acause == "neo_gallbladder"
    append using `gallbladder_data'

** ***************************
** Handle Exceptions
** ****************************

// Duplicate breast cancer in males
    preserve
        keep if acause == "neo_breast" & sex == 2
        replace sex = 1
        tempfile male_breast_data
        save `male_breast_data', replace
    restore
    append using `male_breast_data'

// Duplicate _neo for neo_other
    preserve
        keep if acause == "_neo"
        replace acause = "neo_other"
        tempfile neo_other_data
        save `neo_other_data', replace
    restore
    append using `neo_other_data'

// Duplicate melaonma for NMSC and set best survival to 1 for all time periods
    preserve
        keep if acause == "neo_melanoma"
        replace acause = "neo_nmsc_scc"
        replace survival_best = 1
        tempfile neo_nmsc_data
        save `neo_nmsc_data', replace
    restore
    append using `neo_nmsc_data'

** ***************************
** Finalize and Save
** ****************************
// create monthly column
    gen survival_month=12*survived_years
    rename survived_years survival_years

// Reformat
    order acause sex survival_year survival_month survival_best survival_worst
    sort acause sex survival_year survival_month

// Save
    compress
    save "`output_file'", replace
    save "`long_term_copy'", replace


** *******
** END
** *******
