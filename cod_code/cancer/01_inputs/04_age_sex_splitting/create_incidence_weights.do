// Purpose:    Create incidence rates baset on "Gold Standard Data" that will be used to generate weights in age age/sex splitting and acause disaggregation


** **************************************************************************
** Configure and Set Folders
** **************************************************************************
    // Clear memory and set memory and variable limits
        clear all
        set more off

    // Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
        run "FILEPATH" "registry_input"   // loads globals and set_common function
        set_common, process(3) data_set_group("_parameters") data_set_name("mapping") data_type("inc") h("`h'")

    // Input folder
        local registry_inputs = "$prep_process_storage"

    // Output folders
        local temp_folder = r(temp_folder)
        local code_folder = "$registry_input_code/04_age_sex_splitting"

** ****************************************************************
** GET ADDITIONAL RESOURCES
** ****************************************************************
// Load cause restrictions
    import delimited using "$common_cancer_data/causes.csv", clear
    keep if mi_model == 1
    keep acause male female yll_age_start yll_age_end
    rename yll_* *

// edit age formats
    foreach var in age_start age_end {
        replace `var' = floor(`var'/5) + 6 if `var' >= 5
        replace `var' = 0 if `var' < 5
    }

// save
    tempfile cause_restrictions
    save `cause_restrictions', replace

** ********************
** Get Data
** ********************
// // Pool all incidence datasets: Get list of sources in cancer prep folder and append all sets together
    clear
    local loopnum = 1
    foreach folder in "CI5" "USA" "NORDCAN" {
        local subfolders: dir "`registry_inputs'/`folder'" dirs "*", respectcase
        foreach subFolder in `subfolders' {
            if substr("`subFolder'", 1, 1) == "_" | substr("`subFolder'", 1, 1) == "0" continue
            if regexm(upper("`subFolder'"), "APPENDIX") continue
            if "`folder'" == "USA" & "`subFolder'" != "usa_seer_1973_2008_inc"  continue  // only use only gbd2010 seer incidence

            local data_path = "`registry_inputs'/`folder'/`subFolder'"
            local incidence_file = "`data_path'/03_mapped_inc.dta"
            capture confirm file "`incidence_file'"
            if !_rc {
                noisily di "`subFolder'"
                use "`incidence_file'", clear
                rename (frmat im_frmat) (frmat_inc im_frmat_inc)
                merge m:1 registry_id sex year* using "`data_path'/00_formatted_inc_pop.dta", keep(3) assert(1 3)
                rename (frmat im_frmat) (frmat_pop im_frmat_pop)
                drop _merge
                gen data_set = "`subFolder'"
                if `loopnum' != 1 append using "`temp_folder'/all_mapped_inc_data.dta"
                else local loopnum = 0
                save "`temp_folder'/all_mapped_inc_data.dta", replace
            }
            else display "MISSING: `subFolder' inc data"
        }
    }
    tempfile all_mapped_inc_data
    save `all_mapped_inc_data', replace

** ********************
** Format and Keep data of interest
** ********************
// // Keep only "gold standard" data
    ** use "`temp_folder'/all_mapped_inc_data.dta", clear
    // generate country_id and location_id
        split registry_id, p(.)
        drop registry_id3
        rename (registry_id1 registry_id2) (country_id location_id)
        destring country_id location_id, replace
        replace location_id = country_id if location_id == 0

    // generate an average year to be used for dropping data
        gen year = floor((year_start+year_end)/2)
        gen year_span = 1 + year_end - year_start

    // drop USA data that is not from SEER, since SEER is the most trustworthy
        drop if country_id == 102 & !regexm(lower(data_set), "seer")

    // drop NORDCAN data except for special causes with little data
        drop if regexm(lower(data_set), "nordcan") & regexm(gbd_cause,"neo_meso") & regexm(gbd_cause,"neo_leukemia")

    // more known data than unknown data
        drop cases1 pop1
        egen cases1 = rowtotal(cases*), missing
        egen pop1 = rowtotal(pop*), missing
        drop if cases1 == .
        drop if cases26 != 0

    // drop non-CI5 data if it can be obtained from CI5
        egen uid = concat(registry_id year* sex), punct("_")
        gen is_nord = 1 if regexm(data_set_name,"NORDCAN") | regexm(data_set_name,"nordcan")
        bysort uid: egen has_nord = total(is_nord)
        drop if has_nord > 0 & is_nord == .
        drop has_nord is_nord
        gen is_ci5 = 1 if regexm(data_set_name,"CI5") | regexm(data_set_name,"ci5")
        bysort uid: egen has_ci5 = total(is_ci5)
        drop if has_ci5 > 0 & is_ci5 == .
        drop is_ci5 has_ci5 uid
        duplicates tag registry_id gbd_cause sex year* data_set_name, gen(tag)
        if tag > 0 {
            pause on
            pause duplications exist for registry, cause, sex, year & data_set_name. Issue needs to be resolved before moving on
        }
        drop tag

    // Drop within-data_set duplications due to multiple year spans
        sort location_id sex registry_id gbd_cause year
        egen uid = concat(location_id sex registry_id gbd_cause year), punct("_")
        duplicates tag uid, gen(duplicate)
        bysort uid: egen smallestSpan = min(year_span)
        drop if duplicate != 0 & year_span != smallestSpan
        drop year year_span

    // keep only data in the correct format
        keep if inlist(frmat_inc, 0, 1, 2, 131)

    // save
        tempfile good_format
        save `good_format', replace

** ********************
** Correct formatting
** ********************
    // Correct population, if necessary
        count if !inlist(frmat_pop, 0,1,2,131) | !inlist(im_frmat_pop, 2, 8, 9)
        if r(N) != 0 {
            // save data without population for later merge
            preserve
                drop pop*
                save "`temp_folder'/inc_data_without_pop.dta", replace
            restore

            // save population data to enable split
            keep registry_id frmat_pop im_frmat_pop year_start year_end sex pop*
            rename *frmat* *frmat
            duplicates drop
            save "`temp_folder'/inc_temp_pop_only.dta", replace

            // split population
            do "`code_folder'/split_population.do" "$h" "_parameters" "mapping" "inc" "`temp_folder'"

            // merge with incidence data
            use "`temp_folder'/04_age_sex_split_inc_pop.dta", clear
            collapse (mean) pop*, by(location_id sex registry_id year*)
            merge 1:m registry_id sex year* using "`temp_folder'/inc_data_without_pop.dta", keep(3) nogen
        }

    // Combine all 80+ deaths to one age category, if  80+ categories exist
        foreach n of numlist 23/25 {
            capture confirm variable cases`n'
            if !_rc {
                replace cases22 = cases22 + cases`n'
                drop cases`n'
            }
        }

    // Population data entered for a year range (where year_start != year_end) are entered as the mean of that year range, unlike the incidence & mortality data
    // Multiply population data by the span of the year range so that the values are on the scale of the incidence data
        gen year_span = year_end - year_start + 1
        foreach p of varlist pop* {
            replace `p' = `p' * year_span
        }
        drop year_span

** ********************
** Create dataset for "average cancer"
** ********************
    // Create "average_cancer"
        preserve
            keep location_id registry_id year* sex pop*
            duplicates drop
            collapse (sum) pop*, by (sex)
            tempfile average_cancer_pop
            save `average_cancer_pop', replace
        restore
        preserve
            collapse (mean) cases*, by(sex)
            merge 1:1 sex using `average_cancer_pop', nogen
            gen gbd_cause = "average_cancer"
            tempfile average_cancer
            save `average_cancer', replace
        restore

    // Make weights for other cancers
        preserve
            keep location_id registry_id year* sex pop*
            collapse (mean) pop*, by(location_id registry_id year* sex)
            tempfile pop_byLocationYear
            save `pop_byLocationYear', replace
        restore
            drop pop*
            merge m:1 location_id registry_id year* sex using `pop_byLocationYear', nogen
        append using `average_cancer'
        drop if gbd_cause == "_gc"

    // Append, rename, and save
        collapse cases* pop*, by(sex gbd_cause) fast
        rename gbd_cause acause

    // save
        save "`temp_folder'/gold_standard_mapped_inc_data.dta", replace

** ********************
** Create Rates
** ********************
// Rename and format variables
    drop if acause == ""
    aorder
    keep acause sex cases2 cases7-cases22 pop2 pop7-pop22
    gen obs = _n
    reshape long cases@ pop@, i(sex acause obs) j(gbd_age)
    gen age = (gbd_age - 6)*5
    replace age = 0 if gbd_age == 2
    _strip_labels*
    drop age
    rename gbd_age age

// Apply sex restrictions
    merge m:1 acause using `cause_restrictions', keep(1 3)
    replace cases = 0 if _merge == 3 & sex == 1 & male == 0
    replace cases = 0 if _merge == 3 & sex == 2 & female == 0
    drop male female

// Apply age restrictions
    replace cases = 0 if _merge == 3 & age_start > age
    replace cases = 0 if _merge == 3 & age_end < age
    drop age_start age_end _merge

// Generate sex = 3 data
    tempfile split_sex
    save `split_sex', replace
    collapse (sum) cases* pop*, by(age acause)
    gen sex = 3
    append using `split_sex'

// ensure non-zero data if data should not be zero
    egen uid = concat(acause sex), p("_")
    sort uid age
    replace cases = ((cases[_n-1]+cases[_n+1])/2) if cases == 0 &  cases[_n-1] > 0 & cases[_n+1] > 0 & uid[_n-1] == uid & uid[_n+1] == uid
    drop uid

// Create Rate
    gen double inc_rate = cases/pop
    drop cases pop obs
    replace inc_rate = 0 if inc_rate == . | inc_rate < 0
    reshape wide inc_rate, i(sex acause) j(age)

// // Finalize and Save
    keep sex acause inc_rate*
    order sex acause
    sort acause sex
    save_prep_step, process(3) parameter_file("$age_sex_weights_inc")

** ****
** END
** ****
