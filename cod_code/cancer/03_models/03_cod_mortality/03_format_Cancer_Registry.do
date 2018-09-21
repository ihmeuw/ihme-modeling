*****************************************
** Description: Loads and formats mortality estimates for entry into the CoD prep pipeline.
**      Drops data if total cancer deaths are greater than a certain percentage of
**      total deaths for all causes
** Input(s): USER
** Output(s): formatted dta file within the CoD storage structure
** How To Use: simply run script

*****************************************
// Clear memory and set memory and variable limits
    clear all
    set more off

// Load and run set_common function based on the operating system
    if "$database_name" == "" global database_name = "cod_mortality"
    run "FILEPATH" "database" // loads globals and functions
    local username = c(username)

// cc_code threshold: what maximum percentage of deaths do we believe could come from cancer
    local death_threshold = 0.70

// location of cod prep code
    local cod_prep_code_location = "[FILEPATH]"

** ***********************
** ESTABLISH DIRECTORIES
** ***********************
// data_name (same as folder name)
    global data_name = "Cancer_Registry"
// input dataset
    global in_file "$cod_mortality_model_storage/02_mortality_estimate.dta"
// input error folder
    global error_dir "$cod_mortality_database_workspace/errors"
    make_directory_tree, path("$error_dir")
// output directory
    global out_dir "$j/WORK/03_cod/01_database/03_datasets/${data_name}/data/intermediate"
    // map directory
    global list_dir "$j/WORK/03_cod/01_database/03_datasets/${data_name}/maps/cause_list"

** ***************************************
** GET ADDITIONAL RESOURCES
** ***************************************
// Get Envelope Deaths and Population to generate cc code
    use "$common_cancer_data/mortality_envelope.dta", clear
    merge 1:1 location_id year sex age_group_id using "$common_cancer_data/populations.dta", keep(3) nogen
    keep if age_group_id == 1 | inrange(age_group_id, 6, 20) | inlist(age_group_id, 30, 31, 32, 235)

// convert age groups
    replace age_group_id = age_group_id + 1 if age_group_id <= 20
    replace age_group_id = 22 if inlist(age_group_id, 30, 31, 32, 235)
    keep location_id year sex age_group_id mortality pop
    collapse (sum) mortality pop, by(location_id year sex age_group_id)

// Generate death rate
    gen double env_death_rate = mortality / pop

// Rename for merge
    rename (pop age_group_id) (e_pop age)

// Save
    compress
    tempfile env_death_rate
    save `env_death_rate', replace

** **************************************
** Get data and begin formatting to fit with CoD standards
** **************************************
// Get Data
    use "$in_file", clear

// Drop data before 1970
    drop if year < 1980

// Drop missing data
    drop if deaths == .

// create missing variables
    gen iso3 = substr(ihme_loc_id, 1, 3)
    gen source = "data_set " + data_set_id + ", registry " + registry_id

// rename, remove unwanted labels and variables
    rename MI mi_ratio

// remove labels
    capture _strip_labels*

** ******************************************************
** Generate CC_code: an estimate of non-cancer deaths for each location
** ******************************************************
// merge dataset with envelope data
    merge m:1 location_id year age sex using `env_death_rate', keep(1 3) nogen

// Multiply pop by envelope death rate to get all-cause death total
    gen double all_cause_deaths = pop * env_death_rate

// Compare the all-cause death total with the total cancer deaths by location_id, year, age, and sex
    bysort iso3 location_id year age sex national: egen double total_cancer_deaths = total(deaths)
    count if total_cancer_deaths >= `death_threshold' * all_cause_deaths & total_cancer_deaths != . & deaths != . & all_cause_deaths != .
    local non_credible_entries = r(N)
    local total_entries = _N

// If unrealistic data exist, save a list of those data that are unrealistic then drop. make an exception for Germany in GBD2015 only.
    if `non_credible_entries' {
        preserve
        display in red "ERROR: For `non_credible_entries' of `total_entries', the number of cancer deaths is greater than the believable percentage of total predicted deaths for the location-sex-age-year. These data will be dropped, but the error should be investigated."
        order iso3 location_id year age sex national acause
        sort iso3 location_id year age sex national acause
        keep if (total_cancer_deaths >= `death_threshold' * all_cause_deaths) & total_cancer_deaths != . & deaths != .
        save "$error_dir/cancer_death_total_not_credible_${today}.dta", replace
        restore
        drop if total_cancer_deaths >= `death_threshold'*all_cause_deaths & all_cause_deaths != .
    }

// Generate cc_code data
    preserve
        // generate cc_code data
            gen cc_code_deaths = all_cause_deaths - total_cancer_deaths

        // ensure that data is present for all cc_code entries
            count if cc_code_deaths == .
            assert !r(N)

        // keep only one instance of the cc_code
            keep iso3 location_id country_id year age sex national cc_code_deaths
            duplicates drop

        // save
            gen acause = "cc_code"
            rename cc_code_deaths deaths
            tempfile cc_code_data
            save `cc_code_data', replace
    restore

// append cc_code data to the original data, then drop irrelevant rows
    append using `cc_code_data'

// Drop irrelevant rows and return to wide shape
    keep iso3 location_id country_id age sex year acause source NID national deaths*
    reshape wide deaths, i(iso3 location_id NID sex year acause source national country_id) j(age)

// Drop duplicate cc_code and verify presence of cc_code
    bysort iso3 location_id year sex national: gen hasCC = 1 if acause == "cc_code"
    bysort iso3 location_id year sex national: egen check = total(hasCC)
    capture count if check == 0
    assert !r(N)
    drop hasCC check

    bysort iso3 location_id year sex national: gen onlyCC = 1 if acause != "cc_code"
    bysort iso3 location_id year sex national: egen check = total(onlyCC)
    capture count if check == 0
    assert !r(N)
    drop onlyCC check

// Recalculate deaths1
    capture drop deaths1
    egen deaths1 = rowtotal(deaths*)

** ***************************************
** Generate, Adjust, and Check Required Variables
** ***************************************
// Remove location_id information from subnationally modeled locations
    replace location_id = . if location_id == country_id

// Age formats
    capture drop frmat
    capture drop im_frmat
    gen frmat = 2
    gen im_frmat = 9
    gen deaths91 = deaths2

// Source information. source refers to the CoD directory source folder, source_type is translated to a data_type_id in the CoD database , source_label can be used as a descriptor, but must be uniform within a location_id
    gen source_label = source
    sort iso3 location_id sex year acause national source
    replace source_label = source_label[_n+1] if acause == "cc_code"  //  "cc_" will always come before "neo_" in a group
    replace NID = NID[_n+1] if acause == "cc_code"  //  "cc_" will always come before "neo_" in a group
    replace source = "Cancer_Registry"  // name of the source folder in the cod database
    gen source_type = "Cancer Registry"

// list (string): tabulation of the cause list: ICDs are "10det" "10tab" "9det" "9BTL" "8A". if custom, give the source (e.g. DSP, VA-custom). used to determine map in CoD mapping
    gen list = "none"

// Generate cause and cause_name
    gen cause_name = acause
    rename acause cause

// subdiv will be shown as "site" in CodViz.
    gen subdiv = subinstr(source_label, ",", ";", .)

** ****************************
** Data Checks
** ****************************
    foreach v of varlist iso3 cause source_label {
        capture count if "`v'" == ""
        if r(N) > 0 {
            di "There are missing values for `v'. Correct these errors before proceeding."
            pause on
            pause
            pause off
        }
    }
    foreach v of varlist national NID year sex {
        capture count if `v' == .
        if r(N) > 0 {
            di "There are missing values for `v'. Correct these errors before proceeding."
            pause on
            pause
            pause off
        }
    }

    sort iso3 location_id year sex cause
    duplicates tag iso3 location_id year sex cause, gen(dup)
    capture count if dup != 0
    if r(N) > 0 {
        di "There are duplicate entries by cause. Canceling Format"
    }
    drop dup

    // destring NID. CoD requries NIDs to be type(int)
    destring NID, replace
    replace NID = 284465 if NID == .
** ****************************
** STANDARDIZE AGE GROUPINGS
** ****************************
    do "`cod_prep_code_location'/code/format_gbd_age_groups.do"

** *****************************
**             EXPORT
** *****************************
    do "`cod_prep_code_location'/code/export.do" "$data_name"

** *****************************
** End Script
** *****************************
