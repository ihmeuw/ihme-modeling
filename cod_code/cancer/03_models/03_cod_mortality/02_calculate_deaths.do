*****************************************
** Description: Loads incidence and mi_ratio estimates, then genarates
**          mortaltiy estimates that will be sent to CoD database
** Input(s): USER
** Output(s): formatted dta file
** How To Use: simply run script

*****************************************
// Clear memory and set memory and variable limits
    clear all
    set more off

// Load and run set_common function based on the operating system
    if "$database_name" == "" global database_name = "cod_mortality"
  run "FILEPATH" "database"  // loads globals and functions

** **************************************************************************
** Set Macros
** **************************************************************************
// Set macros
    global temp_folder = "$cod_mortality_database_workspace"
    local incidence_input = "$cod_mortality_database_storage/02_combined_incidence.dta"
    local mi_input = "$cod_mortality_model_storage/01_formatted_MI_results.csv"
    local output_file = "$cod_mortality_model_storage/02_mortality_estimate.dta"

** ****************************************************************************
** Get additional redata_sets
** *****************************************************************************
// Load representative status
    import delimited using "$cod_mortality_database_storage/maps/representation_map.csv", clear
    tempfile rep_info
    save `rep_info', replace

// sdi_quintile map
    import delimited using "$common_cancer_data/sdi.csv", varnames(1) clear
    keep location_id sdi_quintile
    destring location_id sdi_quintile, replace
    drop if sdi_quintile == .
    duplicates drop
    tempfile sdi_quintile_map
    save `sdi_quintile_map'

// load mi_input
    import delimited using "`mi_input'", clear
    tempfile mi_data
    save `mi_data', replace

** ****************************************************************************
** Validate
** *****************************************************************************
// Load Data
    use "`incidence_input'", clear

// ensure presence of all necessary location information (country_id, subMod, development status, location_type)
    load_location_info

// set representation status based on map (for CoD, 'national' is data representative of the location_id)
    merge m:1 location_id using `sdi_quintile_map', assert(2 3) keep(3) nogen
    gen grouping = "national"
    replace grouping = "subnational" if location_id != country_id & subMod == 1
    merge m:1 country_id grouping using `rep_info', keep(1 3) nogen
    gen national = 1 if sdi_quintile == 5 // national only if from high_sdi country
    replace national = 1 if representative == 1  // NOTE: for unmerged rows, representative = .
    replace national = 0 if representative == 0 | national == . // NOTE: for unmerged rows, representative = .
    drop grouping representative

// Verify that there is no data_set redundancy
    sort year location_id sex acause
    bysort year location_id sex data_set acause: gen data_set_count = _n == 1
    replace data_set_count = 0 if data_set_count != 1
    bysort location_id year sex acause: egen problem_data_set = total(data_set_count)
    count if problem_data_set > 1
    if r(N) {
        sort location_id year sex acause
        noisily di "ERROR: some datasets are redundant by location_id, year, sex, and acause. See the problem_data_set variable with value greater than 1."
        BREAK
    }
    drop data_set_count problem_data_set

** ****************************************************************************
**  Merge data with MI model results to generate death estimates
** *****************************************************************************
// // save original sex and cause variables, then update to handle exceptions
    gen orig_sex = sex
    gen orig_acause = acause

    // convert sex for breast cancer
        replace sex = 2 if acause == "neo_breast"

    // convert sex for neo_meso
        replace sex = 1 if inlist(acause, "meo_meso", "neo_nasopharynx")

// merge incidence data with MI model results
    merge m:1 location_id sex year acause using `mi_data', keep(3) nogen

// revert cause and sex
    drop acause sex
    rename (orig_acause orig_sex) (acause sex)

// Reshape and Save tempfile
    capture rename mi* MI*
    keep location_id ihme_loc_id sex year acause data_set_id registry_id national country_id pop* cases* MI* NID
    reshape long pop cases MI, i(ihme_loc_id location_id sex year acause data_set_id registry_id national country_id NID) j(age)
    capture _strip_labels*

// mark missing MI's except for when age == 1
    drop if age == 1
    gen missing_MI = 1 if cases != . & MI == .
    count if missing_MI == 1
    if r(N) {
        pause on
        pause there's a missing MI when we have data for the location-time. investigate and fix.
    }

// Calculate deaths
    gen deaths = cases * MI

// check to make sure no negative deaths exist (some reason they randomly showed up during a clean run)
    count if deaths < 0
    if r(N) {
        pause on
        pause negative values exist and they shouldn't. resolve before moving forward (either negative values exist in MI or cases)
    }

// Save
    order ihme_loc_id location_id sex year acause deaths cases MI data_set_id registry_id NID
    compress
    save "`output_file'", replace

** ************************************
** END
** ************************************
