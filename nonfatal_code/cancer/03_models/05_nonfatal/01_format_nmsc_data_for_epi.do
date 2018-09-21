*****************************************
** Description: Loads and formats skin cancer registry data for epi uploader.
** Note: Exception handling or outlier marking must currently be managed outside of this script.
** Input(s): USER
** Output(s): Four Files:
**                Two .dta files of scc/bcc registry data to cancer nonfatal database storage
**                    scc_registry_CURRENTDATE.dta
**                    bcc_registry_CURRENTDATE.dta
**                 Two .xlsx files of scc/bcc registry data to the epi nonlit/registry folder
**                    scc_registry_CURRENTDATE.xlsx
**                    bcc_registry_CURRENTDATE.xslx
*****************************************
** *************************************************************************************************************
** Prepare stata and define filepaths
** *************************************************************************************************************
// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
    run "[FILEPATH]" "nonfatal_model"
    run "$nonfatal_code_folder/subroutines/format_for_epi_upload.ado"

// define filepaths
    local input_data = "$nonfatal_database_storage/selected_nmsc_data.dta"

** *************************************************************************************************************
** Load Relevant Data
** *************************************************************************************************************
   set more off

// Get data
    use "`input_data'" if inlist(acause, "neo_nmsc_bcc", "neo_nmsc_scc"), clear
    recast double pop* cases*
    reshape long cases pop, i(location_id year_start year_end sex acause registry* data_set_name NID underlying_nid) j(age)

// divide cases by year_span to get the average yearly cases
    replace cases = cases/year_span

// generate age_group_id from age
    gen age_group_id = age -1
    drop if age_group_id == 0
    drop age

// add representiveness and urbanicity
    gen national = 1 if registry_name == "National Registry"
    gen representative_name ="Nationally and subnationally representative" if national == 1
    replace representative_name ="Representative for subnational location only" if national!= 1
    gen urbanicity_type="Unknown"

// finish generating required epi variables
    rename NID nid
    gen mean=cases/pop
    gen lower=.
    gen upper=.
    gen uncertainty_type=.
    gen uncertainty_type_value=.
    rename registry_name site_memo
    gen cv_first_dx = 0
    gen extractor = "[username]"

// Handle Exceptions
    // drop CANADA BC since the data we have from lit review 
        drop if registry_id == "101.0.3"
    // drop USA data since SEER has the only reliable data 
        drop if substr(registry_id, 1, 4) == "102."
    // drop if there is no data for cases
        drop if cases == .

** *************************************************************************************************************
** Format BCC data
** *************************************************************************************************************
    preserve
    keep if acause == "neo_nmsc_bcc"
    gen case_name="BCC"

// run function to finish formatting
    format_for_epi_upload, additional_variables("cv_first_dx cases")

// save
    save "$nonfatal_database_storage/bcc_registry_$S_DATE.dta", replace
    export excel "[FILEPATH]", firstrow(variables) sh("extraction") replace

** *************************************************************************************************************
** Format SCC data
** *************************************************************************************************************
    restore
    keep if acause == "neo_nmsc_scc"
    gen case_name="SCC"

// run function to finish formatting
    format_for_epi_upload, additional_variables("cv_first_dx cases")

// save
    save "$nonfatal_database_storage/scc_registry_$S_DATE.dta", replace
    export excel "[FILEPATH]", firstrow(variables) sh("extraction") replace
** **********
** END
** **********
