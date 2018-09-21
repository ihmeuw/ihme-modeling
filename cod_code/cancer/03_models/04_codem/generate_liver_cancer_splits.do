*****************************************
** Description: Runs centralized functions to split the liver cancer mortality estimate into subcauses
** Input(s)/Output(s): USER (runs code that works within a separate database)
** How To Use: run script on cluster
*****************************************
// Clear memory and set memory and variable limits
    clear
    set maxvar 32000

// Set to run all selected code without pausing
    set more off

// load common paths
  run "FILEPATH"

// accept arguments and set ouput folder
    args storage_folder gbd_iteration
    if "`gbd_iteration'" == "" local gbd_iteration = 2016
    if "`storage_folder'" == "" local storage_folder "$codem_model_storage/liver_splits/GBD`gbd_iteration'"

// Ensure presence of storage directories
    foreach subfolder of numlist 418/421 {
        make_directory_tree, path("`storage_folder'/`subfolder'")
    }

// get additional resources
    do "$shared_functions_folder/split_cod_model.ado"
    do "$shared_functions_folder/save_results.do"

// split liver cancer
    split_cod_model, source_cause_id(417) target_cause_ids(418 419 420 421) target_meids(2470 2471 2472 2473) output_dir("`storage_folder'")

// save results
    save_results, cause_id(418) description(Liver cancer due to hepatitis B) in_dir("`storage_folder'/418")
    save_results, cause_id(419) description(Liver cancer due to hepatitis C) in_dir("`storage_folder'/419")
    save_results, cause_id(420) description(Liver cancer due to alcohol) in_dir("`storage_folder'/420")
    save_results, cause_id(421) description(Liver cancer due to other causes) in_dir("`storage_folder'/421")


** ******
** END
** ******
