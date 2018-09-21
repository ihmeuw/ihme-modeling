** Purpose:    Standardize format of manually formatted cancer data_sources

** **************************************************************************
** CONFIGURATION  (AUTORUN)
**         Define J drive location. Sets application preferences (memory allocation, variable limits). Set standard folders based on the arguments
** **************************************************************************
// Clear memory and set STATA to run without pausing
    clear all
    set more off

// Accept Arguments
    args data_set_group data_set_name data_type

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)
    run "FILEPATH" "registry_input"   // loads globals and set_common function
    set_common, process(1) data_set_group("`data_set_group'") data_set_name("`data_set_name'") data_type("`data_type'")

// set folders
    local metric = r(metric)
    local data_folder = r(data_folder)
    local temp_folder = r(temp_folder)
    local error_folder = r(error_folder)

// set location of subroutines
    local standardize_causes = "$standardize_causes_script"

** ****************************************************************
** Part 1: Get data.
** ****************************************************************
// GET DATA
    use "`data_folder'/$step_0_output", clear

** ****************************************************************
** Part 2: Standardize Causes
** ***************************************************************
// // // Standardize cause and cause_name
    do `standardize_causes'

** ******************************************************************
** Part 3: Standardize order and variable widths. Upadate variable names
** ******************************************************************
// Standardize column widths
    // long strings
        format %30s cause_name

    // short strings
        format %10s cause coding_system


** ******************************************************************
** SAVE
** ******************************************************************
// SAVE
    save_prep_step, process(1)
    capture log close


** *********************************************************************
** End standardize_format.do
** *********************************************************************
