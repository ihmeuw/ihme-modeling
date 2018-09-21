
// Purpose:        Prepare sequela durations for use in calculating prevalence for YLDs for Cancer


** **************************************************************************
** CONFIGURATION
** **************************************************************************
    // Set application preferences
    // Clear memory and set memory and variable limits
        clear all
        set mem 1G
        set maxvar 32000

    // Set to run all selected code without pausing
        set more off

    // Set graph output color scheme
        set scheme s1color


    // Get date
        local today = date(c(current_date), "DMY")
        local year = year(`today')
        local month = string(month(`today'),"%02.0f")
        local day = string(day(`today'),"%02.0f")
        local today = "`year'_`month'_`day'"


** **************************************************************************
** SET LOCALS
** **************************************************************************
    // main directory
        local data_dir = [FILEPATH]

    // raw sequela durations files
        local gbd2010_seq_durs "`data_dir'/raw/GBD_2010/sequelae_durations_from_pp_57_58_EG_report.csv"
        local gbd2013_seq_durs_1 "`data_dir'/raw/main_four_sequela_durations_expert_opinion.csv"
        local gbd2013_seq_durs_2 "`data_dir'/raw/Cancer_sequelae_durations_2014_07_16.xlsx"

** **************************************************************************
** RUN PROGRAM
** **************************************************************************
// GBD 2010 sequla durations, for comparison
    insheet using "`gbd2010_seq_durs'", clear
    rename (primary metastat terminal) (primarydxrx disseminated terminal)
    keep primary dissem term acause
    drop if primary==.

// Add on a row for mesothelioma so we can run it with old version (5 year death deadline)
    count
    local newobs= `r(N)'+1
    set obs `newobs'
    replace acause="neo_meso" if _n==`newobs'
    replace primary = 4 if acause=="neo_meso"
    replace disseminated = 7.75 if acause=="neo_meso"

// Save
    saveold "`data_dir'/final/old_sequela_durations.dta", replace
    saveold "`data_dir'/_archive/old_sequela_durations_`today'.dta", replace


// July 16, 2014 sequelae durations
    import excel using "`raw_seq_durs'", sheet("Sequela duration ") firstrow case(lower) clear

// Save
    saveold "`data_dir'/final/sequela_durations.dta", replace
    saveold "`data_dir'/_archive/sequela_durations_`today'.dta", replace


** *************
