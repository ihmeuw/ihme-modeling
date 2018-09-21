*****************************************
** Description: Calculate prevalence of sequela that cannot be easily calculated otherwise

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** Generate Log Folder and Start Logs
** ****************************************************************
// Log folder
    local log_folder "$log_folder_root/sequelae_adjustment"
    make_directory_tree, path("`log_folder'")

// Start Logs
    capture log close _all
    log using "`log_folder'/_master.txt", text replace

** ****************************************************************
** Set Values
** ****************************************************************
// Accept arguments
    args resubmission
    if "`resubmission'" == "" local resubmission = 0

// Set lists of relevant modelable entity ids
    local procedure_me_id = 1724
    local sequelae = "1725 1726"

// If not present, create troubleshooting global to enable submission of sub-jobs. This variable will be equal to 1 if running just_check
    if "$troubleshooting" == "" global troubleshooting = 0

// set output folder
    local output_folder "$modeled_procedures_folder/1724"
    local upload_folder = "$sequelae_adjustment_folder"
    make_directory_tree, path("`output_folder'")
    make_directory_tree, path("`upload_folder'")

// set location of draws download
    local draws_file = "`output_folder'/1724_draws.dta"

// get measure_ids
    use "$parameters_folder/constants.dta", clear
    local prevalence_measure = prevalence_measure_id[1]

// Import function to retrieve epi estimates
    run "$get_draws"

** **************************************************************************
**
** **************************************************************************
// remove old outputs if not resubmitting
    if !`resubmission' & !$troubleshooting {
        better_remove, path("`output_folder'") recursive(1)
    }

// // download data from dismod
    capture confirm file "`draws_file'"
    if _rc | (!$troubleshooting & !`resubmission') {
        get_draws, gbd_id_field(modelable_entity_id) gbd_id(1724) source(dismod) measure_ids(5) clear
        keep location_id year_id sex_id age_group_id draw*
        order location_id year_id sex_id age_group_id draw*
        compress
        save "`draws_file'", replace
    }
    noisily di "`draws_file' present

// // Submit jobs
    local submission_cause = substr("`acause'", 5, .)
    if "$run_dont_submit"!="1" {
        foreach sequela in `sequelae' {
            if !$troubleshooting {
                 ** $qsub -pe multi_slot 3 -l mem_free=6g -N "seqW_`sequela'" "$stata_shell" "$sequelae_worker" "`sequela' `resubmission'"
            }
            else do "$sequelae_worker" `sequela' `resubmission'
        }
    }

// // compile formatted downloads (generated at beginning of the worker script) for adjustment of remission prevalence. 
    // combine data
        noisily di "Generating total sequelae data..."
        clear
        foreach sequela in `sequelae' {
            local checkFile = "`upload_folder'/`sequela'_data.dta"
            check_for_output, locate_file("`checkFile'") timeout(30) failScript("$sequelae_worker") scriptArguments("`sequela' 1")
            noisily di "    appending sequela `sequela' data..."
            append using "`upload_folder'/`sequela'_data.dta"
        }

    // format
        keep if !inlist(age_group_id, 22, 27, 164)
        rename (year_id sex_id) (year sex)


    // calculate the total number of procedures
        merge m:1 location_id year sex age_group_id using "$population_data",  keep(3)  assert(2 3) nogen
        foreach d of varlist draw* {
            local p = subinstr("`d'", "draw", "procedures",.)
            gen `p' = `d' * pop
        }
        keep location_id year sex age_group_id procedures_*
        fastcollapse procedures_*, type(sum) by(location_id year sex age_group_id)

    // save
        compress
        save "`output_folder'/modeled_prevalence_`procedure_me_id'.dta", replace

// // Check for completion of upload
    noisily di "Verifying success of uploads... "
    foreach sequela in `sequelae' {
        local checkfile = "`upload_folder'/`sequela'/sequela_`sequela'_uploaded.dta"
        check_for_output, locate_file("`checkfile'") timeout(1) failScript("$sequelae_worker") scriptArguments("`sequela' 1")
    }

// close log
    capture log close

** ************
** END
** ************
