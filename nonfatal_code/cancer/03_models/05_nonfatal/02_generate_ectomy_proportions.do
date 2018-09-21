*****************************************
** Description: Calculate proportion of ectomy procedures for the given
**          hospital patient population in preparation for upload into Epi/Dismod
** IMPORTANT NOTE: this script is currently functional but does not correctly calculate
**      the coverage population. Hospital data for only some but not all countries covers
**      the entire country and logic has not yet been written to account for this

*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "$h/cancer_estimation/00_common/set_common_roots.do" "nonfatal_model"

** ****************************************************************
** SET MACROS FOR CODE
** ****************************************************************
// set location of step1_output
    local step1_output = "$ectomy_folder/01_formatted_ectomy_data.dta"

// Set output
    local output_folder "$procedure_proportion_folder"
    make_directory_tree, path("`output_folder'")

// set adjustment for colostomy proportions based on lit review
    local colo_prop_adjustment = .65

** ****************************************************************
** Part 1: Create Maps
** ****************************************************************
// location map
    use "$parameters_folder/locations.dta", clear
    keep ihme_loc_id location_id
    tempfile locations
    save `locations', replace

// Modelable Entity Map
     // get data
         use "$parameters_folder/causes.dta", clear

    // create lists of procedure codes for later use
         drop if cancer_procedure == ""
        levelsof procedure_proportion_id, clean local(p_ids)
        levelsof procedure_proportion_id if cancer_procedure == "stoma", clean local(stoma_id)

     // keep relevant data
         keep cancer_procedure procedure_proportion_id
        duplicates drop
        rename cancer_procedure procedure_name

     // save
         tempfile pr_ids
         save `pr_ids', replace

** ***********************************
** Part 2: Format Hospital Data
** ***********************************
// aggregate data
    // list files in folder
    local files : dir "`input_folder'/raw/" files "*.dta"

    // format each file and append to full dataset
    local first_loop = 1
    foreach f in `files' {
        noisily di "`f'"
        use "`input_folder'/raw/`f'",
        keep location_id year age sex cancer_procedure_*
        compress

        // Map procedure codes
            quietly foreach var of varlist cancer_procedure_* {
                if !regexm("`f'", "_cancer_data.dta") continue
                count if `var' != ""
                if !r(N) continue
                noisily display in red "     mapping procedure for `var'"
                rename `var' procedure_name
                merge m:1 procedure_name using `pr_ids', keep(1 3) nogen
                rename procedure_name `var'
                local nn = subinstr("`var'","cancer_procedure_","proportion_id_",.)
                rename proportion_id `nn'
            }

        // mark data location
            capture gen ihme_loc_id = substr("`f'", 1, 3)

        // Save
        if !`first_loop' append using "`step1_output'"
        local first_loop = 0
        save "`step1_output'", replace
    }

    // keep relevant variables, compress and save
    drop cancer_procedure_*
    compress
    save "`step1_output'", replace

** ***********************************
** Part 3: Calculate Proportion
** ***********************************

// count the total number of hospital patients for each category
    gen patient = 1
    bysort year sex age: egen total_patients = total(patient)

// calculate the  for each modelable entity
foreach m of local p_ids {
    preserve
    // Count amount of data containing the target cancer procedure
        gen procedure_performed = 0
        foreach var of varlist proportion_id_* {
            replace procedure_performed = 1 if `var' == `m'
        }
        keep if procedure_performed == 1
        collapse (sum) procedure_performed (mean) total_patients, by(year sex age)
        if `m' == `stoma_id' procedure_performed = procedure_performed*`colo_prop_adjustment'

    // Calculate proportion equal to number_of_procedures/total_patients_in_hospital
        gen double procedure_ = procedure_performed/total_patients
        keep year sex age procedure_
        compress
        save "`output_folder'/procedure_proportion_`m'.dta", replace

    restore
}

** ******
** END
** ******
