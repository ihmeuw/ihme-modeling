*****************************************
** Description: Formats sequela duration estimates
*****************************************

// Set STATA workspace
    set more off
    capture set maxvar 32000

// Set common directories, functions, and globals (shell, finalize_worker, finalization_folder...)
  run "[FILEPATH]" "nonfatal_model"

** ****************************************************************
** SET MACROS
** ****************************************************************
    local input_data = "$cancer_storage/02_database/03_sequela_duration/data/final/sequela_durations.dta"
    local output_file = "$scalars_folder/sequela_durations.dta"
    local long_term_copy = "$long_term_copy_scalars/sequela_durations.dta"

** **************************************************************************
** RUN PROGRAM
** **************************************************************************
// Format
    use "`input_data'", clear
    gen st_in_remission = 0
    rename (primary disseminated terminal) (st_primary st_disseminated st_terminal)
    reshape long st_, i(acause) j(stage) string
    rename st_ sequela_duration

// Save
    compress
    save "`output_file'", replace
    save "`long_term_copy'", replace


** **********
** END
** **********
