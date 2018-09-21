**Purpose: This simply compiles the error files together as well as creates a new file that only contains the surveys that we can analyze

clear all
set more off
pause off
set trace off
capture restore, not

if c(os) == "Windows" {
	global prefix "J:"
}
if c(os) == "Unix" {
	global prefix "/home/j"
}

** Working directories
global raw_dir "$prefix/DATA/"
global log_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs"
global data_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data"
global analysis_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_analysis_file.csv"
global temp_error_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs/errors"
global temp_error_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/temp"
global error_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/failed_DHS.dta"
global error_file_archive "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/archive/failed_DHS_$S_DATE.dta"
global used_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_used_file.csv"

** start log
cap log close
cd "$log_dir"
log using "dp2.smcl"

** compile the error files
cd "$temp_error_dir"
tempfile errors
local files: dir "$temp_error_dir" files "*", respectcase
foreach file of local files {
    use "`file'", clear
    cap append using `errors'
    save `errors', replace
}

** save error file and an archived version of the error file
cap use `errors', clear
if (_rc == 0) {
    saveold "$error_file", replace
    saveold "$error_file_archive", replace
}
else {
    clear
    set obs 1
    gen data_name = "NA"
    gen iso3 = "NA"
    gen year = "NA"
    gen read_fail = 0
    gen no_maternal = 0
    gen note = "NO ERRORS! YAY!"
    saveold "$error_file", replace
    saveold "$error_file_archive", replace
}

** get a file of only the DHS's that didn't error out
insheet using "$analysis_file", clear
merge 1:1 data_name using `errors', keep(1) nogen
drop iso3 read_fail no_maternal
outsheet using "$used_file", c replace

log close
