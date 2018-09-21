// Author: NAME 
// Date: 04/12/2016


*******************************************************************************
** SET-UP
*******************************************************************************
local username "`1'"
local source "`2'"
local timestamp "`3'"
local temp_dir "`4'"
local source_dir "`5'"

local in_dir "PATH"
local shell_dir "PATH"
local code_dir "PATH"
local log_dir "PATH"

local ndraws 1000 

log close _all
log using "`log_dir'/02_agesexsplit_`timestamp'", replace

set more off


*******************************************************************************
** DUE DILIGENCE
*******************************************************************************

display "data name: `source'"
display "timestamp: `timestamp'"
display "tmpdir: `temp_dir'"
display "source_dir: `source_dir'"
display "in_dir: `in_dir'"


// Make sure there are not files already present in the save directory
local datafiles: dir "`in_dir'" files "*.dta"
foreach datafile of local datafiles {
    di "`in_dir'/`datafile'"
    rm "`in_dir'/`datafile'"
}
local datafiles: dir "`in_dir'" files "*.dta"
local l : list sizeof datafiles
assert `l'==0

*******************************************************************************
** SUBMIT JOBS
*******************************************************************************

forval i=1/`ndraws' {
    !qsub -P proj_shocks -pe multi_slot 8 -l mem_free=16g -N "CoD_02_`source'_`i'" "`shell_dir'/prep/code/shellstata13_`username'.sh" "`code_dir'/age_sex_splitting/code/split_agesex_shocks_AT.do" "`username' `source' `timestamp' `temp_dir' `source_dir' `i'" 
}

// wait a minute
sleep 6000
clear

*******************************************************************************
** SEARCH FOR DRAW FILES
*******************************************************************************

forvalues i = 1/`ndraws' {
    local check_file "`in_dir'/02_agesexsplit_`i'.dta"
    
    cap confirm file "`check_file'"
    while _rc {
        ** wait a minute
        di "draw `i' not found, sleeping for 6 seconds"
        sleep 6000
        cap confirm file "`check_file'"
    }
    
    sleep 5
    append using "`check_file'"
    di "Draw `i' found!"
}

*******************************************************************************
** SAVE
*******************************************************************************

// Save dataset
save "`source_dir'/`source'/data/intermediate/02_agesexsplit.dta", replace
save "`source_dir'/`source'//data/intermediate/_archive/02_agesexsplit_`timestamp'.dta", replace

// Save dataset for compile
save "`source_dir'/`source'/data/intermediate/02_agesexsplit_compile.dta", replace
save "`source_dir'/`source'/data/intermediate/_archive/02_agesexsplit_compile_`timestamp'.dta", replace
