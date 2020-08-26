    capture restore
    clear all
    set mem 1000m

    set more off
    ssc install bygap
     
  // set the prefix for whichever os we're in
    if c(os) == "Unix" {
        set mem 10G
        set odbcmgr unixodbc
        global j "FILEPATH"
        local cwd = c(pwd)
        cd "~"
        global h = c(pwd)
        cd `cwd'
    }
    else if c(os) == "Windows" {
        global j "J:"
        global h "H:"
    }

** Set where the CoD repository lives
    global repo_dir "FILEPATH"
// Set the time
    // Get create_timestamp function
    run "FILEPATH"
    // Get time of completion 
    create_timestamp
    global timestamp = "`r(timestamp)'"

** Working directories
    global raw_dir "FILEPATH"
    global log_dir "FILEPATH"
    global cod_out_dir "FILEPATH" 
    global cod_archive "FILEPATH"
    global temp_error_dir "FILEPATH"


cap log close
cd "$log_dir"
log using "dp2.smcl", replace


cd "FILEPATH"
tempfile errors
local files: dir "$temp_error_dir" files "*", respectcase
foreach file of local files {
    use "`file'", clear
    capture append using `errors'
    save `errors', replace
}

cap use `errors', clear
if (_rc == 0) {
    save "FILEPATH", replace
    save "FILEPATH", replace
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
    saveold "FILEPATH", replace
    saveold "FILEPATH", replace
}

import delimited using "FILEPATH", clear
replace data_name = subinstr(data_name, ".DTA", "",.)
merge 1:1 data_name using "FILEPATH", keep(1) nogen
drop iso3 read_fail no_maternal
outsheet using "FILEPATH", c replace
outsheet using "FILEPATH",c replace 

capture log close
