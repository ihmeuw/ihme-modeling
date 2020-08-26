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

** create file with all DHS and AIS women's modules
** Generate local that contains filenames of all possibly relevant datasets to loop through.
    global dhsdir "FILEPATH"
    global aisdir "FILEPATH"

** start a new log
	cd "$log_dir"
	capture log close
	log using "dp0.smcl", replace

** New data option (0 means run all DHS, 1 means just run new DHS)
	set obs 1
	gen a = "$arg"
	destring a, replace
	global newflag  = a


	di in red "newflag $newflag"

** create tempfile
	tempfile cb

** loop through all DHS folders
	local jdirs: dir "FILEPATH" dirs "*", respectcase
	di `"`jdirs'"'

**
local i=1

foreach iso of local jdirs {
qui {
    noi di "`iso'"
    ** these folders don't have any DHS's in them
    if ("`iso'" == "CRUDE" | "`iso'" == "DHS_REPORTS" | "`iso'" == "QUESTIONNAIRES") continue
    
    ** get the years in the DHS-country folder
    local jdir`iso': dir "FILEPATH`iso'" dirs "*", respectcase
    noi di `" `'jdir`iso''"' 
    
    **     
    foreach yr of local jdir`iso' {
        noi di " `yr'"
        
        ** get the files in the DHS-country-year folder
        local jdir`iso'_`yr': dir "FILEPATH/`iso'/`yr'" files "FILEPATH", respectcase
        
        cd "$dhsdir/`iso'/`yr'"
        noi di `"`jdir`iso'_`yr''"'
        ** "
        if `"`jdir`iso'_`yr''"'=="" { 
        ** "
            continue    
        }
        
        ** Loop through all of the files with _WN_ in the crude DHS folder. 
        foreach file of local jdir`iso'_`yr' { 
            clear 
            set obs 1
            gen data_name = "`file'"
            gen survey = "MACRO_DHS"
            gen country = "`iso'"
            gen year = "`yr'"
            gen maternal = 1
            

            replace maternal = 0 if regexm(data_name,"FILEPATHS") & regexm(data_name,"1998_2000")
            replace maternal = 0 if regexm(data_name,"FILEPATHS") & regexm(data_name, "1992_1993")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")
            replace maternal = 0 if inlist(data_name,"FILEPATHS")

            cap append using `cb'
            save `cb', replace
        }
    }
}
}

** loop through all AIS folders
local jdirs: dir "$aisdir" dirs "*", respectcase
di `"`jdirs'"'
** "
local i=1

foreach iso of local jdirs {
qui {
    noi di "`iso'"
    ** these folders don't have any DHS's in them
    if ("`iso'" == "CRUDE" | "`iso'" == "DHS_REPORTS" | "`iso'" == "QUESTIONNAIRES") continue
    
    ** get the years in the DHS-country folder
    local jdir`iso': dir "FILEPATH'" dirs "*", respectcase
    noi di `"    `jdir`iso''"' 
    ** "    
    foreach yr of local jdir`iso' {
        noi di "    `yr'"
        
        ** get the files in the DHS-country-year folder
        local jdir`iso'_`yr': dir "FILEPATH" files "FILEPATH", respectcase
        
        cd "FILEPATH'"
        noi di `"      `jdir`iso'_`yr''"'
        ** "
        if `"`jdir`iso'_`yr''"'=="" { 
        ** "
            continue    
        }
        
        foreach file of local jdir`iso'_`yr' { 
            clear 
            set obs 1
            gen data_name = "`file'"
            gen survey = "MACRO_AIS"
            gen country = "`iso'"
            gen year = "`yr'"
            gen maternal = 1
            
            cap append using `cb'
            save `cb', replace
        }
    }
}
}

** drop AIS-DHS duplicates
duplicates tag data_name, g(d)
drop if d == 1 & survey == "MACRO_AIS"
drop d

sort survey country year

tempfile survs
save `survs', replace

clear
set obs 1

** set variables
gen data_name = ""
gen survey = ""
gen country = ""
gen year = ""
gen maternal = 1

** add in surveys
replace data_name = "" if _n == 1
replace survey = ""  if _n == 1
replace country = "" if _n == 1
replace year = "" if _n == 1

** add these onto the DHS and AIS surveys
append using `survs'
drop if data_name == ""

preserve
	import delimited using "FILEPATH", clear
	save "FILEPATH", replace
restore


export delimited using "FILEPATH", replace
export delimited using "FILEPATH", replace

 
merge 1:1 data_name survey country year using "FILEPATH"
gen new = 0 
replace new = 1 if _m == 1
drop _m

keep if maternal == 1
e
cap erase "FILEPATH"
cd "$temp_error_dir"

local files: dir "$temp_error_dir" files "*", respectcase
foreach file of local files {
    erase "`file'"
}

** save file for looping
sort survey country year

export delimited using "FILEPATH", replace
export delimited using "FILEPATH"     


capture log close
