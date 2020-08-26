** PREP STATA**
    clear all
    set mem 1000m

    set more off
    ssc install bygap
    
    ** set the prefix for whichever os we're in
    if c(os) == "Windows" {
        global j "J:"
        global h "H:"
    }
    if c(os) == "Unix" {
        global j "FILEPATH"
        set odbcmgr unixodbc
        global h "~"
    }

    ** timestamp should already be set when running this, but make sure here
    local timestamp = "$timestamp"
    
** *********************************************************************************************
**
** What we call the data: same as folder name
    local data_name India_SCD_states_rural

    local new_model_results = 1
    if `new_model_results'==1 {
        !bash "FILEPATH"
        ** give bash file time to delete the old deaths_split_scd file
        sleep 1000
        capture confirm file "FILEPATH"
        while _rc {
            sleep 3000
            capture confirm file "FILEPATH"
        }
    }

** get a map of cause_names
    use "FILEPATH", clear
    keep if acause=="_gc"
    keep cause cause_name
    duplicates drop
    tempfile gc_names
    save `gc_names', replace

    import delimited using "FILEPATH", clear

    replace state =  "Odisha" if state == "Orissa"

** misc cleaning
    drop v1

    replace cause = "cc_code" if cause=="all" & inlist(state, "Arunachal Pradesh", "Jammu and Kashmir", "Nagaland", "West Bengal")
    drop if cause=="all"

** get cause_names back
    merge m:1 cause using `gc_names', assert(1 3) keep(1 3) nogen

** get the location_ids back
    gen location_name = state + ", Rural"
    tempfile preloc
    save `preloc', replace
    odbc load, exec("SELECT location_id, location_ascii_name as location_name FROM shared.location WHERE path_to_top_parent LIKE '%,163,%'") clear dsn(prodcod)
    tempfile locs
    save `locs', replace
    merge 1:m location_name using `preloc', assert(1 3) keep(3) nogen
    drop location_name
    drop state


    gen age_id = 91 if age=="0-365"
    replace age_id = 3 if age=="1-4"
    replace age_id = 7 if age=="5-14"
    replace age_id = 9 if age=="15-24"
    replace age_id = 11 if age=="25-34"
    replace age_id = 13 if age=="35-44"
    replace age_id = 15 if age=="45+"
    count if age_id ==.
    assert `r(N)'==0
    ** now has a new age format from the model
    gen frmat = 12
    gen im_frmat = 9
    drop age
    reshape wide deaths, i(location_id cause sex year) j(age_id)


** fix identifiers
    capture drop source 
    gen source = "`data_name'"
    gen source_label = "`data_name'"
    gen subdiv = ""
    gen NID = .
    replace NID = 157475 if year==1980
    replace NID = 157476 if year==1981
    replace NID = 157477 if year==1982
    replace NID = 157478 if year==1983
    replace NID = 162707 if year==1984
    replace NID = 157479 if year==1985
    replace NID = 162708 if year==1986
    replace NID = 93751 if NID == .

    gen source_type = "VA National"
    gen national = 1
    gen list = "`data_name'"
    gen iso3="IND"

    ** some assertions
    foreach var in iso3 source source_label source_type list cause {
        count if `var'==""
        assert `r(N)'==0        
    }

    foreach var of varlist location_id national NID frmat im_frmat sex year deaths* {
        count if `var'==.
        assert `r(N)'==0
        count if `var'<0
        assert `r(N)'==0
    }

** run through age group formatting
    do "FILEPATH" 

** ************************************************************************************************* ** 
    ** All of the following variables should be present
        order iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94

    ** Drop any variables not in our template of variables to keep
        keep iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15 deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94

** this is 'mapping' for SCD 
    gen acause = cause if !regexm(cause, "[0-9]")
    replace acause = "_gc" if regexm(cause, "[0-9]")
    replace cause = "acause_" + cause if !regexm(cause, "[0-9]")
    gen dev_status = "D0"
    gen region = 159

** save to 02a before agesex file
    save "FILEPATH", replace
    save "FILEPATH", replace

** DONE
