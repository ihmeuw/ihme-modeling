** PREP STATA**
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
    
** *********************************************************************************************
**

** What we call the data: same as folder name
    local data_name India_MCCD_states_ICD10

** Establish data import directories 
    local jdata_in_dir "FILEPATH"
    local cod_in_dir "FILEPATH"
    local abie_output_dir "FILEPATH"
    
** *********************************************************************************************
**

    import excel using "FILEPATH", firstrow clear
    tempfile cause_map
    save `cause_map', replace

    ** state names to location_ids
    odbc load, exec("SELECT location_id, location_ascii_name FROM ADDRESS WHERE path_to_top_parent LIKE '%,163,%' AND location_level=3 AND location_name LIKE '%Urban'") dsn(FILEPATH) clear
    rename location_ascii_name state
    replace state = subinstr(state, ", Urban", "", .)    
    tempfile location_ids
    save `location_ids'

    import delimited using "FILEPATH", clear
    tempfile raw
    replace state = "Odisha" if state == "Orissa"
    save `raw', replace

    keep source cause sex year age state deaths
    duplicates tag cause sex year age state source, gen(dups_with_source)
    count if dups_with_source==1
    assert `r(N)' == 0
    duplicates tag cause sex year age state, gen(dup)
    bysort cause sex year state: egen count_dup = total(dup)
    drop if count_dup>0 & source=="1st-order_four-way_log-lin"
    drop dup count_dup dups_with_source

    ** get the cause names
    merge m:1 cause using `cause_map', assert(2 3) keep(3) nogen
    drop cause_level
    
    ** get the location_ids for the state
    replace state = "Odisha" if state == "Orissa"
    replace state = "Union Territories other than Delhi" if state == "The Six Minor Territories"
    merge m:1 state using `location_ids', assert(2 3) keep(3) nogen
    
    ** fix location_ids for union territories
    ** will require a collapse later
    replace location_id = 44540 if state == "Puducherry" | state == "Andaman and Nicobar Islands"
    replace state = "Union Territories other than Delhi" if location_id ==44540
    
** AGE **
    gen age_group_id=.
    replace age_group_id=91 if age=="0-365"
    replace age_group_id=3 if age=="1-4"
    replace age_group_id=7 if age=="5-14"
    replace age_group_id=9 if age=="15-24"
    replace age_group_id=11 if age=="25-34"
    replace age_group_id=13 if age=="35-44"
    replace age_group_id=15 if age=="45-54"
    replace age_group_id=17 if age=="55-64"
    replace age_group_id=19 if age=="65-69"
    replace age_group_id=20 if age=="70+"
    replace age_group_id=26 if age=="unknown"
    drop age
    collapse (sum) deaths, by(cause sex year cause_name location_id state source age_group_id) fast 

    drop source 
    
    gen frmat = 11
    gen im_frmat=8
    
** *********************************************************************************************
**
    gen source = "`data_name'"
    gen subdiv = ""

    gen source_label = "India_MCCD_states_ICD10"
    gen NID = .
    replace NID = 157427 if year==1999
    replace NID = 157428 if year==2000
    replace NID = 157429 if year==2001
    replace NID = 157430 if year==2002
    replace NID = 157431 if year==2003
    replace NID = 157432 if year==2004
    replace NID = 107348 if year==2005
    replace NID = 107349 if year==2006
	replace NID = 260091 if year==2007
    replace NID = 108743 if year==2008
    replace NID = 157361 if year==2009
    replace NID = 157365 if year==2010
	replace NID = 233154 if year==2011
	replace NID = 233155 if year==2012
	assert NID != .
    ** APPEND - add new source_labels if needed

    gen source_type = "VR"
    
    gen national = 1
        
    gen list = "`data_name'"
    
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
        
    ** iso3 (string)
    gen iso3 = "IND"

    tempfile formatted
    save `formatted', replace

    local targets_file "FILEPATH"
    do "FILEPATH" "`targets_file'"
    reshape wide deaths, i(cause sex year location_id) j(age_group_id)

    isid cause sex year location_id

** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
    do "FILEPATH" 
** ************************************************************************************************* **
    drop if location_id==43896 & year>=2009 & year<=2013

    do "FILEPATH"

** *********************************************************************************************
** EXPORT **
    do "FILEPATH" `data_name'

    
