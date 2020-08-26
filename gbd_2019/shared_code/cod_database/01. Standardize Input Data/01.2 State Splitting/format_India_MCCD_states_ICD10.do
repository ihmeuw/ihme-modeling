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
    odbc load, exec("SELECT location_id, location_ascii_name FROM shared.location WHERE path_to_top_parent LIKE '%,163,%' AND location_level=3 AND location_name LIKE '%Urban'") dsn(prodcod) clear
    rename location_ascii_name state
    replace state = subinstr(state, ", Urban", "", .)    
    tempfile location_ids
    save `location_ids'

    import delimited using "FILEPATH", clear
    ** sometimes useful to have a tempfile on hand if things go wrong
    tempfile raw
    replace state = "Odisha" if state == "Orissa"
    save `raw', replace

    keep source cause sex year age state deaths

    * ** if data was in-source, it was kept, but the cause-sex-state-years were still modeled, so reconcile the two sources
    ** Assert that all the dups are because of the source being different, allowing us to drop if source is not MCCD_ICD10 and dup==1
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

    ** the reason that the 'age_state_time_est'/'MCCD_ICD10' distinction in the source variable can't be kept is that they create duplicates after disaggregation when two causes with different sources have the same target code
    drop source 
    
    gen frmat = 11
    gen im_frmat=8
    
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
    ** First create a source variable, so we know the origin of each country year
    gen source = "`data_name'"
    gen subdiv = ""

    ** // if the source is different across appended datasets, give each dataset a unique source_label and NID
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

    ** source_types: Burial; Cancer registry; Census; Hospital; Mortuary; Police; Sibling history, survey; Surveillance; Survey; VA National; VA Subnational; VR; VR Subnational
    gen source_type = "VR"
    
    ** national (numeric) - Is this source nationally representative? should be denoted in the source itself. Otherwise consult a researcher. 1= yes 0=no
    gen national = 1
        
    ** list (string): what is the tabulation of the cause list? What is the name of the sheet in master_cause_map.xlsx which will merge with the causes in this dataset?
    ** For ICD-based datasets, this variable with be the name of the ICD-type. For all other datasets, this variable will be the source name. There must only be one value for list in the whole dataset; for example, if
    ** the data contains ICD10 and ICD9-detail, the source should be split into two folders in 03_datasets.
    gen list = "`data_name'"
    
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
    ** location_id (numeric) if it is a supported subnational site located in this directory: FILEPATH
        
    ** iso3 (string)
    gen iso3 = "IND"

    tempfile formatted
    save `formatted', replace

** DO DISAGGREGATION HERE SO THAT WE KEEP cause sex year location_id AS IDENTIFIERS
    local targets_file "FILEPATH"
    do "MCCD_states_disaggregation.do" "`targets_file'"
    reshape wide deaths, i(cause sex year location_id) j(age_group_id)

    ** now make sure the duplicate dropping method used towards the beginning was worth it and that disaggregation didn't mess it up
    isid cause sex year location_id
    ** so good.

** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
    do "format_gbd_age_groups.do"
** ************************************************************************************************* **
** SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
    ** HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "FILEPATH"
    
    ** HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE
    ** Now have ICD10 detail data for Orissa
    drop if location_id==43896 & year>=2009 & year<=2013

    do "replace_round_4_disagg_results_with_round_5_cause.do"

** *********************************************************************************************
**
** EXPORT **
    do "export.do" `data_name'

    
