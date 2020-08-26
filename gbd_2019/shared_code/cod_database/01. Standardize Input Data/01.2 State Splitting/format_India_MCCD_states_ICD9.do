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
    local data_name India_MCCD_states_ICD9

** Establish data import directories 
    local jdata_in_dir "FILEPATH"
    local cod_in_dir "FILEPATH"
    local abie_output_dir "FILEPATH"
    
** *********************************************************************************************
**

    ** state names to location_ids
    odbc load, exec("SELECT location_id, location_ascii_name FROM shared.location WHERE path_to_top_parent LIKE '1,163,%' AND location_level=3 AND location_name LIKE '%Urban'") dsn(prodcod) clear
    rename location_ascii_name state
    replace state = subinstr(state, ", Urban", "", .)
    tempfile location_ids
    save `location_ids'

    import delimited using "FILEPATH", clear
    ** sometimes useful to have a tempfile on hand if things go wrong
    tempfile raw
    save `raw', replace
    rename cause cause_code
    
    merge m:1 cause_code using "FILEPATH", assert(2 3) keep(3) nogen
    replace cause1983 = cause199x if cause1983==""
    rename (cause1983 cause_code) (cause_name cause)
    keep cause cause_name sex year age state deaths
    
    ** get the location_ids for the state
    replace state = "Odisha" if state == "Orissa"
    replace state = "Union Territories other than Delhi" if state == "The Six Minor Territories"
    merge m:1 state using `location_ids', assert(2 3) keep(3) nogen
    
    // fix union territories
    replace location_id = 44540 if state == "Andaman and Nicobar Islands" | state == "Lakshadweep" | state == "Puducherry"
    replace state = "Union Territories other than Delhi" if location_id==44540
    
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
    collapse (sum) deaths, by(cause sex year cause_name location_id state age_group_id) fast 
    
    gen frmat = 11
    gen im_frmat=8
    
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
    ** First create a source variable, so we know the origin of each country year
    capture drop source
    gen source = "`data_name'"

    ** if the source is different across appended datasets, give each dataset a unique source_label and NID
    gen subdiv=""
    gen source_label = ""
    gen NID = .
    replace NID = 5112 if year==1980
    replace NID = 33404 if year==1981
    replace NID = 33398 if year==1982
    replace NID = 33373 if year==1983
    replace NID = 33367 if year==1984
    replace NID = 33363 if year==1986
    replace NID = 33351 if year==1987
    replace NID = 5323 if year==1988
    replace NID = 32461 if year==1989    
    replace NID = 157418 if year==1990
    replace NID = 157420 if year==1991
    replace NID = 157421 if year==1992
    replace NID = 157422 if year==1993
    replace NID = 157423 if year==1994
    replace NID = 157424 if year==1995
    replace NID = 157425 if year==1996
    replace NID = 162711 if year==1997
    replace NID = 157426 if year==1998

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

    ** DO DISAGGREGATION HERE WHILE DATA IS LONG
    local targets_file "FILEPATH"
    do "MCCD_states_disaggregation.do" "`targets_file'"
    reshape wide deaths, i(cause sex year cause_name location_id state) j(age_group_id)

    ** now make sure the duplicate dropping method used towards the beginning was worth it and that disaggregation didn't mess it up
    isid cause sex year location_id
    ** so good.

** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
    do "format_gbd_age_groups.do"
** ************************************************************************************************* **
** SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
    ** HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "FILEPATH"

    ** SPECIAL TREATMENT FOR CODE 02.07.03 TO FIX LYMPHOMA AND MYELOMA (NOT SURE HOW/WHEN TO CLEAN THIS UP)
    tempfile t
    save `t', replace
    keep if cause=="02.07.03"
    su deaths1
    local deaths_before = `r(sum)'
    ds cause deaths*, not
    collapse (sum) deaths*, by(`r(varlist)')
    reshape long deaths, i(location_id sex year frmat im_frmat) j(age)
    tempfile other_lymph
    save `other_lymph', replace


    use "FILEPATH", clear
    keep if acause=="neo_lymphoma" | acause=="neo_myeloma"
    collapse (sum) deaths*, by(location_id acause sex) fast 
    reshape long deaths, i(location_id acause sex) j(age)
    bysort location_id sex age: egen total = total(deaths)
    gen pct_deaths = deaths/total
    gen cause = "acause_" + acause
    keep location_id age sex cause pct_deaths
    joinby location_id age sex using `other_lymph', unmatched(using)
    count if _merge==2
    assert `r(N)'==0
    drop _merge

    gen newdeaths = deaths*pct_deaths
    drop deaths pct_deaths
    rename newdeaths deaths
    reshape wide deaths, i(location_id sex year frmat im_frmat cause) j(age)
    su deaths1
    assert abs(`r(sum)'-`deaths_before')<.01
    tempfile disaggregated
    save `disaggregated', replace

    use `t', clear
    drop if cause=="02.07.03"
    append using `disaggregated'
    
    ** HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE

    do "replace_round_4_disagg_results_with_round_5_cause.do"

** *********************************************************************************************
**
** EXPORT **
    do "export.do" `data_name'
    
    
