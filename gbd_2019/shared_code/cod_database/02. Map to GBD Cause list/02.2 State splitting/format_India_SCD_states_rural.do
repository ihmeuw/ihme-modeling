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
		global h "~"
        set odbcmgr unixodbc
    }
    
** *********************************************************************************************
**
** What we call the data: same as folder name
    local data_name India_SCD_states_rural

** Establish data import directories 
    local jdata_in_dir "FILEPATH"
    global cod_in_dir "FILEPATH"
    
** *********************************************************************************************
**
** If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
    
    ** Get a bunch of mappings first
    use "FILEPATH", clear
    keep stid year dths
    gen age_group=26
    rename dths deaths
    tempfile coverage
    save `coverage', replace
    
    use "FILEPATH", clear
    rename nml08 cause
    rename nmlcause cause_name
    drop grpcd
    tempfile cause_names
    save `cause_names', replace
    
    use "FILEPATH", clear
    tempfile state_names
    save `state_names', replace
    

    ** this dataset contains state, year, cause, some age, and percent deaths for the top ten causes
    use "FILEPATH", clear

    drop if inlist(year, 1994, 1995)
    
** INITIAL CLEANING **
    ** fix age groupings
    gen age_group=.
    replace age_group = 26 if agegrps=="P0-w"

    drop agegrps
    ** reshape the top ten so that we can calculate a remainder percentage to attribute to cc_code
    reshape wide pctdths, i(stid year age_group) j(nml08cd) string
    egen total = rowtotal(pctdths*)

    drop if total==0

    gen pctdths9999 = 100-total
    drop total
    ** keep the matching and make sure that there weren't any datapoints without coverage
    merge 1:1 stid year age_group using `coverage', assert(2 3) keep(3) nogen

    ** multiply by coverage envelope to get number of deaths for each cause
    foreach var of varlist pctdths* {
        ** turn to fraction out of 1
        replace `var' = `var'/100
        ** multiply by total number of deaths
        replace `var' = `var'*deaths
    }
    drop deaths
    rename pctdths* deaths*

    reshape long deaths, i(stid year age_group) j(cause)
    tostring cause, replace
    replace cause="cc_code" if cause=="9999"

    gen frmat = 9 if year<1990

    drop if inlist(year, 1990, 1991, 1992, 1994)
    tempfile data
    save `data', replace
    ** format the other data
    do "FILEPATH"
    append using `data'
    
    ** map cause name and state name
    merge m:1 cause using `cause_names', keep(1 3) nogen
    ** 892 is not in the cause list map provided so move to remainder
    replace cause = "cc_code" if cause=="892"
    replace cause_name = "Remainder of all causes" if cause=="cc_code"
    count if cause_name==""
    assert(r(N)==0)
    merge m:1 stid using `state_names', assert(2 3) keep(3) nogen
    
    drop stid
    
    gen sex=9

    drop if deaths==.
    drop if deaths==0

    
    ** then reshape wide
    reshape wide deaths, i(year cause cause_name state sex frmat) j(age_group)

    ** im_frmat (numeric): from the same file as above
    gen im_frmat = 8
    
    
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
    ** First create a source variable, so we know the origin of each country year
    capture drop source
    gen source = "`data_name'"

    gen source_label = ""
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
    
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
    gen location_id = .
    replace location_id=43908 if state=="Andhra Pradesh"
    replace location_id=43910 if state=="Assam"
    replace location_id=43911 if state=="Bihar"
    replace location_id=43918 if state=="Gujarat"
    replace location_id=43919 if state=="Haryana"
    replace location_id=43920 if state=="Himachal Pradesh"
    replace location_id=43921 if state=="Jammu & Kashmir"
    replace location_id=43923 if state=="Karnataka"
    replace location_id=43924 if state=="Kerala"
    replace location_id=43926 if state=="Madhaya Pradesh"
    replace location_id=43927 if state=="Maharastra"
    replace location_id=43928 if state=="Manipur"
    replace location_id=43932 if state=="Orissa"
    replace location_id=43934 if state=="Punjab"
    replace location_id=43935 if state=="Rajasthan"
    replace location_id=43937 if state=="Tamil Nadu"
    replace location_id=43939 if state=="Tripura"
    replace location_id=43940 if state=="Uttar Pradesh"
    replace location_id=43942 if state=="West Bengal"
    replace location_id=43916 if state=="NCT of Delhi"
    replace location_id=43931 if state=="Nagaland"
    replace location_id=43917 if state=="Goa"
    replace location_id=43909 if state=="Arunachal Pradesh"
    replace location_id=43912 if state=="Chandigarh"
    replace location_id=43929 if state=="Meghalaya"
    replace location_id=43914 if state=="Dadra & Nagar Haveli"

    replace location_id = 44539 if inlist(location_id, 43912, 43914)

    ** get a range of location_ids to speed up query and merge
    summarize location_id
    local min = `r(min)'
    local max = `r(max)'
    tempfile beforesubdiv
    save `beforesubdiv', replace
    odbc load, exec("ADDRESS") dsn("ADDRESS") clear
    tempfile map
    save `map', replace
    use `beforesubdiv', clear
    merge m:1 location_id using `map', assert(2 3) keep(3) nogen
    gen subdiv = location_ascii_name
    replace subdiv = subinstr(subdiv, ",", ":", .)
    drop location_ascii_name

        
    ** iso3 (string)
    gen iso3 = "IND"
    
    tempfile formatted
    save `formatted', replace
    
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
    do "FILEPATH" 
** ************************************************************************************************* **
    drop if year==1995

    replace cause = "710" if cause=="222" & year<1990

** *********************************************************************************************
**
** EXPORT **
    do "FILEPATH'
    
    
