** PREP STATA**
    capture restore
    clear all
    set mem 1000m

    set more off
    ssc install bygap
    
    // set the prefix for whichever os we're in
    if c(os) == "Windows" {
        global j "J:"
    }
    if c(os) == "Unix" {
        global j "FILEPATH"
        set odbcmgr unixodbc
    }
    
** *********************************************************************************************
**
// What we call the data: same as folder name
    local data_name Bangladesh_VA

// Establish data import directories 
    local jdata_in_dir "FILEPATH"
    local cod_in_dir "FILEPATH"
    
** *********************************************************************************************
**
// If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
    import excel using "FILEPATH", clear
    

** INITIAL CLEANING **
    // Remove unnecessary rows and columns from data

    foreach var of varlist * {
        replace `var' = subinstr(`var',"/","_",.)
        replace `var' = subinstr(`var',"+","",.)
        local new = `var' in 15
        di "`new'"
        capture rename `var' `new'
    }
    rename age_* deaths*
    
    // Under 5 was not collected in this VA 
    drop sum_age_groups deaths0_6days deaths7_28days deaths29_365days deaths1_4years
    keep in 16/l
    
** CAUSE AND CAUSE_NAME (cause, cause_name) **
    // Make cause long if needed

    // cause (string)
    // Format the cause list from the compiled datasets to be consistent accross them
    // NOTE: IF THIS DATASET DOES NOT COVER ALL DEATHS (FOCUSES ON JUST INJURIES OR SOMETHING) YOU MUST CREATE
    // A ROW FOR 'CC' (COMBINED CODE) FOR EACH COUNTRY-YEAR SO THAT CAUSE FRACTIONS CAN BE CALCULATED LATER. 
    // CC SHOULD REPRESENT ALL DEATHS ATTRIBUTED TO CAUSES OTHER THAN THOSE THAT ARE THE FOCUS OF THE STUDY.
    // THE NUMBERS FOR THIS SHOULD BE FOUND SOMEWHERE IN THE ORIGINAL DATA SOURCE. 
    rename ICD cause
    
    // cause_name (string)
    // If the causes are just codes, we will need a cause_name to carry its labels, if available. Otherwise leave blank.
    gen cause_name = ""

** YEAR (year) **
    // Make year long if needed
    // Generate a year value that is appropriate for each observation
    // year (numeric) - whole years only
    destring year, replace 
    drop if year == .
    
** SEX (sex) **
    // Make sex long if needed
    // Make sex=1 if Male, sex=2 if Female, sex=9 if Unknown
    destring sex, replace

** AGE VARIABLES (deaths#, frmat, im_frmat) **
    // Make age wide, using the prefix "deaths"
    // Format into WHO age codes
    destring deaths*, replace
    collapse (sum) deaths*, by(year District Upozila_Branch urbanicity sex cause cause_name)
    reshape long deaths, i(year District Upozila_Branch urbanicity sex cause) j(age)s
    replace age = substr(age,1,2)
    replace age = "5" if age == "5_"
    destring age, force replace
    replace age = (age/5)+6
    reshape wide deaths, i(year District Upozila_Branch urbanicity sex cause) j(age)
    
    // deaths (numeric): If deaths are in CFs or rates, find the proper denominator to format them into deaths    

    // frmat (numeric): find the WHO format here "FILEPATH"
    gen frmat = 2

    // im_frmat (numeric): from the same file as above
    gen im_frmat = 8
    
    
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
    // First create a source variable, so we know the origin of each country year
    capture drop source
    gen source = "`data_name'"

    // if the source is different across appended datasets, give each dataset a unique source_label and NID
    gen source_label = "`data_name'"
    gen NID = 243436

    // APPEND - add new source_labels if needed

    // source_types: Burial; Cancer registry; Census; Hospital; Mortuary; Police; Sibling history, survey; Surveillance; Survey; VA National; VA Subnational; VR; VR Subnational
    gen source_type = "VA"
    
    // national (numeric) - Is this source nationally representative? should be denoted in the source itself. Otherwise consult a researcher. 1= yes 0=no
    gen national = 0
        
    // list (string): what is the tabulation of the cause list? What is the name of the sheet in master_cause_map.xlsx which will merge with the causes in this dataset?
    // For ICD-based datasets, this variable with be the name of the ICD-type. For all other datasets, this variable will be the source name. There must only be one value for list in the whole dataset; for example, if
    // the data contains ICD10 and ICD9-detail, the source should be split into two folders in 03_datasets.
    gen list = "INDEPTH_ICD10_VA"
    
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
    // location_id (numeric) if it is a supported subnational site located in this directory: FILEPATH
    gen location_id = .
        
    // iso3 (string)
    gen iso3 = "BGD"

    // subdiv (string): if you can't find a location id for the specific locatation (e.g. rural, urban, Jakarta, Southern States...) enter it here, otherwise leave ""
    // THIS FIELD CANNOT HAVE COMMAS (OTHER CHARACTERS ARE FINE)
    gen subdiv = ""
    replace subdiv = District + "-" + Upozila_Branch + " (" + urbanicity + ")"
    drop District Upozila_Branch urbanicity
    
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
    do "format_gbd_age_groups.do"
** ************************************************************************************************* **
// SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
    // HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "FILEPATH"
    
    // HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE

** *********************************************************************************************
**
** EXPORT **
    do "export.do" `data_name'
    
    
