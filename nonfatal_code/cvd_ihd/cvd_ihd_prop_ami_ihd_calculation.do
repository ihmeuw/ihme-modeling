
// PREP STATA
    clear
    set more off
    if c(os) == "Unix" {
        global prefix "FILEPATH"
        set odbcmgr unixodbc
        set mem 10g
    }
    else if c(os) == "Windows" {
        global prefix "FILEPATH"
        set mem 2g
    }
    
    // SPECIFY DATASETS USED
    local filelist England_UTLA_ICD10 Brazil_SIM_ICD10 China_2004_2012 Ghana_Accra Greenland_1995_2013 Guyana_PAHO Japan_by_prefecture_ICD10 Mongolia_2010 Saudi_Arabia_96_2012 South_Africa_by_province Sweden_ICD10 Taiwan_2008_2012 UK_2001_2011 US_NCHS_counties_ICD10 US_NCHS_terr_ICD10 West_Bank_2004_2009 Zimbabwe_2007 NZL_MOH_ICD10 Philippines_2006_2012 India_MCCD_Orissa_ICD10 Brazil_SIM_ICD9 Japan_by_prefecture_ICD9 Sweden_ICD9 UK_1981_2000 US_NCHS_counties_ICD9 US_NCHS_terr_ICD9  Zimbabwe_95 NZL_MOH_ICD9 Brazil_SIM_ICD9 Japan_by_prefecture_ICD9 Sweden_ICD9 UK_1981_2000 Russia_ROSSTAT US_NCHS_counties_ICD9 US_NCHS_terr_ICD9 Zimbabwe_95 NZL_MOH_ICD9 ICD10
    
    // Where to save data?
    local save_dir "FILEPATH"
    
    // Where to log data
    local log_dir "FILEPATH"
    
** *********************************************************************
    // Log
    capture log close
    log using "`log_dir'/gen_mi_ihd_ratio_log.smcl", replace
    
    // Prep country-region-super_region meta data
    odbc load, exec("SELECT location_id, location_name, map_id AS iso3, path_to_top_parent FROM shared.location") dsn(prodcod) clear
    replace iso3 = "USA" if substr(path_to_top_parent, 1,6) == "1,102,"
    replace iso3 = "IND" if substr(path_to_top_parent, 1,6) ==  "1,163,"
    replace iso3 = "SWE" if inlist(location_id, 4940, 4944)
    replace iso3 = "KEN" if substr(path_to_top_parent, 1,6) ==  "1,180,"
    replace iso3 = "SAU" if substr(path_to_top_parent, 1,6) ==  "1,152,"
    replace iso3 = "BMU" if location_name == "Bermuda"
    replace iso3 = "GRL" if location_name == "Greenland"
    replace iso3 = "PRI" if location_name == "Puerto Rico"
    replace iso3 = "ASM" if location_name == "American Samoa"
    replace iso3 = "VIR" if location_name == "Virgin Islands, U.S."
    replace iso3 = "MNP" if location_name == "Northern Mariana Islands"
    replace iso3 = "TWN" if location_name == "Taiwan"
    replace iso3 = "GUM" if location_name == "Guam"
    replace iso3 = "GBR" if inlist(location_id, 433, 434, 4618, 4619, 4620, 4621, 4622, 4623, 4624, 4625, 4626, 4636)
    replace iso3 = "GBR" if substr(path_to_top_parent, 1,5) == "1,95,"
    replace iso3 = "MEX" if location_id>=4643 & location_id<=4674
    replace iso3 = "CHN" if (location_id>=491 & location_id<=521) | inlist(location_id, 354, 361)
    replace iso3 = "ZAF" if inlist(location_id, 482, 483, 484, 485, 486, 487, 488, 489, 490)
    replace iso3 = "BRA" if location_id>=4750 & location_id<=4776
    drop if iso3 == ""
    replace location_id = . if ! inlist(iso3, "BMU", "PRI", "ASM") & ! inlist(iso3, "VIR", "MNP")
    
    tempfile tem
    save `tem', replace

    // Generate MI/IHD proportions
    clear
    gen foo = ""
    tempfile master
    save `master', replace
  
    foreach system of local filelist {
        di in red "starting `system'"
         // Read raw MI data and aggregate codes in long form
         use "FILEPATH/04_before_redistribution.dta", clear
         if list == "ICD10" {
            levelsof cause if substr(cause,1,3)=="I21"|substr(cause,1,3)=="I22"|substr(cause,1,3)=="I23", local(MI_codes) c
            noisily di in red "MI Causes : `MI_codes'"
            replace cause = "MI" if substr(cause,1,3)=="I21"|substr(cause,1,3)=="I22"|substr(cause,1,3)=="I23"
         }
         else if list == "ICD9_detail" {
            levelsof cause if substr(cause,1,3)=="410"|cause=="4110"|cause=="4113"|cause=="4118", local(MI_codes) c
            noisily di in red "MI Causes : `MI_codes'"
            replace cause = "MI" if substr(cause,1,3)=="410"|cause=="4110"|cause=="4113"|cause=="4118"
         }
        keep if cause == "MI"
         collapse (sum) deaths*, by(iso3 location_id sex year subdiv) fast
         reshape long deaths, i(iso3 location_id sex year subdiv) j(age)
         drop if deaths == 0
         rename deaths MI_deaths
         tempfile raw
         save `raw'
         // Read in redistributed IHD data, make long and load in MI data
         use iso3 location_id sex year acause  subdiv deaths* using "FILEPATH/04_before_redistribution.dta" if acause == "cvd_ihd", clear
         collapse (sum) deaths*, by(iso3 location_id sex year subdiv)
         reshape long deaths,i(iso3 location_id sex year subdiv) j(age)
         drop if deaths == 0
         rename deaths IHD_deaths
         replace location_id = . if location_id == 0
         merge 1:1 iso3 location_id sex year subdiv age using `raw'
         count if _m == 2
         if `r(N)' > 0 {
            di "Inconsistent mapping, MI showing up where no IHD exists"
            BREAK
         }
         count if _m == 1
         if `r(N)' > 0 {
            di "IHD in country-site-year-age-sex groups where no MI exists"
            replace MI_deaths = 0 if MI_deaths == .
         }
         drop _m
         
         // Mehrdad's formatting
         capture label drop _all
         gen double age_new = (age-6)*5 if age >= 7 & age <= 25
         tab age_new
         tostring age_new, replace force
         replace age_new = "0" if age == 91
         replace age_new = "0.01" if age == 93
         replace age_new = "0.1" if age == 94
         replace age_new = "All ages" if age == 1
         replace age_new = "Under 1" if age == 2
         replace age_new = "1" if age == 3
         replace age_new = "95+" if age_new == "95"
         drop age
         rename age_new age
         compress
        
        replace location_id = . if inlist(iso3, "PRI", "GUM", "ASM", "MNP", "VIR")
        collapse (sum) *deaths, by(iso3 location_id sex year subdiv age) fast
        ** // Load in region information
        ** merge m:1 iso3 location_id using `tem', assert(2 3) keep(3) nogen
        keep iso3 location_id sex year subdiv age *deaths 
        
        // Generate proportion
        gen proportion =  MI_deaths/IHD_deaths
        
        gen dataset = "`system'"
        
        // US counties are not needed after redistribution. Aggregate to state level and save dataset for compile. 
        preserve
            odbc load, exec("SELECT location_id, location_parent_id FROM shared.location WHERE location_parent_id BETWEEN 523 and 573 OR location_parent_id = 385") dsn(prodcod) clear
            tempfile states
            save `states', replace
        restore

        if inlist(dataset, "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10") {
            merge m:1 location_id using `states', keep(3) nogen
            ** Aggregate
            collapse (sum) *deaths, by(iso3 sex year subdiv age dataset location_parent_id)
            rename location_parent_id location_id
        }
    
        
        // Compile all datasets
        di in red "appending `system'"
        append using `master'
        tempfile master
        save `master', replace
    }

drop foo
 
    // Save final dataset
    ** local date = date(current)
    save "`save_dir'/mi_ihd_ratio.dta", replace
    capture saveold "`save_dir'/mi_ihd_ratio.dta", replace
    capture log close
    