** **************************************************************************
** CONFIGURATION
** **************************************************************************
    ** ****************************************************************
    ** Prepare STATA for use
    **
    ** This section sets the application preferences.  The local applications
    **  preferences include memory allocation, variables limits, color scheme,
    **  defining the J drive (data), and setting a local for the date.
    **
    ** ****************************************************************
        // Set application preferences
            // Clear memory and set memory and variable limits
                clear all
                set mem 10G
                set maxvar 32000

            // Set to run all selected code without pausing
                set more off

            // Set graph output color scheme
                set scheme s1color

            // Define J drive (data) for cluster (UNIX) and Windows (Windows)
                if c(os) == "Unix" {
                    global prefix "/FILEPATH/j"
                    set odbcmgr unixodbc
                }
                else if c(os) == "Windows" {
                    global prefix "J:"
                }

            // Get date
                local today = date(c(current_date), "DMY")
                local year = year(`today')
                local month = string(month(`today'),"%02.0f")
                local day = string(day(`today'),"%02.0f")
                local today = "`year'_`month'_`day'"


        ** ****************************************************************
        ** SET LOCALS
        **
        ** Set data_name local and create associated folder structure for
        **  formatting prep.
        **
        ** ****************************************************************
            // Data Source Name
                local data_name "USA_NHAMCS_92_10"
            // Original data folder
                local input_folder "$prefix{FILEPATH}/"
            // Log folder
                local log_folder "$prefix{FILEPATH}/logs"
                capture mkdir "`log_folder'"
            // Output folder
                local output_folder "$prefix{FILEPATH}/"
                local archive_folder "`output_folder'/_archive"
                capture mkdir "`output_folder'"
                capture mkdir "`archive_folder'"


** **************************************************************************
** RUN FORMAT PROGRAGM
** **************************************************************************
    // GET DATA
        // Get list of files
            local file_set: dir "`input_folder'/" files "USA_NHAMCS_*.DTA", respectcase
        // Append together
            clear
            gen cause1 = ""
            tempfile master_data
            save `master_data', replace
            foreach f of local file_set {
                display in red "Appending in `f'"
                use "`input_folder'/`f'", clear
                label drop _all
                // Convert certain variables to strings in each data set
                ** NOTE: these variables appear as both numbers and strings depending on the data set. Converting these to strings to ensure no data is lost if we were to do a append, force
                foreach var of varlist * {
                    local nn = lower("`var'")
                    rename `var' `nn'
                }
                foreach var in diagsc1 proc2 cause1 scopewi1 therpr1 rx1 arrtime cancer owner examwi1 {
                    capture gen `var' = ""
                }
                foreach var of varlist diag* cause* proc* scopewi* therpr* rx* arrtime* cancer* owner* examwi* med* {
                    tostring(`var'), replace force
                }
                // Determine whether this is an outpatient or ED visit
                    gen platform = ""
                    replace platform = "Outpatient" if substr("`f'",17,3) == "OPD"
                    replace platform = "Emergency" if substr("`f'",17,2) == "ED"
                append using `master_data'
                tempfile master_data
                save `master_data', replace
            }
    // ENSURE ALL VARIABLES ARE PRESENT
        // source (string): source name
            gen source = "`data_name'"
        // iso3 (string)
            gen iso3 = "USA"
        // subdiv (string)
            gen subdiv = ""
        // location_id (numeric)
            gen location_id = .
        // national (numeric): 0 = no, 1 = yes
            gen national = 1
        // year (numeric)
            replace vyear = year if vyear == .
            drop year
            rename vyear year
            replace year = 1900 + year if year < 100 & year > 50
            replace year = 2000 + year if year < 50
        // NID (numeric)
            gen NID = .
            replace NID = 13334 if year <= 1992
            replace NID = 68719 if year == 1993
            replace NID = 68722 if year == 1994
            replace NID = 68725 if year == 1995
            replace NID = 68728 if year == 1996
            replace NID = 68731 if year == 1997
            replace NID = 68734 if year == 1998
            replace NID = 68737 if year == 1999
            replace NID = 68743 if year == 2000
            replace NID = 68746 if year == 2001
            replace NID = 68740 if year == 2002
            replace NID = 68749 if year == 2003
            replace NID = 68752 if year == 2004
            replace NID = 68755 if year == 2005
            replace NID = 68758 if year == 2006
            replace NID = 68761 if year == 2007
            replace NID = 68764 if year == 2008
            replace NID = 68767 if year == 2009
            replace NID = 114636 if year == 2010
            // added in on 2017_11_06
            replace NID = 209949 if year == 2011
            replace NID = 256692 if year == 2012
            replace NID = 283639 if year == 2013
            replace NID = 304745 if year == 2014
        // age (numeric)
            gen age_start=.
            replace age_start = 0 if age == 0
            replace age_start = 0 if ageday >=0 & ageday <= 6
            replace age_start = .01 if ageday >= 7 & ageday <= 27
            replace age_start = .1 if ageday >= 28 & ageday <=365
            replace age_start = 1 if (age >=1 & age <=4)
            replace age_start = 5 if (age >= 5 & age <=9)
            replace age_start = 10 if (age >=10 & age <= 14)
            replace age_start = 15 if (age >=15 & age <= 19)
            replace age_start = 20 if (age >=20 & age <= 24)
            replace age_start = 25 if (age >=25 & age <= 29)
            replace age_start = 30 if (age >=30 & age <= 34)
            replace age_start = 35 if (age >=35 & age <= 39)
            replace age_start = 40 if (age >=40 & age <= 44)
            replace age_start = 45 if (age >=45 & age <= 49)
            replace age_start = 50 if (age >=50 & age <= 54)
            replace age_start = 55 if (age >=55 & age <= 59)
            replace age_start = 60 if (age >=60 & age <= 64)
            replace age_start = 65 if (age >=65 & age <= 69)
            replace age_start = 70 if (age >=70 & age <= 74)
            replace age_start = 75 if (age >=75 & age <= 79)
            replace age_start = 80 if (age >=80 & age <= 84)
            replace age_start = 85 if (age >=85)
            drop age agedays
            rename age_start age
        // frmat (numeric): find the WHO format here "FILEPATH\Age formats documentation.xlsx"
            gen frmat = 2
        // im_frmat (numeric): from the same file as above
            gen im_frmat = 2
        // sex (numeric): 1=male 2=female 9=missing
            gen female = 1 if sex == 1
            replace female = 0 if sex == 2
            replace female = 8 if female == .
            drop sex
            gen sex = female + 1
        // platform (string): "Inpatient", "Outpatient"
            ** already generated
        // patient_id (string)
            gen patient_id = ""
        // icd_vers (string): ICD version - "ICD10", "ICD9_detail"
            gen icd_vers = "ICD9_detail"
        // dx_* (string): diagnoses
            forvalues i = 1/3 {
                display "Generating dx_`i'"
                gen dx_`i' = diag`i'
                replace dx_`i' = subinstr(dx_`i', "-","",.)
                replace dx_`i' = subinstr(dx_`i', " ","",.)
                replace dx_`i' = subinstr(dx_`i', ".","",.)
                replace dx_`i' = "" if dx_`i' == "9"
                replace dx_`i' = "" if dx_`i' == "9"
                replace dx_`i' = "" if dx_`i' == "90000"
                replace dx_`i' = "" if dx_`i' == "900000"
                local all_zeros = "0"
                forvalues n = 1(1)8 {
                    replace dx_`i' = "" if dx_`i' == "`all_zeros'"
                    local all_zeros = "`all_zeros'" + "0"
                }
            }
        // ecode_* (string): variable if E codes are specifically mentioned
            forvalues i = 1/3 {
                display "Generating ecode_`i'"
                gen ecode_`i' = "E"+cause`i' if cause`i' != "0000" & cause`i' != "" & cause`i' != "0"
                replace ecode_`i' = subinstr(ecode_`i', "-","",.)
                replace ecode_`i' = subinstr(ecode_`i', " ","",.)
                replace ecode_`i' = subinstr(ecode_`i', ".","",.)
                replace ecode_`i' = "" if ecode_`i' == "0"
                replace ecode_`i' = "" if ecode_`i' == "E90000"
                // There are "missing" codes (i.e. codes that indicate that a cause was not given) for before 1995
                ** Also during this time span, the preceding 8 in these should be replaced by an E
                replace ecode_`i' = "" if (ecode_`i' == "E80019" | ecode_`i' == "E80000" | ecode_`i' == "E80010") & year <= 1994
                replace ecode_`i' = subinstr(ecode_`i',"E8","E",1) if year <= 1994
            }
        // Outpatient variables
            // visits (numeric)
                gen metric_visits_unweighted = 1
                gen metric_visits_weighted = patwt

    // VARIABLE CHECK
        // If any of the variables in our template are missing, create them now (even if they are empty)
        // All of the following variables should be present
            #delimit;
            order
            iso3 subdiv location_id national
            source NID
            year
            age frmat im_frmat
            sex platform patient_id
            icd_vers dx_* ecode_*
            metric_*;
        // Drop any variables not in our template of variables to keep
            keep
            iso3 subdiv location_id national
            source NID
            year
            age frmat im_frmat
            sex platform patient_id
            icd_vers dx_* ecode_*
            metric_*;
            #delimit cr

    // DO FINAL COLLAPSE ON DATA
        collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

** **************************************************************************
** RUN SELECT PRIMARY PROGRAGM
** **************************************************************************
    // drop bed days of zero
        // DON'T HAVE A BED DAYS COLUMN
    // Select primary GBD code
    ** NOTE: use the primary GBD code unless there is an E code
        gen cause_primary = dx_1
        // Prioritize External Cause of Injury & Service Codes over Nature of Injury
            // Diagnosis codes
                local dx_count = 0
                foreach var of varlist dx_* {
                    local dx_count = `dx_count' + 1
                }
                local dx_count = `dx_count'
                // Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
                    forvalues dx = `dx_count'(-1)1 {
                        display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in dx_`dx'"
                        replace cause_primary = dx_`dx' if (inlist(substr(dx_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx',1,1),"E") & icd_vers == "ICD9_detail")
                    }
            // External cause set
                local ecode_count = 0
                foreach var of varlist ecode_* {
                    local ecode_count = `ecode_count' + 1
                }
                local ecode_count = `ecode_count'
                // Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
                    forvalues dx = `ecode_count'(-1)1 {
                        display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in ecode_`dx'"
                        replace cause_primary = ecode_`dx' if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
                    }
    // Reshape long
        drop dx_1
        rename cause_primary dx_1

        // drop Ecodes since we've already replaced dx_1 with an ecode where appropriate
        drop ecode_*
        . gen long id = _n
        reshape long dx_, i(id) j(dx_ecode_id)
        replace dx_ecode_id = 2 if dx_ecode_id > 2
        drop if missing(dx_)

    // Collapse
        keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_
        collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id dx_) fast


** **************************************************************************
** RUN EPI COMPILE CLUSTER PROGRAGM
** **************************************************************************
    // STANDARDIZE PLATFORMS
        drop if platform == "Outpatient"
        replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1" | platform == "Inpatient 2"
        replace platform = "2" if platform == "Emergency"
        destring platform, replace
        assert platform != .

    // STANDARDIZE METRICS
        gen cases = .
            capture confirm var metric_discharges
            if !_rc {
                replace cases = metric_discharges if platform == 1
            }
            capture confirm var metric_discharges_weighted
            if !_rc {
                replace cases = metric_discharges_weighted if platform == 1
            }
            capture confirm var metric_visits
            if !_rc {
                replace cases = metric_visits if platform == 2
            }
            capture confirm var metric_visits_weighted
            if !_rc {
                replace cases = metric_visits_weighted if platform == 2
            }
        // There aren't any deaths
        gen deaths = .
            capture confirm var metric_deaths
            if !_rc {
                replace deaths = metric_deaths
            }
            capture confirm var metric_deaths_weighted
            if !_rc {
                replace deaths = metric_deaths_weighted
            }


    // STANDARDIZE AGES
        // There aren't any null ages
        drop if age == .
        replace age = 85 if age > 85
        replace age = 0 if age < 1

        rename age age_start
        gen age_end = age_start + 4
        replace age_end = 1 if age_start == 0
        replace age_end = 89 if age_start == 85
        replace age_end = 4 if age_start == 1

    // LOCATION_ID
        replace location_id = 102

    // DO FINAL COLLAPSE ON DATA
    collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_start age_end sex platform patient_id icd_vers dx_* dx_ecode_id NID) fast

    // modify columns to fit our structure
        rename dx_ cause_code
        rename dx_ecode_id diagnosis_id

    // SAVE
        compress
        save "$prefix{FILEPATH}/formatted_NHAMCS.dta", replace
        save "prefix{FILEPATH}/formatted_NHAMCS_`today'.dta", replace


** *******************************************************************************************************************************************************************
** *******************************************************************************************************************************************************************
