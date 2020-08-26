
// File:		standardize_causes.do
// Project:	GBD - Cancer Registry
** Description:	Standardize causes to standardize format

** *****************************************************************************
** Standardize cause and cause_name	
** *****************************************************************************
// Ensure correct data format for cause and cause name (type may have been converted if all values were empty upon save)
capture tostring cause, replace
capture tostring cause_name, replace

// set script location for formatting icd10_codes. global is set by FILEPATH
local format_ICD10_codes = "$standardize_ICD10_codes"

** *****************************************************************************
** Correct Coding Systems
** *****************************************************************************
// mark rdptarget codes (codes used to assist redistribution packages that should not be found in the data)
gen rdp_target = 1 if regexm(cause, "rdptarget")

// Specially handle "And" characters in ICD coding Systems
local andChars = "& + ; /"
foreach andChar in `andChars' {
    replace cause = subinstr(upper(cause),"`andChar'",",",.) if rdp_target != 1
}
local andChars = "AND Y ET"
foreach andChar in `andChars' {
    replace cause = subinstr(upper(cause)," `andChar' ",",",.) if rdp_target != 1
}

// Re-assign entries to "CUSTOM" if incorrectly categorized
// Assign "CUSTOM" to ICD10 codes with alphabetic characters other than "C" or "D"
gen cause_test = subinstr(subinstr(cause, "C", "", .), "D", "", .)
gen fails_test = 1 if regexm(cause_test, "[a-zA-Z]") & inlist(coding_system, "ICD10", "ICD9_detail", "NPCR") & rdp_target != 1
replace cause_name = cause if cause != "" & fails_test == 1
replace coding_system = "CUSTOM" if fails_test == 1
drop cause_test fails_test

// Assign "CUSTOM" if cause is blank (data_check.do should have verified that either cause or cause_name exist)
replace coding_system = "CUSTOM" if inlist(coding_system, "ICD10", "ICD9_detail", "NPCR", "ICCC3") & cause == ""

// Remove "_cancer"
replace cause_name = subinstr(cause_name, "_cancer", "", .)
replace cause_name = substr(cause_name, 1, length(cause_name) -1) if inlist(substr(cause_name, -1, 1), ","  , "_")
    
// Re-assign CUSTOM causes to GBD if in the GBD cause list
count if coding_system == "CUSTOM"
if r(N) {
    // get gbd causes
    tempfile temp_standardize_causes
    save `temp_standardize_causes', replace
    run "${code_prefix}/_database/load_database_table" "cancer_cause_map"
    keep if coding_system == "GBD"
    levelsof cause_name, clean local(gbd_causes)
    use `temp_standardize_causes', clear

    // replace coding_system where necessary
    foreach entry in `gbd_causes'{
        replace coding_system = "GBD" if lower(cause_name) == lower("`entry'")
    }
}

** *****************************************************************************
** Remove Special Characters, Standardize Spacing and Commas for cause and cause name
** *****************************************************************************
// // Format entries
// Reformat capitalization
foreach var in cause cause_name {
        replace `var' = itrim(`var')
        replace `var' = trim(`var')
        replace `var' = lower(`var')
}	

// adjust special characters in cause
local andChars = "& + ;"  
foreach andChar in `andChars' {
    replace cause = subinstr(cause,"`andChar'",", ",.)
}
local specialChars = "’ : ~ ' ` ! @ # $"
foreach special in `specialChars' {
    replace cause = subinstr(cause,"`special'","",.)
}
local extraSpecialChars = "\[ \^ \% \? \* \( \) \]"
foreach special in `extraSpecialChars' {
    replace cause = regexr(cause,"`special'","")
}


// cause_name
// standardize "and" characters"
local andChars = "& + ;"  
foreach andChar in `andChars' {
    replace cause_name = subinstr(cause_name,"`andChar'"," And ",.)
}
local specialChars = "’ : ; , ~ ' ` * ! @ # $ / Cancers Cancer ."
foreach special in `specialChars' {
    replace cause_name = subinstr(cause_name,"`special'","",.)
}
local extraSpecialChars = "\[ \/ \^ \% \. \| \? \* \( \) \]"
foreach special in `extraSpecialChars' {
    replace cause_name = regexr(cause_name,"`special'","")
}

// remove special characters
replace cause_name = subinstr(cause_name,"ç","c",.)
replace cause_name = subinstr(cause_name,"æ","ae",.)
replace cause_name = subinstr(cause_name,"œ","oe",.)
replace cause_name = subinstr(cause_name,"ñ","n",.)
foreach letter in ã â á à ä{
    replace cause_name = subinstr(cause_name,"`letter'","a",.)
}
foreach letter in é è ê ë {
    replace cause_name = subinstr(cause_name,"`letter'","e",.)
}
foreach letter in í ì î ï {
    replace cause_name = subinstr(cause_name,"`letter'","i",.)
}
foreach letter in ó ò ô õ ö {
    replace cause_name = subinstr(cause_name,"`letter'","o",.)
}
foreach letter in ú ù û ü {
    replace cause_name = subinstr(cause_name,"`letter'","u",.)
}
foreach letter in ý ÿ {
    replace cause_name = subinstr(cause_name,"`letter'","y",.)
}
replace cause_name = trim(itrim(cause_name))

// remove hyphens
replace cause_name = subinstr(cause_name, "-", " ", .)

// Standardize spaces
replace cause_name = subinstr(cause_name, "_", " ", .) if coding_system != "GBD" & rdp_target != 1

// cause		
// standardize type of hyphen
replace cause = subinstr(cause,"–","-",.)  
replace cause = subinstr(cause,"–", "-",.)
replace cause = subinstr(cause,"‐","-",.)  
replace cause = subinstr(cause,"‑","-",.)  
replace cause = subinstr(cause,"‒","-",.)  
replace cause = subinstr(cause,"–", "-",.)
replace cause = subinstr(cause,"—", "-",.)
replace cause = subinstr(cause,"―", "-",.)


// Standardize spaces
replace cause = subinstr(cause, "_", " ", .) if coding_system != "GBD" & rdp_target != 1
replace cause = subinstr(cause, " ", "", .)


// add remaining formatting		
foreach var in cause cause_name {
    // remove trailing commas or underscores
        replace `var' = substr(`var', 1, length(`var') -1) if inlist(substr(`var', -1, 1), ","  , "_")
    
    // Reformat capitalization
        replace `var' = lower(`var') if coding_system == "GBD" | rdp_target == 1
        replace `var' = strproper(`var') if coding_system != "GBD" & coding_system != "ICCC" & rdp_target != 1
        replace `var' = itrim(`var')
        replace `var' = trim(`var')
}


// Remove cause_name from defined coding systems. Remove cause from CUSTOM/GBD coding systems
replace cause_name = cause if !inlist(coding_system, "ICD10", "ICD9_detail", "NPCR", "ICCC3") & cause_name == ""
replace cause = "" if !inlist(coding_system, "ICD10", "ICD9_detail", "NPCR", "ICCC3")
replace cause_name = "" if inlist(coding_system, "ICD10", "ICD9_detail", "NPCR", "ICCC3")

// Ensure that all data still have cause Entries
count if cause == "" & cause_name == ""
if r(N) {
    di "ERROR: Both cause and cause name were dropped for some entries"
    break
}

// drop rdp_target marker
drop rdp_target

// Save tempfile
tempfile master_data
save `master_data', replace

** *****************************
** Set variable to check for ICD codes
** *****************************
// Assume no ICDcodes unless indicated later in the script
local hasICDcodes = 0

** *****************************
** Format ICD10 codes
** *****************************
use `master_data', clear
count if coding_system == "ICD10" 	
if `r(N)' > 0 {
    // Get source name for later use
    capture levelsof source, clean local(source_name)

    // save status for later use
    local hasICDcodes = 1
    local hasICD10 = 1

    // subset data and prepare for formatting
    keep if coding_system == "ICD10"
    keep cause coding_system
    quietly duplicates drop
    gen orig_cause = cause
    
    // // Reformat common issues
    // Replace "O" with "0"
    replace cause = subinstr(cause,"O","0",.)

    // Ensure that there are no ranges that include both C and D codes
    quietly foreach 1 of numlist 0/9 {
        foreach 2 of numlist 0/9 {
            replace cause = subinstr(cause, "C`1'`2'-D", "C`1'`2'-C96, D00-D", .)
            foreach 3 of numlist 0/9 {
                replace cause = subinstr(cause, "C`1'`2'.`3'-D", "C`1'`2'.`3'-C96.9, D00.0-D", .)
            }
        }
    }
    
    // // Add letters between distinct codes that are adjacent 
    // separate "C" causes from "D" causes, if "D" causes exist"
    gen pos_D = strpos(cause, "D")
    count if pos_D > 0
        noisily di "Standardizing ICD10 'C' codes for `source_name'"
        if r(N) == 0 do "`format_ICD10_codes'" "C" "cause"
    else {
        // generate separate causes
            gen cause1 = substr(cause, 1, pos_D)
            replace cause1 = cause if pos_D == 0
            gen cause2 = substr(cause, pos_D, .)
        // remove trailing commas
            replace cause1 = substr(cause1, 1, length(cause1) -1) if substr(cause1, -1, 1) == "D"  
            replace cause1 = substr(cause1, 1, length(cause1) -1) if substr(cause1, -1, 1) == ","
            replace cause1 = trim(cause1)
            replace cause2 = trim(cause2)
            
        // run formatting script on "D" codes
            noisily di "Standardizing ICD10 'C' codes for `source_name'"
            do "`format_ICD10_codes'" "C" "cause1"
            noisily di "Standardizing ICD10 'D' codes for `source_name'"
            do "`format_ICD10_codes'" "D" "cause2"
            replace cause = cause1
            replace cause = cause1 + "," + cause2 if cause1 != "" & cause2 != ""
            replace cause = cause2 if cause1 == "" & cause2 != ""
            drop cause1 cause2
    }

    // Save
    keep cause coding_system orig_cause
    compress
    tempfile icd10_codes
    save `icd10_codes', replace
}	

** *****************************
** Format ICD9_detail codes
** *****************************
use `master_data', clear
count if coding_system == "ICD9_detail"
if `r(N)' > 0 {
    // Alert user
    if "`source_name'" == "" capture levelsof source, clean local(source_name)
    noisily di "Standardizing ICD9 codes for `source_name'"

    // save status for later use
    local hasICDcodes = 1
    local hasICD9 = 1
    
    // subset data and prepare for formatting
    keep if coding_system == "ICD9_detail"
    keep cause coding_system
    quietly duplicates drop
    gen orig_cause = cause
    
    // Add decimals to 4 and 5 digit codes (probably? make sure this isn't cause9 stuff)
    quietly foreach 1 of numlist 0/9 {
        foreach 2 of numlist 0/9 {
            foreach 3 of numlist 0/9 {
                foreach 4 of numlist 0/9 {
                    replace cause=subinstr(cause, "`1'`2'`3'`4'", "`1'`2'`3'.`4'", .)
                }
            }
        }	
    }	
        
    // Save
    compress
    tempfile icd9_detail_codes
    save `icd9_detail_codes', replace
}
    
** *****************************
** Append if ICD codes are present
** *****************************
if `hasICDcodes' == 1 {
    // Append together
    clear
    capture if `hasICD10' == 1 append using `icd10_codes'
    capture if `hasICD9' == 1 append using `icd9_detail_codes'

    // Rename to fit original data set
    rename cause cause_new
    rename orig_cause cause
    // Save
    compress
    tempfile cleaned_icd_codes
    save `cleaned_icd_codes', replace

    // // Merge with cleaned codes.  Replace codes with cleaned codes.
    use `master_data', clear
    merge m:1 cause coding_system using `cleaned_icd_codes', assert(1 3)
    replace cause = cause_new if _merge == 3
    drop _merge cause_new
} 
else {
    use `master_data', clear
}

** *****************************
** End Standardize Causes
** *****************************
