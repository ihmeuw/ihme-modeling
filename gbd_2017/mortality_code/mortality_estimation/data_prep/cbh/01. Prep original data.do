// Purpose: This code obtains the relevant variables from Complete BH Questionnaires for direct 5q0 estimation
// This example is based off of a standard DHS Woman's individual recode file from Macro DHS 

clear
set more off
set maxvar 10000

local survey_name =  "strSurveyName"
local data_path = "FILEPATH"
local iso3 = "iso3Code"
local svdate1 = "strSurveyYear"
local svdate2 = "strSurveyYear"
local save_path = "FILEPATH"


// Make save folder if it doesn't exist
if "`save_path'" == "" {
    run "FILEPATH/get_paths.ado"
    get_paths, key("5q0_input") update_file(1)
    local save_stub = r(path)
    local save_path = "`save_stub'/cbh/`survey_name'/data" 
}

foreach sex in "both" "females" "males" {
     capture confirm file "`save_path'/prob of child dying/by survey/`sex'/nul"
        if _rc > 0 {    // Doesn't exist
            if "`c(os)'" == "Windows" {
                    shell mkdir "`save_path'/prob of child dying/by survey/`sex'"
            }
            else{
                    shell mkdir -p "`save_path'/prob of child dying/by survey/`sex'"
            }
        }
}

// Begin formatting data
noisily di "Beginning formatting process..."
use `data_path', clear

if (`svdate2' == 0){
    local svyear = "`svdate1'"
} 
else {
    local svyear = "`svdate1'" + "-" + "`svdate2'"
}

// Standardize DHS variables
cap renpfix V v
cap renpfix BORD bord
cap renpfix B b
cap drop b*_*_*
        
cap keep v001 v005 v008 v021 bord_* b3_* b4_* b6_* b7_*
local v021_error = _rc
if( `v021_error' != 0 ) keep v001 v005 v008 bord_* b3_* b4_* b6_* b7_*
	else {
        qui inspect v021
        if ( r(N_unique) == 0 ) drop v021
        else drop v001
    }
cap rename v001 psu
cap rename v021 psu
rename v005 sample_weight
rename v008 svdate

gen mother_id = _n
local country = "`iso3'"
generate country = "`iso3'"
		
// Perform birth history calculations
noisily di "   Reshaping... `country' file"
foreach var in bord_ b3_ b4_ b6_ b7_ {
	renpfix `var'0 `var'
}

// Format labels and fix date formats
reshape long bord_ b3_ b4_ b6_ b7_, i(mother_id) j(sib)			// creates individual observations for each child
drop if bord_ == .											    // drops empty rows where sib-number is not observed for a mother  
drop sib
rename bord_ sib
rename b3_ birthdate
rename b4_ sex
rename b6_ death_age_fine
rename b7_ death_age
drop mother_id sib  

foreach var of varlist * {
	destring `var', replace
}

order country svdate psu sample_weight sex birthdate death_age death_age_fine
        
        
// Fix death labels
cap label list b7_20
if( _rc == 0 ) {
    drop if death_age >= r(min) & death_age <= r(max)
    foreach x of numlist 1/20 {
        if(`x' >= 10) cap label drop b7_`x'
        else cap label drop b7_0`x'
    }
}

// Format death age variables 
// Make neonatal death ages
// ------------------------------
replace death_age = (death_age_fine-300)*12 + 6 if death_age_fine > 300 	// center deaths recorded in years  
gen nn_death = 1 if death_age_fine >= 100 & death_age_fine <= 106		// mark early neonatal deaths
replace nn_death = 2 if death_age_fine >= 107 & death_age_fine <= 127	// mark late neonatal deaths
replace nn_death = 0 if nn_death == . 									// marke non-neonatal deaths
replace death_age = 1 if death_age_fine > 127 & death_age == 0 			// move day 28-30 deaths into month 1 
drop death_age_fine


// Finalize formatting and save
// Test if have at least 20 unique death-date values
inspect death_age
local death_values = r(N_unique)
if (`death_values' < 20) {
     noi di in red "`survey_name' does not contain enough values. File will not be saved."
     shell echo File was dropped due to containing only `death_values' unique values. > ERROR_LOG.txt
}
else {
    noi di "    Saving..."
    cd "`save_path'/prob of child dying/by survey"

    // Save sexes separately
    preserve
    drop sex
    saveold "both/`country'_`svyear'_raw_both.dta", replace
    restore
        
    preserve
    keep if sex == 1
    drop sex
    saveold "males/`country'_`svyear'_raw_males.dta", replace
    restore

    preserve
    keep if sex == 2
    drop sex
    saveold "females/`country'_`svyear'_raw_females.dta", replace
    restore
    }

noi di "Extraction complete!"
