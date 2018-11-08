** Description: Runs the functions to estimate under-5 mortality from complete birth history data 
clear all 
macro drop _all 
capture cleartmp
set more off 
pause off 
set mem 1g
set matsize 11000
	
local survey_name =  "strSurveyAbbreviation"
local data_path = "FILEPATH"
local iso3 = "iso3Code"
local svdate1 = "strSurveyDate"
local svdate2 = "strSurveyDate"

//run CBH methods
	local neonatal 1	
	local save_GBD 1
	local save_sims 0
	local subset 0
	local restart 0
	local exclude 0
	local shock 0
	local save_gbd 1
	local shock_start 0
	local shock_end 0

    // Set up paths
    local cbh_functions = "FILEPATH"
	local cbh_prep = "FILEPATH"
	local save_path = "FILEPATH/`survey_name'"

    run "FILEPATH/FUNCTION_person_months.ado"
    run "FILEPATH/FUNCTION_estimate_mort_indiv_surv.ado"

    foreach sex in "both" "males" "females" {
		noisily dis "`sex'"
        global sex = "`sex'"
        if ("$sex" == "both") global sims 1 
	    if ("$sex" != "both") global sims 0 
        if "$sex" == "both" {
            noisily di "Running methods for `survey_name', both sexes."
        }
        else {
            noisily di "Running methods for `survey_name', `sex'."
        }
        noisily di in red "This may take a while."
    
        noisily di "Converting to person-months..."
        CBH_person_months, directory("`save_path'") sex("$sex") neonatal (`neonatal') subset(`subset') subset_country("`subset_country'") ///
                           restart(`restart') restart_country("`restart_country'") ///
                           exclude(`exclude') exclude_country("`exclude_country'") ///
                           
        // Run CBH methods on 2 year periods for GPR
        noisily di "Running mort estimation for GPR..."
        CBH_estimate_mort_indiv_surv, directory("`save_path'") sex("$sex") period(2) ///
                                        neonatal(`neonatal') save_GBD(`save_gbd') save_sims($sims) source("`survey_name'") ///
                                        subset(`subset') subset_country("`subset_country'") ///
                                        restart(`restart') restart_country_survey("`restart_country_survey'") ///
                                        exclude(`exclude') exclude_country("`exclude_country'") ///
                                        shock(`shock') shock_start(`shock_start') shock_end(`shock_end')

        // Run CBH methods on 5 year periods for age/sex model
        noisily di "Running mort estimation for age/sex model..."
        CBH_estimate_mort_indiv_surv, directory("`save_path'") sex("$sex") period(5) ///
                                        neonatal(`neonatal') save_GBD(0) save_sims(`save_sims') source("`survey_name'") ///
                                        subset(`subset') subset_country("`subset_country'") ///
                                        restart(`restart') restart_country_survey("`restart_country_survey'") ///
                                        exclude(`exclude') exclude_country("`exclude_country'") ///
                                        shock(`shock') shock_start(`shock_start') shock_end(`shock_end')        
    }

    // Collate files 
    clear
    cd "FILEPATH/results/by survey/direct 5q0 for GBD - survey"
    cap erase "direct 5q0 for GBD.dta"
    local files: dir . files "direct 5q0 for GBD*.dta"
    local count : word count `files'
	
    if `count' > 1 {
        noisily di "Collating files."
        local i = 0
        foreach f of `files' {
            drop _all
            use "`f'"
            if `i' > 0 append using "direct 5q0 for GBD.dta"
            saveold "direct 5q0 for GBD.dta", replace
            local i = 1
        }
    }
	else {
		local surveyname "`iso3'_`svdate1'-`svdate2'"
		use "direct 5q0 for GBD - `surveyname'.dta"
	}

    // Save and exit
    duplicates drop
    saveold "FILEPATH/results/by survey/direct 5q0 for GBD.dta", replace
    noisily di "Results saved successfully."
	