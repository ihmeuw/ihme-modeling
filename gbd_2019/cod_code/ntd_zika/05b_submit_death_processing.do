* do FILEPATH/ntd_zika/05b_submit_death_processing.do

/******************************************************************************\
            SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
clear all
set maxvar 10000
set more off

adopath + FILEPATH
adopath + FILEPATH
 
local description Restructured code (first run)



*** SETUP DIRECTORIES ***
local inputDir FILEPATH
local codeDir FILEPATH
local outDir FILEPATH 

capture mkdir `outDir'

!rm -rf `outDir'/deaths
capture mkdir `outDir'/deaths

capture mkdir `outDir'/temp

!rm -rf `outDir'/logs/progress
capture mkdir `outDir'/logs/progress

use FILEPATH, clear
drop country_id
duplicates drop location_id, force



*** LOAD ESTIMATE TEMP FILES AND PROCESS BY LOCATION ***
quietly levelsof location_id if is_estimate==1, local(locations) clean  

foreach location of local locations {
    quietly {
        preserve
        keep if location_id==`location' 
        save `outDir'/temp/cfr_`location'.dta , replace 
        restore
        drop if location_id==`location' 
        cp `outDir'/temp/beta_fixedMaster.dta `outDir'/temp/beta_fixed_`location'.dta, replace
        }

    ! qsub  -P ADDRESS -pe multi_slot 8 -N zika_`location' "FILEPATH"
    sleep 100
    }



/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/

*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***
use `inputDir'/processing/deathMaster.dta, clear
keep if is_estimate==1
keep location_id 
duplicates drop
generate complete = 0



*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***
local pause 2
local complete 0
local incompleteLocations `locations'

display _n "Checking to ensure all locations are complete" _n



*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***
while `complete'==0 {
    * Are all locations complete?
    foreach location of local incompleteLocations {
        capture confirm file `outDir'/logs/progress/`location'.txt 
        if _rc == 0 quietly replace complete = 1 if location_id==`location'
        }
    quietly count if complete==0

    * If all locations are complete submit save results jobs
    if `r(N)'==0 {
        display "All locations complete." _n "Submitting save_results. "
        local complete 1
        run FILEPATH/save_results.do
        save_results, cause_id(935) description("`description'") in_dir(`outDir'/deaths) in_rate(yes)
        }

    * If all locations are not complete, inform the user and pause before checking again
    else {
        quietly levelsof location_id if complete==0, local(incompleteLocations) clean
        display "The following locations remain incomplete:" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue

        forvalues sleep = 1/`=`pause'*6' {
            sleep 10000
            if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
            else di "." _continue
            }
        di _n
        }
    }
