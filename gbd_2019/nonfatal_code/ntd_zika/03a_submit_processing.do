* do FILEPATH/ntd_zika/03a_submit_processing.do

/******************************************************************************\
            SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
clear all

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

adopath + FILEPATH
adopath + FILEPATH
run FILEPATH



*** SETUP DIRECTORIES ***
local inputDir FILEPATH 
local outDir FILEPATH

capture mkdir `outDir'

foreach subDir in master_logs logs temp fatalities total inf_mod _asymp gbs congenital preg cases {
    !rm -rf `outDir'/`subDir'
    sleep 2000
    capture mkdir `outDir'/`subDir'
    } 
 
!rm -rf `outDir'/logs/progress
capture mkdir `outDir'/logs/progress


local allAge FILEPATH
local ageSpecific FILEPATH
local outcomes FILEPATH
local prBirthsBySex FILEPATH



*** GET LIST OF ESTIMATION LOCATIONS ***
get_demographics, gbd_team(epi) clear
local locations `r(location_id)'



*** LOAD ESTIMATE TEMP FILES AND PROCESS BY LOCATION ***
foreach file in allAge ageSpecific outcomes prBirthsBySex {
    di _n "Processing locations for `file' file"
    use ``file'', replace
    if "`file'" == "allAge" quietly levelsof location_id, local(locations) clean

    local i 1
    
    foreach location of local locations {
        quietly {
            if "`file'"=="outcomes"{
                ! cp `outcomes' `outDir'/temp/`file'_`location'.dta
                }
            else {
                preserve
                keep if location_id==`location' 
                save `outDir'/temp/`file'_`location'.dta , replace 
                restore
                drop if location_id==`location' 
                }
            }
            di "." _continue
            if mod(`i', 100)==0 di ""
            local ++i
        }
        di _n
    
    }



    foreach location of local locations {
        ! qsub -P ADDRESS -l m_mem_free=20G -l fthread=8 -l archive=True -l h_rt=04:00:00 -q all.q -N zika_`location' "FILEPATH" "`location'"
        sleep 500
        }
