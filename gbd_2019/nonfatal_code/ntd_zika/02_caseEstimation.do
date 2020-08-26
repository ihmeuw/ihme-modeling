/******************************************************************************\    
    Zika Model

    Background: As a newly emerging infectious disease with relatively little data, Zika is a poor fit to be modeled in Dismod. 

    Purpose:    This script pulls toghether and manages the Zika data,
                and estimates incidence and proportions

    Pathway to run on cluster: do FILEPATH/ntd_zika/03_caseEstimation.do
\******************************************************************************/


**Stata Set Up**
clear all
set more off, perm
set maxvar 10000

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }


*** ESTABLISH TEMPFILES & FILEPATHS ***

local inputDir FILEPATH 

local ageFile  FILEPATH        // age Distribution
local pregFile FILEPATHS
local outcomes FILEPATH
local efFile   FILEPATH        // reporting Rates

tempfile allAge incidence locations master age_specific_pop all_age_pop dataFull



*** LOAD SHARED FUNCTIONS ***
adopath + FILEPATH
adopath + FILEPATH
run FILEPATH
run FILEPATH
run FILEPATH

run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"
run "FILEPATH"



*** CREATE NEW ZIKA ENDEMICITY LOCAL - GBD 2019 ***
import delimited FILEPATH        // Zika geo restrictions

drop if value_endemicity==0
levelsof location_id, local(zCountries) clean



*** PULL IN INCIDENCE DATA ***
get_bundle_data, bundle_id(ADDRESS) decomp_step("iterative") clear

keep if value_keep_row==1

levelsof location_id, local(dataLocs) clean
preserve



*** GET LOCATION METADATA FOR ALL ESTIMATION LOCATIONS, COUNTRIES, AND LOCATIONS WITH INCIDENCE DATA ***
get_location_metadata, location_set_id(35) clear 
keep if is_estimate==1 | location_type=="admin0" 
*keep if location_id==`dataLocs'
keep location_id location_name parent_id location_type ihme_loc_id *region* is_estimate
generate countryIso = substr(ihme_loc_id, 1, 3)
tempfile locationTemp
save `locationTemp'



*** COMBINE INCIDENCE & LOCATION METADATA ***
restore

merge m:1 location_id using `locationTemp', keep(1 3) nogenerate
generate year_id = ceil((year_start + year_end)/2)
save `dataFull'



*** COLLAPSE AGE-SPECIFIC DATA TO ALL AGES ***
collapse (sum) cases value_imported sample_size, by(location_id year_id ihme_loc_id)

generate age_start = 0
generate age_end   = 99
generate sex       = 3

rename value_imported imported

gen iso3 = substr(ihme_loc_id, 1, 3)
bysort iso3 year_id: egen countryCases = total(cases)

keep cases imported countryCases location_id year_id sample_size 
save `incidence'



*** CREATE MASTER SKELETON ***
* Pull locations *
use `locationTemp', clear


* Flag endemic locations *
generate endemic = 0
foreach i of local zCountries {
    quietly replace endemic = 1 if location_id==`i'
    }

save `locationTemp', replace


* Make dataset of years *
get_demographics, gbd_team(epi) clear

clear
set obs `=`=word("`r(year_id)'", -1)'-1979'
generate year_id = _n + 1979


* Cross to make master *
cross using `locationTemp'
save `master'



*** BRING IN POPULATION ESTIMATES ***
levelsof location_id, local(locationList) clean
levelsof year_id, local(yearList) clean

get_population, location_id("`locationList'") sex_id("1 2") age_group_id("-1") year_id("`yearList'") decomp_step(step4) clear
keep year_id age_group_id sex_id location_id population


preserve
keep if !inlist(age_group_id, ADDRESS1, ADDRESS2, ADDRESS3)
rename population pop_age_specific
save `age_specific_pop'

restore
keep if age_group_id==ADDRESS1 
fastcollapse population, by(year_id location_id) type(sum)
rename population pop_all_age

save `all_age_pop'



*** MERGE EVERYTHING TOGETHER ***
*use `master', clear
merge 1:1 location_id year_id using `master', assert(3) nogenerate
merge 1:1 location_id year_id using `incidence', keep(1 3) nogenerate
merge m:1 countryIso using `efFile', keep(1 3) nogenerate 



*** CLEAN UP MISSING OR EXTREME EXPANSION FACTORS ***
gen ef = (efAlpha + efBeta) / efAlpha
gen hasEf = !missing(ef)
bysort region_name year_id hasEf: gen count = _N
bysort region_name year_id hasEf (ef): gen index = _n
gen mark = index==round(count/2)
sum ef if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
local caribbeanEf = `r(mean)'
sum efAlpha if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
replace efAlpha = `r(mean)' if region_name=="Caribbean" & ef>`caribbeanEf'
sum efBeta if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
replace efBeta = `r(mean)' if region_name=="Caribbean" & ef>`caribbeanEf'
* Where EFs are not available, use region means *
foreach parm in Alpha Beta {
    quietly {
        bysort region_id: egen meanEf`parm' = mean(ef`parm')
        replace ef`parm' = meanEf`parm' if missing(ef`parm')
        drop meanEf`parm'
        }
    }



*** RESOLVE MISSING DATA FOR NON-ENDEMIC COUNTRIES ***
replace cases = 0 if missing(cases)
replace countryCases = 0 if missing(countryCases)

replace sample_size = pop_all_age if missing(sample_size) | sample_size==0 

replace endemic = 1 if cases>0 & !missing(cases)



*** EXPORT ALL-AGE DATA FILE ***
preserve
keep location_id year_id pop_all_age endemic cases imported countryCases sample_size efAlpha efBeta
save FILEPATH, replace    / all-age estimate



*** EXPAND TO AGE-SPECIFIC ***
restore
cross using `ageFile'
merge 1:1 location_id year_id age_group_id sex_id using `age_specific_pop', assert(2 3) keep(3) nogenerate

merge m:1 location_id year_id age_group_id sex_id using `pregFile', keep(1 3) nogenerate

* This fills in prPreg missing numbers in Oceania 
bysort year_id region_id age_group_id sex_id: egen pregMean = mean(prPreg)
replace prPreg = pregMean if missing(prPreg) & sex==2 & inrange(age_group_id, 7, 15)
drop pregMean



*** EXPORT ***
save FILEPATH, replace    // master Estimate

keep location_id year_id sex_id age_group_id ageSexCurve ageSexCurveSe pop_age_specific prPreg
replace prPreg = 0 if missing(prPreg)
save FILEPATH, replace    // age Specific
