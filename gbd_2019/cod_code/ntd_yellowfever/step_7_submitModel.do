
/******************************************************************************\
    YELLOW FEVER MODELLING:

  Background: The few cases and episodic nature of yellow fever makes it
        poorly suited to modelling in DisMod.  And the limited and 
        unreliable death reports make custom natural history model
        necessary.

  Purpose: This script pulls together and manages the yellow fever data,
        and estimates incidence and mortality

\******************************************************************************/

* PREP STATA *
clear all 
set more off, perm
set maxvar 10000


* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 

if c(os) == "Unix" {
    local ADDRESS
    local user : env USER
    local ADDRESS
    set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
    local ADDRESS
    local ADDRESS
}

tempfile crossTemp appendTemp mergingTemp envTemp drawMaster sevInc zero

local date = subinstr(trim("`: display %td_CCYY_NN_DD date(c(current_date), "DMY")'"), " ", "_", .)

adopath + FILEPATH
run FILEPATH


* CREATE A LOCAL CONTAINING THE ISO3 CODES OF COUNTRIES WITH YELLOW FEVER *
local yfCountries AGO ARG BEN BOL BRA BFA BDI CMR CAF TCD COL COG CIV COD ECU GNQ ETH GAB GHA GIN GMB GNB GUY KEN LBR MLI MRT NER NGA PAN PRY PER RWA SEN SLE SDN SSD SUR TGO TTO UGA VEN ERI SOM STP TZA ZMB


* STORE FILE PATHS IN LOCALS *
local inputDir FILEPATH
local newinputFILEPATH

local ageDist`inputDir'/FILEPATH
local cf `inputDir'/FILEPATH
local ef `inputDir'/FILEPATH

local data `newinput'/FILEPATH
local skeleton `newinput'/FILEPATH

local outDir /FILEPATH/`=subinstr(trim("`c(current_date)'"), " ", "_", .)'_`=subinstr("`c(current_time)'", ":", "_", .)'
cap mkdir `outDir'
cap mkdir "`outDir'/FILEPATH"

foreach subDir in temp progress deaths total _asymp inf_mod inf_sev {
    !rm -rf `outDir'/`subDir'
    sleep 2000
    cap mkdir `outDir'/`subDir'
}

run "FILEPATH"


* LOAD COVARIATES *
tempfile covars
foreach covar_id in ADDRESS {
    get_covariate_estimates, covariate_id(`covar_id') gbd_round_id(6) decomp_step(step4) clear
    levelsof covariate_name_short, local(name) clean
    rename mean_value `name'
    keep location_id year_id `name'
    if `covar_id'!=ADDRESS merge 1:1 location_id year_id using `covars', nogenerate
    save `covars', replace
}



/******************************************************************************\
MODEL YELLOW FEVER CASES
\******************************************************************************/

use `data', clear

rename sex_id dataSexId

rename year_start year_id 

replace effective_sample_size = population if missing(effective_sample_size)

merge 1:1 location_id year_id using `covars', assert(2 3) keep(3) nogenerate

predict predFixed, fixedonly fitted nooffset
predict predFixedSe, stdp nooffset
predict predRandom, remeans reses(predRandomSe) nooffset

bysortcountryIso: egen countryRandom = mean(predRandom)
replace predRandom = countryRandom if missing(predRandom)

bysortregion_id: egen regionRandom = mean(predRandom)
replace predRandom = regionRandom if missing(predRandom)

bysortsuper_region_id: egen superRegionRandom = mean(predRandom)
replace predRandom = superRegionRandom if missing(predRandom)

replace predRandomSe = _se[var(_cons[countryIso]):_cons] if missing(predRandomSe)

bysort countryIso year_id: egen cntryCases = mean(cases)
replace cases = cntryCases if inlist(countryIso, "BRA", "KEN", "ETH", "NGA") & missing(cases) & !missing(cntryCases)
bysort countryIso year_id: egen cntryMean = mean(mean)
replace mean = cntryMean if inlist(countryIso, "BRA", "KEN", "ETH", "NGA") & missing(mean) & !missing(cntryMean)
bysort countryIso year_id: egen cntrySe = mean(standard_error)
replace standard_error = cntrySe if inlist(countryIso, "BRA", "KEN", "ETH", "NGA") & missing(standard_error) & !missing(cntrySe)

*dropping national level data for which we have subnat level data
drop if location_id==135
drop if location_id==214
drop if location_id==179
drop if location_id==180


drop age_group_id sex year_end
rename population allAgePop


*output preliminary file
export delimited "FILEPATH"



/******************************************************************************\
   BRING IN THE DATA ON YELLOW FEVER AGE-SEX DISTRIBUTION, EF, & CASE FATALITY
\******************************************************************************/

cross using `ageDist'
merge m:1 countryIso using `ef', assert(3) nogenerate

merge 1:1 location_id year_id age_group_id sex_id using `skeleton', assert(2 3) keep(3) nogenerate

rename population ageSexPop

keep ef_* year_id age_group_id sex sex_id location_id countryIso ihme_loc_id ageSexCurve pred* ageSexPop allAgePop mean standard_error cases effective sample_size location_name yfCountry 

gen ageSexCurveCases = ageSexCurve * ageSexPop
bysort location_id year_id: egen totalCurveCases = total(ageSexCurveCases)
gen prAgeSex = ageSexCurveCases / totalCurveCases

merge m:1 location_id year_id using `newinput'/braSubPr.dta, assert(1 3) nogenerate

replace allAgePop = mean_pop_bra if !missing(mean_pop_bra)
forvalues i = 0/999 {
    quietly replace braSubPr_`i' = 1 if missing(braSubPr_`i')
}


preserve
use `cf', clear
local deathsAlpha = alphaCf in 1
local deathsBeta = betaCf in 1

get_demographics, gbd_team(cod) clear
local locations `r(location_id)'
restore

levelsof location_id if yfCountry==1, local(yfCntryIds) clean


