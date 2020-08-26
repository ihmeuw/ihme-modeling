
*** BOILERPLATE ***
clear all
set maxvar 10000
set more off

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

adopath + FILEPATH
adopath + FILEPATH


*** GET BIRTH DATA ***

get_covariate_estimates, covariate_id(ADDRESS) gbd_round_id(6) decomp_step("step4") clear
drop if year_id<1980
rename mean_value live_births_mean

/* Create a file of proportion of births that are male & female 
    for use in splitting out Zika-related births for congenital outcomes    */
drop if sex_id==3

bysort year_id location_id: egen prBirthsBySex = pc(live_births_mean), prop

keep year_id location_id sex_id prBirthsBySex 

save FILEPATH, replace    // prBirthsBySex


/*  ESTIMATE REMAINING COMPONENTS OF PROPORTION PREGNANT 
    in part 2 of step 01d:   FILEPATH/ntd_zika/01d_pcPregnant_part2of2.R    */
