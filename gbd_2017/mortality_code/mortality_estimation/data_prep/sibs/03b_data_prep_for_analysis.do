
** *******************************************************************************************************

** ******************************************************************************************
** SET UP STATA                         
** *****************************************************************************************

clear
capture clear matrix
set more off

use "FILEPATH", clear
capture drop _merge

** *****************************************************************************************
** DATA PREPARATION                         
** *****************************************************************************************

// Generate survey ID
preserve
contract country surveyyear
gen survid = _n
tempfile survid
sort country surveyyear
save `survid'
restore

sort country surveyyear
merge m:1 country surveyyear using `survid'
drop _freq _merge

// Create weights and dummies for sex 
** SEX dummies 
gen male = 1 if sex == 1
replace male = 0 if sex != 1
gen female = 1 if sex == 0 
replace female = 0 if sex != 0 

// Age categories for deaths, survivors, and exploration of unknown sexes
gen death = 1 if yod ~=.                        // Generate indicator variable for deaths. 
replace death = 0 if yod ==.

replace yob = yob + 1900 if missing_sib != 1                        // Convert 2 digit years into 4 digits. 
replace yod = yod + 1900 if missing_sib != 1
gen aged = yod - yob
replace aged = . if aged < 0                    
gen agedcat = 0 if aged < 1                     // Generate categories of age at death. 
replace agedcat = 1 if aged >= 1 & aged < 5

forvalues a = 5(5)70 {
    local apn = `a' + 5
    replace agedcat = `a' if aged >= `a' & aged < `apn'
    }
    replace agedcat = 75 if aged>= 75

tab sex, miss                                   // Some values of sex are coded as 9 or are missing 
tab sex sibid, miss
tab sex death, miss r                           // Missing sexes are disproportionately deaths. 
tab sex agedcat, miss r
replace sex = . if sex!=0 & sex!=1              // Code all unknown sex the same. 

** Generating Age Blocks for survivors (age the sib was or would have been at the time of the survey):

gen age = yr_interview -  yob                   // Age at the time of the interview                 

gen ageblock = .                                // Generate age blocks:  5 year categories 15-19, 20-24, etc. 
forvalues age = 0(5)60   {
    replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
    }
replace ageblock = 45 if ageblock == 50 & id_sm==0 
                                                   

** *****************************************************************************************
** 1. For alive sibs, compute sex distribution by age and survey
** *****************************************************************************************

//  Redistribute siblings of unknown sex to males and females

//  For alive siblings of unknown sex, redistribute according to sex distribution of alive sibs within each age group, by survey
bysort survid ageblock: egen males = total(male) if death==0
sort survid ageblock males
by survid ageblock: replace males = males[1] if males==.

bysort survid ageblock: egen females = total(female)
sort survid ageblock females
by survid ageblock: replace females = females[1] if females==.

bysort survid ageblock: gen pctmale = males/(males+females)

** *****************************************************************************************
** 2. Randomly assigns sex to unknowns based on sex distribution of age group and survey
** *****************************************************************************************
set seed 123456789
gen rnd = runiform()

bysort survid ageblock: replace sex = 1 if sex==. & death==0 & rnd <= pctmale
bysort survid ageblock: replace sex = 0 if sex==. & death==0 & rnd > pctmale

drop males females rnd pctmale

** *****************************************************************************************
** 3. For dead siblings, computes sex distribution by age (pools surveys)
** *****************************************************************************************

// For dead sibs of unknown sex, redistribute according to sex distribution of dead sibs 
// within age group of death, pooling over all surveys (not enough deaths to do within survey)
bysort agedcat: egen males = total(male) if death==1
sort agedcat males
by agedcat: replace males = males[1] if males==.

bysort agedcat: egen females = total(female) if death==1
sort agedcat females
by agedcat: replace females = females[1] if females==.

bysort agedcat: gen pctmale = males/(males+females)

** *****************************************************************************************
** 4.
** *****************************************************************************************
set seed 123456789

gen rnd = runiform()
bysort survid: replace sex = 1 if sex==. & death==1 & rnd <= pctmale
bysort survid: replace sex = 0 if sex==. & death==1 & rnd > pctmale

drop males females rnd pctmale male female survid death aged agedcat age ageblock

drop if alive > 1

** *****************************************************************************************
** 5. Drop surveys to exclude from analysis
** *****************************************************************************************
compress

** *****************************************************************************************
** 6. Create a list of surveys analyzed along with the sample size (number of total siblings)
** *****************************************************************************************
preserve
contract iso3 surveyyear svy
replace surveyyear=surveyyear+1
outsheet using "FILEPATH", c replace 
restore

// Save output dataset
save "FILEPATH", replace 
