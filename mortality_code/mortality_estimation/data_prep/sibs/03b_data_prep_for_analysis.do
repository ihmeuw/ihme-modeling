** ********************************************************************************************************************************************************************
** 	Adult Mortality through Sibling Histories: #3b. Redistributing sibs with unknown sex to males and females
**
**	Description: This do-file assigns sexes to siblings with a missing sex variable and drops siblings with a missing alive status variable. It drops surveys that 
**					should be excluded from the analysis and creates a file that displays all surveys analyzed with the sample size of siblings.
**		Input: Output from step 3 (allcountrysurveys_sexunk.dta)	
**		Steps: 
**				1. For alive sibs, compute sex distribution by age and survey
**				2. Randomly assigns sex to unknowns based on sex distribution of age group and survey
**				3. For dead sibs, computes sex distribution by age (pools surveys)
**				4. Randomly assigns sex to unknowns based on sex distribution by age (across surveys) 
**				5. Drop surveys with issues
**				6. Create a list of surveys analyzed along with the sample size (number of total siblings)
**
**		Output: A list of all surveys analyzed with sample size of siblings and one large dataset with all of the countries' sib modules, with no missing sex and alive status variables
**		Files: sibhistlist.csv, allcountrysurveys.dta
**		Variables in output dataset:		
**			iso3 country (full country name) yr_interview (Year of Interview) surveyyear (last full year for analysis) samplesize (Survey Sample Size) id_sm (sibship ID)
**			sibid (sibling ID) v005 (Sampling Weight) v008 (cmc date of interview) psu sex (0=Female 1=Male) yod (Year of death) yob (Year of birth) alive (Alive status) 
**
**	NOTE: IHME OWNS THE COPYRIGHT		 	
**
** ********************************************************************************************************************************************************************

** ***************************************************************************************
** SET UP STATA							
** ***************************************************************************************

clear
capture clear matrix
set more off

use "$datadir/allcountrysurveys_sexunk.dta", clear
capture drop _merge
                                   
** ***************************************************************************************
** DATA PREPARATION 						
** ***************************************************************************************

// Generate survey ID
preserve
contract country surveyyear
gen survid = _n
tempfile survid
sort country surveyyear
save `survid'
restore

sort country surveyyear
merge country surveyyear using `survid'
drop _freq _merge

// Create weights and dummies for sex 
** SEX dummies 
gen male = 1 if sex == 1
replace male = 0 if sex != 1
gen female = 1 if sex == 0 
replace female = 0 if sex != 0 

// Age categories for deaths, survivors, and exploration of unknown sexes
gen death = 1 if yod ~=.						// Generate indicator variable for deaths. 
replace death = 0 if yod ==.

gen aged = yod - yob
replace aged = . if aged < 0					// Some ages at death seem to be negative.

gen agedcat = 0 if aged < 1						// Generate categories of age at death. 
replace agedcat = 1 if aged >= 1 & aged < 5

forvalues a = 5(5)70 {
	local apn = `a' + 5
	replace agedcat = `a' if aged >= `a' & aged < `apn'
	}
	replace agedcat = 75 if aged>= 75

tab sex, miss									// Some values of sex are coded as 9 or are missing 
tab sex sibid, miss								// Who are unknowns? Sibs or respondents? 
tab sex death, miss r							// Missing sexes are disproportionately deaths. 
tab sex agedcat, miss r							// More likely to be child deaths but still majority are deaths over age 15. 
replace sex = . if sex!=0 & sex!=1				// Code all unknown sex the same. 

** Generating Age Blocks for survivors (age the sib was or would have been at the time of the survey):

gen age = yr_interview -  yob					// Age at the time of the interview 				

gen ageblock = . 								// Generate age blocks:  5 year categories 15-19, 20-24, etc. 
forvalues age = 0(5)60   {
	replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
	}
replace ageblock = 45 if ageblock == 50 & id_sm==0	// Technically, the respondent is aged 15-49. However, because the specific month calculation cannot be done in our analysis, there are a couple of ages that fall in the 50-54 category. 
													// Since these are very few (anywhere from 10-100), especially in each country survey, these are included in the 45-49 age category 

** ***************************************************************************************
** 1. For alive sibs, compute sex distribution by age and survey
** ***************************************************************************************

//  Redistribute siblings of unknown sex to males and females

//  For alive siblings of unknown sex, redistribute according to sex distribution of alive sibs within each age group, by survey
bysort survid ageblock: egen males = total(male) if death==0
sort survid ageblock males
by survid ageblock: replace males = males[1] if males==.

bysort survid ageblock: egen females = total(female)
sort survid ageblock females
by survid ageblock: replace females = females[1] if females==.

bysort survid ageblock: gen pctmale = males/(males+females)


** ***************************************************************************************
** 2. Randomly assigns sex to unknowns based on sex distribution of age group and survey
** ***************************************************************************************

gen rnd = runiform()

bysort survid ageblock: replace sex = 1 if sex==. & death==0 & rnd <= pctmale
bysort survid ageblock: replace sex = 0 if sex==. & death==0 & rnd > pctmale

drop males females rnd pctmale


** ***************************************************************************************
** 3. For dead siblings, computes sex distribution by age (pools surveys)
** ***************************************************************************************

// For dead sibs of unknown sex, redistribute according to sex distribution of dead sibs 
// within age group of death, pooling over all surveys (generally not enough deaths to do within survey-year)
bysort agedcat: egen males = total(male) if death==1
sort agedcat males
by agedcat: replace males = males[1] if males==.

bysort agedcat: egen females = total(female) if death==1
sort agedcat females
by agedcat: replace females = females[1] if females==.

bysort agedcat: gen pctmale = males/(males+females)

** ***************************************************************************************
** 4. Randomly assigns sex to unknowns based on sex distribution by age (across surveys) 
** ***************************************************************************************

gen rnd = runiform()

bysort survid: replace sex = 1 if sex==. & death==1 & rnd <= pctmale
bysort survid: replace sex = 0 if sex==. & death==1 & rnd > pctmale

drop males females rnd pctmale male female survid death aged agedcat age ageblock

// Drop siblings for whom alive/dead status is unknown
drop if alive > 1


** ***************************************************************************************
** 5. Drop surveys to exclude from analysis
** ***************************************************************************************

	// drop surveys to exclude here

** ***************************************************************************************
** 6. Create a list of surveys analyzed along with the sample size (number of total siblings)
** ***************************************************************************************

compress
preserve
contract iso3 surveyyear svy
replace surveyyear=surveyyear+1
export delimited using "$datadir/sibhistlist.csv", replace
restore

// Save output dataset
save "$datadir/allcountrysurveys.dta", replace

