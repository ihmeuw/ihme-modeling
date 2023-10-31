
/******************************************************************************\	
	
    STEP 4 YELLOW FEVER MODELLING:
	
	This script fits the negative binomial regression model : 
		outcome: Reported YF incidence (all-age, both sex)
		covariates: year (adjusted) and SDI
		random effects (intercept): CountryIso
		
	Training dataset is flagged by training_set=1
	
	Inputs to this script
	1) age distribution 
	2) Case fatality parameters
	3) expansion factors
	4) dataset (all age shell+training set)
	5) skeleton by age 
	
	
	Outputs from this script: 
	1)stored fixed and random effects for each location/year
	2) reported cases (where not missing)
	3) expansion factor adjustment draws
	4) proportion of cases by age
	
	
	
				   
\******************************************************************************/


* PREP STATA *
  clear all 
  set more off, perm
  set maxvar 10000


* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 

  if c(os) == ADDRESS {
    local FILEPATH
	local user : env USER
	local FILEPATh
    set odbcmgr unixodbc
    }
  else if c(os) == ADDRESS {
    local FILEPATH
	local FILEPATH
    }
	 
  tempfile crossTemp appendTemp mergingTemp envTemp drawMaster sevInc zero
  
  local date = subinstr(trim("`: display %td_CCYY_NN_DD date(c(current_date), "DMY")'"), " ", "_", .)
   
   *load central functions
  adopath + FILEPATH
  run FILEPATH/get_demographics.ado

   
* STORE FILE PATHS IN LOCALS *	
  local inputDir FILEPATH
  local newinput  FILEPATH

  *FOR GBD 2020 UPDATED AGE PATTERN TO ACCOUNT FOR NEW AGE GROUPS 
 local ageDist  `newinput'/ageStep2_20.dta
 local cf       `newinput'/caseFatalityAB.dta
 local ef       `newinput'/expansionFactors.dta

 local data     `newinput'/data2Model_20.dta
 
  local outDir FILEPATH=subinstr(trim("`c(current_date)'"), " ", "_", .)'_`=subinstr("`c(current_time)'", ":", "_", .)'
  cap mkdir `outDir'
  cap mkdir "`outDir'/temp"
  
  foreach subDir in temp progress deaths total _asymp inf_mod inf_sev {
	!rm -rf `outDir'/`subDir'
	sleep 2000
	cap mkdir `outDir'/`subDir'
	}

  ***MODEL CODE TO ESTIMATE BETAS AND RANDOM EFFECTS FROM TRAINING SET =1

  *input data:  training set + all-age shell 
  use "FILEPATH",clear
  
	*code here changes denominator for subnational locations to match total national location-this is done because we only execute a national-level analysis
	*then split cases to subnational by proportion later 
  replace population = sub_nat_pop if subnat==1
  replace cases=cases_sub if subnat==1
  
   *need to drop the brazil 2017 and 2018 case data from fitting the model-this is skewing the random effects overall
	drop if location_id==135 & year_id>2016 & training_set==1
  
  *covs are yearC and sdi and only if cases > 0, with a fixed effect by countryIso (so for both national and subnats) 
  menbreg cases yearC sdi if cases>0 & training_set==1, exp(population) || countryIso2: 
    predict predFixed, fixedonly fitted nooffset
    predict predFixedSe, stdp nooffset
    predict predRandom, remeans reses(predRandomSe) nooffset

	bysort  countryIso2: egen countryRandom = mean(predRandom)
    replace predRandom = countryRandom if missing(predRandom)
	
	bysort  region_id: egen regionRandom = mean(predRandom)
    replace predRandom = regionRandom if missing(predRandom)
	
	bysort  super_region_id: egen superRegionRandom = mean(predRandom)
    replace predRandom = superRegionRandom if missing(predRandom)
	
	bysort  region_id: egen regionRandomSe = mean(predRandomSe)
    replace predRandomSe = regionRandomSe if missing(predRandomSe)
	
	bysort  super_region_id: egen superRegionRandomSe = mean(predRandomSe)
    replace predRandomSe = superRegionRandomSe if missing(predRandomSe)
*output dataset of training data 

save "FILEPATH" , replace
 
*create a variable called "mean" = total incidence of reported cases 
  generate mean=cases/population
 
  *dropping national level data for which we have subnat level data
  drop if location_id==135
  drop if location_id==214
  drop if location_id==179
  drop if location_id==180
 
*drop training set rows of data
 drop if training_set==1

   
  * ESTIMATE STANDARD ERRORS & CIs (N.B. These SE estimation methods are from the central uploader and used to ensure consistency with other causes) *   
  * Use linear-interpolation approach where cases <= 5 
  * This is used later in the model to propogate uncertainty
   generate standard_error = ((5 - cases) * (1/population) + cases * (sqrt( (5/population) / population ))) / 5  if cases<=5 
   
  * Use Poisson SE where cases > 5   
   replace standard_error = sqrt(mean / population)  if cases>5 
   
  *  Calculate CIs
   generate upper = mean + (invnormal(0.975) * standard_error) 
   generate lower = mean + (invnormal(0.025) * standard_error) 
   replace lower = 0 if lower < 0
  
  *rename population variable to indicate it is population of all ages (age_group=22)	
	rename population allAgePop
  
   *expand dataset into both sex
   
   expand 2, generate(sex_id)
   replace sex_id=2 if sex_id==0
   
   *expand to new age categories 
   
expand 25, generate(age_group_id)
bysort location_id year_id sex_id: generate counter= _n

*new age groups 
	replace age_group_id=counter
	replace age_group_id=30 if counter==1
	replace age_group_id=31 if counter==21
	replace age_group_id=32 if counter==22
	replace age_group_id=235 if counter==23
	replace age_group_id=388 if counter==4
	replace age_group_id=389 if counter==5
	replace age_group_id=34 if counter==24
	replace age_group_id=238 if counter==25

	
drop counter 
 

/******************************************************************************\	
   BRING IN THE DATA ON YELLOW FEVER AGE-SEX DISTRIBUTION, EF, & CASE FATALITY
\******************************************************************************/ 
drop countryIso
rename countryIso2 countryIso

*Merge age pattern for YF incidence
	merge m:1 sex_id age_group_id using `ageDist', nogenerate
	
*Merge location-specific Expansion factors
	merge m:1 countryIso using `ef', nogenerate
	
*merge age specific populations to the dataset
	merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", assert(2 3) keep(3) nogenerate

	rename population ageSexPop

	keep ef_* year_id age_group_id  sex_id location_id countryIso ageSexCurve pred* ageSexPop allAgePop mean cases standard_error countryIso

*apply age pattern to all cases to get the proportion of cases that should be assigned to each age group
	gen ageSexCurveCases = ageSexCurve * ageSexPop
	bysort location_id year_id: egen totalCurveCases = total(ageSexCurveCases)
	gen prAgeSex = ageSexCurveCases / totalCurveCases

*Merge the subnational split- this file includes brazil, ET, KEN and NGA
	merge m:1 location_id year_id using "FILEPATH", assert(1 3) nogenerate
		
	replace allAgePop = mean_pop_bra if !missing(mean_pop_bra)
	forvalues i = 0/999 {
		quietly replace braSubPr_`i' = 1 if missing(braSubPr_`i')
		}
	

*create index for YF endemic countries to faciliate outputting draws
generate yfCountry=1	
  
	preserve
	use `cf', clear
	local deathsAlpha = alphaCf in 1
	local deathsBeta = betaCf in 1
	
	get_demographics, gbd_team(ADDRESS) gbd_round_id(7) clear
	local locations `r(location_id)'
	restore

	levelsof location_id if yfCountry==1, local(yfCntryIds) clean
	
	
	*store temp file for reference
	save "FILEPATH", replace
	
	
	
	*************************************************************************
 
*** CREATE LOCATION-SPECIFIC INCIDENCE ESTIMATE FILES & SUBMIT PARALLEL PROCESS JOBS *** 	 

	
	
	foreach location of local locations {
		if `: list location in yfCntryIds' == 0 {
			local endemic = 0
			}
			
		else {
			local endemic = 1 
			quietly { 
				preserve
				keep if location_id==`location'
				save `outDir'/temp/`location'.dta, replace
				restore
				drop if location_id==`location'
				}
			}
			   * Launch parallel task script:
			   "FILEPATH/step_6_02c_processModel.do"
		}
