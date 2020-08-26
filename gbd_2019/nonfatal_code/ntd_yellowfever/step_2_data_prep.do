
/******************************************************************************\  

    YELLOW FEVER MODELLING:

  Background: The few cases and episodic nature of yellow fever makes it
        poorly suited to modelling in DisMod.  And the limited and 
        unreliable death reports make custom natural history model
        necessary.

  Purpose:    This script pulls together and manages the yellow fever data,
        and estimates incidence and mortality

\******************************************************************************/

* PREP STATA *
  clear all 
  set more off, perm
  set maxvar 10000


* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 

  if c(os) == "Unix" {
      local "ADDRESS"
      local "ADDRESS"
      set odbcmgr ADDRESS
  }
  else if c(os) == "Windows" {
      local "FILEPATH"
      local  "FILEPATH"
  }

  tempfile appendTemp mergingTemp skeleton pop

  run "FILEPATH"
  run "FILEPATH"
  run "FILEPATH"
  run "FILEPATH"
  run "FILEPATH"
  run "FILEPATH"
  run "FILEPATH"

  * CREATE A LOCAL CONTAINING THE ISO3 CODES OF COUNTRIES WITH YELLOW FEVER *
  local yfCountries AGO ARG BEN BOL BRA BFA BDI CMR CAF TCD COL COG CIV COD ECU GNQ ETH GAB GHA GIN GMB GNB GUY KEN LBR MLI MRT NER NGA PAN PRY PER RWA SEN SLE SDN SSD SUR TGO TTO UGA VEN ERI SOM STP TZA ZMB

* STORE FILE PATHS IN LOCALS *
  local epiDir    FILEPATH
  local inputDir  FILEPATH
  local newinput   FILEPATH

  local bundle_id ADDRESS
  local decomp_step iterative
  local bundle_version_id ADDRESS


******************************************************************************
*         IMPORT DATA AND LIMIT TO COUNTRIES WITH ENDEMIC YELLOW FEVER
******************************************************************************

* CREATE LIST OF ENDEMIC LOCATION_IDS *
  generate endemic = 0
  foreach i of local yfCountries {
      quietly replace endemic = 1 if strmatch(ihme_loc_id, "`i'*")==1
  }

  levelsof location_id if endemic==1, local(endemicLids) sep( | location_id==)

  * IMPORT THE INCIDENCE DATASHEET *    
  *get_bundle_data, bundle_id(`bundle_id') decomp_step(`decomp_step') clear
  get_bundle_version, bundle_version_id(`bundle_version_id') clear

  gen crosswalk_parent_seq = seq


  * DROP NON-ENDEMIC COUNTRIES & DUPLICATE DATA *
  keep if location_id == `endemicLids'

  *remove any true duplicates (by nid)
  duplicates drop nid location_id year_start year_end sex age_start age_end cases, force

  *making sure that there is only one source per demographic cohort (nid not taken into account)
  duplicates drop location_id year_start year_end age_start age_end sex cases, force

  tempfile master
  save `master'

  get_location_metadata, location_set_id(35) clear
  keep location_id ihme_loc_id

  merge 1:m location_id using `master', assert(1 3) keep(3) nogenerate



/******************************************************************************\  

              RESOLVE PROBLEM OF LIMITED AGE-SPECIFIC DATA:

  The vast majority of the data are all age data (0-100), & there is, therefore,
  inadequate age-specific data to inform the age pattern in this dataset.  To 
  resolve this problem I will determine the age-sex distribution of cases
  based on Brazilian hopital data.  With that, all data here need to be for 
  all ages to simplifiy modelling and ensure consistency.

  This next block of code finds age-specific data, determines if data exist 
  for all age groups and collapses to all age numbers

\******************************************************************************/ 

* SAVE A COPY OF ALL-AGES DATA POINTS *  
  egen ageCat = concat(age_start age_end), punc(-)
  preserve

  keep if ageCat=="0-99"
  save `appendTemp', replace


* DROP ALL-AGES DATA POINTS *
  restore
  keep if ageCat!="0-99"

/* IDENTIFY AND KEEP COMPLETE DATA SOURCES:
  To offer complete data sources that report age-specific data must cover
  ages 0 through 99 with no gaps (1 year gap okay due to demographic notation) */

  bysort year_start nid age_start sex location_id: gen count = _N
  egen grp = group(nid location_id year_start year_end sex)
  keep if count==1
  bysort grp (age_start): gen ageGap = age_start[_n] - age_end[_n-1]
  bysort grp: egen maxAge = max(age_end)
  bysort grp: egen minAge = min(age_start)
  bysort grp: egen maxGap = max(abs(ageGap))

  keep if minAge==0 & maxAge==99 & maxGap<=1


* WITH COMPLETE DATA ISOLATED, COLLAPSE AGE-SPECIFIC TO ALL-AGE *  
  preserve
  bysort grp: generate groupIndex = _n
  keep if groupIndex==1
  drop cases effective_sample_size upper lower nid ihme_loc_id year_start year_end mean standard_error age_start age_end
  save `mergingTemp', replace

  restore
  replace cases = mean*effective_sample_size if missing(cases)
  replace effective_sample_size = sample_size if missing(effective_sample_size)
  collapse (sum) cases effective_sample_size, by(grp nid location_id year_start year_end)

  merge m:1 grp using `mergingTemp', nogenerate
  save `mergingTemp', replace


* COMBINE THESE NEWLY COLLAPSED ALL-AGE OBSERVATIONS WITH THE ORIGINAL ALL-AGE DATA *  
  use `appendTemp', clear
  append using `mergingTemp'

  replace age_start=0 if age_start==.
  replace age_end=99 if age_end==.

  drop ihme_loc_id

  tempfile master_1
  save `master_1'

  get_location_metadata, location_set_id(35) clear
  keep location_id ihme_loc_id

  merge 1:m location_id using `master_1', assert(1 3) keep(3) nogenerate


* COLLAPSE SEX-SPECIFIC DATA TO BOTH SEXES *
  generate sex_id = (sex=="Male") + (2*(sex=="Female")) + (3*(sex=="Both"))

  forvalues i = 1/2 {
      bysort nid location_id year_start year_end: egen maxSex = max(sex_id)
      bysort nid location_id year_start year_end: egen minSex = min(sex_id)

      if `i'==1 {
          drop if maxSex==3 & sex_id<3
          drop maxSex minSex
      }
  }

  assert minSex==maxSex | minSex==1 & maxSex==2

  preserve

  keep if maxSex<3
  bysort nid location_id year_start year_end (sex): replace cases = sum(cases)
  bysort nid location_id year_start year_end (sex): replace effective_sample_size = sum(effective_sample_size)

  foreach var of varlist mean lower upper sample_size standard_error {
      replace `var' = .
  }

  keep if sex_id==2
  replace sex_id = 3
  replace sex = "Both"
  save `appendTemp', replace

  restore 
  drop if maxSex<3
  append using `appendTemp'

replace site_memo="" if site_memo=="."
replace sampling_type="" if sampling_type=="."
replace case_name="" if case_name=="."
replace case_definition="" if case_definition=="."
replace case_diagnostics="" if case_diagnostics=="."
replace note_modeler="" if note_modeler=="."
replace note_SR="" if note_SR=="."
replace value_confirmed_or_suspected="" if value_confirmed_or_suspected=="."
replace underlying_field_citation_value="" if underlying_field_citation_value=="."
replace clinical_data_type="" if clinical_data_type=="."

replace sample_size=effective_sample_size if missing(sample_size)



/******************************************************************************\  
         CLEAN UP VARIABLES, & CALCULATE UNCERTAINTY WHERE MISSING
\******************************************************************************/  

  replace mean = cases / effective_sample_size if missing(mean)
  replace cases = mean * effective_sample_size if missing(cases)

* ESTIMATE STANDARD ERRORS & CIs  *
  * Use linear-interpolation approach where cases <= 5 
  replace standard_error = ((5 - cases) * (1/effective_sample_size) + cases * (sqrt( (5/effective_sample_size) / effective_sample_size ))) / 5  if cases<=5 & missing(standard_error)

* Use Poisson SE where cases > 5 
  replace standard_error = sqrt(mean / effective_sample_size)  if cases>5 & missing(standard_error)

*  Calculate CIs
  replace upper = mean + (invnormal(0.975) * standard_error) if missing(upper)
  replace lower = mean + (invnormal(0.025) * standard_error) if missing(lower)
  replace lower = 0 if lower < 0

export excel using `newinput'/FILEPATH, firstrow(variables) sheet(extraction) replace

keep nid location_name location_id sex sex_id year_start year_end mean lower upper standard_error effective_sample_size cases sample_size
generate age_group_id = 22



/******************************************************************************\  
                                RESOLVE DUPLICATES
\******************************************************************************/ 

  drop if year_start!=year_end
  bysort location_id year_start year_end sex (cases): gen count = _N

  bysort location_id year_start year_end sex (cases): gen index = _n

  keep if count==index 
  save `appendTemp', replace   

  expand 2 if year_start==2017, gen(new)
  replace year_start=2018 if new==1
  replace year_end=. if new==1
  replace nid=. if new==1
  replace sex="" if new==1
  replace mean=. if new==1
  replace lower=. if new==1
  replace upper=. if new==1
  replace standard_error=. if new==1
  replace cases=. if new==1
  replace sample_size=. if new==1
  replace effective_sample_size=. if new==1
  replace count=. if new==1
  replace index=. if new==1
  drop new


  expand 2 if year_start==2018, gen(new)
  replace year_start=2019 if new==1
  replace year_end=. if new==1
  replace nid=. if new==1
  replace sex="" if new==1
  replace mean=. if new==1
  replace lower=. if new==1
  replace upper=. if new==1
  replace standard_error=. if new==1
  replace cases=. if new==1
  replace sample_size=. if new==1
  replace effective_sample_size=. if new==1
  replace count=. if new==1
  replace index=. if new==1
  drop new


  *these have data till 2015 
  expand 2 if inlist(location_id,125,122,121,118) & year_start==2015, gen(new)
  replace year_start=2018 if new==1
  replace year_end=. if new==1
  replace nid=. if new==1
  replace sex="" if new==1
  replace mean=. if new==1
  replace lower=. if new==1
  replace upper=. if new==1
  replace standard_error=. if new==1
  replace cases=. if new==1
  replace sample_size=. if new==1
  replace effective_sample_size=. if new==1
  replace count=. if new==1
  replace index=. if new==1
  drop new


  expand 2 if inlist(location_id,125,122,121,118) & year_start==2018, gen(new)
  replace year_start=2019 if new==1
  replace year_end=. if new==1
  replace nid=. if new==1
  replace sex="" if new==1
  replace mean=. if new==1
  replace lower=. if new==1
  replace upper=. if new==1
  replace standard_error=. if new==1
  replace cases=. if new==1
  replace sample_size=. if new==1
  replace effective_sample_size=. if new==1
  replace count=. if new==1
  replace index=. if new==1
  drop new

  tempfile appendTemp_2
  save `appendTemp_2', replace   



/******************************************************************************\  
 CREATE A SKELETON DATASET CONTAINING EVERY COMBINATION OF ISO, AGE, SEX & YEAR
\******************************************************************************/ 

  numlist "1980(1)2019"
  get_demographics, gbd_team(cod) gbd_round_id(6) clear
  get_population, location_id(-1) age_group_id(-1) year_id(`r(year_ids)') sex_id(1 2 3) decomp_step(step2) clear
  get_population, location_id(-1) age_group_id(-1) year_id(-1) sex_id(1 2 3) decomp_step(step2) clear

  get_population, location_id("all") age_group_id("all") year_id("all") sex_id("all") decomp_step("step2") clear

  get_population, location_id(-1) age_group_id(-1) year_id(`r(year_id)') sex_id(1 2 3) decomp_step(step2) clear

  get_population, location_id(-1) age_group_id(-1) year_id(-1) sex_id(-1) decomp_step(step2) clear

  get_population, location_id(-1) age_group_id(-1) year_id(`r(numlist)') sex_id(1 2 3) decomp_step(step2) clear

  local years 1980 1981 1982 1983 1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019   

  get_population, location_id(-1) age_group_id(-1) year_id(`years') sex_id(1 2 3) decomp_step("step2") clear

  save `pop'

  get_location_metadata, location_set_id(8) gbd clear
  keep location_id location_name parent_id location_type ihme_loc_id *region* is_estimate
  merge 1:m location_id using `pop', nogenerate

  keep if is_estimate==1 | location_type=="admin0"
  generate countryIso = substr(ihme_loc_id, 1, 3)


  save `inputDir'/FILEPATH, replace
  */

  use `newinput'/FILEPATH 


* WITH THE FULL SKELETON DATASET SAVED, LIMIT TO BE YEAR-ISO SPECIFIC *
  keep if sex_id==3 & age_group_id==22
  rename year_id year_start


* CREATE YELLOW FEVER ENDEMICITY VARIABLE *
  generate yfCountry = location_id==`endemicLids'



/******************************************************************************\  
      MERGE THE YEAR-ISO SPECIFIC SKELETON TO THE YELLOW FEVER DATASET

\******************************************************************************/ 

  merge 1:m location_id year_start using `appendTemp_2',  nogenerate
  keep if yfCountry == 1

* CLEAN UP A FEW VARIABLES * 
  replace cases = . if cases == 0
  replace cases = mean * effective_sample_size if missing(cases)
  gen yearC = year_start - ((2019 - 1990)/2)
  order location* ihme_loc_id *region* year* age* sex* mean lower upper standard_error effective_sample_size cases population nid
  drop count index

replace cases=. if countryIso=="BRA" & location_id!=135
replace mean=. if countryIso=="BRA" & location_id!=135
replace lower=. if countryIso=="BRA" & location_id!=135
replace upper=. if countryIso=="BRA" & location_id!=135
replace standard_error=. if countryIso=="BRA" & location_id!=135
replace effective_sample_size=. if countryIso=="BRA" & location_id!=135


*removing the outbreak in 2017 and running model without it 
replace cases=. if location_id==135 & cases==777
replace mean=. if location_id==135 & year_start==2017
replace lower=. if location_id==135 & year_start==2017
replace upper=. if location_id==135 & year_start==2017
replace standard_error=. if location_id==135 & year_start==2017
replace effective_sample_size=. if location_id==135 & year_start==2017

*/

save `newinput'/FILEPATH, replace
