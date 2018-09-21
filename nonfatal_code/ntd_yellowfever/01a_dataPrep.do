

* PREP STATA *
  clear all 
  set more off, perm
  set maxvar 10000


* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 

  if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }
	 
  tempfile appendTemp mergingTemp skeleton pop
  
  adopath + FILEPATH
  run "FILEPATH/get_demographics.ado"
  run "FILEPATH/get_population.ado"
  run "FILEPATH/get_location_metadata.ado"

  
  
* CREATE A LOCAL CONTAINING THE ISO3 CODES OF COUNTRIES WITH YELLOW FEVER *
  local yfCountries AGO ARG BEN BOL BRA BFA BDI CMR CAF TCD COL COG CIV COD ECU GNQ ETH GAB GHA GIN GMB GNB ///
     GUY KEN LBR MLI MRT NER NGA PAN PRY PER RWA SEN SLE SDN SSD SUR TGO TTO UGA VEN ERI SOM STP TZA ZMB
	 

* STORE FILE PATHS IN LOCALS *	 
  local epiDir    FILEPATH
  local inputDir  FILEPATH

	 
/******************************************************************************\	
         IMPORT DATA AND LIMIT TO COUNTRIES WITH ENDEMIC YELLOW FEVER
\******************************************************************************/ 

* CREATE LIST OF ENDEMIC LOCATION_IDS *
  get_location_metadata, location_set_id(8) clear
  keep location_id ihme_loc_id
  
  generate endemic = 0
  foreach i of local yfCountries {
	quietly replace endemic = 1 if strmatch(ihme_loc_id, "`i'*")==1
	}
	
  levelsof location_id if endemic==1, local(endemicLids) sep( | location_id==)
	 
* IMPORT THE INCIDENCE DATASHEET *	  
  use `inputDir'/fullIncidenceData2016.dta, clear


  * DROP NON-ENDEMIC COUNTRIES & DUPLICATE DATA *
 
  keep if location_id == `endemicLids'
  duplicates drop nid location_id year_start year_end sex age_start age_end mean, force

  
  
/******************************************************************************\	

              RESOLVE PROBLEM OF LIMITED AGE-SPECIFIC DATA
  
\******************************************************************************/ 


* SAVE A COPY OF ALL-AGES DATA POINTS *  
  egen ageCat = concat(age_start age_end), punc(-)
  preserve
  
  keep if ageCat=="0-100"
  save `appendTemp', replace

* DROP ALL-AGES DATA POINTS *
  restore
  keep if ageCat!="0-100"


/* IDENTIFY AND KEEP COMPLETE DATA SOURCES:
  To offer complete data sources that report age-specific data must cover
  ages 0 through 100 with no gaps */
  
  bysort year_start nid age_start sex location_id: gen count = _N
  egen group = group(nid location_id year_start year_end sex)
  keep if count==1
  bysort group (age_start): gen ageGap = age_start[_n] - age_end[_n-1]
  bysort group: egen maxAge = max(age_end)
  bysort group: egen minAge = min(age_start)
  bysort group: egen maxGap = max(abs(ageGap))

  keep if minAge==0 & maxAge==100 & maxGap<=1
  

* WITH COMPLETE DATA ISOLATED, WE NOW COLLAPSE AGE-SPECIFIC TO ALL-AGE *  
  preserve
  bysort group: generate groupIndex = _n
  keep if groupIndex==1
  drop cases effective_sample_size upper lower nid ihme_loc_id year_start year_end mean standard_error age_start age_end
  save `mergingTemp', replace

  restore
  replace cases = mean*effective_sample_size if missing(cases)
  replace effective_sample_size = sample_size if missing(effective_sample_size)
  collapse (sum) cases effective_sample_size, by(group nid location_id year_start year_end)

  merge m:1 group using `mergingTemp', nogenerate
  save `mergingTemp', replace

* COMBINE THESE NEWLY COLLAPSED ALL-AGE OBSERVATIONS WITH THE ORIGINAL ALL-AGE DATA *  
  use `appendTemp', clear
  append using `mergingTemp'
  
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
  


   
   
/******************************************************************************\	
         CLEAN UP VARIABLES, & CALCULATE UNCERTAINTY WHERE MISSING
\******************************************************************************/  
 

  keep nid location_name location_id sex sex_id year_start year_end mean lower upper standard_error effective_sample_size cases sample_size
  generate age_group_id = 22
  
  replace mean = cases / effective_sample_size if missing(mean)
  replace cases = mean * effective_sample_size if missing(cases)


* ESTIMATE STANDARD ERRORS & CIs *   
  * Use linear-interpolation approach where cases <= 5 
   replace standard_error = ((5 - cases) * (1/effective_sample_size) + cases * (sqrt( (5/effective_sample_size) / effective_sample_size ))) / 5  if cases<=5 & missing(standard_error)
   
  * Use Poisson SE where cases > 5   
   replace standard_error = sqrt(mean / effective_sample_size)  if cases>5 & missing(standard_error)
   
  *  Calculate CIs
   replace upper = mean + (invnormal(0.975) * standard_error) if missing(upper)
   replace lower = mean + (invnormal(0.025) * standard_error) if missing(lower)
   replace lower = 0 if lower < 0

   
   
/******************************************************************************\	
                                RESOLVE DUPLICATES
\******************************************************************************/ 

  drop if year_start!=year_end
  bysort location_id year_start year_end sex (mean): gen count = _N
  bysort location_id year_start year_end sex (mean): gen index = _n

  keep if count==index 
  save `appendTemp', replace   
   

   
/******************************************************************************\	
 CREATE A SKELETON DATASET CONTAINING EVERY COMBINATION OF ISO, AGE, SEX & YEAR
\******************************************************************************/ 


* PULL POPULATION DATA *	
  get_demographics, gbd_team(cod) clear
  get_population, location_id(-1) age_group_id(-1) year_id(`r(year_ids)') sex_id(1 2 3) clear
  save `pop'

  get_location_metadata, location_set_id(8) clear
  keep location_id location_name parent_id location_type ihme_loc_id *region* is_estimate
  merge 1:m location_id using `pop', nogenerate
  
 
  keep if is_estimate==1 | location_type=="admin0"
  generate countryIso = substr(ihme_loc_id, 1, 3)
  
  save `inputDir'/skeleton.dta, replace
  
  

* WITH THE FULL SKELETON DATASET SAVED, LIMIT NOW TO BE YEAR-ISO SPECIFIC *
  keep if sex_id==3 & age_group_id==22
  rename year_id year_start
 
* CREATE YELLOW FEVER ENDEMICITY VARIABLE *
  generate yfCountry = location_id==`endemicLids'
 

 
/******************************************************************************\	
      MERGE THE YEAR-ISO SPECIFIC SKELETON TO THE YELLOW FEVER DATASET
	  
  This ensures that we now have an observation for every year and country, not
  just an observation for years and countries with data.
\******************************************************************************/ 
  
  merge 1:m location_id year_start using `appendTemp',  nogenerate

  keep if yfCountry == 1
 
 
  replace cases = . if cases == 0
  replace cases = mean * effective_sample_size if missing(cases)

  gen yearC = year_start - ((2015 - 1990)/2)
  
  order location* ihme_loc_id *region* year* age* sex* mean lower upper standard_error effective_sample_size cases population nid
  
  drop count index

  save `inputDir'/dataToModel2016.dta, replace
