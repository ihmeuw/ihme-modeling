/******************************************************************************\
Purpose: 
  This script creates a dataset to be used to build custom CoD models

  
Details: 
  1) Create master skeleton dataset: the script first builds a master skeleton 
     dataset containing one row for every combination of location, year, age, 
	 and sex. 
	 
  2) Create model results datase: the script then cycles through the specified
     get_model_results options. It cycles through each  get_model_results
	 command, loads the model results, preps that data for merging. For each
	 get_model_results command, the data are loaded, prepped and merged into  
	 the master skeleton.  
	 
  3) Create covariate dataset: the script then cycles through the specified
     list of covariate_ids.  It first pulls the meta-data for each covariate
	 (i.e. covariate name, age/sex specificity). It then cycles through each
	 covariate_id, loads the covariate data, preps that data for merging 
	 (note, we pulled age/sex specificity to determine how to appropriately
	 prep the file for the merge and determine the correct merge command). 
	 For each covariate, the data are loaded, prepped and merged into the 
	 master skeleton.  
	 
  4) Bring in mortality envelope and population estimates.
  
  5) Create all-age versions of all age-specific model results and 
     covariates (all-age are often more useful than age-specific 
	 (e.g. if there's a lag between onset of disease and cause-specific 
	 mortality, then the age patterns will be dramatically different for
	 prevalence and mortality.  Here, we don't want prevalence to inform the
	 age patterns for mortality, rather we only want prevalence estimates 
	 to inform the spatial/temporal patterns).
  
  4) Bring in CoD data: the script then loads the cause-specific data from 
     the CoD database and merges these data into the master
	 
  5) Clean up: finally, the script cleans things up by ordering and sorting 
     data, and saving the dataset.
	 
	 

Syntax:
  build_cod_dataset <i>cause_id</i> [, covariate_ids(<i>covariate_ids</i> saveto(<i>filepath</i>)]
   
\******************************************************************************/ 


capture program drop build_cod_dataset
program define build_cod_dataset
syntax  anything(name=cause_id id=cause_id), [covariate_ids(string) get_model_results(string) saveto(string) min_age(integer 2) outliers clear]


quietly {

/******************************************************************************\
                            SETUP THE ENVIRONMENT                             
\******************************************************************************/

*** CHECK IF CLEAR WOULD ERASE DATA ***
    describe
    if "`clear'" == "" & (`r(N)' > 0 | `r(width)' > 0) {
      noisily di as error "If data is in memory you must specify the option clear"
      exit
	  }
		
    clear
	
*** BOILERPLATE ***
	if c(os) == "Unix" {
	  local j FILEPATH
	  local h /FILEPATH
	  }
	else {
	  local j FILEPATH
	  local h FILEPATH
	  }

	local codeDir "FILEPATH/Other_Intestinal"
	local tempDir "/FILEPATH/other_intestinal"

	adopath + /FILEPATH/current 
	adopath + /FILEPATH/stata
	
	run /FILEPATH/get_demographics.ado
	run /FILEPATH/get_location_metadata.ado
	run /FILEPATH/get_cod_data.ado
	run /FILEPATH/get_population.ado
	run /FILEPATH/get_envelope.ado
	//run FILEPATH/resolver.ado
	run "`codeDir'/resolver.ado"
	
*** CREATE TEMPORARY VARIABLES ***
	tempfile cod allAgeCod codLocations estimates locations age master resolverMaster year covars env allAgeMaster
	tempfile age_groupListTemp locationListTemp yearListTemp sexListTemp
	  

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"

	run /FILEPATH/create_connection_string.ado
	
	create_connection_string, database(shared)
	local shared = r(conn_string)
	
	sleep 5000
	
	create_connection_string, server(modeling-epi-db) database(epi)
	local epi = r(conn_string)

	
	
*** PULL GBD DEMOGRAPHIC & ROUND INFO ***
	get_demographics, gbd_team("cod") release_id(16) clear
	local ages `r(age_group_id)'
	local ageList = subinstr("`ages'", " ", ",",.)
	local years `r(year_id)'
	local yearList = subinstr("`years'", " ", ",",.)
	
	*odbc load, exec("SELECT release_id FROM release WHERE release = `=max(`yearList')'") `shared' clear
	local release = 16	

	
	
/******************************************************************************\
                               BRING IN COD DATA                               
\******************************************************************************/ 	  
	
	noisily di _n "Getting the CoD data (this will take a few minutes)"

	
	
	use "/FILEPATH/cod_data_trimmed1.dta", clear
	
	if "`outliers'"=="outliers" {
		drop data_id
		save `cod'
		get_cod_data, cause_id(`cause_id') is_outlier(1) release_id(16) refresh_id(70) clear
		append using `cod'
		}

	drop cause_id upload_date data_id
	egen data_group = group(cod_source_label nid data_type_name location_id year_id sex_id site_id is_representative), missing
	
	save `FILEPATH/cause321_codData_gbd23.dta, replace

	keep if !inlist(age_group_id, 22, 27)
	
	
	
	*** ESTIMATE ALL-AGE VALUES ***
	levelsof age_group_id, local(dataAges) clean 
	local nDataAges = wordcount("`dataAges'")
	local nExpAges  = wordcount(substr("`ages'", `=strpos("`ages'", "`min_age' ")', .))
	
	bysort data_group: gen data_group_count = _N
	
	generate aaComplete = inrange(data_group_count, `nDataAges', `nExpAges' ) | inrange(data_group_count, `nExpAges', `nDataAges' ) 
	
	fastcollapse deaths, by(location_id year_id sex_id data_group aaComplete) type(sum) append
	replace age_group_id = 22 if missing(age_group_id)
	
	save `cod', replace
	use `cod', clear
	save "/FILEPATH/cod_line194.dta", replace
	
/******************************************************************************\
                         CREATE MASTER SKELETON DATASET                         
\******************************************************************************/ 

	noisily di _n "Creating a master skeleton dataset"
	
	
  
*** CREATE DATASET WITH EVERY COMBINATION OF YEAR, AGE, & SEX ***
	clear
	set obs `=max(`yearList') - 1979'
	generate year_id = _n + min(`yearList') - 1

	generate age_group_id = 2

	foreach age_id in `ages' 22 {
		expand 2 if age_group_id==2, gen(newObs)
		replace age_group_id = `age_id' if newObs==1
		drop newObs
		}
		
	bysort year_id age_group_id: generate sex_id = _n
	save `age'
	
	
	use `age', clear
	export excel using "FILEPATH/age.xlsx", replace


*** BRING IN AGE GROUP DETAILS ***	
	
*get_age_metadata, age_group_set_id(24) release_id(16) clear
	*keep age_group_id age_group_name age_group_years_start age_group_years_end
	odbc load, exec("SELECT age_group_id, age_group_name, age_group_years_start, age_group_years_end FROM age_group") `shared' clear
	egen age_mid = rowmean( age_group_years_start age_group_years_end)


	merge 1:m age_group_id using `age', assert(1 3) keep(3) nogenerate
	save `master'

	use `master', clear
	export excel using "FILEPATH/master.xlsx", replace


*** BRING IN AGE WEIGHTS ***
	noisily di " . get age weights"
	get_age_weights, release_id(16) clear
	keep age_group_id age_group_weight_value	
	noisily di " . merge age weight"
	export excel using "/FILEPATH/age_weights.xlsx", replace
	merge 1:m age_group_id using `master', keep(2 3) nogenerate
	save `master', replace
	use `master', clear
	*export excel using "/FILEPATH/age_weights.xlsx", replace

*** BRING IN LIST OF COD ESTIMATION LOCATIONS ***
	noisily di " . Loading locations"
	get_location_metadata, location_set_id(35) release_id(16) clear
	keep location_id parent_id path_to_top_parent level is_estimate most_detailed location_name location_type_id location_type super_region_id super_region_name region_id region_name ihme_loc_id
	*odbc load, exec("SELECT location_id, parent_id, path_to_top_parent, level, is_estimate, most_detailed, location_name, location_type_id, location_type, super_region_id, super_region_name, region_id, region_name, ihme_loc_id FROM location_hierarchy_history WHERE location_set_id = 8 AND location_set_version_id = 158") `shared' clear
	
	/* Now we merge in the locations file from above and keep all countries, 
	estimation units, & locations with CoD data */
	merge 1:1 location_id using `codLocations'
	keep if is_estimate == 1 | level == 3 | _merge == 3
	drop _merge
	
	* Most regions have sufficient data to model at the region level
	generate mod_region_id = region_id 
	generate mod_region_name = region_name
	* Oceania typically has little data; if <25% of locations in Oceania have data, then add it to SE Asia
	replace  mod_region_id = 9 if region_id==21 
	replace  mod_region_name = "SE Asia and Oceania" if mod_region_id==9
	* Central, Eastern, & Western Sub-Saharan Africa typically have little data and will be modelled together
	replace  mod_region_id = super_region_id if inlist(region_id, 167, 174, 199)
	replace  mod_region_name = "Central, Eastern, & Western Sub-Saharan Africa" if inlist(region_id, 167, 174, 199)
	
	* Create country id variable
	gen country_id = word(subinstr(path_to_top_parent, ",", " ", .), 4)
	destring country_id, replace
	


*** CREATE DATASET WITH EVERY COMBINATION OF AGE AND LOCATION (MASTER) ***	
	cross using `master'
	save  `master', replace

/*
***	CREATE DATASET WITH EVERY COD ESTIMATION YEAR ***
	noisily di " . Loading years"
	get_demographics, gbd_team(cod) clear
	local years `r(year_id)'
	local yearList = subinstr("`years'", " ", ",",.)

	clear
	set obs `=max(`yearList') - 1979'
	generate year_id = _n + 1979
	save `year'
	
	
*** CREATE DATASET WITH EVERY COMBINATION OF AGE, LOCATION, & YEAR (MASTER, UPDATED) ***	
	cross using `master'
	save  `master', replace

	
***	CREATE DATASET WITH EVERY COD ESTIMATION SEX ***	
	noisily di " . Loading sexes"
	clear
	set obs 2
	generate sex_id = _n 


*** CREATE DATASET WITH EVERY COMBINATION OF AGE, LOCATION, YEAR, & SEX (MASTER, UPDATE 2) ***
	cross using `master'
	save  `master', replace
*/	
	
	keep location_id year_id age_group_id sex_id parent_id
	save `resolverMaster'
	
*** CREATE TEMPORARY DATASETS WITH EVERY VALUE OF AGE, LOCATION & YEAR (TO BE USED TO DEAL WITH MISSING VALUES OF COVARIATES) ***
	noisily di " . Storing vectors"
	foreach x in age_group location year sex {
		preserve
		keep `x'_id
		duplicates drop
		
		save ``x'ListTemp'
		levelsof `x'_id, local(`x'FullVector) clean
		
		restore
		}
	

	
/******************************************************************************\
                           BRING IN MODEL RESULTS                               
\******************************************************************************/ 
	
if "`get_model_results'" != "" {	
  tokenize `get_model_results', parse(|)	

  while "`1'" != "" {
  
  * PULL IN MODEL RESULTS & CLEAN UP A COUPLE VARIABLES ***
	noisily di _n "Getting model results: `1'"
	
	* Get the modelable entity name to label the variable produced by the resolver *
	local mvid = word(subinstr(substr("`1'", strpos("`1'", "model_version_id") + length("model_version_id") + 1, .), ")", " ", 1), 1)
	odbc load, exec("SELECT modelable_entity_name FROM epi.model_version JOIN epi.modelable_entity USING (modelable_entity_id) WHERE model_version_id = `mvid'") `epi' clear
	levelsof modelable_entity_name, local(me_name) clean
	
	* Run the resolver *
	use `resolverMaster', clear
	noisily resolver `1', estimate_type(results) by(age_group_id sex_id) covariate_name("`me_name'") age_groupFullVector(`age_groupFullVector') locationFullVector(`locationFullVector') yearFullVector(`yearFullVector') 
	*local covars_to_collapse `covars_to_collapse' `r(new_covars)'
	
  * MERGE IT ALL TOGETHER *		
	merge 1:1 location_id year_id age_group_id sex_id using `master', assert(1 3) keep(3) nogenerate
	save `master', replace
	macro shift 2
	}
  }



/******************************************************************************\
                           CREATE COVARIATE DATASET                             
\******************************************************************************/	

if "`covariate_ids'" != "" {
	noisily di _n "Loading covariate metadata" _continue
	
  * LOAD IN COVARIATE META-DATA TO DETERMINE NAME & AGE/SEX SPECIFICITY *
	odbc load, exec("SELECT covariate_id, covariate_name_short, covariate_name, by_age, by_sex, dichotomous FROM covariate WHERE covariate_id IN (`=subinstr(trim(itrim("`covariate_ids'")), " ", ",", .)')") `shared' clear

	generate sex = "sex_id" if by_sex==1
	generate age = "age_group_id" if by_age==1
	
	levelsof covariate_name_short, local(covarList) clean

	forvalues i = 1/`=_N' {
		foreach var of varlist covariate_name* dichotomous age sex by_age {
			local `var'_`=covariate_id[`i']'  `=`var'[`i']'
			}
		}

  * LOAD IN COVARIATE DATA, PROCESS TO ACCOUNT FOR AGE/SEX SPECIFICITY & MERGE INTO MASTER ***  
	foreach covariate_id of local covariate_ids {
		noisily di _n "Getting covariate `covariate_id' (`covariate_name_`covariate_id'')"
	  	  
		use `resolverMaster', clear
		noisily resolver `covariate_id', estimate_type(covariates) by(`age_`covariate_id'' `sex_`covariate_id'') covariate_name(`covariate_name_`covariate_id'') covariate_name_short(`covariate_name_short_`covariate_id'') age_groupFullVector(`age_groupFullVector') locationFullVector(`locationFullVector') yearFullVector(`yearFullVector') 
		*if `by_age_`covariate_id''==1 local covars_to_collapse `covars_to_collapse' `r(new_covars)'
		
		merge 1:1 location_id year_id age_group_id sex_id using `master', keep(2 3) nogenerate
		save `master', replace
		}
	}
	
macro dir
  
/******************************************************************************\
                     BRING IN POPULATION & ENVELOPE ESTIMATES                              
\******************************************************************************/ 	

*** GET THE ENVELOPE ***
	noisily di _n "Getting the mortality envelope"
	get_envelope, location_id("`locationFullVector'") sex_id("1 2") release_id(16) age_group_id("`age_groupFullVector'") year_id("`yearFullVector'") with_hiv(1) clear		 
	rename mean envelope
	keep age_group_id location_id year_id sex_id envelope
	save `env'
	save "/FILEPATH/envelope_gbd23.dta", replace

*** GET THE POPULATIONS ***	
	noisily di "Getting population estimates"
	get_population, location_id("`locationFullVector'") sex_id("1 2") release_id(16) age_group_id("`age_groupFullVector'") year_id("`yearFullVector'") clear	 
	keep age_group_id location_id year_id sex_id population
	save "/FILEPATH/population_gbd23.dta", replace

*** MERGE IT ALL TOGETHER & CLEAN UP ***	
	noisily di "Merging envelope & population estimates"
	merge 1:1 age_group_id location_id year_id sex_id using `env', gen(envPopMerge) //assert(3) nogenerate
	
	noisily di "Merging envelope + population estimates to the master"
	merge 1:1 age_group_id location_id year_id sex_id using `master', gen(envPopMasterMerge) //assert(3) nogenerate
	
	save `master', replace
	save "/FILEPATH/master_line476.dta", replace  




/******************************************************************************\
           CREATE ALL-AGE VERSIONS OF MODEL RESULTS & COVARIATES                               
\******************************************************************************/
  
  if "`get_model_results'" != "" | "`covariate_ids'" != "" {
  
	noisily di "Creating age-standardized versions of age-specific covariates and model results"
  
  	drop age_group_id age_group_name age_group_years_start age_group_years_end age_mid sex_id envelope level is_estimate most_detailed location_name location_type* *region* ihme_loc_id country_id *parent*
	
	ds location_* year_id age_group_weight population, not
	local covars_to_collapse `r(varlist)'
	
	foreach var of varlist `covars_to_collapse' {
		generate `=substr("`var'", 1, 29)'_27 = `var' * age_group_weight / 2
		generate `=substr("`var'", 1, 29)'_22 = `var' * population
		}

	fastcollapse *_22 *_27 population, by(location_id year_id) type(sum)

	foreach var of varlist *_22 {
		replace `var' = `var' / population
		}
		
	drop population	
	
	merge 1:m location_id year_id using `master', assert(3) nogenerate
	}
	save `master', replace
	save "/FILEPATH/master_line510.dta", replace  


	  
/******************************************************************************\
                MERGE IN COD DATA, CLEAN UP & SAVE DATASET                               
\******************************************************************************/

*** MERGE IN COD DATA ***
	noisily di "Merging CoD data to master"  
	merge 1:m location_id year_id age_group_id sex_id using `cod', nogenerate
	save `master', replace
	save "/FILEPATH/master_line522.dta", replace  
	
*** CREATE ALL-AGE DATA POINTS ***	
	keep if age_group_id==22 & aaComplete==1 & !missing(data_group)
	keep data_group deaths envelope population
	
	generate cf = deaths / envelope
	generate rate = deaths / population
	
	rename * *_22
	rename data_group_22 data_group
	
	merge 1:m data_group using `master', assert(2 3) nogenerate
	bysort data_group: gen aaIndex = _n==1
	save `master', replace
	save "/FILEPATH/master_line538.dta", replace 

	
/******************************************************************************\
               MERGE IN COD DATA, CLEAN UP & SAVE ALL-AGE DATASET                               
\******************************************************************************/	



*** CLEAN UP ***
	order year_id location_id location_name ihme_loc_id  ///
	      super_region_id super_region_name region_id region_name mod_region_id country_id parent_id path_to_top_parent ///
	      location_type_id location_type level is_estimate most_detailed  ///
		  age_group_id age_group_name age_group_years_start age_group_years_end age_mid sex_id ///
	      `covarList' *_2? cf rate sample_size study_deaths pop deaths env data_type_name is_representative ///
		  is_outlier cod_source_label nid citation 
		  
	sort  location_id year_id age_group_id sex_id data_type_name cod_source_label
		  
	
*** SAVE FILE ***
	noisily di "Saving the dataset"
	if "`saveto'"!="" {
		save `saveto'
		}
	else {
		save `FILEPATH/cause`cause_id'_`=subinstr(subinstr(itrim(trim("`c(current_date)'_`c(current_time)'")), " ", "_", .), ":", "_", .)'.dta, replace
		}
}	
end