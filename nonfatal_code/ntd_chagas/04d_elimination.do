	
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
  
* PULL IN LOCATION_ID FROM BASH COMMAND *  
  local location "`1'"
  
  
* LOAD SHARED FUNCTIONS *  
  adopath + FILEPATH
  run FILEPATH/get_draws.ado
  run FILEPATH/get_demographics.ado
  run FILEPATH/create_connection_string.ado
  
  tempfile ageMeta master appendTemp mergeTemp preElimPrev streamingTemp

  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid_master  1450

  
 
* SET UP LOCALS WITH CHAGAS ELIMINATION YEARS *  
  if `location' == 98 local eYear 1999
  else if `location' == 99 local eYear 1997
  

* CREATE CONNECTION STRING TO SHARED DATABASE *
  noisily di "{hline}" _n _n "Connecting to the database"
		
  create_connection_string, database(ADDRESS)
  local shared = r(conn_string)
	
	
* PULL AGE GROUP DATA *
  get_demographics, gbd_team(epi) clear
  odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
  save `ageMeta'
	
	
  
* CREATE EMPTY ROWS FOR INTERPOLATION *
  get_demographics, gbd_team(epi) clear
  local age_group_id `r(age_group_ids)'
  local ages `r(age_group_ids)'
  local year_id `r(year_ids)'
  local sex_id `r(sex_ids)'
  
  local maxYear = max(`=subinstr("`year_id'", " ", ",", .)')
  
  clear 
  set obs `=`maxYear'-1979'
  generate year_id = _n + 1979
  drop if inlist(year_id, `=subinstr("`year_id'", " ", ",", .)')
  save `master'
  
  foreach var in age_group_id sex_id {
	clear
	set obs `=wordcount("``var''")'
	generate `var' = .
	forvalues i = 1 / `=wordcount("``var''")' {
		quietly replace `var' = `=word("``var''", `i')' in `i'
		}
	cross using `master'
	save `master', replace
	}

  
  generate location_id = `location'
   
  save `appendTemp'
  

  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR INCIDENCE AND PREVALENVCE *
      get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid_master') source(dismod) location_ids(`location') age_group_ids(`ages') measure_ids(5 6 9) status(best) clear
	  
      drop model*
      rename draw_* draw_*_
      reshape wide draw_*_, i(location_id year_id age_group_id sex_id) j(measure_id)

	  rename draw_*_5 prev_*
	  rename draw_*_6 inc_*
	  rename draw_*_9 em_*
	  
      preserve
	  
	  drop inc_* em_*
	  keep if year_id <= `eYear'
	  save `preElimPrev'
	  
	  restore
	  
	  
	
/******************************************************************************\
                             INTERPOLATE DEATHS
\******************************************************************************/			


append using `appendTemp'



foreach metric in prev inc em {

  fastrowmean `metric'_*, mean_var_name(`metric'Mean)	
	
  forvalues year = 1980/`maxYear' {
	
	local index = `year' - 1979

	if `year'< 1990  {
	  local indexStart = 1990 - 1979
	  local indexEnd   = 2016 - 1979
	  }	
	else if inrange(`year', 2011, 2015) {
	  local indexStart = 31
	  local indexEnd   = 37
	  }
	else {
	  local indexStart = 5 * floor(`year'/5) - 1979
	  local indexEnd   = 5 * ceil(`year'/5)  - 1979
	  if `indexStart'==`indexEnd' | `year'==2016 continue
	  }
  
	foreach var of varlist `metric'_* {
		quietly {
		bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(`metric'Mean[`indexEnd']/`metric'Mean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
        }
		
		di "." _continue
		}	
	}
  drop `metric'Mean
  }
				  

	  

/******************************************************************************\
                             BIRTH PREVALENCE
\******************************************************************************/				  



* DERIVE PARAMETERS OF BETA DISTRIBUTION FOR RATE OF VERTICAL TRANSMISSION *
  local mu    = 0.047  // mean and SD here are from meta-analysis by Howard et al (doi: 10.1111/1471-0528.12396)
  local sigma = (0.056 - 0.039) / (invnormal(0.975) * 2)
  local alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
  local beta  = `alpha' * (1 - `mu') / `mu'  
 
  
	
* BRING IN DATA ON PROPORTION ON NUMBER OF PREGNANCIES BY AGE, YEAR, LOCATION (USING ESTIMATES CREATED FOR HEPATITIS E ESTIMATION) *  
  merge 1:1 location_id age_group_id year_id sex_id using FILEPATH/prPreg.dta, keep(1 3) nogenerate
  merge m:1 age_group_id using `ageMeta', assert(3) nogenerate
	 
	 
	 
* ESTIMATE BIRTH PREVALENCE *  
  replace nPreg = 0 if sex==1
  bysort location_id year_id: egen nPregTotal = total(nPreg)
  sort location_id year_id sex_id age_group_id  
	  
  save `streamingTemp'  
	  
  forvalues year = `eYear'/`=`maxYear'-1' { 
	
	preserve
	drop if year_id==`=`year'+1'
	save `streamingTemp', replace
	restore

	keep if year_id == `year'	
	replace year_id = `year' + 1

	forvalues i = 0 / 999 {
		local vertical    = rbeta(`alpha', `beta')  
		quietly generate posPregTemp = nPreg * prev_`i' * `vertical'
		bysort location_id year_id: egen posPregTempTotal = total(posPregTemp)
		replace prev_`i' = posPregTempTotal / nPregTotal if age_start<1
	  
		replace em_`i' = em_`i' * (age_end - age_start) if age_start<1
		bysort sex_id (age_group_id): replace em_`i' = sum(em_`i') if age_start<1
	
		bysort sex_id (age_group_id): replace prev_`i' = (4/5 * prev_`i') + (1/5 * prev_`i'[_n-1]) if age_start>=5
		bysort sex_id (age_group_id): replace prev_`i' = (3/4 * prev_`i') + (1/4 * prev_`i'[1]) if age_start==1
		replace prev_`i' = prev_`i' * (1 - em_`i')
		
		drop posPregTemp*
		}
 
	append using `streamingTemp'
	save `streamingTemp', replace
    }

	
keep if inlist(year_id, `=subinstr("`year_id'", " ", ",", .)')	
keep location_id year_id age_group_id sex_id prev_*
generate measure_id = 5
