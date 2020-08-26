
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 10000
	set more off

	adopath + "FILEPATH"
	run FILEPATH/get_draws.ado
	run FILEPATH/get_demographics.ado
 
 
*** PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *** 
	local location "`1'"
	local income   "`2'"
	local nfModel  "`3'"
	local codModel "`4'"
	local rootDir "`5'"

	capture log close
	log using FILEPATH/log_`location', replace
  
  
*** SET UP OUTPUT DIRECTORIES ***  
	local outDir_para `rootDir'/paratyphoid
	local outDir_typh `rootDir'/typhoid

	
*** SET UP LOCALS WITH MODELABLE ENTITY IDS ***  
	if "`nfModel'"=="global" {
		local inc_meid 10140
		}
		
	else {
		local inc_meid 10139
		}
	
	local cod_step STEP
	local cod_typh_male   CODEM_MODEL_ID	  
	local cod_typh_female CODEM_MODEL_ID
	local cod_para_male   CODEM_MODEL_ID
	local cod_para_female CODEM_MODEL_ID

	local parent_meid = 2523

	local typh_meid 1247
	local para_meid 1252

	local typh_cid 319
	local para_cid 320

	local typh_inf_mod_meid 1249
	local typh_inf_sev_meid 1250
	local typh_abdom_sev_meid 1251
	local typh_gastric_bleeding_meid 3134

	local para_inf_mild_meid 1253
	local para_inf_mod_meid 1254
	local para_inf_sev_meid 1255
	local para_abdom_mod_meid 1256


	get_demographics, gbd_team(epi) clear
	local ages `r(age_group_id)'
	local ageList = subinstr("`ages'", " ", ",",.)
	local years `r(year_id)'
	local yearList = subinstr("`years'", " ", ",",.)

	tempfile appendTemp mergeTemp

  
*** CREATE EMPTY ROWS FOR INTERPOLATION (NOT NEEDED IF PULLING DR COD MODEL) ***
	if "`codModel'"=="global" {
		set obs `=max(`yearList') - 1979'
		generate year_id = _n + 1979
		drop if inlist(year_id, `yearList')

		generate age_group_id = 2

		foreach age of local ages {
			expand 2 if age_group_id==2, gen(newObs)
			replace age_group_id = `age' if newObs==1
			drop newObs
			}
			

		bysort year_id age_group_id: generate sex_id = _n

		save `appendTemp'
		}
  

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
*** PULL IN DRAWS FROM DISMOD MODELS FOR OVERALL INCIDENCE, PR TYPHOID & PR PARATYPHOID ***
	foreach x in inc para { 
			if "`x'" == "inc" {
			get_draws, gbd_id_type(modelable_entity_id) gbd_id(``x'_meid') location_id(`location') source(epi) status(best) decomp_step(`step') clear  
			}

		else {
			get_draws, gbd_id_type(modelable_entity_id) gbd_id(``x'_meid') location_id(`location')  source(epi) status(best) decomp_step(`step') clear
			}

		rename draw_* `x'_*
		keep if inlist(age_group_id, `ageList')

		if "`x'" != "inc" merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(`x'Merge)  
		save `mergeTemp', replace
		}

	  
	  
*** MERGE IN CASE FATALITY DATA *** 
	if "`codModel'"=="global" merge m:1 age_group_id using `rootDir'/temp/cfDraws_`location'.dta, assert(3) nogenerate
	

*** PERFORM DRAW-LEVEL CALCULATIONS TO SPLIT TYPHOID & PARATYPHOID, & CALCULATE MRs ***
	
	forvalues i = 0/999 {
		quietly {
			replace inc_`i' = 0 if age_group_id<3  | missing(inc_`i') // given incubation period typhoid not possible in first week of life

			generate prtyph_`i' = 1 - para_`i'
			generate prpara_`i' = para_`i' 
			
			generate typh_`i' = inc_`i' * prtyph_`i'
			replace  typh_`i' = 0 if missing(typh_`i') & age_group_id==235
			replace  para_`i' = inc_`i' * prpara_`i'
			replace  para_`i' = 0 if missing(para_`i') & age_group_id==235

			if "`codModel'"=="global" replace cf_typh_`i' = cf_typh_`i' * typh_`i'
			if "`codModel'"=="global" replace cf_para_`i' = cf_para_`i' * para_`i'
			}
		}

		
		


        
	  
    
	
/******************************************************************************\
                             INTERPOLATE DEATHS
\******************************************************************************/			

if "`codModel'"=="global" {

	append using `appendTemp'

	foreach type in typh para {

		fastrowmean cf_`type'_*, mean_var_name(cfMean_`type')	

		forvalues year = 1980/`=max(`yearList')' {
		
		if inlist(`year', `yearList') continue
		
		local index = `year' - 1979
		
			if `year'< 1990  {
				local indexStart = 1990 - 1979
				local indexEnd   = `=max(`yearList')' - 1979
				}	
				
			else {
				tokenize "`years'"
				while `1'<`year' {
					local indexStart = `1' - 1979
					macro shift
					}
				local indexEnd = `1' - 1979
				}


			foreach var of varlist cf_`type'_* {
				quietly {
					bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(cfMean_`type'[`indexEnd']/cfMean_`type'[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
					replace  `var' = 0 if missing(`var') & year_id==`year'
					}

				di "." _continue
				}	
			}	
		}
	replace location_id = `location'
	}


	
/******************************************************************************\
                    SPLIT OUT INDIAN SUBNATIONS, IF INDIA
\******************************************************************************/			

	if `location'==163 {
		drop location_id
		cross using `rootDir'/indiaSplit_ur_cod.dta

		levelsof location_id, local(indLocs) clean
		levelsof year_id, local(indYears) clean
		levelsof age_group_id, local(indAges) clean
		levelsof sex_id, local(indSexes) clean

		tempfile indMerge
		save `indMerge'

		run FILEPATH/get_population.ado
		get_population, location_id(`indLocs') year_id(`indYears') age_group_id(`indAges') sex_id(`indSexes') decomp_step(step3) clear
		capture drop process_version_map_id
		capture drop run_id
		
		merge 1:1 location_id year_id age_group_id sex_id using `indMerge', assert(3) nogenerate


		foreach stub in inc typh para cf_typh cf_para {	
			quietly {
				egen raw_cases_`stub' = rowmean(`stub'_*)
				replace raw_cases_`stub' = raw_cases_`stub' * population
				bysort year_id: egen raw_total_`stub' = total(raw_cases_`stub')
				drop raw_cases_`stub'
				}
			di "." _continue
			}

			
		foreach var of varlist inc_* typh_* para_* cf_typh_* cf_para_* {
			quietly replace `var' = `var' * rr
			di "." _continue
			}

		foreach stub in inc typh para cf_typh cf_para {	
			quietly {
				egen adj_cases_`stub' = rowmean(`stub'_*)
				replace adj_cases_`stub' = adj_cases_`stub' * population
				bysort year_id: egen adj_total_`stub' = total(adj_cases_`stub')
				drop adj_cases_`stub'
				}
			di "." _continue
			}			

		foreach stub in inc typh para cf_typh cf_para {
			foreach var of varlist `stub'_*  {
				quietly {
					replace `var' = `var' * raw_total_`stub'  / adj_total_`stub'
					}
				di "." _continue
				}
			quietly drop raw_total_`stub' adj_total_`stub'
			}
		quietly drop population rr	
		}

	
	
	
/******************************************************************************\
                                 EXPORT DEATHS
\******************************************************************************/		
		
*** EXPORT DEATHS IF USING GLOBAL COD MODEL ***
	if "`codModel'"=="global" {
		levelsof location_id, local(location_ids) clean

		foreach x in typh para {
			rename cf_`x'_* draw_* 
			replace measure_id = 1
			capture drop cause_id
			generate cause_id = ``x'_cid'

			foreach location_id of local location_ids { 
				outsheet location_id year_id sex_id age_group_id cause_id measure_id draw_* using `outDir_`x''/death/`location_id'.csv if location_id==`location_id', comma replace
				}

			drop draw_*
			}
		}
      
	  
	  
*** EXPORT DEATHS IF USING DATA RICH CODEM MODEL ***
	else {
		preserve
		
		get_demographics, gbd_team(cod) clear
		local codYears `r(year_id)'

		tempfile codAppend
		
		foreach x in typh para {
			get_draws, gbd_id_type(cause_id) gbd_id(``x'_cid') source(codem)  version_id(`cod_`x'_male')  location_id(`location') age_group_id(`ages') year_id(`codYears') measure_id(1) decomp_step(`cod_step') clear
			save `codAppend', replace
			
			get_draws, gbd_id_type(cause_id) gbd_id(``x'_cid') source(codem)  version_id(`cod_`x'_female') location_id(`location') age_group_id(`ages') year_id(`codYears') measure_id(1) decomp_step(`cod_step') clear
			append using `codAppend'
			
			expand 3 if age_group_id==4, gen(newObs)
			bysort age_group_id sex_id year_id location_id newObs: replace age_group_id = _n + 1 if newObs==1
			drop newObs
			
			forvalues i = 0 /999 {
				quietly {
					replace draw_`i' = draw_`i' / pop
					replace draw_`i' = 0 if age_group_id<4
					}
				di "." _continue
				}
			

			outsheet location_id year_id sex_id age_group_id cause_id measure_id draw_* using `outDir_`x''/death/`location'.csv, comma replace
			}
			
		restore	
		}

	
/******************************************************************************\
                PERFORM SEQUELA SPLITS & EXPORT NON-FATAL DRAWS
\******************************************************************************/


*** EXPORT TOTAL INCIDENCE ***		
	capture drop cause_id 
	capture drop measure_id
	keep if inlist(year_id, `yearList')
		 
	rename inc_* draw_*
	generate measure_id = 6
	replace modelable_entity_id = `parent_meid'

	levelsof location_id, local(location_ids) clean
	
	foreach location_id of local location_ids { 
		outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using `rootDir'/parent/`location_id'.csv if location_id==`location_id', comma replace
		}
		
	rename draw_* inc_*
	
	

*** EXPORT SQUEEZED PROPORTION MODELS ***		
	replace measure_id = 18
	
	levelsof location_id, local(location_ids) clean
	
	foreach x in para typh {
		replace modelable_entity_id = ``x'_meid'
		
		rename pr`x'_* draw_*
	
		foreach location_id of local location_ids { 
			outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using `outDir_`x''/pr/`location_id'.csv if location_id==`location_id', comma replace
			}
			
		drop draw_*
		}
		
		

*** BRING IN SEQUELA SPLIT DATA ***
	cross using `rootDir'/sequela_splits_`location'.dta

	levelsof state_para, clean local(state_para)
	levelsof state_typh, clean local(state_typh)
	  
	  
	  
*** SPLIT OUT INCIDENCE ***
	foreach x in typh para {
		forvalues i = 0/999 {
			quietly {
	  
				generate splitTemp = rbeta(alpha_`x', beta_`x')
				bysort age_group_id year_id sex_id location_id: egen correction = total(splitTemp)
				replace splitTemp  = splitTemp / correction
 
				replace `x'_`i' = `x'_`i' * splitTemp
			
				drop splitTemp correction
				}
			}
		}
	 
	drop measure_id 
	expand 2, gen(measure_id)
	replace measure_id = measure_id + 5
	
	
*** CALCULATE PREVALENCE ***	
	foreach x in typh para {
		forvalues i = 0/999 {
			quietly replace `x'_`i' = `x'_`i' * (rbeta(v_`x', w_`x') * range_`x' + min_`x') / 365.25  if measure_id==5  // apply sequela-specific duration from PERT-distribution
			}
		}
	
		
		
		
*** EXPORT SEQUELA INCIDENCE DRAWS ***
	foreach x in typh para {
		rename `x'_* draw_*
  
		foreach state of local state_`x' {
			replace modelable_entity_id = ``x'_`state'_meid'
			
			foreach location_id of local location_ids { 
				outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using `outDir_`x''/`state'/`location_id'.csv if state_`x'=="`state'" & location_id==`location_id', comma replace
				}
			}
		drop draw_*
		}


*** CLOSE LOG AND PROCESS FILES ***		
	file open progress using `rootDir'/logs/progress/`location'.txt, text write replace
	file write progress "complete"
	file close progress

	log close
  
  
  
