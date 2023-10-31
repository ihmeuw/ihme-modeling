 
*** BOILERPLATE ***
	clear all
	set more off, perm

	if c(os) == "Unix" {
		local j "ADDRESS"
		local k "ADDRESS"
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j "ADDRESS"
		local k "ADDRESS"
		}

	adopath + FILEPATH

	local step STEP
	
*** ENSURE THAT OUTPUT DIRECTORIES EXIST ***
	local states death death_hiv inf_sev inc_nohiv inc_hiv
	local modelMeidGlobal 20291
	local modelMeidDr 20292
	
	capture mkdir FILEPATH
	
	local rootDir FILEPATH
		

	!rm -rf `rootDir'

	capture mkdir `rootDir'
	capture mkdir `rootDir'/temp
	capture mkdir `rootDir'/parent
	capture mkdir `rootDir'/logs
	capture mkdir `rootDir'/logs/progress

	foreach state of local states {
		capture mkdir `rootDir'/`state'
		}




		
*** GET LIST OF LOCATIONS ***	
	get_location_metadata, location_set_id(35) clear
	keep if most_detailed == 1 & is_estimate == 1 	
	levelsof location_id, local(locations) clean
		
	tempfile locMeta
	save `locMeta'


*** GET LIST OF DATA RICH LOCATIONS (COD DEF) ***
	get_location_metadata, location_set_id(43) gbd_round_id(6) clear
	gen isDr = (parent_id==44640)
	keep location_id isDr	
	
	
	merge 1:1 location_id using `locMeta', assert(1 3) keep(3) nogenerate
	
	
	generate nfModel = `modelMeidGlobal' if inlist(super_region_id, 4, 158, 166, 137) 
	replace  nfModel = `modelMeidDr' if inlist(super_region_id, 31, 64, 103) 

	generate codModel = "global" if isDr!=1  |  nfModel==`modelMeidGlobal' 
	replace  codModel = "dr"     if isDr==1	 & nfModel==`modelMeidDr' 

	tempfile submitData
	save `submitData'

*** PREP CASE FATALITY & COINFECTION DRAWS *** 
	cp FILEPATH FILEPATH, replace
	import delimited using FILEPATH, clear case(preserve)
	
	reshape wide logitPred logitPredSeSm, i(location_id year_id age_group_id) j(estPrHiv)

	merge 1:m location_id year_id age_group_id using FILEPATH, gen(rrMerge) 
	merge m:1 location_id using `locMeta', gen(locMerge)
	assert rrMerge==3 if is_estimate==1
	
	keep if is_estimate==1
	keep location_id - lnHivRrSe
	
	tempfile cf
	save `cf'
	
   
    use `submitData', clear
*** SUBMIT BASH FILES ***
	
	forvalues i = 1/`=_N' {	
		local location = location_id[`i']
		
		use `cf', clear
		keep if location_id==`location'
		save FILEPATH/cfr_`location'.dta, replace
		
		use `submitData', clear
		
		! qsub -l m_mem_free=4G -l fthread=8  -l archive -P ihme_general -q all.q -o FILEPATH/output -e FILEPATH/errors -N split_`location'  "FILEPATH/02b_submit_split.sh" "`location'" "`=nfModel[`i']'" "`=codModel[`i']'" "`step'"
		sleep 100
		}

			
	use `locMeta', clear
	keep if is_estimate==1
	

/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** GET DISMOD MODEL IDS FOR DESCRIPTIONS ***
	preserve
	get_best_model_versions, entity(modelable_entity) ids(`modelMeidGlobal' `modelMeidDr') decomp_step(`step') clear
	levelsof model_version_id, sep(,) clean local(dm)
	restore

*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
	keep location_id 
	duplicates drop
	generate complete = 0
	levelsof location_id, local(locations) clean 
   
*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***   
    local pause 2
	local complete 0
	local incompleteLocations `locations'
	
	display _n "Checking to ensure all locations are complete" _n

	
*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***	
	while `complete'==0 {
	
	* Are all locations complete?
	  foreach location of local incompleteLocations {
		capture confirm file FILEPATH 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best True
		
		get_demographics, gbd_team(cod) clear
		local codYears `r(year_id)'
		
		run FILEPATH/save_results_epi.ado
		run FILEPATH/save_results_cod.ado

		save_results_cod, cause_id(959) mark_best("`mark_best'") decomp_step(`step') description("Natural history model using estimates of CFR and HIV coinfection (dm models `dm')") input_dir(FILEPATH) input_file_pattern({location_id}.csv) metric_id(3) clear

		save_results_epi, modelable_entity_id(19680) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe iNTS (dm models `dm')") input_dir(FILEPATH) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(18651) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Incidence attributable to HIV (dm models `dm')") input_dir(FILEPATH) measure_id(6) decomp_step(`step') year_id(`codYears') clear
		save_results_epi, modelable_entity_id(18651) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Incidence not attributable to HIV (dm models `dm')") input_dir(FILEPATH) measure_id(6) decomp_step(`step') year_id(`codYears') clear
		}
	

	  * If all locations are not complete, inform the user and pause before checking again
	  else {
	    quietly levelsof location_id if complete==0, local(incompleteLocations) clean
	    display "The following locations remain incomplete:" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue
		
		forvalues sleep = 1/`=`pause'*6' {
		  sleep 10000
		  if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
		  else di "." _continue
		  }
		di _n
		
		}
	  }
	
	
  
