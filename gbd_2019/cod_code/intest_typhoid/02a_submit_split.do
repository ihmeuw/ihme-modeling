 
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "ADDRESS"
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j "ADDRESS"
		}

	** set up central functions
	adopath + FILEPATH

	run FILEPATH/get_best_model_versions.ado	
	run FILEPATH/get_location_metadata.ado

	tempfile dr

	** date
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr("`date'"," ","_",.)

	local step STEP

*** ENSURE THAT OUTPUT DIRECTORIES EXIST ***
	local states_typhoid pr death inf_mod inf_sev abdom_sev gastric_bleeding death2
	local states_paratyphoid pr death inf_mild inf_mod inf_sev abdom_mod death2

	local rootDir FILEPATH

	!rm -rf `rootDir'

	capture mkdir `rootDir'
	capture mkdir `rootDir'/temp
	capture mkdir `rootDir'/parent
	capture mkdir `rootDir'/inputs
	capture mkdir `rootDir'/logs
	capture mkdir `rootDir'/logs/progress


	foreach x in typhoid paratyphoid {
		capture mkdir `rootDir'/`x'
		foreach state of local states_`x' {
			capture mkdir `rootDir'/`x'/`state'
			}
		}

 
*** PREP CASE FATALITY DRAWS *** 
	use FILEPATH/cfDrawsByIncomeAndAge, clear

	forvalues i = 1/3 {
		preserve
		keep if incomeCat == `i'
		drop incomeCat
		save `rootDir'/temp/cfDrawsByIncomeAndAge_`i'.dta, replace
		restore
		}

		
*** GET LIST OF IND SUBNATIONALS ***	
	get_location_metadata, location_set_id(35) clear
	levelsof location_id if strmatch(ihme_loc_id, "IND_*") & is_estimate==1, local(indSubs) clean sep(,)


*** GET LIST OF DATA RICH LOCATIONS (COD DEF) ***
	get_location_metadata, location_set_id(43) gbd_round_id(6) clear
	keep if parent_id==44640
	keep location_id 
	gen isDr = 1
	save `dr'
	
	get_location_metadata, location_set_id(35) gbd_round_id(6) clear
	keep if most_detailed == 1 & is_estimate == 1
	merge 1:1 location_id using `dr', nogen keep(3)
	keep location_id
	gen isDr = 1
	save `dr', replace
	

*** LOAD FILE WITH LOCATION_IDS AND INCOME ***  
	do FILEPATH/02b_sequela_split_builder.do

	use FILEPATH/submit_split_data.dta, clear

	* transfer India fild onto the cluster
	cp FILEPATH/indiaSplit_ur_cod.dta `rootDir'/indiaSplit_ur_cod.dta, replace

	merge 1:1 location_id using `dr'
	keep if (is_estimate==1 | location_id==163) & !inlist(location_id, `indSubs')
	
	generate nfModel = "global" if inlist(super_region_id, 4, 158, 166, 137) | location_id==114  
	replace  nfModel = "dr" if inlist(super_region_id, 31, 64, 103) & location_id!=114 

	generate codModel = "global" if isDr!=1  | nfModel=="global"
	replace  codModel = "dr"     if isDr==1	& nfModel=="dr" 

	levelsof location_id if codModel=="dr", local(drLocs)
	foreach loc of local drLocs {
		! rm `rootDir'/logs/progress/`loc'.txt
		}
		
	preserve


*** SUBMIT BASH FILES ***
	forvalues i = 1/`=_N' {
		capture confirm file `rootDir'/logs/progress/`=location_id[`i']'.txt 
		if _rc {				
			cp `rootDir'/inputs/sequela_splits.dta `rootDir'/sequela_splits_`=location_id[`i']'.dta, replace
			cp `rootDir'/temp/cfDrawsByIncomeAndAge_`=incomeCat[`i']'.dta `rootDir'/temp/cfDraws_`=location_id[`i']'.dta, replace
			
			! qsub -l m_mem_free=8G -l fthread=8  -l archive -P proj_custom_models -q all.q -o FILEPATH -e FILEPATH -N split_`=location_id[`i']' "FILEPATH/02c_submit_split.sh" "`=location_id[`i']'" "`=incomeCat[`i']'" "`=nfModel[`i']'"  "`=codModel[`i']'" "`rootDir'"

			sleep 100
		}
		else {
			di "job was previously completed"
		}
	}
	restore
	
	

/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** GET DISMOD MODEL IDS FOR DESCRIPTIONS ***
	preserve
	get_best_model_versions, entity(modelable_entity) ids(10139 10140 1252) decomp_step(STEP) clear
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
		capture confirm file `rootDir'/logs/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best True
		
		run FILEPATH/save_results_epi.ado
		run FILEPATH/save_results_cod.ado
		
		save_results_cod, cause_id(319) mark_best("`mark_best'") description("Natural history model (dr/global hybrid; dm models `dm'; codem models)") input_dir(`rootDir'/typhoid/death) input_file_pattern({location_id}.csv) metric_id(3) decomp_step(`step') clear
		save_results_cod, cause_id(320) mark_best("`mark_best'") description("Natural history model (dr/global hybrid; dm models `dm'; codem models)") input_dir(`rootDir'/paratyphoid/death) input_file_pattern({location_id}.csv) metric_id(3) decomp_step(`step') clear
		
		
		local mark_best True
		
		save_results_epi, modelable_entity_id(1249) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Moderate typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/inf_mod) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(1250) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/inf_sev) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(1251) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe abdom typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/abdom_sev) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(3134) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Gastric bleeding typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/gastric_bleeding) decomp_step(`step') clear
		
		save_results_epi, modelable_entity_id(1253) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Mild paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_mild) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(1254) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Moderate paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_mod) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(1255) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_sev) decomp_step(`step') clear
		
		save_results_epi, modelable_entity_id(1256) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Paratyphoid with abdom_mod (dm models `dm')") input_dir(`rootDir'/paratyphoid/abdom_mod) decomp_step(`step') clear
		
		save_results_epi, modelable_entity_id(2523) mark_best("`mark_best'") measure_id(6) input_file_pattern({location_id}.csv) description("All typhoid and paratyphoid cases (dm models `dm' with IND subnat redistribution (VA scaled))") input_dir(`rootDir'/parent) decomp_step(`step') clear
	
		save_results_epi, modelable_entity_id(23991) mark_best("`mark_best'") measure_id(18) input_file_pattern({location_id}.csv) description("Pr typhoid -- squeezed (dm models `dm')") input_dir(`rootDir'/typhoid/pr) decomp_step(`step') clear
		save_results_epi, modelable_entity_id(23992) mark_best("`mark_best'") measure_id(18) input_file_pattern({location_id}.csv) description("Pr paratyphoid -- squeezed (dm models `dm')") input_dir(`rootDir'/paratyphoid/pr) decomp_step(`step') clear


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
	
	
  
