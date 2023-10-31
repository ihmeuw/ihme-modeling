*  do FILEPATH/02a_submit_split.do  
 
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j FILEPATH
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j FILEPATH
		}

	** set up central functions
	adopath + FILEPATH

	run FILEPATH/get_best_model_versions.ado	
	run FILEPATH/get_location_metadata.ado

	tempfile dr

	** date
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr("`date'"," ","_",.)

	local gbd_round_id = 7
	local step iterative
	local saveStep iterative
	local codStep step3

	local cod_typh_male	  682598
	local cod_typh_female 682601
	local cod_para_male   682604
	local cod_para_female 682607
	
	
*** ENSURE THAT OUTPUT DIRECTORIES EXIST ***
	local states_typhoid pr death inf_mod inf_sev abdom_sev gastric_bleeding death2
	local states_paratyphoid pr death inf_mild inf_mod inf_sev abdom_mod death2

	local rootDir FILEPATH
*	local rootDir FILEPATH/`date'

	
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

	capture mkdir `rootDir'/parent



 
*** PREP CASE FATALITY DRAWS *** 
	use FILEPATH, clear

	forvalues i = 1/3 {
		preserve
		keep if incomeCat == `i'
		drop incomeCat
		
		expand 2 if inlist(age_group_id, 4, 5), gen(new)
		replace age_group_id = 388 if age_group_id==4 & new==0
		replace age_group_id = 389 if age_group_id==4 & new==1
		replace age_group_id = 238 if age_group_id==5 & new==0
		replace age_group_id = 34  if age_group_id==5 & new==1
		drop new
  
		save `rootDir'/FILEPATH.dta, replace
		restore
		}
 */
		
*** GET LIST OF IND SUBNATIONALS ***	
	get_location_metadata, location_set_id(35) gbd_round_id(`gbd_round_id') decomp_step(`step') clear
	levelsof location_id if strmatch(ihme_loc_id, "IND_*") & is_estimate==1, local(indSubs) clean sep(,)


*** GET LIST OF DATA RICH LOCATIONS (COD DEF) ***
	get_location_metadata, location_set_id(43) gbd_round_id(`gbd_round_id') clear
	keep if parent_id==44640
	keep location_id 
	gen isDr = 1
	save `dr'
	
	get_location_metadata, location_set_id(35) gbd_round_id(`gbd_round_id') decomp_step(`step') clear
	keep if most_detailed == 1 & is_estimate == 1
	merge 1:1 location_id using `dr', nogen keep(3)
	keep location_id
	gen isDr = 1
	save `dr', replace
	
	/*
	get_location_metadata, location_set_id(41) gbd_round_id(4) clear
	keep location_id
	gen isDr = 1
	save `dr'
	*/
		
	
*** LOAD FILE WITH LOCATION_IDS AND INCOME ***  
	do FILEPATH.do

	use FILEPATH.dta, clear

	* transfer India fild onto the cluster
	cp `FILEPATH.dta `rootDir'/FILEPATH.dta, replace

	*merge 1:1 location_id using FILEPATH.dta
	merge 1:1 location_id using `dr'
	keep if (is_estimate==1 | location_id==163) & !inlist(location_id, `indSubs')
	
	generate nfModel = "global" if inlist(super_region_id, 4, 158, 166, 137) | location_id==114  
	replace  nfModel = "dr" if inlist(super_region_id, 31, 64, 103) & location_id!=114 

	generate codModel = "global" if isDr!=1  | nfModel=="global" // location_id==17 | inrange(location_id, 4709, 4742) 
	replace  codModel = "dr"     if isDr==1	& nfModel=="dr"  //!(location_id==17 | inrange(location_id, 4709, 4742))
	
	/*
	* generate model = "global" 
	* keep if inlist(location_id, 4732,4734,35662,44646,44665,44668,44671,44672,44676,44679,44683,44688,44692,44696,44697,44698,44699,44704,44710,44713,44715,44717,44720,44721,44725,44732,44733,44735,44738,44741,44746,44750,44753,44758,44759,44760,44763,44767,44773,44774,44779,44782,44789,44790,44853,44855,44857,44859,44957,44962,53565,53568,53571,53574,53575,53579,53582,53586,53588,53595,53603,53606,53607,53614,53618,53661,53666,53671)
	
	levelsof location_id if codModel=="dr", local(drLocs)
	foreach loc of local drLocs {
		! rm `rootDir'/logs/progress/`loc'.txt
		}
		
    *keep if location_id==17 | inrange(location_id, 4709, 4742)
	*/
	
	preserve

*** SUBMIT BASH FILES ***
	forvalues i = 1/`=_N' {
		capture confirm file `rootDir'/FILEPATH/`=location_id[`i']'.txt 
		if _rc {				
			cp `rootDir'/FILEPATH.dta `rootDir'/FILEPATH_`=location_id[`i']'.dta, replace
			cp `rootDir'/FILEPATH_`=incomeCat[`i']'.dta `rootDir'/FILEPATH_`=location_id[`i']'.dta, replace
			
			*! qsub -l m_mem_free=8G -l fthread=8  -l archive -P ihme_general -q all.q -o FILEPATH -e FILEPATH -N split_`=location_id[`i']' "FILEPATH/02c_submit_split.sh" "`=location_id[`i']'" "`=incomeCat[`i']'" "`=nfModel[`i']'"  "`=codModel[`i']'" "`rootDir'"
			! qsub -l m_mem_free=8G -l fthread=8  -l archive -P proj_erf -q all.q -o FILEPATH -e FILEPATH -N split_`=location_id[`i']' "`j'FILEPATH/02c_submit_split.sh" "`=location_id[`i']'" "`=incomeCat[`i']'" "`=nfModel[`i']'"  "`=codModel[`i']'" "`rootDir'"

			*! qsub -P proj_custom_models -l fthread=4,m_mem_free="5G",archive=TRUE -q all.q -o FILEPATH -e FILEPATH -N split_`=location_id[`i']' "FILEPATH/02c_submit_split.sh" "`=location_id[`i']'" "`=incomeCat[`i']'" "`=nfModel[`i']'"  "`=codModel[`i']'" "`rootDir'"
			sleep 100
		}
		else {
			di "job was previously completed"
		}
	}
	
	*/
	restore
	
	

/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** GET DISMOD MODEL IDS FOR DESCRIPTIONS ***
	preserve
	get_best_model_versions, entity(modelable_entity) ids(10139 10140) decomp_step(iterative) clear
		levelsof model_version_id, sep(,) clean local(dm1)

	get_best_model_versions, entity(modelable_entity) ids(1252) decomp_step(iterative) clear
		levelsof model_version_id, sep(,) clean local(dm2)
	
	local dm `dm1' `dm2'
	
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
		
		local mark_best False
		
		run FILEPATH/save_results_epi.ado
		run FILEPATH/save_results_cod.ado
		
		save_results_cod, cause_id(319) mark_best("`mark_best'") description("Natural history model (dr/global hybrid; dm models `dm'; codem models `cod_typh_male', `cod_typh_female')") input_dir(`rootDir'/typhoid/death) input_file_pattern({location_id}.csv) metric_id(3) gbd_round_id(`gbd_round_id') decomp_step(`codStep') clear
		save_results_cod, cause_id(320) mark_best("`mark_best'") description("Natural history model (dr/global hybrid; dm models `dm'; codem models `cod_para_male', `cod_para_female')") input_dir(`rootDir'/paratyphoid/death) input_file_pattern({location_id}.csv) metric_id(3) gbd_round_id(`gbd_round_id') decomp_step(`codStep') clear
		
		
		local mark_best True
		local bundle 556
		local xwalk  17891
		
		save_results_epi, modelable_entity_id(1249) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Moderate typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/inf_mod) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(1250) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/inf_sev) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(1251) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe abdom typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/abdom_sev) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(3134) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Gastric bleeding typhoid cases (dm models `dm')") input_dir(`rootDir'/typhoid/gastric_bleeding) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		
		save_results_epi, modelable_entity_id(1253) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Mild paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_mild) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(1254) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Moderate paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_mod) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(1255) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe paratyphoid cases (dm models `dm')") input_dir(`rootDir'/paratyphoid/inf_sev) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		
		save_results_epi, modelable_entity_id(1256) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Paratyphoid with abdom_mod (dm models `dm')") input_dir(`rootDir'/paratyphoid/abdom_mod) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		
		save_results_epi, modelable_entity_id(2523) mark_best("`mark_best'") measure_id(6) input_file_pattern({location_id}.csv) description("All typhoid and paratyphoid cases (dm models `dm' with IND subnat redistribution (VA scaled))") input_dir(`rootDir'/parent) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
	
		save_results_epi, modelable_entity_id(23991) mark_best("`mark_best'") measure_id(18) input_file_pattern({location_id}.csv) description("Pr typhoid -- squeezed (dm models `dm')") input_dir(`rootDir'/typhoid/pr) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
		save_results_epi, modelable_entity_id(23992) mark_best("`mark_best'") measure_id(18) input_file_pattern({location_id}.csv) description("Pr paratyphoid -- squeezed (dm models `dm')") input_dir(`rootDir'/paratyphoid/pr) decomp_step(`saveStep') gbd_round_id(`gbd_round_id') bundle_id(`bundle') crosswalk_version_id(`xwalk') clear
*/

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
	
	
  
