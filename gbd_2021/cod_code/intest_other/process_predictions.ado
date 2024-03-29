/******************************************************************************\

Author: name
Date created:  5 Jan 2017
Last modified: 9 Feb 2017

Syntax:

process_predicitions cause_id, link(ln/logit) [random(yes/no) min_age(age_group_id) multiplier(varname) description("Model description here") mark_best(yes/no) saving_cause_ids(cause_id list)]

\******************************************************************************/


capture program drop process_predictions
program define process_predictions

syntax anything(name=cause_id id="cause_id"), link(string) [random(string) min_age(integer 2) multiplier(varname) inflator_draw_stub(string) project(string) description(string) mark_best(string) saving_cause_ids(string) endemic(varname)]

*noisily macro dir


*** PROCESS SYNTAX ***
	if "`description'" == "" local description "Custom model for cause `cause_id' (`=trim("`c(current_date)'")' @ `c(current_time)')"
	
	local mark_best = trim(lower("`mark_best'"))
	if "`mark_best'" == "" {
		local mark_best False
		}
	else if "`mark_best'" != "False" & "`mark_best'" != "True" {
		if "`mark_best'" != "yes" {
			local mark_best True
			}
		else if "`mark_best'" != "no" {
			local mark_best False
			}
		else {
			di "Allowable values for option mark_best are True & False.  Defaulting to False."
			local mark_best False
			}
		}
		
	local random = trim(lower("`random'"))
	if "`random'" == "" {
		local random no
		}
	else if "`random'" != "no" & "`random'" != "yes" {
		di "Allowable values for option random are yes & no.  Defaulting to no."
		local random no
		}
		
	/*
	if "`cf'"=="" & "`rate'"=="" & "`exp'"=="" display "Must specify cf, rate or exp(varname)"
	else if "`exp'"!="" generate multiplier = `exp'
	else if "`cf'"!="" & "`rate'"!="" display "Must specify cf or rate, but not both"
	else if "`cf'"=="cf" generate multiplier = envelope 
	else generate multiplier = population
	*/
	
	if "`multiplier'"!="" generate multiplier = `multiplier'
	else generate multiplier = 1

	if "`inflator_draw_stub'"!=""  {
		quietly ds `inflator_draw_stub'*
		if wordcount("`r(varlist)'")!=1000 {
			di "<1000 draws found for inflator variable"
			}
		}
	else {
		local inflator_draw_stub 0
		}
	
	
	if "`link'"=="ln" | "`link'"=="log" local link exp
	else if "`link'"=="logit" local link invlogit
	else display "Expecting to see either ln or logit for the link option"
	
	if "`saving_cause_ids'"=="" local saving_cause_ids `cause_id'

	if "`project'"=="" local project proj_custom_models
	else if "`project'"=="none"  local project  
  
*** CREATE OUTPUT DIRECTORIES ***
	local rootDir  FILEPATH
	local causeDir FILEPATH
	local modelDir `causeDir'/`=subinstr(trim("`c(current_date)'"), " ", "_", .)'_`=subinstr("`c(current_time)'", ":", "_", .)'
	local tempDir `modelDir'/temp
	local logDir `modelDir'/logs
	local progressDir `logDir'/progress
	
	foreach dir in root cause model temp log progress {
		capture mkdir ``dir'Dir'
		}
		
	foreach saving_cause_id of local saving_cause_ids {
		capture mkdir `modelDir'/`saving_cause_id'
		capture mkdir `modelDir'/`saving_cause_id'/draws
		}

	local codeDir FILEPATH
	/*
	capture mkdir `codeDir'/logs
	capture mkdir `codeDir'/logs/progress
  
	capture !rmdir `codeDir'/logs/progress/`cause_id'  /s /q
	capture  mkdir `codeDir'/logs/progress/`cause_id'
	*/

*** CLEAN UP FILE ***
	bysort location_id age_group_id sex_id year_id: gen keep = _n==1
	keep if is_estimate==1  //& keep==1	
	
	if "`random'"=="yes" local rVars randomMean* randomSe*
	if !missing(dispersion) local dispVars beta_dispersionTemp
	if "`inflator_draw_stub'"!="0"  local inflatorVars `inflator_draw_stub'*
	
	keep location_id age_group_id sex_id year_id  covarTemp_* beta_*Temp* mergeIndex multiplier dispersion `rVars' `dispVars' `inflatorVars' `endemic' keep
	*keep location_id age_group_id sex_id year_id fixed* covarTemp_* beta_*Temp* mergeIndex multiplier dispersion `rVars' `dispVars' `endemic' keep

	
*** CYCLE THROUGH LOCATIONS AND SUBMIT BASH FILES TO PROCESS DRAWS ***
	tempfile submitTemp
	save `submitTemp'
		
	if "`endemic'"!="" {
		bysort location_id: egen locEndemic = mean(`endemic')
		levelsof location_id if locEndemic>0, local(endemicLocations) clean
		levelsof location_id if locEndemic==0, local(nonendemicLocations) clean
	
		tempfile submitTemp
		save `submitTemp', replace

		keep if locEndemic==0
		if "`inflator_draw_stub'"!="0" drop `inflator_draw_stub'*
		
		macro dir 
		
		foreach location of local nonendemicLocations {                
			quietly {
				preserve
				keep if location_id==`location'
				save `tempDir'/`location'.dta, replace
				restore
				drop if location_id==`location'
		
				capture ! rm `progressDir'/`location'.txt
				}
		
			! qsub -e /FILEPATH -o /FILEPATH -N customCod_`cause_id'_`location' -l fthread=2 -l m_mem_free=8G -q long.q -l archive=TRUE -P proj_tb "/FILEPATH/submit_prediction_processor.sh" "`cause_id'" "`location'" "`link'" "`min_age'" "`random'" "`=subinstr("`saving_cause_ids'", " ", "_", .)'" "`modelDir'" "`endemic'" "`inflator_draw_stub'"
			}	
			
		
		use `submitTemp', clear
		keep if locEndemic>0

		foreach location of local endemicLocations {
			quietly {
				preserve
				keep if location_id==`location'
				save `tempDir'/`location'.dta, replace
				restore
				drop if location_id==`location'
		
				capture ! rm `progressDir'/`location'.txt
				}
		
			! qsub -e /FILEPATH/ -o /FILEPATH/ -N customCod_`cause_id'_`location' -l fthread=4 -l m_mem_free=15G -q long.q -l archive=TRUE -P proj_tb "/FILEPATH/submit_prediction_processor.sh" "`cause_id'" "`location'" "`link'" "`min_age'" "`random'" "`=subinstr("`saving_cause_ids'", " ", "_", .)'" "`modelDir'" "`endemic'" "`inflator_draw_stub'"
			}
		}
		
		
	else {	
		levelsof location_id, local(locations) clean
		foreach location of local locations {
			quietly {
				preserve
				keep if location_id==`location'
				save `tempDir'/`location'.dta, replace
				restore
				drop if location_id==`location'
		
				capture ! rm `progressDir'/`location'.txt
				}
		
			! qsub -e /FILEPATH/ -o /FILEPATH/ -N customCod_`cause_id'_`location' -l fthread=4 -l m_mem_free=15G -q long.q -l archive=TRUE -P proj_tb "/FILEPATH/submit_prediction_processor.sh" "`cause_id'" "`location'" "`link'" "`min_age'" "`random'" "`=subinstr("`saving_cause_ids'", " ", "_", .)'" "`modelDir'" "0" "`inflator_draw_stub'"
			}	
		}
		
*** CYCLE THROUGH MEIDS AND SUBMIT BASH FILES TO SAVE RESULTS *
    use `submitTemp', clear
    keep location_id
	duplicates drop
	
	generate complete = 0
   
    local pause 2
	local complete 0
	local incompleteLocations `locations'
  
	while `complete'==0 {
	  foreach location of local incompleteLocations {
		capture confirm file `progressDir'/`location'.txt
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  if `r(N)'==0 {
		local complete 1
		
		foreach saving_cause_id of local saving_cause_ids {
		  display "All locations complete.  Submitting save_results for cause_id `saving_cause_id'."
		  run "/FILEPATH/save_results_cod.ado"
		  save_results_cod, cause_id(`saving_cause_id') description("`description'") input_file_pattern({location_id}.csv) input_dir(`modelDir'/`saving_cause_id'/draws) decomp_step(release2) gbd_round_id(7) mark_best(False) clear

		  }
		}
		
	  else {
	    quietly levelsof location_id if complete==0, local(incompleteLocations) clean
	    display "The following locations remain incomplete:" _n "`incompleteLocations'" _n "Pausing for `pause' minutes"
		
		forvalues sleep = 1/`=`pause'*6' {
		  sleep 10000
		  if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
		  else di "." _continue
		  }
		di _n
		
		}
	  }
	
end
		
