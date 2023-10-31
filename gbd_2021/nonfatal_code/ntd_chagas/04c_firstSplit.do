* NTDs - Chagas disease 
* Description: post processing, sequelae are split using the proportions
*
	
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
  local FILEPATH
  local FILEPATH

* PULL IN LOCATION_ID AND ENDEMICITY CATEGORY FROM BASH COMMAND *  
 local location ADDRESS
 local endemic  ADDRESS
 local run_dir  ADDRESS
 local decomp_step ADDRESS
 local gbd_round_id ADDRESS
   
* LOAD SHARED FUNCTIONS *  
  run FILEPATH
  run FILEPATH
  run FILEPATH
  run FILEPATH
  run FILEPATH
  
* SET UP LOCALS WITH OUTPUT DIRECTORY, MODELABLE ENTITY IDS AND AGE GROUPS *  
  get_demographics, gbd_team(ADDRESS) clear
  local ages `r(ADDRESS)'
   
  local meid_master ADDRESS
  local meid_acute  ADDRESS
  local meid_afib   ADDRESS
  local meid_hf     ADDRESS
  local meid_asymp  ADDRESS
  local meid_total  ADDRESS
  local meid_digest_mild ADDRESS
  local meid_digest_mod  ADDRESS 
  
  tempfile appendTemp mergeTemp prevalence incidence chronic acute

  
  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR INCIDENCE AND PREVALENVCE *
      if `endemic'==0 {
	    use FILEPATH, clear
		generate measure_id = 5
		rename draw_* prev_*
		capture generate metric_id = 3
	    }
		
	  else if inlist(`location', 98, 99) {
	    do FILEPATH
		capture generate metric_id = 3
		}

	  else {
	    get_draws, gbd_id_type(ADDRESS) gbd_id(`ADDRESS') source(ADDRESS) location_id(`location') age_group_id(`ages') measure_id(5) gbd_round_id(`ADDRESS') decomp_step(`ADDRESS') status(ADDRESS) clear
        rename draw_* prev_*
		capture generate metric_id = 3
		}
	
	  capture drop model_id
      save `mergeTemp'
	  
	  preserve
	  rename prev_* draw_*
	  keep location_id year_id sex_id age_group_id measure_id metric_id draw_* 
      save `prevalence'
	  restore
      
	  joinby age_group_id sex_id using FILEPATH

	* PERFORM DRAW-LEVEL CALCULATIONS  *
	  forvalues i = 0 / 999 {
		    quietly {
			  replace draw_`i' = draw_`i' * prev_`i'
			  replace draw_`i' = 0 if draw_`i'==0 | missing(draw_`i')
			  }
			di "." _continue
			}
		 			
	 levelsof model_id, local(meids) clean
	capture generate metric_id = 3
	 foreach meid of local meids {
       export delimited location_id year_id sex_id age_group_id measure_id metric_id model_iddraw_* using FILEPATH if model_id==`meid', replace
	   }
	 
	 keep if measure_id==5
	 fastcollapse draw_*, by(location_id year_id sex_id age_group_id) type(sum)
	 forvalues i = 0 / 999 {
		quietly replace draw_`i' = -1 * draw_`i'
		}
	 save `chronic'
	 
  get_draws, gbd_id_type(ADDRESS) decomp_step(`ADDRESS') gbd_round_id(`gbd_round_id') gbd_id(`ADDRESS') source(ADDRESS) location_id(`location') age_group_id(`ages') measure_id(6) status(ADDRESS) clear

	  if inlist(`location', 98, 99) | `endemic'==0 {
	    if `location' == 98 local eYear 1999
        else if `location' == 99 local eYear 1997
		else local eYear 1980
		  
		forvalues i = 0 / 999 {
		  quietly replace draw_`i' = 0  if year_id>=`eYear'
		  }
	    }
	  capture generate metric_id = 3	  
	  save `incidence' 
	  
	  
	expand 2, gen(newObs)
	replace measure_id = measure_id - newObs
	  
  	forvalues i = 0 / 999 {
	 quietly {
		local prAcute = rbeta(10, 190)
		replace draw_`i' = draw_`i' * `prAcute' * 6/52 if measure_id==5
		}
	  }
	
	capture generate metric_id = 3
	keep location_id year_id sex_id age_group_id measure_id metric_id draw_* 
	
	tempfile 
	  
	generate model_id= `meid_acute'
	capture generate metric_id = 3
    export delimited using FILEPATH, replace
	
	keep if measure_id==5
	forvalues i = 0 / 999 {
		quietly replace draw_`i' = -1 * draw_`i'
		}
	save `acute'
	
	clear 
	append using `prevalence' `incidence'
	
	replace model_id= `meid_total'
	
	export delimited location_id year_id sex_id age_group_id model_idmetric_id measure_id draw_* using FILEPATH, replace
	
	keep if measure_id==5
	append using `acute'
	append using `chronic'
	
	fastcollapse draw_*, by(location_id year_id sex_id age_group_id) type(sum)
	forvalues i = 0 / 999 {
		quietly replace draw_`i' = 0 if draw_`i' < 0
		}
	generate measure_id = 5
	generate model_id= `meid_asymp'
	capture generate metric_id = 3
	
	export delimited location_id year_id sex_id age_group_id measure_id metric_id model_iddraw_* using FILEPATH, replace

  file open progress using FILEPATH, text write replace
  file write progress "complete"
  file close progress

	log close
	
	 
