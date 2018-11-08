

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
	 
  tempfile crossTemp appendTemp mergingTemp envTemp drawMaster sevInc zero
  
  local date = subinstr(trim("`: display %td_CCYY_NN_DD date(c(current_date), "DMY")'"), " ", "_", .)
   
  adopath + FILEPATH
  run FILEPATH/get_demographics.ado

   
* CREATE A LOCAL CONTAINING THE ISO3 CODES OF COUNTRIES WITH YELLOW FEVER *
  local yfCountries AGO ARG BEN BOL BRA BFA BDI CMR CAF TCD COL COG CIV COD ECU GNQ ETH GAB GHA GIN GMB GNB ///
     GUY KEN LBR MLI MRT NER NGA PAN PRY PER RWA SEN SLE SDN SSD SUR TGO TTO UGA VEN ERI SOM STP TZA ZMB
	 

* STORE FILE PATHS IN LOCALS *	
  local inputDir FILEPATH 
  local ageDist  `inputDir'/ageDistribution.dta
  local cf       `inputDir'/caseFatalityAB.dta
  local ef       `inputDir'/expansionFactors.dta
  local data     `inputDir'/dataToModel2016.dta
  local skeleton `inputDir'/skeleton.dta
  

  local outDir FILEPATH
  capture mkdir `outDir'
  
  foreach subDir in temp progress deaths total _asymp inf_mod inf_sev {
	!rm -rf `outDir'/`subDir'
	sleep 2000
	capture mkdir `outDir'/`subDir'
	}

* LOAD COVARIATES *
  tempfile covars  
  foreach covar_id in 881 1099 {
	get_covariate_estimates, covariate_id(`covar_id') clear
	levelsof covariate_name_short, local(name) clean
	rename mean_value `name'
	keep location_id year_id `name'
	if `covar_id'!=881 merge 1:1 location_id year_id using `covars', nogenerate
	save `covars', replace
	}
	
/******************************************************************************\	
                            MODEL YELLOW FEVER CASES
\******************************************************************************/ 

  use `data', clear
  
  rename sex_id dataSexId
  rename year_start year_id
 
  replace effective_sample_size = population if missing(effective_sample_size)
  merge 1:1 location_id year_id using `covars', assert(2 3) keep(3) nogenerate


  menbreg cases yearC sdi if cases>0, exp(effective_sample_size) || countryIso: 
    predict predFixed, fixedonly fitted nooffset
    predict predFixedSe, stdp nooffset
    predict predRandom, remeans reses(predRandomSe) nooffset

	bysort  countryIso: egen countryRandom = mean(predRandom)
    replace predRandom = countryRandom if missing(predRandom)
	
	bysort  region_id: egen regionRandom = mean(predRandom)
    replace predRandom = regionRandom if missing(predRandom)
	
	bysort  super_region_id: egen superRegionRandom = mean(predRandom)
    replace predRandom = superRegionRandom if missing(predRandom)

	replace predRandomSe = _se[var(_cons[countryIso]):_cons] if missing(predRandomSe)


  	


  bysort countryIso year_id: egen cntryCases = mean(cases)
  replace cases = cntryCases if inlist(countryIso, "BRA", "KEN") & missing(cases) & !missing(cntryCases)
  bysort countryIso year_id: egen cntryMean = mean(mean)
  replace mean = cntryMean if inlist(countryIso, "BRA", "KEN") & missing(mean) & !missing(cntryMean)
  bysort countryIso year_id: egen cntrySe = mean(standard_error)
  replace standard_error = cntrySe if inlist(countryIso, "BRA", "KEN") & missing(standard_error) & !missing(cntrySe)
  
  

   drop age_group_id sex year_end
   rename population allAgePop


/******************************************************************************\	
   BRING IN THE DATA ON YELLOW FEVER AGE-SEX DISTRIBUTION, EF, & CASE FATALITY
\******************************************************************************/ 

	cross using `ageDist'
	merge m:1 countryIso using `ef', assert(3) nogenerate
	merge 1:1 location_id year_id age_group_id sex_id using `skeleton', assert(2 3) keep(3) nogenerate


	rename population ageSexPop

	keep ef_* year_id age_group_id sex sex_id location_id countryIso ihme_loc_id ageSexCurve pred* ageSexPop allAgePop mean standard_error cases effective sample_size location_name yfCountry 


	gen ageSexCurveCases = ageSexCurve * ageSexPop
	bysort location_id year_id: egen totalCurveCases = total(ageSexCurveCases)
	gen prAgeSex = ageSexCurveCases / totalCurveCases


	merge m:1 location_id year_id using FILEPATH/braSubPr.dta, assert(1 3) nogenerate
	replace allAgePop = mean_pop_bra if !missing(mean_pop_bra)
	forvalues i = 0/999 {
		quietly replace braSubPr_`i' = 1 if missing(braSubPr_`i')
		}
	
  
	preserve
	use `cf', clear
	local deathsAlpha = alphaCf in 1
	local deathsBeta = betaCf in 1
	
	get_demographics, gbd_team(cod) clear
	local locations `r(location_ids)'
	restore

	levelsof location_id if yfCountry==1, local(yfCntryIds) clean
 
*** CREATE LOCATION-SPECIFIC INCIDENCE ESTIMATE FILES & SUBMIT PARALLEL PROCESS JOBS *** 	 
	
	foreach location of local locations {
		if `: list location in yfCntryIds' == 0 {
			local endemic = 0
			}
			
		else {
			local endemic = 1 
			quietly { 
				preserve
				keep if location_id==`location'
				save `outDir'/temp/`location'.dta, replace
				restore
				drop if location_id==`location'
				}
			}
			   
			! qsub -P proj_custom_models -pe multi_slot 8 -N yf_`location' "FILEPATH/02b_submitProcessModel.sh" "`location'" "`outDir'" "`deathsAlpha'" "`deathsBeta'" "`endemic'"
			}
		
 

 
 
 	
/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	


*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
	clear
	set obs `=wordcount("`locations'")'
	generate location_id = .

	forvalues i = 1 / `=wordcount("`locations'")' {
		quietly replace location_id = `=word("`locations'", `i')' in `i'
		}
		
	generate complete = 0

   
*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***   
    local pause 2
	local complete 0
	local incompleteLocations `locations'
	
	display _n "Checking to ensure all locations are complete" _n

	
*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***	
	while `complete'==0 {
	
	* Are all locations complete?
	  foreach location of local incompleteLocations {
		capture confirm file `outDir'/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best no
		
		run FILEPATH/save_results.do

		local totalN   1509
		local _asympN  3338
		local inf_sevN 1511
		local inf_modN 1510 
		local deathsN  358
		 
		save_results, cause_id(`deathsN') description("Deaths from yellow fever (version `version')") in_dir("`outDir'/deaths")  
		save_results, modelable_entity_id(`totalN')   description("All infections with yellow fever (`date')") in_dir("`outDir'/total") file_pattern({location_id}.csv) env("prod")
		save_results, modelable_entity_id(`_asympN')  description("Asymptomatic infection with yellow fever (`date')") in_dir("`outDir'/_asymp") file_pattern({location_id}.csv) env("prod")
		save_results, modelable_entity_id(`inf_modN') description("Moderate infection with yellow fever (`date')") in_dir("`outDir'/inf_mod") file_pattern({location_id}.csv) env("prod")
		save_results, modelable_entity_id(`inf_sevN') description("Severe infection with yellow fever (`date')") in_dir("`outDir'/inf_sev") file_pattern({location_id}.csv) env("prod")  
	
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
	
 
  
/******************************************************************************\	
                                SAVE RESULTS
\******************************************************************************/  


  
  di "ERROR REPORT:"
  di "`errors'"
  
  