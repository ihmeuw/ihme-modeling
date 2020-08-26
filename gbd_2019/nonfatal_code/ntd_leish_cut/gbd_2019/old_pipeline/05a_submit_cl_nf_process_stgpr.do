
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local ADDRESS "FILEPATH"
		set odbcmgr ADDRESS
		}
	else if c(os) == "Windows" {
		local ADDRESS "FILEPATH"
		}

	local ageModel   ADDRESS
	local stGprModel hybrid


*** LOAD SHARED FUNCTIONS ***
	adopath + FILEPATH
	run FILEPATH.ado
	run FILEPATH.ado
	run FILEPATH.ado


*** CREATE DIRECTORIES ***
	local tmp_in_dir FILEPATH
	local outDir /FILEPATH


	tokenize "`outDir'", parse(/)
	while "`1'"!="" {
		local path `path'`1'`2'
		capture mkdir `path'
		macro shift 2
		}


	foreach subDir in ADDRESS1 temp progress {
		sleep 2000
		!rm -rf `outDir'/`subDir'
		capture mkdir `outDir'/`subDir'
		}


	tempfile gr locMeta hsa



	get_demographics, gbd_team(cod) clear
	local years `r(year_ids)'
	local yearList `=subinstr("`r(year_ids)'", " ", ",", .)'
	local maxYear = max(`yearList')







*** LOAD IN LOCATION META DATA ***
	get_location_metadata, location_set_id(35) clear
	gen country_id = word(subinstr(path_to_top_parent, ",", " ", .), 4)
	destring country_id, replace
	keep location_id path_to_top_parent parent_id country_id is_estimate location_name location_type *region* ihme_loc_id
	keep if is_estimate==1 | location_type=="admin0"
	save `locMeta'

	levelsof location_id, local(locations) clean



*** GENERATE NORMALIZED HSA VARIABLE ***
	* Bring in HSA_capped covariate and normalize within each year
	get_covariate_estimates, covariate_id(208) year_id(`years') clear
	keep if inrange(year_id, 1980, `maxYear')

	bysort year_id: egen min = min(mean_value)
	bysort year_id: egen max = max(mean_value)

	generate hsa_norm = (mean_value - min)/(max-min)
	replace  hsa_norm = 0 if hsa_norm < 0

    keep location_id year_id hsa_norm
    save `hsa'


*** GET ALL-AGE POPULATION ESTIMATES ***
	get_population, location_id(`locations') year_id(`years') age_group_id(22) sex_id(3) clear
	keep if inrange(year_id, 1980, 2016)
	keep location_id year_id population

	merge m:1 location_id using `locMeta', assert(3) nogenerate
	merge 1:1 location_id year_id using `hsa', gen(hsaMerge)



*** LOAD ALL-AGE INCIDENCE ESTIMATES FROM ST-GPR ***
	merge 1:1 location_id year_id using FILEPATH, keep(3) nogenerate
	keep if is_estimate==1



	generate sigma = ((10/population) + upper - lower) / (2 * invnormal(0.975))
	generate alpha = mean * (mean - mean^2 - sigma^2) / sigma^2
	generate beta  = alpha * (1 - mean) / mean

	replace alpha = alpha + 1e-4 if endemic==1
	replace beta  = (alpha / mean) - alpha if endemic==1

	keep location_id year_id endemic alpha beta hsa_norm





*** SUBMIT LOCATION-SPECIFIC JOBS ***
	levelsof location_id, local(locations) clean

	foreach location of local locations {

		quietly {
			preserve
			keep if location_id==`location'
			save `outDir'/FILEPATH, replace
			restore

			cp "`tmp_in_dir'/FILEPATH" "`outDir'/FILEPATH", replace
			}

		! qsub -P ADDRESS -pe multi_slot 8 -N cl_nf_`location' "FILEPATH" "`location'" "`ageModel'"
		sleep 100
		}



/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/



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

		local mark_best no

		run FILEPATH/save_results.do
		save_results, modelable_entity_id(ADDRESS1) mark_best(`mark_best') file_pattern({measure_id}_{location_id}.csv) description("CL (ST-GPR model `stGprModel' with age pattern dm-`ageModel')") in_dir(`outDir'/ADDRESS1) env("prod")
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
