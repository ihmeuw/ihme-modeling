
*** BOILERPLATE ***
	clear all
	set maxvar 32000
	set more off



*** PULL IN LOCATION_ID AND ENDEMICITY CATEGORY FROM BASH COMMAND ***
	local location   `1'
	local ageModel   `2'



*** START LOG ***
	capture log close
	log using /FILEPATH, replace



*** LOAD SHARED FUNCTIONS ***
	adopath + 
	run /FILEPATH
	run /FILEPATH
	run /FILEPATH
	run /FILEPATH

*** SET UP LOCALS & TEMPFILES ***  MODELABLE ENTITY IDS AND AGE GROUPS ***
	local outDir /FILEPATH

	tempfile inc draws pop ageMeta ageCross appendTemp mergeTemp

	get_demographics, gbd_team(epi) clear
	local ages `r(age_group_ids)'
	local years `r(year_ids)'
	local yearList `=subinstr("`r(year_ids)'", " ", ",", .)'
	local maxYear = max(`yearList')




*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)


*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	save `ageMeta'

	keep age_group_id
	save `ageCross'




*** PULL ALL-AGE INCIDENCE ESTIMATES FROM ST-GPR MODEL ***
	use `outDir'/FILEPATH, clear

	forvalues i = 0 / 999 {
		generate gammaA = rgamma(alpha, 1)
		generate gammaB = rgamma(beta, 1)
		generate draw_`i' =  gammaA / (gammaA + gammaB)
		drop gammaA gammaB
		}

	save `inc'
    levelsof year_id, local(years) clean



*** CREATE EMPTY ROWS FOR INTERPOLATION ***
	keep year_id
	cross using `ageCross'
	drop if inlist(year_id, `yearList')

	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1

	save `appendTemp'




*** PULL THE DRAWS FROM THE INCDIENCE AGE/SEX CURVE MODEL (DISMOD USED ONLY FOR AGE-SEX SPLIT) ***
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(ADDRESS1) source(dismod) location_ids(`location') measure_ids(6) age_group_ids(`ages') model_version_id(`ageModel') clear
	drop measure_id modelable_entity_id model_version_id
	rename draw_* ageCurve_*
	save `draws'







/******************************************************************************\
                       INTERPOLATE AGE/SEX SPLIT DRAWS
\******************************************************************************/

	append using `appendTemp'

	fastrowmean ageCurve_*, mean_var_name(ageCurveMean)

	  forvalues year = 1980/2016 {

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


		foreach var of varlist ageCurve_* {
			quietly {
			bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(ageCurveMean[`indexEnd']/ageCurveMean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
			replace  `var' = 0 if missing(`var') & year_id==`year'
			}

			di "." _continue
			}
		}


	replace location_id = `location'
	save `draws', replace



*** GET THE POPULATION ESTIMATES ***
	get_population, location_id(`location') year_id(`years') age_group_id(`ages') sex_id(1 2) clear
	drop process_version_map_id
	bysort location_id year_id: egen totalPop = total(population)



*** MERGE POPULATON, ALL-AGE INCIDENCE ESTIMATES, & AGE/SEX CURVE ***
	merge m:1 location_id year_id using `inc',  assert(3) nogenerate
	merge 1:1 location_id year_id sex_id age_group_id using `draws', assert(3) nogenerate
	merge m:1 age_group_id using `ageMeta', assert(3) nogenerate




*** PROCESS DRAWS TO IMPOSE AGE/SEX-PATTERN ON TOTAL CASE ESTIMATES ***
	forvalues i = 0 / 999 {
		quietly {
			replace draw_`i' = draw_`i' * totalPop

			generate casesCurve = ageCurve_`i' * population
			replace  casesCurve = 0 if age_group_id<0.1
			bysort location_id year_id: egen totalCasesCurve = total(casesCurve)

			replace draw_`i' = endemic * casesCurve * (draw_`i' / totalCasesCurve) / population

			drop casesCurve totalCasesCurve
			}
		di "." _continue
		}



*** EXPORT INCIDENCE DRAWS **
	keep if year_id>=1990

	generate modelable_entity_id = ADDRESS1
	generate measure_id = 6
	export delimited using `outDir'FILEPATH, replace





/******************************************************************************\
                        CREATE PREVALENCE DRAWS
\******************************************************************************/

	replace measure_id = 5

	*** EXPORT ZERO PREVALENCE DRAWS IF THIS IS A NON-ENDEMIC LOCATION ***
	 sum endemic
	 if `r(mean)'== 0 {
		export delimited using `outDir'FILEPATH, replace
		}


	*** ESTIMATE PREVALENCE FROM INCIDENCE IF THIS IS AN ENDEMIC LOCATION ***
	else {
		bysort year_id: egen hsaMean = mean(hsa_norm)
		drop hsa_norm alpha beta ageCurve* totalPop


		* We'll need the raw incidence draws to estimate acute prevalence later; save them to a tempfile here *
		tempfile raw
		save `raw'

		rename draw_* draw_*__

		reshape wide draw_*__ population hsaMean, i(age_group_id location_id sex_id endemic modelable_entity_id measure_id) j(year_id)


		* Calculate the duration spent in a given age category *
		generate duration = age_end - age_start
		replace  duration = 1 if age_group_id==4 // adjustment to carryover final prevalence in <1 year old age group correctly through cohort space

		* Loop through draws and years, and move cases through cohort space to estimate prevalence of chronic sequelae
		forvalues i = 0/999 {
			forvalues y = 1990/2016 {
				quietly {
					generate inc_long_`i'__`y' = draw_`i'__`y' * (1 - hsaMean`y') * 0.476 // 0.476 is proportion of those not receiving tx who will have visible scars (from GBD meta-analysis)
					local order `order' draw_`i'__`y' inc_long_`i'__`y'

					generate prev_`i'__`y' = 0 if age_start == 0

					if `y'== 1990 {
						bysort sex_id (age_group_id): replace prev_`i'__`y' = prev_`i'__`y'[_n-1] + (1 - prev_`i'__`y'[_n-1]) * (1 - exp(-(age_end[_n-1] - age_start[_n-1]) * inc_long_`i'__`y'[_n-1])) if age_start > 0
						* Prevalence of longterm sequelae in each age category (half-year correction)
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-(age_end - age_start)/2 * inc_long_`i'__`y')) if age_start < 95
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-(100 - age_start)/2 * inc_long_`i'__`y')) if age_start ==  95
						}
					else {
						bysort sex_id (age_group_id): replace prev_`i'__`y' = prev_`i'__`y'[_n-1] + (1 - prev_`i'__`y'[_n-1]) * (1 - exp(-(age_end[_n-1] - age_start[_n-1]) * inc_long_`i'__`y'[_n-1])) if inrange(age_group_id, 3, 4)
						bysort sex_id (age_group_id): replace prev_`i'__`y' = (((duration-1)/duration) * prev_`i'__`=`y'-1') + ((1/duration) * prev_`i'__`=`y'-1'[_n-1]) if age_group_id>=5
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-1 * inc_long_`i'__`y'))
						}
					}
				}
			quietly drop inc_long_`i'__* draw_`i'__*
			di "." _continue
			}

		drop endemic  population* age_start age_end duration


		* Reshape from wide back to long *
		forvalues y = 1990 / 2016 {
			preserve
			keep age_group_id location_id sex_id modelable_entity_id measure_id *`y'
			rename *`y' *
			generate year_id = `y'
			tempfile y`y'
			save `y`y''

			local append `append' `y`y''
			restore
			}

		clear
		append using `append'


		rename prev_*__ prev_*



		* Bring the raw draws back in to estimate prevalence of acute sequelae
		merge 1:1 location_id year_id age_group_id sex_id using `raw', assert(3) nogenerate

		* Add cases with acute sequelae (ie all cases but long-term cases), assuming 6 month duration
		forvalues i = 0 / 999 {
			replace prev_`i' = prev_`i' + (draw_`i' - (draw_`i' * (1 - hsaMean) * 0.476)) / 2
			}

		drop draw_*
		rename prev_* draw_*

        keep measure_id location_id year_id age_group_id sex_id modelable_entity_id draw_*
		export delimited using `outDir'/FILEPATH, replace
		}


	file open progress using `outDir'/progress/`location'.txt, text write replace
	file write progress "complete"
	file close progress

	log close
