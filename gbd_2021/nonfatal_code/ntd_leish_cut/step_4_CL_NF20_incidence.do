* Purpose: Process incidence draws 
* Notes: Pulls ST-GPR draws, applies age-pattern from DisMod, calculates incidence estimates for each location

*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32767
	local user : env USER
	local code_root "FILEPATH"
	local data_root "FILEPATH"

	local params_dir "FILEPATH/params"
	local draws_dir "FILEPATH/draws"
	local interms_dir "FILEPATH/interms"
	local logs_dir "FILEPATH/logs_dir"

	cap log using "`logs_dir'/step_4_nf_incidence.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH  // source STATA
    run FILEPATH/get_demographics.ado
    run FILEPATH/get_draws.ado
    run FILEPATH/get_population.ado
    run FILEPATH/get_age_metadata.ado
    run FILEPATH/get_covariate_estimates.ado
  *************************
 

 
 *age metadata
get_demographics,gbd_team("ADDRESS") gbd_round_id("ADDRESS")


	*local ages `r(age_group_ids)'
	local years `r(year_ids)'
	local yearList `=subinstr("`r(year_ids)'", " ", ",", .)'
	local maxYear = 2022


get_age_metadata, gbd_round_id("ADDRESS") age_group_set_id(19)	
keep age_group_id 
save "`interms_dir'/age_meta2.dta",replace

levelsof age_group_id, local(ages) clean

tempfile inc draws pop ageMeta ageCross appendTemp mergeTemp pops hsa

***************************
import delimited "`params_dir'/ntd_leish_cut_lgr.csv", clear

keep if most_detailed==1

keep if year_start==2019

keep if value_endemicity==1

*generate variable to index location IDs

	levelsof location_id, local(clCntryIds) clean

    
 
	
***************************************************************

*-----pull alphas and betas from R script-----------
*loop through locations
*pull all-age incidence from st-gpr model
foreach location of local clCntryIds {

tempfile inc draws pop ageMeta ageCross appendTemp mergeTemp pops hsa

	import delimited using "FILEPATH/`location'.csv", clear
	

	save `inc'
    levelsof year_id, local(years) clean
	
	*** CREATE EMPTY ROWS FOR INTERPOLATION ***  
	keep year_id
	cross using "`interms_dir'/age_meta2.dta"
	drop if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)

	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1

	save `appendTemp'
 
    
*** PULL THE DRAWS FROM THE INCDIENCE AGE/SEX CURVE MODEL (DISMOD USED ONLY FOR AGE-SEX SPLIT) ***  
	get_draws, gbd_id_type(ADDRESS) gbd_id("ADDRESS") source("ADDRESS") location_id(`location') measure_id(6) age_group_id(`ages') decomp_step("ADDRESS") version_id(ADDRESS) clear
	drop measure_id id version_id
	rename draw_* ageCurve_*
	drop if age_group_id==164
	drop if age_group_id==27
	drop if age_group_id==33
	save `draws'
  
  
	
/******************************************************************************\
                       INTERPOLATE AGE/SEX SPLIT DRAWS
\******************************************************************************/			

	append using `appendTemp'

	egen ageCurveMean = rowmean(ageCurve_*)
		
	  forvalues year = 1980/2022 {
		
		local index = `year' - 1979

		if `year'< 1990  {
		  local indexStart = 1990 - 1979 
		  *11
		  local indexEnd   = 2022 - 1979
		  *43
		  }	
		else if inrange(`year', 2011, 2019) {
		  local indexStart = 31
		  local indexEnd   = 40
		  }
		else {
		  local indexStart = 5 * floor(`year'/5) - 1979
		  local indexEnd   = 5 * ceil(`year'/5)  - 1979
		  if `indexStart'==`indexEnd' | `year'==2022 continue
		  }

	  
		foreach var of varlist ageCurve_* {
			quietly {
			bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(ageCurveMean[`indexEnd']/ageCurveMean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
			replace  `var' = 0 if missing(`var') & year_id==`year'
			}
			}	
		}
	 
		replace location_id = `location'
	*replace location_id = `location'
	save `draws', replace
  
	levelsof year_id, local(years) clean
  
*** GET THE POPULATION ESTIMATES ***  
	get_population, location_id(`location') year_id(`years') age_group_id(`ages') sex_id(1 2) decomp_step("ADDRESS") gbd_round_id("ADDRESS") clear
	
	bysort location_id year_id: egen totalPop = total(population)
  

  
*** MERGE POPULATION, ALL-AGE INCIDENCE ESTIMATES, & AGE/SEX CURVE ***   
	merge m:1 location_id year_id using `inc',  assert(3) 
	
	drop _merge
	
	
	*need to add location_id here when we execute this for all locations (?)
	merge 1:1  year_id sex_id age_group_id using `draws', assert(3) nogenerate
	
	

	merge m:1 age_group_id using "`params_dir'/age_meta.dta", assert(3) nogenerate
  
  
 
  
*** PROCESS DRAWS TO IMPOSE AGE/SEX-PATTERN ON TOTAL CASE ESTIMATES ***  
	forvalues i = 0 / 999 {
		quietly {
			replace draw_`i' = draw_`i' * totalPop
			
			generate casesCurve = ageCurve_`i' * population
			replace  casesCurve = 0 if age_group_id<0.1
			bysort location_id year_id: egen totalCasesCurve = total(casesCurve)
		
			replace draw_`i' = casesCurve * (draw_`i' / totalCasesCurve) / population
			
			drop casesCurve totalCasesCurve
			}
		di "." _continue
		}


	
*** EXPORT INCIDENCE DRAWS **	
		*drop if year_id<1989
		drop if year_id>2018
		drop ageCurve_* 

	save "FILEPATH/incidence_draws/`location'.dta", replace

	}
