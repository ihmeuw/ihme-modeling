
* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os)=="Unix" {
	local  = "FILEPATH"
	set odbcmgr ADDRESS
	}
  else {
	local  = "FILEPATH"
	}

  	run "FILEPATH"
	run "FILEPATH"
	run "FILEPATH"
    run "FILEPATH"
	run "FILEPATH"

	
	
* ESTABLISH LOCALS AND DIRECTORIES *	

  tempfile dengue covariates locMeta pop cumulativePop deaths income


* BRING IN DENGUE NOTIFICATION DATA *
  import excel "FILEPATH", sheet("extraction") firstrow
  keep if strmatch(lower(source_type), "case notifications*")==1 
  
  
* CLEAN UP AGE-SPECIFIC DATA POINTS (MODEL IS BASED ON ALL-AGE, BOTH-SEX DATA) *  
  bysort location_id year_start: egen anyComplete = max(age_start==0 & age_end>=99 & sex=="Both")
  drop if anyComplete==1 & (age_start!=0 | age_end<99 | sex!="Both")
  
  * Where we have all-age, sex-sepcific data points for a location/year, combine them to be both sex and drop all other points *
  bysort location_id year_start: egen anyCompleteMales = max(age_start==0 & age_end>=99 & sex=="Male")
  bysort location_id year_start: egen anyCompleteFemales = max(age_start==0 & age_end>=99 & sex=="Female")
  gen anyCompleteBySex = anyCompleteMales==1 & anyCompleteFemales==1
  bysort location_id year_start age_start age_end: egen totalCases = total(cases)
  bysort location_id year_start age_start age_end: egen totalSS = total(sample_size)
  replace cases = totalCases if anyCompleteBySex == 1
  replace sample_size = totalSS if anyCompleteBySex == 1
  drop if anyCompleteBySex==1 & (age_start!=0 | age_end<99 | sex=="Female")
  replace sex = "Both" if anyCompleteBySex==1
  replace mean = cases / sample_size if anyCompleteBySex==1
  foreach var of varlist lower upper standard_error effective_sample_size {
    replace `var' = .  if anyCompleteBySex==1
	}
	
  drop anyComplete* totalCases totalSS
	
  * Where we have both sex, age-sepcific data points for a location/year, combine them to be all age and drop all other points *
  bysort location_id year_start sex (age_start): gen ageGap = age_start - age_end[_n-1]
  replace ageGap = 0 if age_start<=1
  bysort location_id year_start: egen maxGap = max(ageGap>1)
  drop if maxGap == 1
  
  bysort location_id year_start: egen totalCases = total(cases)
  bysort location_id year_start: egen totalSS = total(sample_size)
  replace cases = totalCases if age_start>0 | age_end<99
  replace sample_size = totalSS if  age_start>0 | age_end<99
  replace sex = "Both" if age_start>0 | age_end<99
  replace mean = cases / sample_size if age_start>0 | age_end<99
  foreach var of varlist lower upper standard_error effective_sample_size {
    replace `var' = .  if age_start>0 | age_end<99
	}
  

  * Drop duplicates *
  bysort location_id year_start (mean): gen keep = _N==_n
  keep if keep==1
  
  * Clean up *
  rename year_start year_id
  keep location_id year_id mean lower upper cases *sample_size
  
  append using "FILEPATH"
  append using "FILEPATH"
  
  replace year_id = year_start if missing(year_id)
  drop year_start location_name upper lower
  
  replace sample_size = effective_sample_size if missing(sample_size)
  replace effective_sample_size = sample_size if missing(effective_sample_size)
  replace mean = cases / sample_size if missing(mean)
  
  
  bysort location_id year_id (mean): gen keep = _n==_N
  keep if keep==1
  drop keep
  
  
  save `dengue'
 
 
 
 
 
 

* BRING IN AND MERGE COVARIATE DATA *
  
  foreach covar in dengue_prob haqi sdi { //dengueAnomalies {
    get_covariate_estimates, covariate_name_short("`covar'") clear
    rename mean_value `covar' 
	keep `covar' location_id year_id 
	keep if year_id>=1980
	if "`covar'"!="dengue_prob" merge 1:1 location_id year_id using `covariates', nogenerate
	save `covariates', replace
	}

	
* GET LOCATION METADATA *  
  get_location_metadata, location_set_id(8) clear
  generate iso3 = ihme_loc_id 
  split path_to_top_parent, parse(,) gen(pathTemp) destring
  rename pathTemp4 country_id
  keep iso3 ihme_loc_id location_id location_name location_type country_id *region* is_estimate
  save `locMeta'
  
  levelsof location_id if is_estimate==1 | location_type=="admin0", local(locations) clean

  

  
  
  
  * BRING IN MORTALITY & POPULATION ESTIMATES *
  get_demographics, gbd_team(cod) clear
  get_model_results, gbd_team(cod) model_version_id(ADDRESS) location_id(`locations') age_group_id(`r(age_group_ids)') clear
  tempfile deathRaw
  save `deathRaw'
  
  get_model_results, gbd_team(cod) model_version_id(ADDRESS) location_id(`locations') age_group_id(`r(age_group_ids)') clear
  append using `deathRaw'
  
  rename mean_death deaths
  replace deaths = 0 if missing(deaths)
  
  collapse (sum) deaths population, by(location_id year_id)
  generate deathRateRaw = deaths/population
  keep location_id year_id deathRateRaw
  save `deathRaw', replace
  
  */
  get_demographics, gbd_team(cod) clear
  get_outputs, topic(cause) cause_id(357) age_group_id(22) sex_id(3) year_id(`r(year_ids)') location_id(`locations') measure_id(1) metric_id(3) gbd_round_id(4) version(latest)  clear
  rename val deathRate
  keep location_id year_id deathRate

  
  merge 1:1 location_id year_id using `deathRaw', gen(deathMerge)
  tempfile deaths
  save `deaths'
  
  
  get_demographics, gbd_team(cod) clear
  get_population, age_group_id(`r(age_group_ids)') location_id(`locations') year_id(`r(year_ids)') sex_id(3) clear
  drop sex_id process
  preserve
  
  bysort location_id year_id (age_group_id): gen cumulativePop = sum(population)
  drop population
  reshape wide cumulativePop, i(location_id year_id) j(age_group_id)
  save `cumulativePop'
  
  restore
  collapse (sum) population, by(location_id year_id) fast
  merge 1:1 location_id year_id using `deaths', assert(3) nogenerate
 * replace deaths = 0 if missing(deathRate)
*  generate deathRate = deaths / population
  tempfile deaths
  save `deaths'


  
  
  

  
  
* COMBINE DATASETS *
  use `locMeta', clear
  merge 1:m location_id using `covariates', generate(locCovarMerge) //keep(3) nogenerate
  merge 1:1 location_id year_id using `deaths', generate(deathMasterMerge)
  merge 1:1 location_id year_id using `cumulativePop', generate(cumulativePopMerge)
  merge 1:1 location_id year_id using `dengue', generate(masterDengueMerge) //assert(1 3) nogenerate

  keep if is_estimate==1 | location_type=="admin0"
  drop *Merge

  

  merge 1:1 location_id year_id using FILEPATH, assert(1 3) nogenerate

  merge 1:1 location_id year_id using `'FILEPATH(trendMerge)
  
  merge m:1 year_id using FILEPATH, assert(3) nogenerate	

	
* PREP DATA FOR MODELLING *

  rename dengue_prob denguePr
  replace denguePr = 0 if missing(denguePr) & region_id==73
  
  
  gen sqrtMRRaw = sqrt(deathRateRaw)
  bysort iso3: egen meanMRRaw = mean(deathRateRaw)
  bysort iso3: egen meanSqrtMRRaw = mean(sqrtMRRaw)
  quietly sum meanSqrtMRRaw if meanSqrtMRRaw >0
  gen normSqrtMRRaw = (meanSqrtMRRaw) / r(max)  //this normalizes and adds an offset to allow for log conversion below
  gen lnNormSqrtMRRaw = ln(normSqrtMRRaw)

  
  *pca denguePr lnNormSqrtMR if denguePr>0
  pca denguePr normSqrtMRRaw if denguePr>0
  predict scoreRaw
  
  pca denguePr lnNormSqrtMRRaw if denguePr>0
  predict scoreRaw2
  
  gen sqrtMR = sqrt(deathRate)
  bysort iso3: egen meanMR = mean(deathRate)
  bysort iso3: egen meanSqrtMR = mean(sqrtMR)
  quietly sum meanSqrtMR if meanSqrtMR >0
  gen normSqrtMR = (meanSqrtMR) / r(max)  //this normalizes and adds an offset to allow for log conversion below
  gen lnNormSqrtMR = ln(normSqrtMR)

  
  *pca denguePr lnNormSqrtMR if denguePr>0
  pca denguePr normSqrtMR if denguePr>0
  predict score 
  
  pca denguePr lnNormSqrtMR if denguePr>0
  predict score2 
  
  
  

  gen yearC = year-1996.5
  generate yearWindow = round(year_id, 5)
  replace  yearWindow = 2016 if yearWindow==2015
  
  generate sampleM = sample_size
  replace  sampleM = 1 if missing(sample_size)
  
  generate casesM = cases
  replace  casesM = 0.0001 if cases==0 & denguePr>0
  replace  casesM = 0 if denguePr==0
  
  generate meanM = casesM / sampleM
  
    
  gen lnrrMean = ln(rrMean)
  


  
  gen problemNid = 0
  foreach i of local problemNids {
    replace problemNid = 1 if location_id==`i'
    }

save FILEPATH, replace	


	import delimited FILEPATH, clear case(preserve)
	drop if modelled==1
	drop efTemp modelled ihme_loc_id
	save `efToan'

	use FILEPATH, clear
	keep location_id year_id population
	tempfile pop
	save `pop'
	
	use FILEPATH, clear
	drop efTotal - childrenonly population
	capture drop _merge
	
	merge 1:1 location_id year_id using `pop', assert(3) nogenerate

	merge 1:m location_id year_id using `efToan'
	capture drop _merge

	drop is_estimate
	
	save `data'

	get_location_metadata, location_set_id(35) clear
	merge 1:m location_id using `data', keep(1 3)	
	assert is_estimate==0 if _merge==1
	drop if _m==1


	*gen lnTrend = ln(rrMean2010)
	gen lnTrend = ln(rrMean)
	gen lnMr = ln(deathRate)


	save FILEPATH, replace


/*

* PREP AGE DISTRIBUTION DATASET *

  use `deathMaster', clear
  expand 2 if age_group_id==4, gen(new)
	replace age_group_id = 2 if new==1
	drop new
  expand 2 if age_group_id==4, gen(new)
	replace age_group_id = 3 if new==1
	drop new
  
  merge m:1 age_group_id sex_id using FILEPATH, nogenerate
  save FILEPATH, replace



