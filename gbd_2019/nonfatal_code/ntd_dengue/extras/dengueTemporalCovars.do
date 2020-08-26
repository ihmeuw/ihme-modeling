set more off
clear all


adopath + FILEPATH

tempfile locations population denguePr mergingTemp


*** GET LOCATION DATA ***
	odbc load, exec("SELECT location_id, parent_id, path_to_top_parent, level, is_estimate, location_name, location_type_id, location_type, region_name, ihme_loc_id FROM location_hierarchy_history WHERE location_set_id = 22 AND location_type_id NOT IN (1,6,7) AND location_set_version_id = 149") dsn(shared) clear
	save `locations'

	
*** GET POPULATION DATA ***
	odbc ADDRESS, exec("SELECT run_id, year_id AS year_start, location_id, population FROM upload_population_estimate WHERE year_id >=1980 AND age_group_id = 22 AND sex_id = 3") dsn(mortality) clear
	egen maxRun = max(run_id)
	keep if run_id==maxRun
	drop maxRun run_id
	
	merge m:1 location_id using `locations', assert(1 3) keep(3) nogenerate
	save `population'



*** LOAD DENGUE PROBABILITY DATA INTO TEMP FILE ***
	use  "FILEPATH", clear 
	drop if inlist(location_type, "global", "superregion", "region")



*** COMBINE LOCATION INFORMATION, POPULATION, AND DENGUE PR FILES ***	
	merge 1:m location_id using `population', keep(match) nogenerate
	merge 1:1 location_id year_start using "FILEPATH", nogenerate force
	

	
*** CLEAN UP A FEW VARIABLES ***	
	rename year_start year_id
	generate popRound = round(population)
	replace denguePr = 0 if (ihme_loc_id=="URY" & year_id>1996) | denguePr<0.005



*** CLEAN UP THE DATASET A BIT ***
	keep *enguePr sourceFile location_* *id *name age_start age_end mean cases sample_size year_id population
	drop modelable_entity* *nid _data_id representative_name case_name age_start age_end parent_id 
	

*** DROP OUTLIER DATA ***
    replace mean  = . if (location_id==161 & inlist(year_id, 1988, 1998))	
	replace cases = . if (location_id==161 & inlist(year_id, 1988, 1998))	
	
	
*** PREP MEAN, EXPOSURE, AND YEAR VARIABLES FOR MODELLING ***
	replace mean = cases / sample_size if !missing(cases) & !missing(sample_size) & missing(mean)
	*replace mean = 0.1 / sample_size if cases==0 
	replace mean = . if cases==0 & denguePr>0
 
	
	gen lnMean = ln(mean)
	bysort location_id: egen meanLnMean = mean(lnMean)
	egen sdLnMean = sd(lnMean)
	generate lnMeanAnom = (lnMean - meanLnMean) / sdLnMean
	generate lnMeanDif = (lnMean - meanLnMean)
	
	generate exp = sample_size
	replace  exp = population if missing(sample_size)

	quietly sum year
	generate yearZ = year_id - `r(min)'
	generate yearZsqrd = yearZ^2


	


*** CREATE DENGUE CASE REPORT SUMMARY VARIABLES BY COUNTRY, REGION, & SUPERREGION ***
	foreach geog in country region super_region {

		if "`geog'"=="country" local byVar location_id
		else local byVar `=lower("`geog'")'_id

		bysort `byVar': egen `geog'Rate = mean(mean)
		bysort `byVar': egen `geog'Sd = sd(mean)
		bysort `byVar': egen `geog'Count = count(mean)
		bysort `byVar': egen `geog'CountNonZero = total(inrange(cases,1,9999999999))
		
		}

	egen globalRate = mean(mean)
	
	replace meanLnMean = ln(regionRate) if missing(meanLnMean)
	replace meanLnMean = ln(super_regionRate) if missing(meanLnMean)
	replace meanLnMean = ln(globalRate) if missing(meanLnMean)
	

*** CREATE THE MODELLING REGION AND ISO VARIABLES ***  
	gen nonMissTemp = (countryCount>3) if year_id==2000 & countryDenguePr>0 & !inlist(region_id, 32, 42, 167, 56, 174, 192, 73, 199)
	bysort region_id: egen regionNonMissSumTemp  = total(nonMissTemp)
	bysort region_id: egen regionNonMissMeanTemp = mean(nonMissTemp)
	generate regionOk = (regionNonMissSumTemp >= 2) | (regionNonMissMeanTemp >= 0.5 & !missing(regionNonMissMeanTemp))
	bysort super_region_id: egen superOk = min(regionOk)

	generate modRegion = super_region_id
	replace  modRegion = region_id if superOk==1

	generate modIso = ihme_loc_id
	replace  modIso = substr(ihme_loc_id, 1, 3) if countryCount==0
	encode   modIso, generate(modId)

	generate modRegionTemp = super_region_name
	replace  modRegionTemp = region_name if superOk==1
	encode   modRegionTemp, generate(modRegionCode)
	drop modRegionTemp
	
	bysort modId: egen lastYearTemp = max(year_id) if !missing(mean)
	bysort modId: egen lastYear = mean(lastYearTemp)

	
	
	
*** ONTO THE MODELLING ***

	mixed lnMeanAnom i.modRegionCode##c.yearZsqrd if countryDenguePr>0 /*& location_id!=6*/ || super_region_id: R.yearZ || region_id: R.yearZ || modId: R.yearZ, nonrtolerance

	predict fixed
	predict random*, reffects
	egen random = rowtotal(random*)
	
	bysort super_region_id year_id: egen random1_full = mean(random1)
	bysort region_id year_id: egen random2_full = mean(random2)
	bysort modId year_id: egen random3_full = mean(random3)
	egen random_full = rowtotal(random*_full)
	
	egen anomaly = rowtotal(fixed random_full)
	replace anomaly = 0 if countryDenguePr==0
	
	
	
*** GENERATE OUTBREAK COVARIATE ***
	* Define an outbreak as a country/year with a random effect in top 5th percentile *
	centile random if !missing(mean) & countryDenguePr>0, centile(95)
	generate outbreak = (random >= `r(c_1)') & !missing(random3)
	
	* Since we have less data for recent years (reporting lags) we assume that the probability of an outbreak in those years equals the overall global outbreak probability *
	sum outbreak if countryDenguePr>0 & !missing(mean)
	replace outbreak = `r(mean)' if year_id>lastYear & countryDenguePr>0	
	


	gen predRate = exp(anomaly + meanLnMean)
	gen predCases = predRate * population

	bysort location_id: egen meanPredCases = mean(predCases)
	bysort location_id: egen meanPredRate = mean(predRate)
	gen temp2010 = predRate if inrange(year_id, 2005, 2015)
	bysort location_id: egen meanPredRate2010 = mean(temp2010)
	
	generate rrMeanCases = predCases / meanPredCases
	generate rrMean  = predRate / meanPredRate 
	generate rrMean2010  = predRate / meanPredRate2010
	generate rrCenter = exp(anomaly)
	

   	generate age_group_id = 22
	generate sex_id = 3

	
/*
	save FILEPATH, replace
	
	

*** PREP ANOMALY FOR EXPORT ***
	generate mean_value = anomaly 
	generate covariate_name_short = "dengueAnomalies"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace



*** PREP OUTBREAK FOR EXPORT ***
	replace mean_value = outbreak 
	replace covariate_name_short = "dengueOutbreaks"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace



*** PREP RR_MEAN FOR EXPORT ***
	replace mean_value = rrMean
	replace covariate_name_short = "dengueTrendRRs"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace
	


*** PREP RR_CENTER FOR EXPORT ***
	replace mean_value =  rrCenter
	replace covariate_name_short = "expDengueAnomalies"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace
	

*** PREP DENGUE_PR FOR EXPORT ***
	replace mean_value =  denguePr
	replace covariate_name_short = "dengue_prob"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace
	
	
	
	
	
  	sort ihme_loc_id year_id	
levelsof modIso if countryDenguePr>0, local(modIsos)	
foreach modIso of local modIsos {
  	twoway (line anomaly year_id, lcolor(gs6)) (line rrMean year_id, yaxis(2) lcolor(green)) ///
	       (scatter lnMeanAnom year_id if outbreak==0, mcolor(navy) msize(small))   ///
	       (scatter lnMeanAnom year_id if outbreak==1, mcolor(maroon) msize(medium)) ///
		   if ihme_loc_id=="`modIso'", title("`modIso'") 
	
  sleep 1250	
  graph export "FILEPATH", as(pdf) replace
  }	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
/*	
	
	preserve
	keep location_id year_id rrMean* 
	save FILEPATH, replace
	restore
	
	preserve
	rename rrMean mean_value
	generate covariate_name_short = "dengueTrendRRs"

	sort  covariate_name_short location_id year_id age_group_id sex_id mean_value
	order covariate_name_short location_id year_id age_group_id sex_id mean_value

	export delimited covariate_name_short location_id year_id age_group_id sex_id mean_value using "FILEPATH", replace

	restore




	
	

	/*	



	
	
		sort ihme_loc_id year_id	
levelsof modIso if countryDenguePr>0, local(modIsos)	
foreach modIso of local modIsos {
*local modIso BRA
  	twoway (line anomaly year_id, lcolor(gs6)) (line rrMean year_id, yaxis(2) lcolor(green)) ///
	       (scatter lnMeanAnom year_id if outbreak==0, mcolor(navy) msize(small))   ///
	       (scatter lnMeanAnom year_id if outbreak==1, mcolor(maroon) msize(medium)) ///
		   if ihme_loc_id=="`modIso'", title("`modIso'") name(anomaly, replace)
	
	twoway (line anomaly_full year_id, lcolor(gs6)) (line rrMean_full year_id, yaxis(2) lcolor(green)) ///
	       (scatter lnMeanAnom year_id if outbreak==0, mcolor(navy) msize(small))   ///
	       (scatter lnMeanAnom year_id if outbreak==1, mcolor(maroon) msize(medium)) ///
		   if ihme_loc_id=="`modIso'", title("`modIso'") name(anomaly_full, replace)
		   
	graph combine anomaly anomaly_full
  graph export "FILEPATH", as(pdf) replace
  }
  
  
  	sort ihme_loc_id year_id	
levelsof modIso if countryDenguePr>0, local(modIsos)	
foreach modIso of local modIsos {
  	twoway (line anomaly year_id, lcolor(gs6)) (line rrMean year_id, yaxis(2) lcolor(green)) ///
	       (scatter lnMeanAnom year_id if outbreak==0, mcolor(navy) msize(small))   ///
	       (scatter lnMeanAnom year_id if outbreak==1, mcolor(maroon) msize(medium)) ///
		   if ihme_loc_id=="`modIso'", title("`modIso'") 
	
  sleep 1250	
  graph export "FILEPATH", as(pdf) replace
  }

