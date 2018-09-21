

**Stata Set Up**
clear all
set more off, perm
set maxvar 10000

if c(os) == "Unix" {
  local j "/home/j"
  set odbcmgr unixodbc
  }
else if c(os) == "Windows" {
  local j "J:"
  }



*** ESTABLISH TEMPFILES & FILEPATHS ***	
	local inputDir FILEPATH
	local inFile   `inputDir'/incidence.xlsx 
	local ageFile  `inputDir'/ageDistribution.dta
	local pregFile `inputDir'/prPreg.dta
	local outcomes `inputDir'/outcomes.dta
	local efFile   `inputDir'/reportingRates.dta
	
	tempfile allAge incidence locations master age_specific_pop all_age_pop dataFull


*** LOAD SHARED FUNCTIONS ***
	adopath + FILEPATH
	adopath + FILEPATH
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_population.ado
	
	

*** CREATE ZIKA ENDEMICITY LOCAL ***
	local zCountries 10 11 21 21 27 97 102 111 118 125 129 130 133 135 136 190 216 385 422 532 4751 4754 4755 4756 4758 ///
	4759 4761 4764 4766 4767 4768 4769 4774 4776 105 107 108 109 110 112 113 114 115 116 117 119 121 122 123 126 127 128 131 132
			 

	
*** PULL IN INCIDENCE DATA ***
	import excel using "`inFile'", sheet("Sheet1") firstrow clear
	levelsof location_id, local(dataLocs) sep(" | location_id==") clean
	preserve
	
	
*** GET LOCATION METADATA FOR ALL ESTIMATION LOCATIONS, COUNTRIES, AND LOCATIONS WITH INCIDENCE DATA ***	
	get_location_metadata, location_set_id(8) clear 
	keep if is_estimate==1 | location_type=="admin0" | (location_id==`dataLocs')
	keep location_id location_name parent_id location_type ihme_loc_id *region* is_estimate
	generate countryIso = substr(ihme_loc_id, 1, 3)
	tempfile locationTemp
	save `locationTemp'
	
*** COMBINE INCIDENCE & LOCATION METADATA ***	
	restore
	
	merge m:1 location_id using `locationTemp', keep(1 3) nogenerate
	generate year_id = ceil((year_start + year_end)/2)
	save `dataFull'
	
	keep if year_start>=2015
	levelsof location_id, local(locations) clean
	tempfile recent
	save `recent'
	
	
*** PULL ALL AGE/BOTH SEX POPULATION ESTIMATES & MERGE WITH 2015-2016 OUTBREAK DATA ***	
	get_population, location_id(`locations') year_id(2015 2016) sex_id(3) age_group_id(22) clear
	keep location_id year_id population
	
	merge 1:1 location_id year_id using `recent', keep(2 3) nogenerate
	
	replace sample_size = population if missing(sample_size) | sample_size==0 | location_id==69
	bysort location_id: egen meanPop = mean(sample_size)
	replace sample_size = meanPop if missing(sample_size)
	
	merge m:1 location_id using `locationTemp', keep(1 3) nogenerate
	
	bysort location_id (year_start year_end): gen temp_confirmed = value_autochthonous_confirmed - value_autochthonous_confirmed[_n-1] if year_start==2015 & year_end==2016
	replace value_autochthonous_confirmed = temp_confirmed if !missing(temp_confirmed)
	bysort location_id (year_start year_end): gen temp_suspected = value_autochthonous_suspected - value_autochthonous_suspected[_n-1] if year_start==2015 & year_end==2016
	replace value_autochthonous_suspected = temp_suspected if !missing(temp_suspected)
	
	save `recent', replace
	
	
*** CLEAN UP SUBNATIONAL DATA FOR BRA & MEX ***	
	foreach iso in BRA MEX {
		use `recent', clear
		keep if ihme_loc_id == "`iso'"
		tempfile `iso' `iso'Full
		save ``iso'Full'
	
		keep year_id value_autochthonous_confirmed value_autochthonous_suspected value_imported
		save ``iso''
	
		use `recent', clear
		keep if strmatch(ihme_loc_id, "`iso'_*")
		egen allCases = rowtotal(value_autochthonous_confirmed value_autochthonous_suspected)
		egen pcCases = pc(allCases), prop
		drop allCases value_autochthonous* year*
	
		cross using ``iso''
		replace value_autochthonous_confirmed = value_autochthonous_confirmed * pcCases
		replace value_autochthonous_suspected = value_autochthonous_suspected * pcCases
		replace value_imported = value_imported * pcCases
		append using ``iso'Full'
		save ``iso'Full', replace
		}
	
	
	use `dataFull'
	drop if regexm(ihme_loc_id, "BRA") | regexm(ihme_loc_id, "MEX")
	append using `MEXFull'
	append using `BRAFull'
	
	
	drop if missing(location_id)
	
	
*** COLLAPSE AGE-SPECIFIC DATA TO ALL AGES ***
	collapse (sum) value_case value_autochthonous_confirmed value_autochthonous_suspected value_imported sample_size, by(location_id year_id ihme_loc_id)
	generate age_start = 0
	generate age_end   = 99
	generate sex       = 3
	
	
	egen cases = rowtotal(value_autochthonous_confirmed value_autochthonous_suspected)
	replace cases = value_case if missing(value_autochthonous_confirmed) & missing(value_autochthonous_suspected) & missing(value_imported)
	replace cases = value_case if cases==0 & value_case>0 & !missing(value_case)
	rename value_imported imported
	
	gen iso3 = substr(ihme_loc_id, 1, 3)
	bysort iso3 year_id: egen countryCases = total(cases)
	
	
	keep cases imported countryCases location_id year_id sample_size 
	save `incidence'
	
	
	
* CREATE MASTER SKELETON *
  * Pull locations *
	use `locationTemp', clear
	
		
  * Flag endemic locations *
	generate endemic = 0
	foreach i of local zCountries {
		quietly replace endemic = 1 if location_id==`i'
		}

		
	save `locationTemp', replace
	

  * Make dataset of years *	
	get_demographics, gbd_team(epi) clear
	
	clear
	set obs `=`=word("`r(year_ids)'", -1)'-1979'
	generate year_id = _n + 1979	

  * Cross to make master *	
	cross using `locationTemp'
	save `master'

	
	
	
*** BRING IN POPULATION ESTIMATES ***
	levelsof location_id, local(locationList) clean
	levelsof year_id, local(yearList) clean

	get_population, location_id("`locationList'") sex_id("1 2") age_group_id("-1") year_id("`yearList'") clear
	keep year_id age_group_id sex_id location_id population
	
	
	preserve
	keep if !inlist(age_group_id, 22, 27, 28)
	rename population pop_age_specific
	save `age_specific_pop'

	restore
	keep if age_group_id==22 
	fastcollapse population, by(year_id location_id) type(sum)
	rename population pop_all_age
	save `all_age_pop'

	
	
*** MERGE EVERYTHING TOGETHER ***
	*use `master', clear
	merge 1:1 location_id year_id using `master', assert(3) nogenerate
	merge 1:1 location_id year_id using `incidence', keep(1 3) nogenerate
	
	merge m:1 countryIso using `efFile', keep(1 3) nogenerate 


*** CLEAN UP MISSING OR EXTREME EXPANSION FACTORS ***	
	gen ef = (efAlpha + efBeta) / efAlpha
	gen hasEf = !missing(ef)
	bysort region_name year_id hasEf: gen count = _N
	bysort region_name year_id hasEf (ef): gen index = _n
	gen mark = index==round(count/2)
	sum ef if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
		local caribbeanEf = `r(mean)'
	sum efAlpha if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
		replace efAlpha = `r(mean)' if region_name=="Caribbean" & ef>`caribbeanEf'
	sum efBeta if year_id==2016 & region_name=="Caribbean" & hasEf==1 & mark==1, d
		replace efBeta = `r(mean)' if region_name=="Caribbean" & ef>`caribbeanEf'
	
	foreach parm in Alpha Beta {
		quietly {
			bysort region_id: egen meanEf`parm' = mean(ef`parm')
			replace ef`parm' = meanEf`parm' if missing(ef`parm')
			drop meanEf`parm'
			}
		}
	
	
*** RESOLVE MISSING DATA FOR NON-ENDEMIC COUNTRIES ***	
	replace cases = 0 if missing(cases)
	replace countryCases = 0 if missing(countryCases)
	
	replace sample_size = pop_all_age if missing(sample_size) | sample_size==0 | location_id==69
	
	replace endemic = 1 if cases>0 & !missing(cases)
	
	
*** EXPORT ALL-AGE DATA FILE ***
	preserve
	keep location_id year_id pop_all_age endemic cases imported countryCases sample_size efAlpha efBeta
	save FILEPATH/allAgeEstimateTemp.dta, replace
	
	
	

*** EXPAND TO AGE-SPECIFIC AND EXPORT ***	
	restore
	
	cross using `ageFile' 
	merge 1:1 location_id year_id age_group_id sex_id using `age_specific_pop', assert(2 3) keep(3) nogenerate
	
	merge m:1 location_id year_id age_group_id sex_id using `pregFile', keep(1 3) nogenerate
	
	bysort year_id region_id age_group_id sex_id: egen pregMean = mean(prPreg)
	replace prPreg = pregMean if missing(prPreg) & sex==2 & inrange(age_group_id, 7, 15)
	drop pregMean
	
	save FILEPATH/masterEstimateTemp.dta, replace
	
	keep location_id year_id sex_id age_group_id ageSexCurve ageSexCurveSe pop_age_specific prPreg
	replace prPreg = 0 if missing(prPreg)
	save FILEPATH/ageSpecificTemp.dta, replace
	
	
