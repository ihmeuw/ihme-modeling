
*** SET UP ENVIRONMENT, TEMPFILES & LOCALS ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		local j "J:"
		}

	
	local rootDir  FILEPATH
	local inputDir FILEPATH
	local allAge FILEPATH/allAgeEstimateTemp.dta

	tempfile nCongenital locationMeta population appendTemp congenitalPr noDataLocFile dataLocFile zeros no2zeros
	
*** LOAD SHARED FUNCTIONS ***
	adopath + FILEPATH
	adopath + FILEPATH 
	run FILEPATH/get_demographics.ado


*** START LOG ***	
	capture log close
	log using FILEPATH/congenital.smcl, replace

*** GET BIRTH SEX RATIOS ***
	use FILEPATH/births_gbd2016.dta, clear
	drop if strmatch(ihme_loc_id, "S?") | strmatch(ihme_loc_id, "R?") | strmatch(ihme_loc_id, "R1?") | strmatch(ihme_loc_id, "R2?") | ihme_loc_id=="G" | year<1980
	drop if sex_id==3 | year<1990
	bysort location_id year: egen pcBirthSex = pc(births), prop
	keep location_id year sex_id pcBirthSex
	rename year year_id
	tempfile birthSexRatio
	save `birthSexRatio'
	
*** GET CONGENITAL OUTCOMES DATA ***
	import excel `inputDir'/congenital.xls, sheet("Sheet1") firstrow clear
	drop source
	levelsof location_id, local(locations) 
	save `nCongenital'


	
*** GET LOCATION METADATA (NEED TO BE ABLE TO LINK SUBNATIONALS AND THEIR PARENT COUNTRIES) ***	
	get_location_metadata, location_set_id(8) clear 
	keep if is_estimate==1 | location_type=="admin0" | (location_id==`=subinstr("`locations'", " ", " | location_id==", .)')
	
	split path_to_top_parent, parse(,) destring gen(split)
	rename split4 country_id 
	
	keep location_id location_name country_id parent_id location_type ihme_loc_id *region* is_estimate
	
	levelsof location_id if parent_id==135, local(braSubs) clean

	save `locationMeta'
	
	
*** BRING IN POPULATION ESTIMATES ***
	levelsof location_id, local(locationList) clean
	numlist "1980(1)2016"

	get_population, location_id("`locationList'") sex_id("1 2") age_group_id("-1") year_id("`r(numlist)'") clear
	keep year_id age_group_id sex_id location_id population
	keep if !inlist(age_group_id, 22, 27, 28)
	save `population'

	use `locationMeta', clear
	merge 1:1 location_id using `nCongenital'
	keep if inlist(_merge, 2, 3) | strmatch(ihme_loc_id, "BRA*") | strmatch(ihme_loc_id, "MEX*") | strmatch(ihme_loc_id, "USA*")
	drop _merge
	levelsof location_id, local(locations) 
	
	
	

*** GET ZIKA BIRTH DRAWS ***
	local i 1
	foreach location of local locations {
		quietly {
		capture import delimited using FILEPATH/`location'.csv, clear
		if _rc==0 {
			if `i'>1 append using `appendTemp'
			save `appendTemp', replace
			local ++i
			}
		}
		di " . `location'" _continue
		}

	keep if inrange(year_id, 2014, 2017)
	collapse (sum) draw_*, by(location_id)

	egen zikaBirths = rowmean(draw_*)
	drop draw_*	
	
	
*** MERGE CONGENITAL DATA, ZIKA BIRTH ESTIMATES, & LOCATION META-DATA ***	
	merge 1:1 location_id using `nCongenital', gen(congenitalMerge)
	merge 1:1 location_id using `locationMeta', keep(3) nogenerate


	generate modLocation = location_id 
	replace  modLocation = country_id if strmatch(ihme_loc_id, "MEX*") | strmatch(ihme_loc_id, "USA*")
	bysort country_id: egen countryZikaBirths = total(zikaBirths)
	replace zikaBirths = countryZikaBirths if inlist(ihme_loc_id, "BRA", "MEX", "USA")
	set obs `=_N+1'
	
***	RUN MODEL TO ESTIMATE RATE OF CONGENITAL ZIKA SYNDROME AMONG ZIKA BIRTHS ***
	mepoisson congenital if zikaBirths>0 & ihme_loc_id!="BRA", exp(zikaBirths) || modLocation:
	
	gen touse = e(sample)
	local prCongenital = _b[_cons]
	local prCongenitalSe = _se[_cons]
	generate prCongenital = `prCongenital'
	generate prCongenitalSe = `prCongenitalSe'
	predict random, remeans reses(randomSe)

	
*** SAMPLE RANDOM EFFECTS FOR MISSING LOCATIONS ***
	sum random if touse
	local random = `r(mean)'
	local randomSe = `r(sd)'
	sum randomSe if touse
	local randomSe = sqrt(`r(sd)'^2 + `randomSe'^2)
	replace random = `random' if missing(random)
	replace randomSe = `randomSe' if missing(randomSe)

	keep location_id country_id prCongenital* random* congenital 

	merge 1:m location_id using `locationMeta', keep(1 3) nogenerate

	replace random = `random' if missing(random)
	replace randomSe = `randomSe' if missing(randomSe)
	replace prCongenital = `prCongenital' if missing(prCongenital)
	replace prCongenitalSe = `prCongenitalSe' if missing(prCongenitalSe)

	merge 1:m location_id using `population', keep(3) nogenerate
	
	keep if is_estimate==1 & inrange(age_group_id, 2, 4)
	collapse (sum) population (mean) prCongenital* random* congenital, by(location* country_id parent_id *region* ihme_loc_id year_id sex_id)
	save `congenitalPr'

	
*** GET FULL LIST OF ZIKA PREDICION LOCATIONS ***
	levelsof location_id, local(zikaLocations) clean 
	local zikaLocations: list zikaLocations | braSubs


*** CREATE ZERO DRAWS FOR NON-ZERO AGE GROUPS ***	
	clear
	get_demographics, gbd_team(epi)
	local years `r(year_ids)'
	local allLocations `r(location_ids)'

	set obs	`=`=wordcount("`r(age_group_ids)'")'+1'
	generate int age_group_id = .
	local i 1
	foreach age in `r(age_group_ids)' 164 {
		replace age_group_id = `age' in `i'
		local ++i
		}
	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1
	
	expand 6
	bysort age_group_id sex_id: generate year_id = (_n*5) + 1985
	replace year_id = 2016 if year_id==2015

	forvalues i = 0 / 999 {
		quietly generate draw_`i' = 0
		}
	

	generate measure_id = 5
	
	save `zeros'
	
	drop if age_group_id<6 | age_group_id==164
	save `no2zeros'


*** PREP POPULATION FILE ***
	get_population, location_id(`zikaLocations') year_id(`years') age_group_id(2 3 4 5) sex_id(1 2) clear
	drop process_version_map_id
	tempfile zikaPop
	save `zikaPop'
	
	get_population, location_id(`zikaLocations') year_id(`years') age_group_id(28) sex_id(1 2) clear
	drop process_version_map_id age_group_id
	rename population mergePop
	
	merge 1:m location_id year_id sex_id using `zikaPop', assert(3) nogenerate
	replace  mergePop = population if age_group_id==5
	generate ageMerge = age_group_id
	replace  ageMerge = 28 if age_group_id<5
	save `zikaPop', replace
	

*** PROCESS ZIKA BIRTH ESTIMATES TO GET CONGENITAL ZIKA SYNDROME ESTIMATES *
	foreach location of local zikaLocations {
		import delimited using FILEPATH/`location'.csv, clear
		replace age_group_id = 2
		merge m:1 location_id year_id sex_id using `congenitalPr', keep(3) nogenerate	
		merge m:1 location_id year_id sex_id using `birthSexRatio', generate(birthSexMerge)
		keep if location_id == `location'
	
		rename draw_* draw_*_2

		quietly sum draw_0_2
		local drawMean = `r(mean)'
		
		forvalues i = 0 / 999 {
			quietly {
				if `drawMean' > 0 {
					if `i'<=26 {
						replace draw_`i'_2 = congenital if !missing(congenital) & year_id>=2016
						replace draw_`i'_2 = draw_`i'_2 * exp(rnormal(prCongenital, prCongenitalSe) + rnormal(random, randomSe)) if missing(congenital) | year_id<2016
						}
					else {
						replace draw_`i'_2 = draw_`i'_2 * exp(rnormal(prCongenital, prCongenitalSe) + rnormal(random, randomSe)) 
						}
					}
				else {
					generate gammaA = rgamma(congenital, 1)
					generate gammaB = rgamma(population, 1)
					replace draw_`i'_2 =  pcBirthSex * population * gammaA / (gammaA + gammaB) if congenital>0 & !missing(congenital) & year_id>=2016
					replace draw_`i'_2 = 0 if congenital==0 | missing(congenital) | year_id<2016
					drop gammaA gammaB
					}
				bysort location_id sex_id (year_id): generate draw_`i'_5 = draw_`i'_2[_n-1]
				}
			}
			
		quietly {
			keep draw* location_id year_id sex_id
			ds draw_*_2
			reshape long `=subinstr("`r(varlist)' ", "_2 ", "_ ", .)', i(location_id year_id sex_id) j(age_group_id)
			rename draw_*_ draw_*
		
			keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')
		
			generate ageMerge = age_group_id
			replace  ageMerge = 28 if age_group_id<5
			drop age_group_id
	
			merge 1:m location_id year_id ageMerge sex_id using `zikaPop', assert(2 3) keep(3) nogenerate
		
			forvalues i = 0 / 999 {
				quietly replace draw_`i' = draw_`i' / mergePop 
				}
			}
		
		
	
		generate measure_id = 5
		
		expand 2 if age_group_id==2, gen(newObs)
		replace age_group_id = 164 if newObs==1
		drop newObs

			
		append using `no2zeros'
		replace location_id = `location' if missing(location_id)
		
		generate modelable_entity_id = 10403
		replace measure_id = 5
		export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv, replace
		}



	local nonZikaLocations: list allLocations - zikaLocations
	
	macro dir
	
	use `zeros', clear

	generate location_id = .
	generate modelable_entity_id = 10403
	replace measure_id = 5
	foreach location of local nonZikaLocations {
		quietly replace location_id = `location'
		export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv, replace
		}
		

*** SAVE RESULTS ***	
	run FILEPATH/save_results.do	
	save_results, modelable_entity_id(10403) description("Congenital Zika Syndrome") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best(no) birth_prev("yes") env("prod")

	
	
