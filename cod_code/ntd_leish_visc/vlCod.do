

*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "/home/j"
		}
	else if c(os) == "Windows" {
		local j "J:"
		}

	adopath + FILEPATH	
	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model.ado
	run FILEPATH/process_predictions.ado
	run FILEPATHget_population.ado


*** CREATE THE DATASET ***
	local cause_id 347
	local covariate_ids 31 33 57 66 79 107 108 109 114 142 159 160 192 211 212 213 881 1069 1089 1099 1150 1170 1173 44 45 208
	local saveto FILEPATH/data/vl.dta
	local minAge 4

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids')  saveto(`saveto', replace) outliers clear



*** GET GEOGRAPHIC RESTRICTIONS ***	
	import delimited using "FILEPATH/leish_v_cc.csv", clear
	drop cause_id 
	drop if year_id<1980
	tempfile gr
	save `gr', replace		
	

	
*** GET INCIDENCE DATA ***	
	use "FILEPATH/bestCombined.dta", clear
	tsset location_id year_id
	tssmooth ma meanSmooth = mean, window(1 1 1)
	tempfile inc
	save `inc'
	
	levelsof location_id, local(locations) clean
	levelsof year_id, local(years) clean

	get_population, location_id(`locations') age_group_id(`ages') year_id(`years') sex_id(1 2) clear
	keep location_id year_id sex_id population
	
	merge m:1 location_id year_id using `inc', assert(3) nogenerate
	merge m:1 location_id year_id using `gr', gen(grMerge)

	generate vl_inc = meanSmooth
	generate vl_cases = vl_inc * population
	drop population
	
	save `inc', replace

	
***	GET RAW COD DATA ***
	import delimited using FILEPATH/leish_deaths_metdata.csv, clear
	keep if cause_id==347
	drop v1 cause_id
	rename source cod_source_label
	
	preserve
	drop if inlist(age_group_id, 22, 27)
	tempfile raw
	save `raw'
	
	restore 
	keep if age_group_id==22
	drop age_group_id
	rename cf_* cf_*_22
	
	merge 1:m year_id location_id sex_id nid data_type_id cod_source_label site using `raw', assert(2 3) nogenerate
	save `raw', replace
	
	
*** CREATE NEW VARIABLES AS NEEDED AND SAVE TO DATASET ***
	use `saveto', clear
	replace site = "" if site=="."
	merge m:1 location_id year_id sex_id using `inc', keep(3) nogenerate
	merge 1:1 location_id year_id age_group_id sex_id nid site cod_source_label using `raw'



*** FLAG ENDEMIC LOCATIONS ***	
	gen endemic = grMerge==1	
	replace endemic=0 if strmatch(ihme_loc_id, "GBR*")
	replace endemic=1 if inlist(location_id, 43874, 43875, 43882, 43883, 43884, 43885, 43886, 43890, 43899, 43900, 43904, 43905, 43906, 43910, 43911, 43918, 43919, 43920, 43921, 43922, 43926, 43935, 43936, 43940, 43941, 43942)
	



*** MODEL AGE PATTERN ***
	preserve


	gen logitPcByAge = logit(deaths/deaths_22)
	mixed logitPcByAge i.sex_id i.age_group_id || super_region_id:  R.age_group_id || region_id: R.age_group_id
	predict fixedAge
	predict fixedAgeSe, stdp

	predict randomAgeMean*, reffects
	predict randomAgeSe*, reses
		
	foreach rVar of varlist randomAgeMean* {
		quietly {
			sum `rVar' if e(sample)
			replace `rVar' = `r(mean)' if missing(`rVar')
			local seTemp = `r(sd)'
			sum `=subinstr("`rVar'", "Mean", "Se", .)' if e(sample)
			replace `=subinstr("`rVar'", "Mean", "Se", .)' = sqrt(`r(mean)'^2 + `seTemp'^2) if missing(`=subinstr("`rVar'", "Mean", "Se", .)')
			}
		}
		
	egen randomAgeFull = rowtotal(randomAgeMean?)
	gen randomAgeSe = sqrt(randomAgeSe1^2 + randomAgeSe2^2)

	keep location_id year_id sex_id age_group_id fixedAge fixedAgeSe randomAgeFull randomAgeSe
	duplicates drop location_id year_id sex_id age_group_id, force

	save FILEPATH/codAgeForEpi.dta, replace

	gen predAge = invlogit(fixedAge + randomAgeFull)
	 
	keep location_id year_id sex_id age_group_id predAge 


	bysort location_id year_id sex_id: egen totalPredAge = total(predAge)
	replace predAge = predAge / totalPredAge
	drop totalPredAge

	save FILEPATH/codAgePattern.dta, replace

	restore

	
*** PREP DATA FOR MODELLING ***	
	merge m:1 location_id year_id sex_id age_group_id using FILEPATH, assert(3) nogenerate

	rename   vl_cases vl_cases_22
	generate vl_cases = vl_cases_22 * predAge


	
	

*** RUN AND PROCESS MODEL ***	
	run_best_model mixed logitCfr i.age_group_id sex_id, difficult reffects(|| super_region_id: sex_id || region_id: || country_id: R.age_group_id)

	local description "Age-specific cfr (from ST-GRP) with fixed and random effects on age @ country (new extreme approach using non-zero raw data sources (VR exception) with 347 data)"
	process_predictions `cause_id', link(logit) random(yes) min_age(`minAge') description("`description'") multiplier(vl_cases) endemic(endemic) saving_cause_ids(347 348)
		
	
	
