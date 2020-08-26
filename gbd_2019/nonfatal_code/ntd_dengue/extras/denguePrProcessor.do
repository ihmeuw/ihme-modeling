
*** SETUP LOCALS ***
	tempfile population locations level1 level2 level3 level4 level5 level1_clean level2_clean level3_clean master

	local level1_name super_region
	local level2_name region
	local level3_name country


*** GET POPULATION DATA ***
	odbc load, exec("SELECT run_id, location_id, population FROM upload_population_estimate WHERE year_id = 2010 AND age_group_id = 22 AND sex_id = 3") dsn(mortality) clear
	egen maxRun = max(run_id)
	keep if run_id==maxRun
	drop maxRun run_id
	save `population'

	
*** GET LOCATION DATA ***	
	get_location_metadata, location_set_id(22) clear
	keep location_id location_name path_to_top_parent parent_id ihme_loc_id location_type_id location_type super_region_id super_region_name region_id region_name
	
	split path_to_top_parent , parse(,) destring
	rename path_to_top_parent4 country_id
	drop path_to_top_parent?
	
	drop if location_id==1
	
	save `locations'

	
*** LOOP THROUGH LOCATION LEVELS 3 - 5 (COUNTRY - FINEST SUBNATIONAL), IMPORT ZONAL STATS OUTPUT FROM ARCGIS, CALCULATE DENGUEPR, AND PRODUCE CLEAN FILE ***	
	forvalues i = 5(-1)3 {
	  
	  import delimited FILEPATH, delimiter(comma) bindquote(strict) varnames(1) clear
	  drop rowid_
	  rename (count area sum) (denguePopCount denguePopArea denguePopSum)
	  save `level`i''
		
	  import delimited FILEPATH, delimiter(comma) bindquote(strict) varnames(1) clear
	  drop rowid_
	  rename (count area sum) (popCount popArea popSum)
	
	  merge 1:1 loc_id using `level`i'', assert(3) nogenerate
	
	  assert popCount == denguePopCount
	  assert popArea  == denguePopArea
	
	  generate denguePr = denguePopSum / popSum

	  rename loc_id location_id
	  generate level = `i'
	  
	  drop *Count *Area *Sum
	  
	  merge 1:1 location_id using `locations', assert(2 3) keep(3) nogenerate
	  
	  save `level`i'', replace
	  
	  }
	
	
*** AGGREGATE COUNTRY-LEVEL DENGUEPR ESTIMATES UP TO REGION AND SUPERREGION (LEVELS 2 & 1)	
    merge 1:1 location_id using `population', assert(2 3) keep(3) nogenerate
	generate denPop = denguePr * population
	
	forvalues i = 1/2 {
	  preserve
	
	  collapse (sum) denPop population, by(`level`i'_name'_id)
	
	  generate denguePr = denPop / population
	  drop denPop population
	
	  generate location_id = `level`i'_name'_id 
	  generate level = `i'
	
	  merge 1:1 location_id using `locations', assert(2 3) keep(3) nogenerate
	  save `level`i''
	  
	  restore
	  }
	


*** PRODUCE SUPERREGION, REGION, & COUNTRY-LEVEL FILES CONTAINING ONLY LOCATION ID AND DENGUEPR ***	
	forvalues i = 1/3 {
	  use `level`i'', clear
	  keep `level`i'_name'_id  denguePr
	  rename denguePr `level`i'_name'DenguePr
	  save `level`i'_clean'
	  }
	  

	  
*** APPEND ALL LOCATION LEVEL FILES TO PRODUCE MASTER ***	  
	clear
	append using `level1' `level2' `level3' `level4' `level5'
	
	
*** MERGE IN COUNTRY, REGION, & SUPERREGION DENGUEPRs ***	
	forvalues i = 1/3 {
	  merge m:1 `level`i'_name'_id using `level`i'_clean', assert(1 3) nogenerate
	  }

	save `master'

	
	keep if strmatch(ihme_loc_id, "KEN_*")
	merge 1:1 location_id using `population', assert(2 3) keep(3) nogenerate
	
	replace path_to_top_parent = substr(path_to_top_parent, 1, 19)
	
	generate denPop = denguePr * population
    collapse (sum) denPop population, by(*parent* country* *region*)
	generate denguePr = denPop / population
	
	rename parent_id location_id
	drop denPop population
	generate ihme_loc_id = "KEN_" + string(location_id)
	
	append using `master'
	

	merge 1:1 location_id using `locations', assert(2 3)
	replace denguePr = 0 if _merge==2
	drop _merge
	
	
*** CLEAN UP THE FILE & SAVE ***	  
	order location_id location_name location_type_id location_type ihme_loc_id path_to_top_parent country_id region_id region_name super_region_id super_region_name denguePr countryDenguePr regionDenguePr super_regionDenguePr
	sort level location_id
	
	save FILEPATH, replace
