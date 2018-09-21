
*** PULL & PROCESS DATA ON BREAKDOWN OF IMMIGRANTS BY US STATE ***
	import excel "FILEPATH\FINAL_Statistical-Portrait-of-the-Foreign-Born-2012.xlsx", sheet("13. StatebyBirth#") cellrange(A8:L67) firstrow clear
	drop if missing(Nativeborn) 
	rename A location_name
	drop D L


	foreach var of varlist Mexico Caribbean CentralAmerica SouthAmerica MiddleEast {
	  replace `var' = "" if `var'=="***"
	  destring `var', replace force
	  replace `var' = round(Foreignborn * `var'[52] / Foreignborn[52]) if missing(`var')
	  }

	drop in 52

	
	
*** CONVERT TO PROPORTIONS BY REGION IN LATIN AMERICA ***	
	foreach var of varlist Mexico Caribbean CentralAmerica SouthAmerica  {
	  egen pct`var' =  pc(`var')
	  replace pct`var' = pct`var' / 100
	  }  
	  
	replace location_name = trim(location_name)  
	compress  

	tempfile usa
	save `usa'


	
*** GET LOCATION METADATA AND MERGE TO IMMIGRANT PROPORTIONS FROM ABOVE ***	
	get_location_metadata,  location_set_id(8) clear
	keep if parent_id==102
	keep location_id location_name ihme_loc_id
	 
	merge 1:1 location_name using `usa', assert(1 3) keep(3) nogenerate
 
 
 
*** CLEAN UP AND SAVE *** 
	keep location* pct* ihme_loc_id
	reshape long pct, i(location* ihme_loc_id) j(mergeRegion) string

	save FILEPATH\immigrantsByState.dta, replace
