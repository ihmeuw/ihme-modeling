
*** BOILERPLATE ***
    clear all
	set more off
	set maxvar 32000
		
	if c(os) == "Unix" {
		local j ADDRESS
		local k ADDRESS
		local h ADDRESS
		}
	else {
		local j ADDRESS
		local k ADDRESS
		local h ADDRESS
		}



	adopath + FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_cod_data.ado
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/get_crosswalk_version.ado
	
	
	local typhCid = 319
	local paraCid = 320
	
	tempfile data555 data556 ages ageSex drLocs locMeta cod
	
*** BACKUP AND CLEAR OUT OLD DATA ***
	get_epi_data, bundle_id(555) clear
	save FILEPATH, replace
	
	preserve
	keep if nid==292827
	keep seq
	if `=_N'>0 {
		export excel using FILEPATH, sheet("extraction") firstrow(variables) replace
		upload_epi_data, bundle_id(555) filepath(FILEPATH) clear
		}
	
	restore
	drop if nid==292827
	ds *, has(type string)
	foreach var of varlist `r(varlist)' {
		replace `var' = "" if trim(`var')=="."
		}	
	save `data555'
		

*** GET INCIDENCE DATASET FROM GLOBAL BUNDLE ***
	get_epi_data, bundle_id(556) clear
	ds *, has(type string)
	foreach var of varlist `r(varlist)' {
		replace `var' = "" if trim(`var')=="."
		}
	drop seq
	save `data556'
	

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_id)'", " ", ",", .)')") `shared' clear
	save `ages'
	
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `ageSex'
	

*** PULL DATA RICH LOCATION IDS ***
	get_location_metadata, location_set_id(43) gbd_round_id(6) clear
	keep if parent_id==44640
	keep location_id location_name is_estimate *region* ihme_loc_id
	levelsof location_id, local(locs)
	save `drLocs'
	
	get_location_metadata, location_set_id(35) gbd_round_id(6) clear
	keep location_id location_name is_estimate *region* ihme_loc_id
	save `locMeta'

	
*** PULL IN COD DATA ***
	local causeList typh para
	
	foreach cause of local causeList {
		get_cod_data, cause_id(``cause'Cid') decomp_step(STEP) location_id(`locs') clear
		keep if data_type=="Vital Registration"
		duplicates drop 
		drop if inlist(age_group_id, 22, 27) | sample_size<10
		generate deaths_`cause' = cf * env
		keep location_id year age_group_id sex deaths_`cause' pop sample_size nid

		
		if "`cause'"!=word("`causeList'", 1) {
			merge 1:1 location_id year age_group_id sex nid using `cod', keep(3) nogenerate
			}
		if "`cause'"!=word("`causeList'", -1) {
			save `cod', replace
			}
		}


	rename year year_id
	rename sex  sex_id



  *** DROP LOCATIONS WITH FEWER THAN 5 YEARS OF DATA ***
	bysort location_id year_id: gen yearIndex = _n
	bysort location_id: egen yearCount = total(yearIndex==1)
	drop if yearCount<5	

	

 *** COLLAPSE TO FIVE-YEAR ESTIMATES ***  
	generate yearCat = round(year_id, 5)
	replace  yearCat = 2019 if yearCat==2020
	drop if yearCat < 1990
	
	bysort location_id age_group_id sex_id yearCat: egen year_start = min(year_id)
	bysort location_id age_group_id sex_id yearCat: egen year_end = max(year_id)

	
	
*** CLEAN UP NIDS ***
	rename nid nidDetail

	bysort location_id age_group_id sex_id yearCat (year_id): gen nidIndex = nidDetail if round(_N/2)==_n
	bysort location_id age_group_id sex_id yearCat : egen long nid = mean(nidIndex)
	
	drop year_id yearCat 
	
	
*** BRING IN INCOME CATEGORY & CASE FATALITY DATA ***
	merge m:1 location_id using FILEPATH, assert(2 3) keep(3) nogenerate
	drop *region* is_estimate
	merge m:1 incomeCat age_group_id  using FILEPATH, assert(2 3) keep(3) nogenerate 
	merge m:1 age_group_id using `ages', assert(2 3) keep(3) nogenerate  
	  
  
   

 
*** CREATE CASE DRAWS BASED ON DEATHS AND CASE FATALITY DRAWS ***  
	forvalues i = 0/999 {
		quietly {
			foreach x in typh para {
				generate `x'_`i' = deaths_`x' / cf_`x'_`i'
				}
			}
		di "." _continue
		}

 



*** COLLAPSE AGE GROUPS ***
	replace age_start = floor(age_start/10) * 10 if age_start>=20
	replace age_end   = age_start + 10 if age_start>=20

	fastcollapse typh_* para_* pop sample_size, by(location_id age_start age_end sex_id year_start year_end nid) type(sum) 


	
*** MAKE FINAL DRAWS ***	
	forvalues i = 0/999 {
		quietly {
			generate inc_`i'    = (typh_`i' + para_`i') / pop
			generate prTyph_`i' = (typh_`i' / pop) / inc_`i'
			generate prPara_`i' = (para_`i' / pop) / inc_`i'
			}
		di "." _continue
		}

	foreach metric in inc prTyph prPara { 	 
		fastrowmean `metric'_*,  mean_var_name(mean_`metric')
		fastpctile `metric'_*, pct(2.5 97.5) names(lower_`metric' upper_`metric')
		egen standard_error_`metric' = rowsd(`metric'_*)
		}

	
	
*** CLEAN UP AND PUT IN EPI-UPLOADER FORMAT ***	
	keep nid location_id age_start age_end sex_id year_start year_end mean_* lower_* upper_* standard_error_* sample_size
	  
	merge m:1 location_id using `locMeta', assert(2 3) keep(3) nogenerate


	local metric inc 
	
	save FILEPATH, replace

	rename *_inc *
	generate effective_sample_size = sample_size


	generate sex = "Male" if sex_id==1
	replace  sex = "Female" if sex_id==2
	replace  sex = "Both" if sex_id==3

	generate source_type = "Vital registration - national" 

	replace nid = 292827
	generate unit_type = "Person"
	generate unit_value_as_published = 1
	generate measure_adjustment = 0
	generate uncertainty_type_value = 95
	generate urbanicity_type = "Mixed/both"
	generate representative_name = "Nationally representative only"
	replace  representative_name = "Representative for subnational location only" if strmatch(ihme_loc_id, "*_*")
	generate recall_type = "Point"
	generate extractor = USERNAME
	generate cv_diag_mixed = 0
	generate cv_passive = 0
	generate smaller_site_unit = 0
	generate sex_issue = 0
	generate year_issue = 0
	generate age_issue = 0
	generate age_demographer = 0
	generate measure = "incidence"
	generate measure_issue = 0
	generate response_rate = .

	drop *_prTyph *_prPara is_estimate *region*

	generate seq = .


	
*** UPLOAD TO EPI DATABASE ***	
	export delimited using FILEPATH
	export excel using  FILEPATH, sheet("extraction") firstrow(variables) replace

	
	upload_epi_data, bundle_id(555) filepath(FILEPATH) clear


