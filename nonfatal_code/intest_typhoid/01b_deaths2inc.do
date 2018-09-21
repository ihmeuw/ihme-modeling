* do FILEPATH/deaths2inc.do

*** BOILERPLATE ***
    clear all
	set more off
	set maxvar 32000
		
	if c(os) == "Unix" {
		local j FILEPATH
		}
	else {
		local j FILEPATH
		}


	adopath + FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_cod_data.ado
	run FILEPATH/get_epi_data.ado
	run FILEPATH/upload_epi_data.ado
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/create_connection_string.ado
	
	tempfile data556 ages ageSex drLocs cod
	
*** BACKUP AND CLEAR OUT OLD DATA ***
	get_epi_data, bundle_id(555) clear
	save FILEPATH/555_backup_`=subinstr(trim("`c(current_date)'"), " ", "_", .)'_`=subinstr("`c(current_time)'", ":", "_", .)'.dta, replace
	keep seq
	if `=_N'>0 {
	ds *, has(type string)
	foreach var of varlist `r(varlist)' {
		replace `var' = "" if trim(`var')=="."
		}		upload_epi_data, bundle_id(555) filepath(FILEPATH/clear555.xlsx) clear
		}
		
	
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
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	save `ages'
	
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `ageSex'
	

*** PULL DATA RICH LOCATION IDS ***
	get_location_metadata, location_set_id(41) clear
	keep location_id location_name is_estimate *region* ihme_loc_id
	save `drLocs'
	

*** PULL IN COD DATA ***
	get_cod_data, cause_id(319) clear
	merge m:1 location_id using `drLocs', keep(3) nogenerate
	keep if data_type=="Vital Registration"
	keep location_id location_name is_estimate *region* ihme_loc_id year age_group_id sex pop env cf sample_size nid
	generate deaths_typh = cf * env
	save `cod'

	get_cod_data, cause_id(320) clear
	merge m:1 location_id using `drLocs', keep(3) nogenerate
	keep if data_type=="Vital Registration"
	keep location_id year age_group_id sex pop env cf sample_size nid
	generate deaths_para = cf * env

	merge 1:1 location_id year age_group_id sex using `cod', keep(3) nogenerate

	drop if inlist(age_group_id, 22, 27) | sample_size<10
	rename year year_id
	rename sex  sex_id



  
 

*** DROP LOCATIONS WITH FEWER THAN 5 YEARS OF DATA ***
	bysort location_id year_id: gen yearIndex = _n
	bysort location_id: egen yearCount = total(yearIndex==1)
	drop if yearCount<5	

	
*** COLLAPSE TO FIVE-YEAR ESTIMATES ***  
	replace year_id = round(year_id, 5)
	replace year_id = 2016 if year_id==2015
	drop if year_id < 1990

	fastcollapse deaths_* sample_size pop env, by(location_* ihme_loc_id age_group_id sex_id year_id nid) type(sum) 

  
*** BRING IN INCOME CATEGORY & CASE FATALITY DATA ***
	merge m:1 location_id using FILEPATH/submit_split_data.dta, assert(2 3) keep(3) nogenerate
	drop *region* is_estimate
	merge m:1 incomeCat age_group_id  using FILEPATH/cfDrawsByIncomeAndAge.dta, assert(2 3) keep(3) nogenerate 
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
	replace age_start = floor(age_start/10) * 10 if inrange(age_start, 20, 45)
	replace age_end   = age_start + 10 if inrange(age_start, 20, 45)
	replace age_start = 50 if age_start >= 50
	replace age_end   = 99 if age_start >= 50

	fastcollapse typh_* para_* pop sample_size, by(location_id age_start age_end sex_id year_id nid) type(sum) 


	
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

	save FILEPATH/dataRichCodDataDraws.dta, replace
	
	
*** CLEAN UP AND PUT IN EPI-UPLOADER FORMAT ***	
	keep nid location_id age_start age_end sex_id year_id mean_* lower_* upper_* standard_error_* sample_size
	  
	merge m:1 location_id using `drLocs', assert(2 3) keep(3) nogenerate


	local metric inc 
	bysort region_id sex: egen regionMean = mean(mean_`metric')
	bysort region_id  sex: egen regionSd = sd(mean_`metric')
	gen regionZ = (mean_`metric' - regionMean) / regionSd
	bysort region_id sex age_start: egen regionAgeMean = mean(mean_`metric')
	bysort region_id  sex age_start: egen regionAgeSd = sd(mean_`metric')
	gen regionAgeZ = (mean_`metric' - regionAgeMean) / regionAgeSd 



	rename *_inc *
	generate effective_sample_size = sample_size

	rename year_id year_start
	generate year_end = year_start

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
	generate extractor = "NAME"
	generate is_outlier = regionAgeZ>2 & regionZ>2
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

	append using `data556'

	
*** UPLOAD TO EPI DATABASE ***	
	export excel using FILEPATH/cod2epiInc.xlsx, sheet("extraction") firstrow(variables) replace
	upload_epi_data, bundle_id(555) filepath(FILEPATH/cod2epiInc.xlsx) clear



