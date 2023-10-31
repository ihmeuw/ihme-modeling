
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


	local crosswalk_version_id 1268
	local step STEP

	adopath + FILEPATH
	
	tempfile lit ageGroups ageSex drLocs cod
	
	* set bundle and step
	local bundle_id 3023
	local decomp_step STEP

	
	
*** BACKUP AND CLEAR OUT OLD DATA ***
	local infile FILEPATH
	import excel "`infile'", clear firstrow
	save `lit'
	
	get_crosswalk_version, crosswalk_version_id(`crosswalk_version_id') clear
	
	
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
	save `ageGroups'
	
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `ageSex'
	

*** PULL DATA RICH LOCATION IDS ***
	get_location_metadata, location_set_id(43) gbd_round_id(6) clear
	keep if parent_id==44640
	keep location_id location_name is_estimate *region* ihme_loc_id
	levelsof location_id, local(locs)
	save `drLocs'
	

*** PULL IN COD DATA ***
	get_cod_data, cause_id(959) decomp_step(`step') location_id(`locs') clear
	keep if data_type=="Vital Registration"
	duplicates drop 
	
	merge m:1 location_id using `drLocs', keep(3) nogenerate


	keep location_id location_name is_estimate *region* ihme_loc_id year age_group_id sex pop env cf sample_size nid
	drop if inlist(age_group_id, 22, 27) | sample_size<10

	generate deaths = cf * env

	rename year year_id
	rename sex  sex_id



*** DROP LOCATIONS WITH FEWER THAN 5 YEARS OF DATA ***
	bysort location_id year_id: gen yearIndex = _n
	bysort location_id: egen yearCount = total(yearIndex==1)
	drop if yearCount<5	

	tempfile cod
	save `cod'

	levelsof location_id, local(locs)
	levelsof age_group_id, local(ages)
	levelsof year_id, local(years)
	
	
*** PULL IN HIV PREVALENCE DRAWS ***
	get_covariate_estimates, covariate_id(49) location_id(`locs') year_id(`years') age_group_id(`ages') decomp_step(`step') clear 
	
	
		
	merge 1:m location_id year_id age_group_id sex_id using `cod', assert(1 3) keep(3) nogenerate
	save `cod', replace
	
*** BRING IN CASE FATALITY DATA ***

	import delimited using FILEPATH, clear case(preserve)
	keep if estPrHiv==0
	
	merge 1:m location_id year_id age_group_id using FILEPATH, gen(rrMerge) 
	merge 1:m location_id year_id age_group_id sex_id using `cod', assert(1 3) keep(3) nogenerate



	
*** CONVERT iNTS DEATHS NOT ATTRIBUTABLE TO HIV TO ALL iNTS CASES ***
 	
	generate sigma = (upper_value - lower_value) / ( 2 * invnormal(0.975))
	generate alpha = mean_value * (mean_value - mean_value^2 - sigma^2) / sigma^2
	generate beta  = alpha * (1 - mean_value) / mean_value 
	
	replace  alpha = mean_value * 0.999e+8 if beta>0.999e+8
	replace  beta  = 0.999e+8 if beta>0.999e+8
	
	
	gen hivPafDirect =  (mean_value * (exp(lnHivRrMean) - 1)) / (mean_value * (exp(lnHivRrMean) - 1) + 1)
	gen noHivCasesDirect = (deaths / invlogit(logitPred))
	gen casesDirect = (deaths / invlogit(logitPred)) / (1 - hivPafDirect)
	gen cfDirect = invlogit(logitPred)
	
	forvalues i = 0/999 {
		quietly {
			local cf_random = rnormal(0, 1)
			local hiv_random = rnormal(0, 1)
		
			generate gammaA = rgamma(alpha, 1)
			generate gammaB = rgamma(beta, 1)
			
			generate hivTemp = gammaA / (gammaA + gammaB)
			replace hivTemp = rnormal(mean_value, sigma) if missing(hivTemp)
			replace  hivTemp = 0 if upper_value==0 | hivTemp<0			
			replace  hivTemp = 1 if lower_value==1 | (hivTemp>1 & !missing(hivTemp))

				
			generate rrTemp = exp(lnHivRrMean + (lnHivRrSe * `hiv_random'))
			replace  rrTemp = 1 if rrTemp < 1
				
			generate hivPafTemp = (hivTemp * (rrTemp - 1)) / (hivTemp * (rrTemp - 1) + 1)
					
			generate cfTemp = invlogit(logitPred + (logitPredSeSm * `cf_random'))
		
			generate cases_`i' = (deaths / cfTemp) / (1 - hivPafTemp)
		
			drop *Temp gamma*
			}
			
		di "." _continue
		}
	
	
 	save FILEPATH, replace
 
   
*** COLLAPSE TO FIVE-YEAR ESTIMATES ***  
	generate yearCat = round(year_id, 5)
	replace  yearCat = 2019 if yearCat==2020
	drop if yearCat < 1990
	
	bysort location_* ihme_loc_id age_group_id sex_id yearCat: egen year_start = min(year_id)
	bysort location_* ihme_loc_id age_group_id sex_id yearCat: egen year_end = max(year_id)

	
	
*** CLEAN UP NIDS ***
	replace nid = 371693 + year_id if nid==373710	
	rename nid nidDetail

	bysort location_* ihme_loc_id age_group_id sex_id yearCat (year_id): gen nidIndex = nidDetail if round(_N/2)==_n
	bysort location_* ihme_loc_id age_group_id sex_id yearCat : egen long nid = mean(nidIndex)
	
	drop year_id yearCat

*** COLLAPSE SOME AGE GROUPS (REDUCE DATA SIZE)	
	merge m:1 age_group_id using `ageGroups', assert(2 3) keep(3) nogenerate
	replace age_start = floor(age_start/10) * 10 if age_start>=20
	replace age_end   = age_start + 10 if age_start>=20
	
	 

	fastcollapse *Direct cases_* deaths sample_size pop env, by(location_* ihme_loc_id age_start age_end sex_id year_start year_end nid) type(sum) 
 
	replace hivPafDirect = hivPafDirect / pop 
 
 
*** CREATE INCIDENCE DRAWS ***  
	fastrowmean cases_*,  mean_var_name(cases_mean)

	forvalues i = 0/999 {
		generate inc_`i' = (cases_`i' / pop) * (casesDirect / cases_mean)
		di "." _continue
		}


*** MAKE FINAL DRAWS ***	
	fastrowmean inc_*,  mean_var_name(mean)
	fastpctile inc_*, pct(2.5 97.5) names(lower upper)


	
	
*** CLEAN UP AND PUT IN EPI-UPLOADER FORMAT ***	
	keep nid location_id age_start age_end sex_id year_start year_end mean lower upper *Direct deaths pop
	
	merge m:1 location_id using `drLocs', assert(2 3) keep(3) nogenerate

	save FILEPATH, replace

	generate sex = "Male" if sex_id==1
	replace  sex = "Female" if sex_id==2

	generate source_type = "Vital registration - national" 

	generate unit_type = "Person"
	generate unit_value_as_published = 1
	generate measure_adjustment = 0
	generate uncertainty_type_value = 95
	generate urbanicity_type = "Mixed/both"
	generate representative_name = "Nationally representative only"
	replace  representative_name = "Representative for subnational location only" if strmatch(ihme_loc_id, "*_*")
	generate recall_type = "Point"
	generate extractor = USERNAME
	generate is_outlier = 0
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

	drop pop deaths *Direct is_estimate *region*

	generate seq = .
	
	save FILEPATH, replace
	export delimited using FILEPATH, replace

	
	append using `lit'

	
*** UPLOAD TO EPI DATABASE ***	
	local outfile FILEPATH
	export excel using  `outfile', sheet("extraction") firstrow(variables) replace
	
	save_bundle_version, bundle_id(`bundle_id') decomp_step(`decomp_step')
	local bundle_version_id = bundle_version_id[1]

	local bundle_id 3023

	upload_bundle_data, bundle_id(`bundle_id') decomp_step(`step') filepath(`in_data') clear

	save_bundle_version, bundle_id(`bundle_id') decomp_step(`step')
	list bundle_version_id
	local bundle_version_id = bundle_version_id[1]
	
	
	local data_filepath FILPATH 
	local description "ints with cod"

	save_crosswalk_version, bundle_version_id(`bundle_version_id') data_filepath(`data_filepath') description(`description') clear

	upload_epi_data, bundle_id(`bundle_id') filepath(`outfile') clear



