
/*====================================================================
project:       GBD2016
Organization:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER CLEAN        
Output:           Estimate Prevalence and Incidence of CE
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/
	

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*local envir (dev or prod)
	local envir = "prod"
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"

	*directory for standard code files
	adopath + FILEPATH



	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/extrapolate2015_`date'_`time'.smcl", replace
	***********************	


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	get_demographics, gbd_team("epi") clear
		local gbdages `r(age_group_ids)'
		local gbdyears `r(year_ids)'
		local gbdsexes `r(sex_ids)'

*--------------------1.2: Location Metadata

	get_location_metadata,location_set_id(35) clear
	
	* Simplify
		keep location_id location_name region_id region_name super_region_id super_region_name path_to_top_parent most_detailed
		save "`local_tmp_dir'/metadata.dta", replace
			levelsof location_id,local(alllocs) clean
		keep if most_detailed==1
			levelsof location_id,local(gbdlocs) clean

*--------------------1.3: Age Metadata

	* Connect to database
		noisily di "{hline}" _n _n "Connecting to the database"
		create_connection_string, database(shared)
		local shared = r(conn_string)
	
	*Create table of all ages in oncho data and/or needed for gbd estimates and their age_start and age_end
		odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`gbdages'", " ", ",", .)')") `shared' clear
		replace age_end=100 if age_end==125
		save "`local_tmp_dir'/ages.dta", replace
		
	*Get age weight to calculate age-standardized death rate
		odbc load, exec("SELECT age_group_id, age_group_weight_value AS age_weight FROM age_group_weight WHERE gbd_round_id = 4 AND age_group_id!=28") `shared' clear
		
		merge 1:1 age_group_id using "`local_tmp_dir'/ages.dta", assert(3) nogenerate
		save "`local_tmp_dir'/ages.dta", replace

*--------------------1.4: Population/GBD Sekeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
	
	* Make skeleton
		drop process_version_map_id
		merge m:1 age_group_id using "`local_tmp_dir'/ages.dta", nogen
		egen age_mid = rowmean(age_start age_end)
		gen year_mid = year_id
		gen skeleton = 1
		save "`local_tmp_dir'/skeleton.dta", replace



/*====================================================================
                        2: Get Prevalence Data
====================================================================*/


*--------------------2.1: Import Data

	*Inpatient hospital data
		import excel "FILEPATH/60_v6_2017_06_07.xlsx", sheet("extraction") firstrow clear
		
		gen mean = mean_2
		gen lower = lower_2
		gen upper = upper_2
		gen source = "inpatiet"
		
		tempfile inpatient
		save `inpatient', replace
	
	*Outpatient hospital data
		import excel "FILEPATH/60_v5_2017_06_06.xlsx", sheet("extraction") firstrow clear
		
		gen source = "outpatient"
		
		tempfile outpatient
		save `outpatient', replace
	
	*Extraction
		import excel "FILEPATH/STEP2of3_GBD2015 Extraction.xlsx", sheet("extraction") firstrow clear
		
		gen source = "extraction"
		
		tempfile extraction
		save `extraction', replace


*--------------------2.2: Clean/Format Data

	* Append all
		use `inpatient', clear
		append using `outpatient'
		gen cv_clinic_hospital = 1
		tostring input_type uncertainty_type sampling_type, replace
		append using `extraction'
	
	*Fill in mean/cases where possible
		replace cases = mean*sample_size if cases ==. & mean !=. & sample_size !=.
		replace mean = cases/sample_size if mean ==. & cases !=. & sample_size !=.
		replace sample_size = cases/mean if sample_size ==. & cases !=. & mean !=.

	*Group review & outliers
		keep if group_review != 0
		keep if is_outlier == 0
			
	*Adjust for age_demographer
		replace age_end = age_end-1 if age_demographer == "0.0"
		
	*Code sex
		gen sex_id = 2 if sex == "Male"
		replace sex_id = 1 if sex == "Female"
		replace sex_id = 3 if sex == "Both"
	
	*Keep only needed variables
		keep location_id year_start year_end age_start age_end sex_id cases sample_size mean* upper* lower* standard_error
		egen age_mid = rowmean(age_start age_end)
		egen year_mid = rowmean(year_start year_end)
		
	*Append with GBD skeleton
		append using "`local_tmp_dir'/skeleton.dta"
			
	*Quick formatting
		replace year_mid = round(year_mid)
		replace age_mid = round(age_mid)
		
	*Save
		save "`local_tmp_dir'/data.dta", replace
		

/*====================================================================
                        3: Get Covariate Data
====================================================================*/


*--------------------3.1: Calculate lnasdr
			
	*Males
		get_model_results, gbd_team("cod") model_version_id("361514") location_id("`alllocs'") year_id("-1") age_group_id("`gbdages'") sex_id("`gbdsexes'") clear
				
		tempfile csmrM
		save `csmrM', replace
	
	*Females
		get_model_results, gbd_team("cod") model_version_id("361526") location_id("`alllocs'") year_id("-1") age_group_id("`gbdages'") sex_id("`gbdsexes'") clear
			
		tempfile csmrF
		save `csmrF', replace
	
	*Append
		append using `csmrM'
		
	*Calculate age-standardized death rate
		merge m:1 age_group_id using "`local_tmp_dir'/ages.dta", assert(3) nogenerate
		
		generate asdr = mean_death_rate * age_weight / 2
		
		collapse (sum) asdr age_weight, by(location_id year_id)
		
	*Save
		save "`local_tmp_dir'/asdr.dta", replace
		
	*Merge with dataset
		rename year_id year_mid
		merge 1:m location_id year_mid using "`local_tmp_dir'/data.dta", keep(matched using)
		
		save "`local_tmp_dir'/data_asdr.dta", replace
			

*--------------------3.2: Other Covariates
		
	*Load and Merge Potential Covariates
	*881 = SDI | 1099 = haqi | 208 = HAS-C | 57 = LDI | 875 = echino_endemicity | 1087 = prop_pop_agg
		use "`local_tmp_dir'/data_asdr.dta", clear
		tempfile allcovs
		save `allcovs', replace
		
		local covs 881 1099 208 57 875 1087
				
		foreach cov in `covs' {
			get_covariate_estimates,covariate_id(`cov') clear
			
			replace covariate_name_short="hsac" if covariate_name_short=="health_system_access_capped"
			
			*Name after the covariate
				local name=covariate_name_short
				rename mean_value `name'
				keep location_id year_id age_group_id sex_id `name'
			
			*Discern if the covariate is age/sex stratified or all-age/sex
				quietly sum age_group_id
				local allage 0
				if `r(min)' == 22 {
					local allage = 1
				}
					
				quietly sum sex_id
				local allsex 0
				if `r(min)' == 3 {
					local allsex = 1
				}
				
				di in red "All-Age: `allage' All-Sex: `allsex'"
			
			*Format/Generate Variables
				*Age: get age_mid, take mean across under 1yos for age-stratified covariates
					if `allage' == 0 {
						merge m:1 age_group_id using "`local_tmp_dir'/ages.dta", nogen
						egen age_mid = rowmean(age_start age_end)
						replace age_mid = round(age_mid)
						collapse(mean) `name',by(location_id year_id sex_id age_mid)
					}
				
				*Year
					rename year_id year_mid
				
				*Sex
					if `allsex' == 0 {
						tempfile tempcovMF
						save `tempcovMF', replace
						
						collapse(mean) `name',by(location_id year_mid age_group_id)
						gen sex_id = 3
						
						append using `tempcovMF'
					
					}
			
			*Merge differently by stratification type 
				*age stratified, sex stratified = merge on both age and sex
					if `allage' == 0 & `allsex' == 0 {
						merge 1:1 location_id sex_id year_mid age_mid using `allcovs', nogen  keep(matched)
					}
				*sex stratified, all age = merge on sex, not age
					if `allage' == 1 & `allsex' == 0 {
						drop age_group_id
						merge 1:m location_id sex_id year_mid using `allcovs', nogen  keep(matched)
					}
				*age stratified, all sex = merge on age, not sex
					if `allage' == 0 & `allsex' == 1 {
						drop sex_id
						merge 1:m location_id year_mid age_mid using `allcovs', nogen  keep(matched)
					}
				*all age, all sex = merge on neither sex nor age
					if `allage' == 1 & `allsex' == 1 {
						drop age_group_id sex_id
						merge 1:m location_id year_mid using `allcovs', nogen  keep(matched)
					}
			
			save `allcovs', replace
			
		}
	
	*Merge with metadata
		merge m:1 location_id using "`local_tmp_dir'/metadata.dta",nogen keep(matched master)
		
	*Save
		save "`local_tmp_dir'/dataset.dta", replace
		

/*====================================================================
                        4: Get Geographic Restrictions
====================================================================*/

	*Read in file, limit to GBD locations we are modeling for
		import delimited FILEPATH/ce_cc.csv, clear
	
	*Include create data rows where mean value of incidence = 0
		gen GR = 1
		
	*Svae GR in simple form
		save "`local_tmp_dir'/GR_simple.dta", replace


/*====================================================================
                        5: Model Incidence
====================================================================*/


*--------------------5.1: Run Model

	use "`local_tmp_dir'/dataset.dta", clear
			
	*lnasdr
		sum asdr
		local offset=.1*`r(min)'
		gen lnasdr=ln(asdr+`offset')
	
	*Other vars
		mkspline ageS = age_mid, cubic nknots(3) displayknots
		
		generate sexOrdered = -1 if sex_id==1
		replace  sexOrdered = 0 if sex_id==3
		replace  sexOrdered = 1 if sex_id==2
		
	*Forward Difference Coding of echino_endemicity
		gen e1 = 3/4 if echino_endemicity == 0
		replace e1 = -1/4 if inlist(echino_endemicity,1,2,3)
		
		gen e2 = 1/2 if inlist(echino_endemicity,0,1)
		replace e2 = -1/2 if inlist(echino_endemicity,2,3)
		
		gen e3 = 1/4 if inlist(echino_endemicity,0,1,2)
		replace e3 = -3/4 if echino_endemicity == 3
	
		generate exp = sample_size
		replace  exp = 1 if missing(exp)	
	
	*MODEL
		poisson mean lnasdr ageS* i.sex_id


*--------------------5.2: Quick Diagnostics
	
	*Test dispersion parameter (negative binomial)
		*test [lnalpha]_cons = 1

	*Test statistics
		estat ic

*--------------------5.3: Predict Prevalence

	*Predict mean and standard deviation
		predict fixed, xb nooffset
		predict fixedSe, stdp nooffset
		
	*Create 1000 draws
		forvalues i = 0 / 999 {
			quietly {
				generate draw_`i' = exp(rnormal(fixed, fixedSe))
			}
			di "." _continue
		}

	*Reduce to estimates for gbd
		keep if skeleton == 1


/*====================================================================
                        6: Convert to Prevalence
====================================================================*/


*--------------------6.1: Calculate Prevalence from Incidence

	*Convert to prevalence
		* Assume 4-6 year duration
		
		forvalues i = 0 / 999 {
			display in red ". `i' " _continue

			quietly {	
				local duration = (runiform()*2) + 4
				generate prev_`i' = draw_`i' / 2 if age_group_id<5
				bysort location_id sex_id year_id (age_group_id):replace  prev_`i' = (draw_`i' * 2.5) + (draw_`i'[_n-1] * (`duration'-2.5)) if age_group>=5		
			}
			
			if `i' == 999 {
			di in red "Prevalence Draws COMPLETE"
			}
		}
	
	*Save
		save "`local_tmp_dir'/PrevalenceIncidence_preGR.dta", replace



/*====================================================================
                        7: Apply Geographic Restrictions
====================================================================*/


*--------------------7.1: Replace restricted location-years with zero prevalence
	
	* Merge Predictions for GBD location-years with Geographic Restrictions
		capture drop year_id
		rename year_mid year_id
		merge m:1 location_id year_id using "`local_tmp_dir'/GR_simple.dta", nogen keep(matched master)
			
	* Replace draws with 0 if in geographically restricted location-year
		forval j=0/999 {
			display in red ". `j' " _continue
			
			quietly {
				replace draw_`j' = 0 if GR==1
			}
		}

	* Save
		save "`local_tmp_dir'/predictions.dta", replace

/*====================================================================
                        8: Output Files
====================================================================*/

*--------------------8.1: Output one file per location and measure

	tempfile predictions
	save `predictions', replace
	
	foreach metric in prev draw {
		use `predictions', clear
		keep location_id year_id age_group_id sex_id `metric'_*
		rename `metric'* draw*
		
		if "`metric'" == "prev" {
			local measureid 5
		}
		if "`metric'" == "draw" {
			local measureid 6
		}
		
		gen measure_id = `measureid'
		
		local meid 1484
		
		foreach location in `gbdlocs' {
			di in red ". `location'" 
			quietly{
			preserve
				keep if location_id==`location'
				outsheet using "`out_dir'/`meid'_prev/`measureid'_`location'.csv", comma replace
			restore
				drop if location_id==`location'
			}	
		}
	}


/*====================================================================
                        9: Save Results - Optional
====================================================================*/

/*
  run "FILEPATH/save_results.do"
  
	local meid 1484
	local description description
	local in_dir `out_dir'/`meid'_prev
	local metrics "prevalence incidence"
	local best no
	local file_pattern "{measure_id}_{location_id}.csv"
 
  save_results, modelable_entity_id(1meid') description("`description'") in_dir("`in_dir'"") metrics("`metrics'"") mark_best("`best'") env(prod) file_pattern("`file_pattern'")
 
 */


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><
