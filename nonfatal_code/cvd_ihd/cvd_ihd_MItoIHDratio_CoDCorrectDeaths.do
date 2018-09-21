// SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS

// BOILERPLATE 
  clear all
  set maxvar 10000
  set more off
  

// PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND
	local location "`1'"
	
	capture log close
  	log using "FILEPATH/mi_ratio_`location'", replace

  adopath + "FILEPATH"
  
// SET UP OUTPUT DIRECTORIES
	local out_dir FILEPATH

  tempfile global_ratio sr_ratio region_ratio ihd_deaths epi_data

// SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
	local meid 2570
	local causeid 493
	local ages 11 12 13 14 15 16 17 18 19 20 30 31 32 235
	
// Set up options for geographies
	get_location_metadata, location_set_id(9) gbd_round_id(4) clear
	preserve
	keep if location_type=="region"
	levelsof location_id, local(regions)
	restore
	keep if location_type=="superregion"
	levelsof location_id, local(superregions)
	
	
// PULL IN DRAWS AND MAKE CALCULATIONS
// Get population
	get_population, location_id(`location') sex_id(1 2) age_group_id(`ages') year_id(1990 1995 2000 2005 2010 2016) gbd_round_id(4) clear
	tempfile pop_temp
	save `pop_temp', replace

// Pull in draws from DisMod proportion model
	//Global only
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') measure_ids(18) location_ids(1) age_group_ids(`ages') status(latest) source(dismod) clear
	forvalues i = 0/999 {
		quietly generate global_ratio_`i' = draw_`i' 
		quietly generate global_ratio_chronic_`i' = 1 - draw_`i'
		drop draw_`i'
	}
	drop location_id
	save `global_ratio', replace
	
// Get death data (CSMR=cause-specific deaths/population)
	// Pulls GBD 2015 deaths
	get_draws, gbd_id_field(cause_id) gbd_id(`causeid') location_ids(`location') age_group_ids(`ages') status(latest) source(codcorrect) measure_ids(1) clear
	merge 1:1 sex_id age_group_id year_id using `pop_temp', keep(3) nogen
	forvalues i = 0/999 {
	    quietly replace draw_`i' = draw_`i'/population //Change to death rate (total deaths/population)
		quietly rename draw_`i' ihd_deaths_`i' 
	}
	save `ihd_deaths', replace
		
// Merge and transform data
	// Calculate acute: IHD deaths are used as the effective sample size (uncertainty) for this data, and MI deaths are the mean/proportion that we want
		use `global_ratio', clear
		merge 1:1 age_group_id year_id sex_id using `ihd_deaths', keep(3) nogen
				
		forvalues i = 0/999 {
			drop global_ratio_chronic_`i'
			quietly replace global_ratio_`i' = global_ratio_`i' * ihd_deaths_`i'
			quietly replace ihd_deaths_`i' = ihd_deaths_`i' * population //Re-convert deaths from rate-space to number space
		}
		
		fastrowmean global_ratio_*, mean_var_name(mean)
		fastrowmean ihd_deaths_*, mean_var_name(sample_size)
		drop global_ratio_* ihd_deaths_*
		gen nid = 239850
		capture drop output_version_id process_version_map_id
		
save "`out_dir'/csmr_mi_`location'.dta", replace

	// Calculate Chronic: IHD deaths are used as the effective sample size (uncertainty) for this data, and MI deaths are the mean/proportion that we want
		use `global_ratio', clear
		merge 1:1 age_group_id year_id sex_id using `ihd_deaths', keep(3) nogen
				
		forvalues i = 0/999 {
			drop global_ratio_`i'
			quietly replace global_ratio_chronic_`i' = global_ratio_chronic_`i' * ihd_deaths_`i'
			quietly replace ihd_deaths_`i' = ihd_deaths_`i' * population //Re-convert deaths from rate-space to number space
		}
		
		fastrowmean global_ratio_chronic_*, mean_var_name(mean)
		fastrowmean ihd_deaths_*, mean_var_name(sample_size)
		drop global_ratio_chronic_* ihd_deaths_*
		gen nid = 239851
		capture drop output_version_id process_version_map_id

save "`out_dir'/csmr_ihd_`location'.dta", replace

log close
