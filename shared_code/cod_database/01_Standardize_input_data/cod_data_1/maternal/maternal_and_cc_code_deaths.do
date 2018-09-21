** ************************************************************************************************* **
** Purpose: Template for formatting maternal inputs missing any or all of the following:  remainder of deaths (cc_code),  mmr converted to deaths, deaths over a range of years . Using surviellance extractions as an example			
** ************************************************************************************************* **

** PREP STATA**
	capture restore
	clear all
	set mem 1000m

	set more off
	ssc install bygap
	
	// set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	
** *********************************************************************************************
**
// Births file from the Mortality Team
	global births_file "$j/WORK/02_mortality/03_models/1_population/results/births_gbd2015.dta"

	global births "$births_our_dir/_births_prepped.dta"
	use "$births_file", clear
	keep if sex == "both"
	keep ihme_loc_id year births 
	rename births births_ihme
	rename ihme_loc_id iso3
	** For CoD we only want location_ids on subnationals in the prep
	split iso3,p("_")
	rename iso32 location_id
	destring location_id, replace
	replace iso3 = iso31
	drop iso31
	save "$births", replace
	
// Mortality envelope
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
	tempfil env
	save `env', replace
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
	tempfil env_long
	save `env_long', replace
	

** *********************************************************************************************
**
// If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
	import excel using "$cod_in_dir/maternal_extractions_12_16_2015.xlsx", firstrow clear
	
** INITIAL CLEANING **
	// Remove unnecessary rows and columns from data
		keep if keep == 1
		keep NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years cause_deaths mortality_parameter_value mortality_parameter sample_size livebirths citation
	
	// These are all maternal (parent). We might replace with subcases in GBD2016.
		replace cause = "maternal"
	
	// All data needs maternal and nonmaternal as causes. Apply treatments depending stats given in the studies. 
	// 1. has mmr and needs to be converted to deaths
		2. has maternal deaths and live births
		3. has materbal deaths only
		4. has maternal deaths and sample size (all causes for 10-54)
		5. has maternal deaths, live births, and sample size (all causes for 10-54)*/
	gen tag = .
	replace tag = 1 if mortality_parameter == "mmr"
	replace tag = 2 if mortality_parameter == "deaths" & livebirths != .
	replace tag = 3 if mortality_parameter == "deaths" & livebirths == . & sample_size == .
	replace tag = 4 if mortality_parameter == "deaths" & livebirths == . & sample_size != .
	replace tag = 5 if mortality_parameter == "deaths" & livebirths != . & sample_size != .
	
	// Studies with data spanning across a period of years needs to be split by age and year using trends from VR
	gen vr_split = 1 if year_start != year_end
	
	// Subnationals will need location_ids to merge on the births and envelope. Make sure the location names in the db and data match first
	replace location = "KwaZulu-Natal" if location == "Kwazulu-Natal"
		preserve
			odbc load, exec("SELECT location_id, map_id AS iso3, location_name AS location FROM shared.location WHERE location_description = 'admin1'") strConnection clear
			tempfile locs
			save `locs', replace
		restore
		merge m:1 location using `locs', keep(1 3) nogen

	// Austrailia studies have deaths in 0-19. Changing this to 15-19 because it says the youngest maternal death was 17yrs in the text
	replace age_start = 15 if age_start == 0 & iso3 == "AUS"
		
	// Merge on IHME births for studies with live births so we can account for coverage later. Only do this where have all maternal ages combined since we don't have IHME births by age of women.
	gen lb_tag = 1 if livebirths != . & inlist(NID, 124778, 124736)
	preserve
		keep if lb_tag == 1
		joinby iso3 location_id using "$births", unmatched(master)
		keep if year >= year_start & year <= year_end
		collapse (sum) births_ihme, by( NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years mortality_parameter mortality_parameter_value cause_deaths sample_size citation tag vr_split location_id lb_tag livebirths)
		tempfile lb
		save `lb', replace
	restore
	drop if lb_tag == 1
	append using `lb'

	// Save and start finding maternal deaths and all other deaths	
	compress
	tempfile orig
	save `orig', replace
	
	 /* 1. MMR studies */
		keep if tag == 1
		** For mmr studies with live births: 
		preserve 
			drop if livebirths = .
			replace cause_deaths = (livebirths/100000)*maternal_parameter_value if livebirths != .
			tempfile tag1
			save `tag1',replace
		restore
		
		//  DNK has deaths for a range of years. These will be split into single years later 
		merge 1:m iso3 using $births, keep(3) assert(2 3) nogen
		keep if year >= year_start & year <= year_end

		// Use births to convert mmr to deaths
		collapse (sum) births, by( NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years mortality_parameter mortality_parameter_value cause_deaths sample_size  citation tag vr_split location_id)
		replace cause_deaths = (births/100000)* mortality_parameter_value
		
		// Calc the cc_code using deaths from the env for maternal ages. Combine year and split later
		merge 1:m iso3 sex using `env', keep(3) assert(2 3) keepusing(env* year) nogen
		keep if year >= year_start & year <= year_end
		egen mat_env = rowtotal(env8-env16)
		collapse (sum) mat_env*, by( NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years mortality_parameter mortality_parameter_value cause_deaths sample_size citation tag vr_split location_id)
		expand 2, gen(new)
		replace cause = "non-maternal" if new == 1
		replace cause_deaths = mat_env - cause_deaths if new == 1
		
		drop if livebirths != .
		append using `tag1'
		tempfile tag1
		save `tag1',replace
	
	/* 2 & 3. Studies with maternal deaths and live births OR studies with maternal deaths only */
		use `orig', replace
		keep if tag == 2 | tag == 3
		tempfile tag23
		save `tag23', replace

		// Calc the cc_code for datasets with no year ranges
		keep if vr_split == .
		drop year_end
		rename year_start year
		merge m:1 iso3 location_id sex year using `env', keep(3) assert(2 3) keepusing(env* year) nogen
		gen year_end = year
		rename year year_start
		** Studies with all maternal ages combined
			gen ages_combined = 1 if inlist(age_start ,10,15) & inlist(age_end,49,54)
			egen mat_env = rowtotal(env8-env16) if ages_combined == 1 
			expand 2 if ages_combined == 1 , gen(new)
			replace cause = "non-maternal" if new == 1
			replace cause_deaths = mat_env - cause_deaths if new == 1
			drop new
		** Studies with age groups. Do this by NID just in case we have weird age groups 
			expand 2 if ages_combined == . , gen(new)
			replace cause = "non-maternal" if new == 1
			replace cause_deaths = env8-cause_deaths if ages_combined ==. & new == 1 & age_start  == 10
			replace cause_deaths = env9-cause_deaths if ages_combined ==. & new == 1 & age_start  == 15
			replace cause_deaths = env10-cause_deaths if ages_combined ==. & new == 1 & age_start  == 20
			replace cause_deaths = env11-cause_deaths if ages_combined ==. & new == 1 & age_start  == 25
			replace cause_deaths = env12-cause_deaths if ages_combined ==. & new == 1 & age_start  == 30
			replace cause_deaths = env13-cause_deaths if ages_combined ==. & new == 1 & age_start  == 35
			replace cause_deaths = env14-cause_deaths if ages_combined ==. & new == 1 & age_start  == 40 & NID == 233802
			replace cause_deaths = env15-cause_deaths if ages_combined ==. & new == 1 & age_start  == 45 & NID == 233802
			replace cause_deaths = env16-cause_deaths if ages_combined ==. & new == 1 & age_start  == 50 & NID == 233802
			replace cause_deaths = (env14 + env15 + env16) -cause_deaths if ages_combined ==. & new == 1 & age_start  == 40 & NID == 233878
		tempfile tag23_by_year
		save `tag23_by_year', replace
		
		// Calc the cc_code for datasets with year range
		use `tag23', clear
		keep if vr_split == 1
		joinby iso3 sex location_id using `env', unmatched(master)
		drop pop* _m
		** Keep relevent years
		levelsof NID, local(nids)
		foreach NID of local nids {
			preserve
				keep if NID == `NID'
				keep if year >= year_start & year <= year_end
				tempfile `NID'
				save ``NID'', replace
			restore
		}
		clear 
		foreach NID of local nids {
			append using ``NID''
		}
		** Studies with all maternal ages combined.
				gen ages_combined = 1 if inlist(age_start ,10,15) & inlist(age_end,49,54)
			** Total up deaths in time period for each set of country years
				egen mat_env8_16 = rowtotal(env8-env16) if ages_combined == 1 
				egen mat_env8_15 = rowtotal(env8-env15) if ages_combined == 1 
				collapse (sum) mat_env* env*, by( NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years mortality_parameter mortality_parameter_value cause_deaths sample_size citation tag vr_split location_id ages_combined livebirths births_ihme lb_tag)
				** If possible get coverage
				gen coverage = livebirths/births_ihme if lb_tag == 1
				replace mat_env8_16 = mat_env8_16 * coverage if lb_tag == 1
				** Do the math
				expand 2 if ages_combined == 1 , gen(new)
				replace cause = "non-maternal" if new == 1
				replace cause_deaths = mat_env8_16 - cause_deaths if new == 1 & inlist(NID,124736,124734)
				replace cause_deaths = mat_env8_15 - cause_deaths if new == 1 & inlist(NID,124844)
				drop new
		** Studies with maternal 5 yr age groups.
	
			expand 2 if ages_combined == . , gen(new)
			replace cause = "non-maternal" if new == 1
			replace cause_deaths = env8-cause_deaths if ages_combined ==. & new == 1 & age_start  == 10
			replace cause_deaths = env9-cause_deaths if ages_combined ==. & new == 1 & age_start  == 15
			replace cause_deaths = env10-cause_deaths if ages_combined ==. & new == 1 & age_start  == 20
			replace cause_deaths = env11-cause_deaths if ages_combined ==. & new == 1 & age_start  == 25
			replace cause_deaths = env12-cause_deaths if ages_combined ==. & new == 1 & age_start  == 30
			replace cause_deaths = env13-cause_deaths if ages_combined ==. & new == 1 & age_start  == 35
			replace cause_deaths = env14-cause_deaths if ages_combined ==. & new == 1 & age_start  == 40 
			replace cause_deaths = env15-cause_deaths if ages_combined ==. & new == 1 & age_start  == 45 
			replace cause_deaths = env16-cause_deaths if ages_combined ==. & new == 1 & age_start  == 50 
		tempfile tag23_year_ranges
		save `tag23_year_ranges', replace
			
		// Tag 2 and 3 data now have maternal and nonmaternal deaths for appropriate ages and year ranges. 
		append using `tag23_by_year'
		** Tag studies that use the env
		gen env_study = 1
		tempfile tag2_tag3
		save `tag2_tag3', replace

	/* 4 & 5. Studies with maternal deaths and sample size OR studies with maternal deaths, livebirths, and sample size */
		use `orig', replace
		keep if tag == 4 | tag == 5
		tempfile tag45
		save `tag45', replace
		
		// Calc the cc_code. Easy in this case!
		expand 2, gen(new)
		replace cause_deaths = sample_size - cause_deaths if new == 1
		replace cause = "non-maternal" if new == 1
		tempfile tag4_tag5
		save `tag4_tag5', replace
		
	// Put all maternal studie with maternal and cc-codes together
	clear
	** append using `tag1'
	append using `tag2_tag3'
	append using `tag4_tag5'
	keep NID iso3 location data_type cause year_start year_end sex age_start_years age_end_years cause_deaths citation tag vr_split location_id env_study
		
	// For studies where the data covers a range of years use the VR trends to split across years.
	rename age_start_years age_start
	rename age_end_years age_end
	tempfile orig
	save `orig'
	
	// Load VR for maternal age groups 
	** Get iso3s 
		odbc load, exec("select map_id as iso3, location_id from shared.location_hierarchy_history where location_set_version_id = 38") dsn(prodcod) clear
		drop if iso3 == ""
		rename location_id loc_parent
		tempfile parent
		save `parent', replace
	** Get deaths for all locations and fix iso3s
		odbc load, exec("SELECT location_id, year_id as year, cf_final, sample_size, age_group_years_start as age_start, age_group_years_end as age_end, path_to_top_parent, level FROM cod.data INNER JOIN cod.data_version using (data_version_id) INNER JOIN shared.location_hierarchy_history using (location_id) INNER JOIN shared.cause using (cause_id) INNER JOIN shared.age_group using (age_group_id) WHERE status = 1 and sex_id = 2 and age_group_id BETwEEN 7 AND 17 and acause = 'maternal' and data_type_id = 9 and location_set_version_id = 38") strConnection clear 
		split path_to_top_parent,p(",")
		rename path_to_top_parent4 loc_parent
		drop path*
		destring loc_parent, replace
		** Its okay to drop the US territories here
		merge m:1 loc_parent using `parent', keep (3) nogen
		replace iso3 = "TWN" if location_id == 8
		replace location_id = . if level == 3
		drop loc_parent level
	** Convert to deaths
		gen deaths = cf_final * sample_size
		drop cf_final sample_size
	** Match data format
		replace age_end = age_end-1
		tempfile mat_vr
		save `mat_vr', replace
			
	//  Split the studies that cover a range of years into single years using VR by location-year 
	// First do it for studies all maternal ages combined
		// Make the mat env for the combine age groups
		use `mat_vr', clear
		do "$j/WORK/03_cod/01_database/03_datasets/Maternal_report/code/mat_age_groups.do"
		append using `mat_vr'
		tempfile mat_vr_final
		save `mat_vr_final', replace
		
		// Apply to data so we can generate poroportions for each year
		use `orig', clear
		keep if vr_split == 1
		drop if cause == "non-maternal"  
		joinby location_id iso3 age_start age_end using `mat_vr_final', unmatched(master)
		assert _m == 3
		drop _m
		keep if year >= year_start & year <= year_end
		drop year_start year_end
		bysort NID iso3 age_start age_end cause: egen prop = pc(deaths),prop
		replace cause_deaths = cause_deaths * prop
		
		// Reformat the env to make cc_code now that we have data for each year
		preserve
			use `env_long', clear
			drop pop
			keep if sex == 2 & age >=10 & age <=50
			rename age age_start
			gen age_end = age_start + 4
			rename env deaths
			tempfile env_intermediate
			save `env_intermediate', replace
			do "$j/WORK/03_cod/01_database/03_datasets/Maternal_report/code/mat_age_groups.do"
			append using `env_intermediate'
			rename deaths env
			tempfile env_final
			save `env_final', replace
		restore
		
		// Create cc_code
		merge m:1 location_id iso3 age_start age_end year using `env_final', assert(2 3) keep(3) nogen
		expand 2, gen(new)
		replace cause = "non-maternal" if new == 1
		replace cause_deaths = env-cause_deaths if new == 1
		rename year year_start
		drop new env deaths prop
		** Document studies that use the env
		replace env_study = 1
		tempfile year_split_data
		save `year_split_data', replace
		
// Put it all together!
	use `orig', clear
	drop if vr_split == 1
	append using `year_split_data'
	drop tag vr_split
