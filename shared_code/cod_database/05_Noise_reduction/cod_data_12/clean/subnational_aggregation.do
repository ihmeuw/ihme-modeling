** *************************************************************************************
** Purpose: Aggregate subnational sites to national, preserving cause fractions and age standardized rates
** *************************************************************************************

** DEFINE ARGUMENTS
	local process_step = "`1'"
	noisily di in green "Step: `process_step'"
	sleep 3000

** SET UP CONNECTION STRING	
	forvalues dv=4(.1)6 {
		local dvfmtd = trim("`: di %9.1f `dv''")
		cap odbc query, conn("DRIVER={strDriver};SERVER=strServer;UID=strUser;PWD=strPassword")
		if _rc==0 {
			local driver "MySQL ODBC `dvfmtd' Unicode Driver"
			local db_conn_str "DRIVER={`driver'};SERVER=strServer; UID=strUser; PWD=strPassword"
			continue, break
		}
	}

** SET UP FASTCOLLAPSE AND GLOBALS 
	do "$j/WORK/04_epi/01_database/01_code/04_models/prod/fastcollapse.ado"
	global dsn prodcod
	global location_set_version_id 46

** STORE THE INPUT DATA
	tempfile input_data
	save `input_data', replace

** IS THE SOURCE IN THE HIERARCHY? SET A BOOLEAN FLAG
	local in_hier = 1
	if regexm("$source", "US_NCHS_counties") | regexm("$source", "UK_deprivation") {
		local in_hier = 0
	}

** GET PATHS TO PARENTS BASED ON HIERARCHY FLAG
	preserve
	if `in_hier'==1 {
		odbc load, exec("SELECT location_id, path_to_top_parent, level as location_level FROM shared.location_hierarchy_history WHERE location_set_version_id = '$location_set_version_id'") conn("`db_conn_str'") clear
		replace path_to_top_parent="1,158,159,163,44538,44540" if location_id==44540
		tempfile paths
		save `paths', replace
	}
	else {
		odbc load, exec("SELECT location_id, path_to_top_parent, location_level FROM shared.location") conn("`db_conn_str'") clear
		tempfile paths
		save `paths', replace
	}
	restore

if "`process_step'" == "clean" {
** IF THE SOURCE DOESN'T REPRESENT ALL THE DATA FOR A NATIONAL GEOGRAPHY, ADD OTHER SOURCES
	** ADD OTHER CHINA SOURCES
	if "$source"=="China_1991_2002" {
		preserve
		** Add hong kong and macau from ICD9
		use "$j/WORK/03_cod/01_database/03_datasets/ICD9_detail/data/final/11_noise_reduced.dta" if inlist(location_id, 354, 361), clear
			tempfile mac_hkg
			save `mac_hkg', replace

			** Add the relevant years of hong kong and macau from ICD10
			use "$j/WORK/03_cod/01_database/03_datasets/ICD10/data/final/11_noise_reduced.dta" if inlist(location_id, 354, 361) & year<=2002, clear
			append using `mac_hkg'
			save `mac_hkg', replace
		restore
		append using `mac_hkg'
	}

	if "$source"=="China_2004_2012" {
		preserve
			use "$j/WORK/03_cod/01_database/03_datasets/ICD10/data/final/11_noise_reduced.dta" if inlist(location_id, 354, 361) & year>=2004, clear
			tempfile mac_hkg
			save `mac_hkg', replace
		restore
		append using `mac_hkg'
	}

	** UK: Append Scotland and Northern Ireland from ICD9, ICD10, then UK_NI_SC_2011_2012
	if "$source"=="UK_1981_2000" {	
		preserve
			use "$j/WORK/03_cod/01_database/03_datasets/ICD9_detail/data/final/11_noise_reduced.dta" if inlist(location_id, 434, 433), clear
			tempfile ni_scot
			save `ni_scot'
			restore
		append using `ni_scot'
	}

	if "$source"=="UK_2001_2011" {
		preserve
		use "$j/WORK/03_cod/01_database/03_datasets/ICD10/data/final/11_noise_reduced.dta" if inlist(location_id, 434, 433)
			tempfile ni_scot
			save `ni_scot', replace
		restore
		append using `ni_scot'
	}

	** ADD OTHER INDIA SOURCES
	if "$source"=="India_MCCD_states_ICD10"{
		preserve
			use "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_Delhi_ICD10/data/final/11_noise_reduced.dta", clear
			tempfile delhi 
			save `delhi', replace
		restore
		append using `delhi'
	}
}

** IF THE SOURCE HAS OTHER DATA IN IT BESIDES THE SUBNATIONAL TO AGGREGATE, DROP THIS
	** KEEP ONLY MEXICO IN ICDS
	if inlist("$source", "ICD10", "ICD9_detail") {
		keep if iso3=="MEX"
	}
	
	** KEEP ONLY INDIA IN CANCER
	else if "$source"=="Cancer_Registry" {
		keep if iso3=="IND"
		replace location_id = 163 if location_id==.
	}
	
	** KEEP ONLY SUBNATIONAL LOCATIONS (NOW THAT THE INDIA THING HAS BEEN TAKEN CARE OF)
	drop if location_id == .
	
	** KEEP ONLY SSA SUBNATIONAL LOCATIONS IN OTHER_MATERNAL
	else if "$source"=="Other_Maternal" {
		if "`process_step'"=="noise_reduction" {
			keep if flag_age_year != 1 & source == "Other_Maternal" & regexm(source_label, "DHS") & inlist(iso3,"KEN","ZAF","BRA")
		}
		else {
			keep if regexm(source_label, "DHS") & inlist(iso3, "KEN", "ZAF", "BRA")
		}
	}
	
	** FOR OTHER SOURCES (COUNTRY-SPECIFIC ONES), ONLY KEEP COUNTRY OF INTEREST
	else if !inlist("$source", "ICD10", "ICD9_detail", "Cancer_Registry", "Other_Maternal") {
		levelsof iso3 if source == "$source", local(keepiso) c
		keep if iso3 == "`keepiso'"
	}
	
	** DONT INCLUDE OTHER_MATERNAL IN AGGREGATES (EFFECTS BRAZIL AND SOUTH AFRICA)
	if "$source" != "Other_Maternal" drop if source == "Other_Maternal"

** KEEP  RECORD OF YEARS IN ORIGINAL DATA
	levelsof year if source == "$source", local(years)
	
** KEEP  RECORD OF CAUSES IN ORIGINAL DATA
	preserve
		keep if source == "$source"
		keep acause
		duplicates drop
		tempfile orig_causes
		save `orig_causes', replace
	restore
	
** Keep record of original locations
	if "`process_step'" == "noise_reduction" levelsof location_id if source == "$source", local(location_ids)

	if inlist("$source", "China_1991_2002", "China_2004_2012") drop if year < 1991
	if inlist("$source", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10") drop if year > 2010
	if inlist("$source", "UK_1981_2000", "UK_2001_2011") drop if year < 1981

** make all values equal to source and save the iso3s in the input data for pop file compression
	foreach var in source_type list {
		levelsof `var' if source == "$source", local(var_rep) c
		replace `var' = "`var_rep'"
	}
	replace source = "$source"
	tempfile appended_input
	save `appended_input', replace

** STORE A MAPPING OF LOCATION IDS TO PATH_TO_TOP_PARENT
	** USE SHARED.LOCATION FOR THIS IF NOT IN HIERARCHY OTHERWISE, USE HIERARCHY HISTORY

** MERGE LOCATION IDS WITH PATH TO TOP PARENT
	** every location_id should have a path_to_top_parent
	capture merge m:1 location_id using `paths', assert(2 3) keep(3) nogen
	if _rc !=0 {
		save "/home/j/WORK/03_cod/01_database/03_datasets/$source/explore/merge_fail_subnat.dta", replace
		use `paths', clear
		save "/home/j/WORK/03_cod/01_database/03_datasets/$source/explore/paths.dta", replace
		use `appended_input', clear
		save "/home/j/WORK/03_cod/01_database/03_datasets/$source/explore/appended_input.dta", replace
		BREAK
	}

** SPECIAL US TWEAK FOR TIME SAVING
	** drop the counties out of US because they are already aggregated to states
	if regexm("$source", "US_NCHS_counties") {
		count if location_level==2
		assert(r(n) != 0)
		drop if location_level==3
		tempfile us_states
		save `us_states'
	}

** DON'T MAKE NATIONAL ESTIMATES BY SUBDIV OR SOURCE_LABEL OR REPRESENTATIVENESS
	drop source_label
	if "`process_step'"!="noise_reduction" & "$source"=="India_SRS_states_report" {
		replace subdiv=subdiv
	} 
	else {
		replace subdiv=""
		replace national=1
	}

** DETERMINE THE NUMBER OF LEVELS OF AGGREGATION
	** USA: input_data already aggregated to states, so only 1
	** India: 1 level - location hierarchy gives natural path up 
	** Sweden: 1 level
	** Brazil: 1 level
	** Japan: 1 level
	** South Africa: 1 level
	** UK deprivations: 3 levels (deprivations->regions of england->countries->national), keep all
	** China: 1 level 
	** Mexico: 1 level
	if `in_hier'==1 {
		** The national level is 3 in the hierarchy 
		local top_level = 3
	}
	else {
		** The national level is 1 in shared.location
		local top_level = 1
	}

** FIND THE BOTTOM LEVEL (THE LARGEST LOCATION LEVEL)
	su location_level
	local bottom = r(max)

** AGGREGATE EACH LOCATION LEVEL UNTIL THERE IS A NATIONAL ESTIMATE --> ASSUMES NO AGGREGATION HAS YET TAKEN PLACE
	tempfile preloop
	save `preloop', replace
	local agg_level = 0
	while `bottom' != `top_level' {
		di in red "AGGREGATING LEVEL `bottom'"
		local ++agg_level
		preserve
			keep if location_level==`bottom'
			** get parent location vars
			gen parent_path_to_top_parent = subinstr(path_to_top_parent, ","+string(location_id), "", .)
			gen parent_location_id = real(regexs(2)) if regexm(parent_path_to_top_parent, "^(.*,)([0-9]+)$")
			gen parent_location_level = location_level-1

			** make sure parent location level will match pop file by making it missing if national
			replace parent_location_id=. if parent_location_level==`top_level'

			** replace the current location vars to the parents
			drop path_to_top_parent location_id location_level
			rename parent_* *
			
			** NID should be set to 'to-be-researched' as another way to show that this is an aggregated estimate
			replace NID = 103215

			** generate deaths and collapse 
			foreach step in final corr rd raw {
				capture drop deaths_`step'
				gen deaths_`step' = cf_`step'*sample_size
			}
			fastcollapse deaths_final deaths_corr deaths_rd deaths_raw sample_size, by(age NID iso3 list location_id national region sex source source_type year acause path_to_top_parent location_level subdiv) type(sum)
			
			** recalculate cause fractions
			foreach step in final corr rd raw {
				gen cf_`step' = deaths_`step'/sample_size
			}
			drop deaths*
			tempfile agg_`agg_level'
			save `agg_`agg_level'', replace
		restore
		append using `agg_`agg_level''

		** reset bottom to level just aggregated
		local bottom = `bottom'-1
	}

	** Use aggregate levels
	clear
	foreach al of numlist 1/`agg_level' {
		append using `agg_`al''
	}

	** SPECIAL FOR CHINA: DON'T WANT THE 'CHINA WITHOUT HONG KONG/MACAO' OBSERVATION
	drop if location_id==44533
	
	** SPECIAL FOR UK: DON'T WANT THE 'ENGLAND' OBSERVATION FOR NOISE REDUCTION
	if "`process_step'" == "noise_reduction" drop if location_id==4749
	
	** SPECIAL FOR INDIA MCCD: DON'T WANT STATES, JUST URBAN AND NATIONAL
	if "`process_step'" == "noise_reduction" drop if iso3 == "IND" & location_level == 4

	** WE MAKE NATIONAL OBSERVATIONS HAVE MISSING AS THEIR LOCATION ID AND CHINA ISNT WORKING JUST FIX IT
	replace location_id = . if location_id==6
	
	** KEEP ONLY YEARS IN ORIGINAL DATA FOR COMPILE, ADJUST SOURCE NAME FOR DROPPING LATER IF NOISE REDUCTION
	gen keep_year = .
	foreach year of local years {
		replace keep_year = 1 if year == `year'
	}
	if "`process_step'" == "clean" keep if keep_year == 1
	if "`process_step'" == "noise_reduction" replace source = "subnat_agg" if keep_year != 1
	drop keep_year

	** KEEP ONLY LOCATIONS IN ORIGINAL DATA FOR COMPILE, ADJUST SOURCE NAME FOR DROPPING LATER IF NOISE REDUCTION
	if "`process_step'" == "noise_reduction" {
		gen keep_loc = .
		foreach loc_id of local location_ids {
			replace keep_loc = 1 if location_id == `loc_id'
		}
		replace source = "subnat_agg" if keep_loc != 1
		drop keep_loc
	}
	
	** KEEP ONLY CAUSES IN ORIGINAL DATA
	merge m:1 acause using `orig_causes', assert(1 3) keep(3) nogen

	** Keep only required variables and in so doing assert that they are there
	keep age NID iso3 list location_id national region sex source source_type year acause subdiv sample_size cf_corr cf_rd cf_raw cf_final

	** For code clarity, add the source_label variable here as empty
	** gen subdiv = ""
	gen source_label = ""
	if "$source" == "Other_Maternal" replace source_label = "DHS sibling history:  " + iso3 + " " + string(year)
	
	** Get list of iso3s we're adding
	levelsof iso3, local(agg_isos)

** ALL DONE ***** :) **
	append using `input_data'
	
** FOR NRA STEP - rename all national data so that it is smoothed together, separately from the subnational locations
if "`process_step'" == "noise_reduction" {
	foreach agg_iso of local agg_isos {
		replace iso3 = iso3 + "_national" if location_id == . & iso3 == "`agg_iso'" & (source != "Other_Maternal" | source_type == "Sibling history, survey") 
	}
}
** *************************************************************************************