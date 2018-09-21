** ****************************************************
** Purpose: Smooth VR, VA, and Cancer Registry data
** ******************************************************************************************************************
// Prep Stata
	clear all
	set more off, perm
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global j "J:"
	}
	
// Set up connection string for dB
	forvalues dv=4(.1)6 {
		local dvfmtd = trim("`: di %9.1f `dv''")
		cap odbc query, conn("DRIVER={strConnection};SERVER=strServer;UID=strUser;PWD=strPassword")
		if _rc==0 {
			local driver "strDriver"
			local db_conn_str "DRIVER={`driver'};SERVER=strServer; UID=strUser; PWD=strPassword"
		}
	}
	
// Source
	global source "`1'"

// Date
	global timestamp "`2'"
	
// Username
	global strUser "`3'"
	
//  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global temp_dir "/ihme/cod/prep/01_database/"

// Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"
	
// program directory
	global prog_dir "$j/WORK/03_cod/01_database/02_programs"

// prepare fastcollapse
	do "$j/WORK/10_gbd/00_library/functions/fastcollapse.ado"

// log output
	capture log close _all
	log using "`in_dir'//$source//logs/11_noise_reduction_${timestamp}", replace
	
// Refresh temp directory
	capture mkdir "$temp_dir/11_noise_reduction"
	!rm -rf "$temp_dir/11_noise_reduction/${source}"
	capture mkdir "$temp_dir/11_noise_reduction/${source}"
	
// read in data
	use "`in_dir'/$source/data/final/10_aggregated.dta", clear
	
// **********************************************************************************************************************************************	
// NOISE REDUCE VR, Cancer Registry, and DHS. Low sample size or low death counts cause a great level of stochastic volatility for some cause demograhic groups 
	// We don't want US counties to be noise reduced, but we do want the states to be noise reduced. Aggregate the counties to states and re-append later.
		local temp_dont_do_it = 1
		if inlist("$source", "US_NCHS_counties_ICD10", "US_NCHS_counties_ICD9") & `temp_dont_do_it'==0 {
			** although sometimes annoying, preserve/restore will make this faster since US counties are massive and I don't want to write a tempfile to disk.
			preserve
				** run aggregation on the counties
				** get all US county location_ids and parent location_ids
				odbc load, exec("SELECT location_id, location_parent_id FROM shared.location WHERE path_to_top_parent LIKE '%,102,%,%'") conn("`db_conn_str'") clear
				tempfile parents
				save `parents', replace
			restore
			** Calculate the total deaths_final in the dataset; this will be used to verify that we aren't about to change death totals
				gen deaths_final = cf_final * sample_size
				su deaths_final
				local old_deaths_final = r(sum)
				drop deaths_final


				** Match each county to it's state
				merge m:1 location_id using `parents', assert(2 3) keep(3)
				replace location_id = location_parent_id
				drop location_parent_id

				** generate deaths and collapse 
				foreach step in final corr rd raw {
					capture drop deaths_`step'
					gen deaths_`step' = cf_`step'*sample_size
				}
				fastcollapse deaths_final deaths_corr deaths_rd deaths_raw sample_size, by(age NID iso3 list location_id national region sex source source_label source_type year acause) type(sum)

				** recalculate cause fractions
				foreach step in final corr rd raw {
					gen cf_`step' = deaths_`step'/sample_size
				}
				drop deaths*

			** Calculate the new deaths_final in the dataset and verify that the difference between the old is no greater than a rounding error	
				gen deaths_final = cf_final*sample_size
				su deaths_final
				local new_deaths_final = r(sum)
				di in red "OLD DEATHS=[`old_deaths_final'],NEW_DEATHS=['new_deaths_final']"
				drop deaths_final
				** subdiv should usually be empty for a location aggregate
				gen subdiv=""
		}

	// For the China data-series we need to prep them together (plus Hong Kong and Macau)
		if inlist("$source", "China_1991_2002", "China_2004_2012") {
			if "$source" == "China_1991_2002" local check_files "`in_dir'/China_2004_2012/data/final/10_aggregated.dta"
			if "$source" == "China_2004_2012" local check_files "`in_dir'/China_1991_2002/data/final/10_aggregated.dta"
			local check_files "`check_files' `in_dir'/ICD9_BTL/data/final/10_aggregated.dta `in_dir'/ICD9_detail/data/final/10_aggregated.dta `in_dir'/ICD10/data/final/10_aggregated.dta"
			foreach check_file of local check_files {
				capture confirm file "`check_file'"
				local round = 1
				while _rc == 601 {
					local time = `round' * 5
					display "`check_file' is missing. It's been `time' minutes. Waiting 5 more..." _newline
					local round = `round' + 1
					sleep 300000
					capture confirm file "`check_file'"
				}
				if _rc == 0 {
					display "`check_file' found!" _newline
					append using "`check_file'"
				}
			}
			keep if iso3 == "CHN"
		}
		
	// For South Africa and Kenya DHS, create 0s for missing years in every subnational unit before smoothing 
		if "$source" == "Other_Maternal" {
			gen flag_age_year = .
			foreach iso in ZAF KEN {
				** Get years and ages
				foreach measure of varlist year age {
					sum `measure' if iso3 == "`iso'" & location_id != . & (regexm(source_label, "DHS") | regexm(source_label, "DLHS")| regexm(source_label, "RHS"))
					local start_`measure' = `r(min)'
					local n_`measure's = (`r(max)' - `r(min)') + 1
				}
				local n_ages = ((`n_ages' - 1) / 5) + 1
				preserve
					odbc load, exec("SELECT location_id, ihme_loc_id AS iso3, region_id AS region FROM shared.location_hierarchy_history WHERE level = 4 AND location_set_version_id = 38 AND ihme_loc_id LIKE '`iso'%'") conn("`db_conn_str'") clear
					replace iso3 = substr(iso3,1,3)
					expand `n_years'
					bysort location_id iso3: gen year = _n - 1
					replace year = year + `start_year'
					expand `n_ages'
					bysort location_id iso3 year: gen age = (_n - 1) * 5
					replace age = age + `start_age'
					gen acause = "maternal"
					gen source = "Other_Maternal"
					gen list = "Other_Maternal"
					gen source_type = "Sibling history, survey"
					gen national = 1
					gen sex = 2
					tempfile dhs_demogs
					save `dhs_demogs', replace
				restore
				capture drop _m
				merge m:1 iso3 location_id region year age sex acause source source_type list national using `dhs_demogs'
				replace flag_age_year = 1 if _m == 2
				replace sample_size = 1 if _m == 2
				replace cf_final = 0 if _m == 2
				replace source_label = "DHS sibling history:  " + iso3 + " " + string(year) if _m == 2
			}
		}
		capture drop _m
	
	// Get region before reading in other datasets (reset Greenland to Greenland + Alaska (to be set later))
		replace region = 10000 if index("${source}","Greenland")
		levelsof region, local(source_regs) sep(",")
	
	// Save population file
		count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source == "$source"
		local VR = `r(N)'
		count if source == "$source"
		if (`VR'/`r(N)'==1) | inlist("$source", "Cancer_Registry", "Other_Maternal", "SUSENAS", "China_MMS_1996_2005", "China_MMS_2006_2012", "China_Child_1996_2012", "China_1991_2002") | strmatch("$source", "ICD*") {
			preserve
				keep iso3
				duplicates drop
				tempfile nr_locations
				save `nr_locations', replace
				do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
				keep if location_id == . & year >= 1990
				collapse (sum) pop, by(year iso3) fast
				collapse (mean) pop, by(iso3) fast
				merge 1:1 iso3 using `nr_locations', assert(1 3) keep(3) nogen
				tempfile nats
				save `nats', replace
				keep if inlist(iso3,"BRA","CHN","GBR","JPN") | inlist(iso3,"KEN","MEX","SAU","SWE","USA","ZAF")
				replace iso3 = iso3 + "_national"
				append using `nats'
				save "$temp_dir/11_noise_reduction/${source}/__mean_pop_1990_2015.dta", replace
			restore
		}
	
	// For the VR data-series we need to prep them together (get populations of the countries in this source prior to doing so)
		count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital"))
		if `r(N)'>0  & !inlist("$source", "China_1991_2002", "China_2004_2012") & "$source" != "Other_Maternal" {
		// Append on sources
		levelsof source, local(this_source) clean
		preserve
		odbc load, exec("select source from cod.data_version where data_type_id = 9 and status = 1 and source<>'`this_source'' and source<>'China_1991_2002' and source<>'China_2004_2012' and source<>'India_CRS'") conn("`db_conn_str'") clear
		levelsof source, local(sources)
		restore
		foreach source of local sources {
			capture confirm file "`in_dir'//`source'//data/final/10_aggregated.dta"
			local round = 1
			while _rc == 601 {
				local time = `round' * 5
				display "`source' file missing. It's been `time' minutes. Waiting 5 more..." _newline
				local round = `round' + 1
				sleep 300000
				capture confirm file "`in_dir'//`source'//data/final/10_aggregated.dta"
			}
			if _rc == 0 {
				display "`source' file found!" _newline
				append using "`in_dir'//`source'//data/final/10_aggregated.dta"
			}
		}
		}

	// Run noise reduction
		count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source == "$source"
		local VR = `r(N)'
		count if source == "$source"
		if (`VR'/`r(N)'==1) | inlist("$source", "Cancer_Registry", "Other_Maternal", "SUSENAS", "China_MMS_1996_2005", "China_MMS_2006_2012", "China_Child_1996_2012", "China_1991_2002") | strmatch("$source", "ICD*") {
			// Keep regions we need (exclude big countries from Caribbean, generate special region for Alaska and Greenland)
			replace region = 9999 if inlist(iso3, "PRI", "HTI", "DOM", "CUB")
			replace region = 10000 if location_id == 524 & index("${source}","Greenland")
			keep if inlist(region,`source_regs')
			// **********************************************************************************************************************************************
			// AGGREGATE SUBNATIONAL DATASETS TO NATIONAL AGGREGATES TO USE IN NOISE REDUCTION
				if inlist("$source", "UK_1981_2000", "UK_2001_2011", "Brazil_SIM_ICD9", "Brazil_SIM_ICD10", "Japan_by_prefecture_ICD10", "Japan_by_prefecture_ICD9") | inlist("$source", "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10", "South_Africa_by_province", "China_1991_2002", "China_2004_2012") | inlist("$source", "ICD9_detail", "ICD10", "Sweden_ICD9", "Sweden_ICD10", "ICD8_detail","Saudi_Arabia_96_2012") | "$source" == "Other_Maternal" {
					do "`out_dir'/code/subnational_aggregation.do" "noise_reduction"
				}
				
				// Confirm changes did not create duplicates
					duplicates tag NID iso3 location_id subdiv age sex year acause source_label, gen(dups)
					count if dups > 0
					if `r(N)' > 0 {
						di in red "Merge has created duplicate observations"
						save "$j/WORK/03_cod/01_database/03_datasets/$source/data/final/dups_fail.dta", replace
						BREAK
					}
					drop dups
			// **********************************************************************************************************************************************
			levelsof acause if source == "$source", local(causes)
			save "$temp_dir/11_noise_reduction/${source}/__causes_appended.dta", replace
			foreach cause of local causes {
				// Keep only the cause we are interested in
					count if acause == "`cause'"
					// only run causes with more than six observations (minimum DoF)
					if `r(N)'>6 {
						// Submit job
							if "$source" == "Cancer_Registry" {
								!qsub -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "CoD_11_${source}_`cause'" "$prog_dir/prep/code/shellstata13_${strUser}.sh" "`out_dir'/code/noise_reduction_Cancer.do" "$source $timestamp $strUser `cause'"
							}
							else if  "$source"!="Cancer_Registry" {
								!qsub -P proj_codprep -pe multi_slot 15 -l mem_free=30g -N "CoD_11_${source}_`cause'" "$prog_dir/prep/code/shellstata13_${strUser}.sh" "`out_dir'/code/noise_reduction.do" "$source $timestamp $strUser `cause'"
							}
						// Submit check
							!qsub -P proj_codprep -N "CoD_11_${source}_`cause'_check" -hold_jid  "CoD_11_${source}_`cause'" "$prog_dir/prep/code/shellstata13_${strUser}.sh" "`out_dir'/code/noise_reduction_check.do" "$source $timestamp $strUser `cause'"
							sleep 1000
					}
					else if `r(N)' <= 6 {
						preserve
							keep if acause == "`cause'"
							display in red "`cause' does not have enough observations!"
							save "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta", replace
							capture saveold "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta", replace
						restore
					}
			}

		// Loop through and append regression results back together
			clear
			foreach cause of local causes {
				// Check for file
					capture confirm file "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta"
					if _rc == 0 {
						display "FOUND!"
					}
					while _rc != 0 {
					display "`cause' not found, checking again in 60 seconds"
						sleep 60000
						capture confirm file "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta"
						if _rc == 0 {
							display "FOUND!"
						}
					}
					display "Appending in `cause'"
					capture append using "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta"
					if _rc != 0 {
						sleep 15000
						append using "$temp_dir/11_noise_reduction/${source}/`cause'_noise_reduced.dta"
					}
			}
			capture drop geo_id
			
			save "$temp_dir/11_noise_reduction/${source}/__noise_reduced_causes_appended.dta", replace
		
		// Fix regions we tinkered with
			replace region = 104 if inlist(iso3, "PRI", "HTI", "DOM", "CUB")
			replace region = 100 if location_id == 524
		}
		
// **********************************************************************************************************************************************
// RAKE SUBNATIONAL NOISE REDUCED DATA TO NATIONAL
	if inlist("$source", "UK_1981_2000", "UK_2001_2011", "Brazil_SIM_ICD9", "Brazil_SIM_ICD10", "Japan_by_prefecture_ICD10", "Japan_by_prefecture_ICD9") | inlist("$source", "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10", "South_Africa_by_province", "China_1991_2002", "China_2004_2012") | inlist("$source", "ICD9_detail", "ICD10", "Sweden_ICD9", "Sweden_ICD10", "ICD8_detail","Saudi_Arabia_96_2012") | "$source" == "Other_Maternal" {
		// keep only subnational countries and national aggregates (don't want DHS data)
		drop if !(regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source != "$source" & !inlist(source,"China_1991_2002", "China_2004_2012")
		* gen zero_val = 1 if deaths_final == 0
		* replace deaths_final = 0.00001 if deaths_final == 0
		preserve
			keep if index(iso3,"_national")
			keep age iso3 sex year acause deaths_final
			rename deaths_final nat_tot
			// reset 0s to incredibly small number for this purpose
			replace nat_tot = 0.0001 if nat_tot == 0
			replace iso3 = subinstr(iso3,"_national","",.)
			tempfile raked
			save `raked', replace
		restore
		// get aggregated 
		egen subnat_sum = total(deaths_final), by(age iso3 sex year acause)
		replace subnat_sum = 0.0001 if subnat_sum == 0
		merge m:1 age iso3 sex year acause using `raked', keep(1 3)
		replace deaths_final = deaths_final*(nat_tot/subnat_sum) if _m == 3
		* replace deaths_final = 0 if deaths_final == 0.00001 & zero_val == 1
		replace cf_final = deaths_final/sample_size if _m == 3
		drop _m 
		* drop zero_val
	}
	keep if source == "$source"
	capture drop if flag_age_year == 1
	capture drop flag_age_year
		
// **********************************************************************************************************************************************
//  APPLY VERBAL AUTOPSY NOISE REDUCTION
	count if index(source_type,"VA") | index(lower(source_type),"verbal autopsy")
	if `r(N)' > 0 {
		// Load all VA data
			preserve	
			odbc load, exec("SELECT source FROM cod.data_version WHERE status = 1 and data_type_id = 8 and source != '$source'") conn("`db_conn_str'") clear
			levelsof source, local(VA_sources)
			restore
			** Append final files
				foreach VA_source of local VA_sources {
					di "Searching for `VA_source' aggregation..."
					capture confirm file "`in_dir'/`VA_source'/data/final/10_aggregated.dta"
					if _rc == 0 {
						display "	...APPENDING"
					}
					while _rc != 0 {
					display "	...searching..."
						sleep 30000
						capture confirm file "`in_dir'/`VA_source'/data/final/10_aggregated.dta"
						if _rc == 0 {
							display "	...APPENDING"
						}
					}
					append using "`in_dir'/`VA_source'/data/final/10_aggregated.dta"
				}
		// Preserve CF prior to noise reduction
			capture gen double cf_before_smoothing = cf_final
			replace cf_before_smoothing = cf_final if cf_before_smoothing == .
		// Generate directories
			!rm -rf "$temp_dir/11_VA_noise_reduction/${source}"
			capture mkdir "$temp_dir/11_VA_noise_reduction"
			capture mkdir "$temp_dir/11_VA_noise_reduction/${source}"
			capture mkdir "$temp_dir/11_VA_noise_reduction/${source}/logs"
		// Drop non-VA , add back on after noise reduction
			preserve
				local no_nra = 0
				keep if (index(source_type,"VA") !=1 & index(source_type,"Verbal Autopsy") !=1) & source == "$source"
				count
				if `r(N)' > 0 {
					local no_nra = 1
					save "$temp_dir/11_VA_noise_reduction/${source}/NO_NRA_results.dta", replace
					capture saveold "$temp_dir/11_VA_noise_reduction/${source}/NO_NRA_results.dta", replace
				}
			restore
			drop if (index(source_type,"VA") !=1 & index(source_type,"Verbal Autopsy") !=1)
		// Prepare metadata and covariates
			preserve
				** Super region
				odbc load, exec("SELECT ihme_loc_id AS iso3, location_id, super_region_id, location_type AS type FROM shared.location_hierarchy_history WHERE location_set_version_id = 34") conn("`db_conn_str'") clear
				replace location_id = . if type == "admin0"
				replace iso3 = substr(iso3,1,3)
				keep iso3 location_id super_region_id
				tempfile sup_regs
				save `sup_regs', replace
				** Site-specific PfPR
				insheet using "$j/Project/Causes of Death/CoDMod/Models/A12/1_pre_modeling/datasets/pfpr_subnational_analysis/GBD2013/va_site_specific_pfpr_final_WITH_ISO_YEAR.csv", comma names clear
				rename site subdiv
				rename nid NID
				rename malaria_pfpr site_specific_pfpr
				drop itn
				drop if NID == .
				tempfile sspfpr
				save `sspfpr', replace
				** National PfPR
				odbc load, exec("SELECT ihme_loc_id AS iso3, location_id, location_type AS type, year_id AS year, mean_value AS national_pfpr FROM covariate.model JOIN covariate.model_version USING (model_version_id) JOIN covariate.data_version USING (data_version_id) JOIN shared.covariate USING (covariate_id) JOIN shared.location_hierarchy_history USING (location_id) WHERE location_hierarchy_history.location_set_version_id = 34 and model_version.status = 1 and data_version.status = 1 and is_best = 1 and covariate_name_short = 'malaria_pfpr'") dsn(prodcov) clear
				replace location_id = . if type == "admin0"
				replace iso3 = substr(iso3,1,3)
				drop type
				tempfile natpfpr
				save `natpfpr', replace
				** Malaria geographic groupings
				insheet using "$j/Project/Causes of Death/CoDMod/Models/A12/1_pre_modeling/datasets/other/countries_with_falciparum_or_vivax.csv", comma names clear
				keep iso3 group
				duplicates drop
					// *********************************************************************
					// Model South Africa, Cape Verde, and Mauritius outside of Africa, and Yemen along with Africa; Also model India by itself
					replace group = "outside" if group != "africa" | inlist(iso3,"ZAF","CPV","MUS")
					replace group = "africa" if iso3 == "YEM"
					replace group = "india" if iso3 == "IND"
					// *********************************************************************
				tempfile malaria_groups
				save `malaria_groups', replace
			restore
			merge m:1 iso3 location_id using `sup_regs', assert(2 3) keep(3) nogen
			merge m:1 iso3 year subdiv NID using `sspfpr', keep(1 3) nogen
			merge m:1 iso3 location_id year using `natpfpr', keep(1 3) nogen
			merge m:1 iso3 using `malaria_groups', keep(1 3) nogen
			** Set thresholds
			gen pfpr_level = site_specific_pfpr if site_specific_pfpr != . & acause == "malaria"
			replace pfpr_level = national_pfpr if site_specific_pfpr == . & acause == "malaria"
			** Identify PfPR groups
			gen pfpr_group = "SSA_hypoendem" if group == "africa" & pfpr_level < 0.05
			replace pfpr_group = "SSA_mesoendem" if group == "africa" & pfpr_level >= 0.05 & pfpr_level < 0.4
			replace pfpr_group = "SSA_hyperendem" if group == "africa" & pfpr_level >= 0.4 & pfpr_level != .
			replace pfpr_group = "IND_hypoendem" if group == "india" & pfpr_level < 0.05
			replace pfpr_group = "IND_mesoendem" if group == "india" & pfpr_level >= 0.05 & pfpr_level < 0.4
			replace pfpr_group = "IND_hyperendem" if group == "india" & pfpr_level >= 0.4 & pfpr_level != .
			replace pfpr_group = "G_hypoendem" if group == "outside" & pfpr_level < 0.05
			replace pfpr_group = "G_mesoendem" if group == "outside" & pfpr_level >= 0.05 & pfpr_level < 0.4
			replace pfpr_group = "G_hyperendem" if group == "outside" & pfpr_level >= 0.4 & pfpr_level != .
		// Set groupings for noise reduction
			gen nra_group = string(super_region_id) if pfpr_group == ""
			replace nra_group = pfpr_group if pfpr_group != ""
			** Split off India
			replace nra_group = "IND" if iso3 == "IND" & acause != "malaria"
		// Save datasets by cause and super region for all causes (except malaria, which is grouped by PfPR)
			levelsof acause if source == "$source", local(acauses)
			foreach acause of local acauses  {
				levelsof nra_group if acause == "`acause'" & source == "$source", local(`acause'_groups)
				foreach group of local `acause'_groups {
					preserve
					keep if acause == "`acause'" & nra_group == "`group'"
					** Save file for regression
					save "$temp_dir/11_VA_noise_reduction/${source}/`acause'_`group'_raw.dta", replace
					capture saveold "$temp_dir/11_VA_noise_reduction/${source}/`acause'_`group'_raw.dta", replace
					** Submit noise reduction job
					!qsub -P proj_codprep -pe multi_slot 2 -l mem_free=4g -N "CoD_11va_${source}_`acause'_`group'" "$prog_dir/prep/code/shellstata13_${strUser}.sh" "`out_dir'/code/noise_reduction_VA.do" "$source $temp_dir $timestamp `acause' `group'"
					restore
				}
			}
			
		// Compile results
			clear
			foreach acause of local acauses {
				foreach group of local `acause'_groups {
					di "Searching for `acause' `group' results..."
					capture confirm file "$temp_dir/11_VA_noise_reduction/${source}/`acause'_`group'_results.dta"
					if _rc == 0 {
						display "	...COMPLETE"
					}
					while _rc != 0 {
					display "	...waiting..."
						sleep 60000
						capture confirm file "$temp_dir/11_VA_noise_reduction/${source}/`acause'_`group'_results.dta"
						if _rc == 0 {
							display "	...COMPLETE"
						}
					}
					append using "$temp_dir/11_VA_noise_reduction/${source}/`acause'_`group'_results.dta"
				}
			}
		
		// Return non-VA data to dataset
			if `no_nra' == 1 {
				append using "$temp_dir/11_VA_noise_reduction/${source}/NO_NRA_results.dta"
			}
			
	}
	capture gen double cf_before_smoothing = cf_final

	// Only keep the data we are noise reducing
	keep if source == "$source"
	display "$source"
	count
	
// **********************************************************************************************************************************************
// APPLY NON-ZERO FLOOR OF 1 DEATH PER 10,000,000
	// prep the envelope
		preserve
			do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
			** only split using the national population distribution
			keep if location_id == .
			** we don't need env for age-sex splitting
			reshape long pop env, i(iso3 year sex) j(age)
			rename env mean_env
			rename pop mean_pop
			replace age = (age-6)*5 if age!=3 & age<90
			replace age = 1 if age==3
			tempfil pop
			save `pop', replace
		restore
		joinby iso3 year age sex using `pop'

	// Make death rates and calculate the cf if the rate were 1 per 10,000,000
		gen double rate = (cf_final * mean_env) / mean_pop
		gen double rate_replace = .0000001
		gen double cf_replace = (rate_replace * mean_pop) / mean_env
		replace cf_replace = 0 if cf_replace == .
	// Replace the CF with the rate-adjusted CF if the rate is less than 1 per 10,000,000
		replace cf_final = cf_replace if rate<rate_replace & rate>0
		
	// Tidy up
		compress
		drop mean_pop rate* cf_replace mean_env

	** For US counties, append back the counties data that is not noise reduced
	if inlist("$source", "US_NCHS_counties_ICD10", "US_NCHS_counties_ICD9") & `temp_dont_do_it'==0 {
		append using "`in_dir'/$source/data/final/10_aggregated.dta"
	}

	// save
		compress
		save "`in_dir'/$source/data/final/11_noise_reduced.dta", replace
		save "`in_dir'/$source/data/final/_archive/11_noise_reduced_$timestamp.dta", replace