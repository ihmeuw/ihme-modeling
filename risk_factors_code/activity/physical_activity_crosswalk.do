// Purpose:  Compile and crosswalk all extracted physical activity data for dismod upload. Note: the regression is intended to adjust for the discrepancies between GPAQ and IPAQ (IPAQ is our gold standard) and to account for discrepancies in activity level  between urban and rural subpopulations 

// Set up STATA
	clear all
	set more off
	capture log close
	set mem 10g
	set maxvar 32767
	
// Create locals for relevant files and folders
	local data_dir "`directory'"
	local files: dir "`data_dir'" files "`files'"
	local outdir "`directory'"
	local logs "`directory'" 
	local dismod_dir "`directory'"
	
// Prepare countrycodes database 
	run "`file'" 
	get_location_metadata, location_set_id(9) clear
	keep if is_estimate == 1 & inlist(level, 3, 4)

	keep ihme_loc_id location_id location_ascii_name super_region_name super_region_id region_name region_id
	
	rename ihme_loc_id iso3 
	tostring location_id, replace
	rename location_ascii_name location_name
	
	
	gen superregion = 1 if super_region_name == "High-income" 
	replace superregion = 2 if super_region_name == "Central Europe, Eastern Europe, and Central Asia" 
	replace superregion = 3 if super_region_name == "Sub-Saharan Africa" 
	replace superregion = 4 if super_region_name == "North Africa and Middle East" 
	replace superregion = 5 if super_region_name == "South Asia"
	replace superregion = 6 if super_region_name == "Southeast Asia, East Asia, and Oceania"
	replace superregion = 7 if super_region_name == "Latin America and Caribbean"
	
	label define region_name 1 "High-income" 2 "Central Europe, Eastern Europe, and Central Asia" 3 "Sub-Saharan Africa" 4 "North Africa and Middle East" 5 "South Asia" 6 "Southeast Asia, East Asia, and Oceania" 7 "Latin America and Caribbean"
	label values superregion region_name
	

	split iso3, p("_") 
	rename iso31 parent_iso3
	
	keep location_id iso3 parent_iso3 location_name superregion 
	tostring location_id, replace
	
	tempfile countrycodes
	save `countrycodes', replace

	
// Prepare proportion urban covariate for crosswalk
	adopath + "`path'" 
	get_covariate_estimates, covariate_name_short(prop_urban) clear
	keep location_id location_name year_id mean_value 
	rename year_id year_start 
	tempfile urbanicity 
	save `urbanicity', replace 
	
// 1.) Append datasets for each extracted microdata survey series/country together 
		use "`data_dir'/whs_prepped", clear
		foreach file of local files {
			if "`file'" != "`data_file'" & "`file'" != "`data_file'" {
				di in red "`file'" 
				append using "`data_dir'/`file'", force
			}
		}

			// Drop if we don't currently have information on METs
		drop if total_mets_mean == . | total_mets_mean == 0


		replace questionnaire = "other" if survey_name == "Behavioral Risk Factor Surveillance System"
		replace questionnaire = "other" if survey_name == "Health Survey of England"
		
		keep if inlist(questionnaire, "GPAQ", "IPAQ", "IPAQ_long", "other")
		drop source 
		gen source = "microdata"
		keep iso3 location_id year_start year_end sex age_start age_end sample_size *active* file national_type urbanicity_type survey_name questionnaire nid survey_name data_type source_type
		gen orig_uncertainty_type = "SE"
	
	// Reshape long: Instead of having a mean activity variable for each activity threshold, will have 1 mean variable and one row per activity level
		foreach cat in inactive lowactive modactive highactive lowmodhighactive modhighactive  {
			rename `cat'_mean mean_`cat'
			rename `cat'_se se_`cat'

		}
	
		reshape long mean_@ se_@, i(file location_id nid iso3 year_start year_end sex age_start age_end sample_size) j(healthstate, string)


	// Format healthstates for Dismod
		foreach category in inactive lowactive modactive highactive lowmodhighactive modhighactive {
			replace healthstate = "activity_`category'" if healthstate == "`category'"
		}
			
		foreach category in low mod high lowmodhigh modhigh {
			replace healthstate = "activity_`category'" if healthstate == "activity_`category'active"
		}
		
	// Rename variables for consistency with other datasets
		rename mean_ mean
		rename se_ standard_error
	
	// Don't have a proportion for all healthstates so drop missing means
		drop if mean == . 
		replace orig_uncertainty_type = "ESS" if (standard_error == 0 | standard_error == .) & sample_size != .
		recode standard_error 0=. // Standard error is calculated to be 0 when mean is 0 or 1, so we will approximate it using sample size below

// 2.) Bring in reformatted/prepped tabulated data that was provided to us by experts/collaborators
	append using "`data_dir'/`data_file'"
	append using "`data_dir'/`data_file'"
	
	tostring location_id, replace
	
// 3.) Bring in extracted tabulated data
	append using "`data_dir'/`data_file'"

	// Etude Natoinale Nutrition Sante (ENNS)
	replace nid = 124439 if regexm(file, "INDEXERS")

	// New South Wales Population Health Survey 
	local nsw "`survey'"

	replace nid = 139725 if file == "`nsw'" & year_start == 2002
	replace nid = 139726 if file == "`nsw'" & year_start == 2003
	replace nid = 139728 if file == "`nsw'" & year_start == 2004
	replace nid = 139729 if file == "`nsw'" & year_start == 2005
	replace nid = 139730 if file == "`nsw'" & year_start == 2006
	replace nid = 139731 if file == "`nsw'" & year_start == 2007
	replace nid = 139732 if file == "`nsw'" & year_start == 2008 
	replace nid = 139733 if file == "`nsw'" & year_start == 2009 
	replace nid = 139734 if file == "`nsw'" & year_start == 2010 
	replace nid = 139735 if file == "`nsw'" & year_start == 2011
	replace nid = 139736 if file == "`nsw'" & year_start == 2012 

	replace nid = 135801 if iso3 == "SWE" & year_start == 2002 
	replace year_start = 2001 if nid == 135801

	// National Nutrition and Physical Activity Survey (NNPAS)
		
		replace nid = 113243 if file == "`survey'"
		
		// Global physical activity questionnaire nine country reliability and validity study 
		
		replace nid = 139425 if regexm(citation, "nine country reliability and validity study")
		replace nid = 30181 if regexm(file, "BRAZZAVILLE_2004") 

		// National Health Survey in Brazil (PNS) - 2013
		replace nid = 195010 if source_name == "Brazil National Health Survey"

// SPLIT LOCATIONS FOR LIT 

		destring location_id, replace
		
		drop if regexm(file, "IND_STEPS_NCD_2007") & location_id == 163 
		expand 2 if nid == 129594 & iso3 == "IND", gen(tag)
		replace location_id = 43904 if nid == 129594 & iso3 == "IND" & tag == 0 
		replace iso3 = "IND_43904" if location_id == 43904
		replace location_id = 43891 if nid == 129594 & iso3 == "IND" & tag == 1 
		replace iso3 = "IND_43891" if location_id == 43891


drop if nid == .

// 4.) Prepare dataset for crosswalk 
// Merge with country codes database to get parent ISO3 because the proportion urban covariate database does not have subnational estimates
		
	// Merge physical activity dataset with proportion urban covariate 
	
	split iso3, p("_") 
	replace location_id = iso32 if location_id == ""
	replace location_id = "97" if iso3 == "ARG"
	replace location_id = "98" if iso3 == "CHL" 
	replace location_id = "144" if iso3 == "JOR" 

	replace iso3 = "CHN" if iso3 == "CHN_6"
	drop iso31 iso32
	rename location_id location_id_old 

	// Merge on country codes 
	merge m:1 iso3 using `countrycodes', nogen keep(match)
	replace location_id = location_id_old if location_id_old != location_id & (location_id_old != "" & location_id_old != ".") 
	drop location_id_old
	destring location_id, replace

	// Merge physical activity dataset with proportion urban covariate 
		merge m:1 location_id year_start using `urbanicity', nogen keep(match)

		/*
		split iso3, p("_")
		rename iso31 parent_iso3
		merge m:1 parent_iso3 year_start using `urbanicity', nogen keep(match)
		merge m:m iso3 using `countrycodes', nogen keep(match)
		*/

	// Track original uncertainty type before crosswalking
		replace orig_uncertainty_type = "SE" if standard_error != . & orig_uncertainty_type == ""
		replace orig_uncertainty_type = "ESS" if standard_error == . & sample_size != . & orig_uncertainty_type == ""
		replace orig_uncertainty_type = "CI" if standard_error == . & sample_size == . & upper != . & orig_uncertainty_type == ""
		
	// Calculate standard deviation for all data points so that 1,000 draws can be generated
		// standard error from upper and lower bounds
			replace standard_error = (upper - lower) / (2 * 1.96) if standard_error == .
		
		// Use Wilson's score interval to approximate standard error where we only have sample size
			gen approximated_se = standard_error == .
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if standard_error == .
			
	// Construct urbanicity covariate (1=urban 0=rural, proportion urban if nationally representative or mixed)

		// Missing values for urbanicity type
		replace urbanicity_type = 2 if survey_name == "VIGITEL" 
		replace urbanicity_type = 2 if regexm(survey_name, "Brazil Household Survey on Risk Factors") 
		replace urbanicity_type = 3 if survey_name == "CIEMS" 

		gen urbanicity = 1 if urbanicity_type == 2
		replace urbanicity = 0 if urbanicity_type == 3
		rename mean_value prop_urban
		replace urbanicity = prop_urban if urbanicity_type == 1 | urbanicity_type == 0
	
	// Dummy variable for GPAQ questionnaire
		gen gpaq = questionnaire == "GPAQ"
		
	// Aggregate ages to make age groups for regression	
		gen age_dif = age_end - age_start
		tostring age_start age_end, replace
		gen agegrp = age_start + "_" + age_end
		drop if inlist(agegrp, "15_17", "15_19", "15_24")
		replace agegrp = "all" if regexm(agegrp, "15 | 16 | 18") | agegrp == "25_100"
		replace agegrp = "65+" if inlist(agegrp, "65_100", "65_84", "65_90")
		replace agegrp = "70_100" if regexm(agegrp, "70|75|80")
		foreach strtage in 30 40 50 60 {
			local o = `strtage' - 5
			local n = `strtage' + 4
			di "`o'_`n'"
			replace agegrp = "`o'_`n'" if agegrp == "`strtage'_`n'"
		}
		foreach strtage in 25 35 45 55 65 {
			local endage = `strtage' + 9
			local n = `strtage' + 4
			replace agegrp = "`strtage'_`endage'" if agegrp == "`strtage'_`n'"
		}
			
		destring age_start age_end, replace
		
	// Create age dummies (all = absorbed) 
		preserve
		keep if inlist(agegrp, "all", "25_34", "35_44", "45_54", "55_64", "65_74", "70_100") & questionnaire != ""
		levelsof agegrp, local(ages)
		restore
		
		foreach a of local ages {
			gen age_`a' = agegrp == "`a'"
		}
		
	// Clean up dataset
		drop age_dif urban 

	// Logit transform proportion (dependent variable)
		gen logit_mean = logit(mean)
		replace logit_mean = logit(.01) if mean == 0
		replace logit_mean = logit(.99) if mean == 1
			
	// Tempfile compiled dataset
		tempfile compiled
		save `compiled'	
	
// 4a.) Run GPAQ-IPAQ regression for females and based on nationally representative data sources only, stratified by healthstate, with an interaction between super-region and GPAQ & controlling for age
		log using "`logs'", replace
			levelsof healthstate, local(healthstates)
			foreach healthstate of local healthstates {
				di in red "Healthstate: `healthstate'"
				use `compiled' if healthstate == "`healthstate'" & sex == 2 & urbanicity_type == 1, clear

				**  Only keep surveys that are gpaq or ipaq and have an age range that I could group for regression  
				keep if inlist(agegrp, "all", "25_34", "35_44", "45_54", "55_64", "65_74", "70_100") & questionnaire != "" & questionnaire != "IPAQ_long"
		
				** Crosswalk
				xi: mixed logit_mean i.superregion*gpaq age_25_34 age_35_44 age_45_54 age_55_64 age_65_74 age_70_100 || iso3 : 
					
				** Extract coefficients and covariance matrices for GPAQ, GPAQ-super region interaction 
				mat b = e(b)' // ' creates columnar matrix rather than dUSERt row matrix
				mat b2 = b[7..13,1]
				mat v = e(V)
				mat v2 = v[7..13,7..13]
				
				** Use the "drawnorm" function to create draws using the mean and standard deviations from the covariance matrix
				clear 
				set obs 1000
				drawnorm gpaq_female sr2_female sr3_female sr4_female sr5_female sr6_female sr7_female, means(b2) cov(v2)

				gen healthstate = "_`healthstate'" 

				tempfile _`healthstate'
				save `_`healthstate''
			}
		log close
	
				**  Combine healthstates
				clear
				foreach healthstate of local healthstates {
					append using `_`healthstate''
				}

				** Reshape wide so that each healthstate and sex has it's own beta variable
				bysort healthstate: gen id = _n
				reshape wide gpaq_* sr*, i(id) j(healthstate, string)
				
				tempfile gpaq_cw
				save `gpaq_cw'
		
		// save crosswalk coefficients for use in relative risks
		save "`filepath'", replace 

	// Apply regression results to non-gold standard data
			use `compiled', clear
			
			** 1,000 draws from normal distribution around raw mean	
			forvalues d = 1/1000 {
				quietly {
					gen double mean_`d' = exp(rnormal(ln(mean), ln(1+standard_error/mean)))
					replace mean_`d' = exp(rnormal(ln(0.999), ln(1+standard_error/0.999))) if mean == 1
					replace mean_`d' = exp(rnormal(ln(0.001), ln(1+standard_error/0.001))) if mean == 0
					gen double adjmean_`d' = .
				}
			}

			** Merge in draws of regression coefficients
			gen id = _n
			merge 1:1 id using `gpaq_cw', nogen
			drop id
			
			** Crosswalk
			levelsof superregion, local(superregions)
			foreach healthstate of local healthstates {
				foreach sregion of local superregions {
					di in red "Healthstate: `healthstate', Super Region: `sregion'"
					quietly {
						** High income super region is reference 
						if `sregion' == 1  {
							forvalues d = 1/1000 {
								 replace adjmean_`d' = invlogit(gpaq_female_`healthstate'[`d'] * (0 - gpaq) + logit(mean_`d')) if healthstate == "`healthstate'" & sex == 2 & superregion == `sregion'
							}
						}
				
						** All other super regions
						if `sregion' != 1 {
							forvalues d = 1/1000 {
								replace adjmean_`d' = invlogit((gpaq_female_`healthstate'[`d'] + sr`sregion'_female_`healthstate'[`d']) * (0 - gpaq) +  logit(mean_`d')) if healthstate == "`healthstate'" & sex == 2 & superregion == `sregion'
								
							}
						}
					}
				}
			}

			** Compute mean and 95% CI from draws
			egen gpaq_mean = rowmean(adjmean_*)
			egen gpaq_lower = rowpctile(adjmean_*), p(2.5)
			egen gpaq_upper = rowpctile(adjmean_*), p(97.5)
			gen gpaq_se = (gpaq_upper - gpaq_lower) / (2 * 1.96)
			
			** Save GPAQ-adjusted dataset
			drop mean_* adjmean_* sr* gpaq_female*
			tempfile gpaq_adjusted
			save `gpaq_adjusted', replace
			
// 4b.) Run urbanicity regression stratified by healthstate, with an interaction between sex and urbanicity & controlling for age
		log using "`logs'", replace
			foreach healthstate of local healthstates {
				di in red "Healthstate: `healthstate'"
				use `gpaq_adjusted' if healthstate == "`healthstate'" & (urbanicity_type == 2 | urbanicity_type == 3), clear
				
					** Drop data points where sex = 3 (both males and females) because there are only a few and they are all nationally representative data points
					drop if sex == 3
				
					** Logit transform gpaq-adjusted mean for women
					replace logit_mean = logit(gpaq_mean) if sex == 2 & gpaq == 1
					
					** Run mixed effects regression
					xi: mixed logit_mean i.sex*urbanicity age_25_34 age_35_44 age_45_54 age_55_64 age_65_74 age_70_100 || iso3 : 
	
					** Extract coefficients and covariance matrices for GPAQ, GPAQ-super region interaction and urbanicity
					mat b = e(b)' // ' creates columnar matrix rather than dUSERt row matrix
					mat b2 = b[2..3,1]
					mat v = e(V)
					mat v2 = v[2..3,2..3]
				
					** Use the "drawnorm" function to create draws using the mean and standard deviations from the covariance matrix
					clear 
					set obs 1000
					drawnorm urbanicity urbanicity2, means(b2) cov(v2)
					
					gen healthstate = "_`healthstate'"
					
				tempfile _`healthstate'
				save `_`healthstate''
			}
		log close
		
		** Combine healthstates
		clear
		foreach healthstate of local healthstates {
			append using `_`healthstate''
		}

		** Reshape wide so that each healthstate and sex has it's own beta variable
		bysort healthstate: gen id = _n
		reshape wide urbanicity urbanicity2, i(id) j(healthstate, string)
			
		
		tempfile urbanicity_cw
		save `urbanicity_cw', replace			
			
	// Apply regression results to non-gold standard data
			use `gpaq_adjusted', clear
			
		// 1,000 draws from normal distribution around gpaq-adjusted mean for women and raw mean for men
			forvalues d = 1/1000 {
				quietly {
					gen double mean_`d' = exp(rnormal(ln(gpaq_mean), ln(1+gpaq_se/gpaq_mean))) if sex == 2
					replace mean_`d' = exp(rnormal(ln(0.999), ln(1+gpaq_se/0.999))) if gpaq_mean >= 1 & sex == 2
					replace mean_`d' = exp(rnormal(ln(0.001), ln(1+gpaq_se/0.001))) if gpaq_mean <= 0 & sex == 2
					
					replace mean_`d' = exp(rnormal(ln(mean), ln(1+standard_error/mean))) if sex == 1
					replace mean_`d' = exp(rnormal(ln(0.999), ln(1+standard_error/0.999))) if mean >= 1 & sex == 1
					replace mean_`d' = exp(rnormal(ln(0.001), ln(1+standard_error/0.001))) if mean <= 0 & sex == 1
					
					gen double adjmean_`d' = .
				}
			}

		// Merge in draws of regression coefficients
			gen id = _n
			merge 1:1 id using `urbanicity_cw', nogen
			drop id
		
		// Crosswalk
			foreach healthstate of local healthstates {
				foreach sex in 1 2 {
					di in red "Healthstate: `healthstate', sex: `sex'"
					quietly {
						// High income super region is reference 
						if `sex' == 1  {
							forvalues d = 1/1000 {
								replace adjmean_`d' = invlogit(urbanicity_`healthstate'[`d'] * (prop_urban - urbanicity) + logit(mean_`d')) if healthstate == "`healthstate'" & sex == `sex' 
							}
						}
				
						// All other super regions
						if `sex' == 2 {
							forvalues d = 1/1000 {
								replace adjmean_`d' = invlogit((urbanicity_`healthstate'[`d'] + urbanicity`sex'_`healthstate'[`d']) * (prop_urban - urbanicity)  + logit(mean_`d')) if healthstate == "`healthstate'" & sex == `sex' 
							}
						}
					}
				}	
			}
			
	// Compute mean and 95% CI from draws
		egen adjusted_mean = rowmean(adjmean_*)
		egen adjusted_lower = rowpctile(adjmean_*), p(2.5)
		egen adjusted_upper = rowpctile(adjmean_*), p(97.5)
		gen adjusted_se = (adjusted_upper - adjusted_lower) / (2 * 1.96)
		
	// Make tempfile for comparing adjusted an unadjusted means
		tempfile crosswalk
		save `crosswalk', replace
		
	// Replace mean and standard error for non-gold standard data points
		replace standard_error = gpaq_se if gpaq == 1 & urbanicity_type == 1 & sex == 2 
		replace mean = gpaq_mean if gpaq == 1 & urbanicity_type == 1 & sex == 2
		replace lower = gpaq_lower if gpaq == 1 & urbanicity_type == 1 & sex == 2
		replace upper = gpaq_upper if gpaq == 1 & urbanicity_type == 1 & sex == 2
		
		replace standard_error = adjusted_se if (urbanicity_type == 2 | urbanicity_type == 3) 
		replace mean = adjusted_mean if (urbanicity_type == 2 | urbanicity_type == 3) 
		replace lower = adjusted_lower if (urbanicity_type == 2 | urbanicity_type == 3) 
		replace upper = adjusted_upper if (urbanicity_type == 2 | urbanicity_type == 3) 
	
	// Clean up
		replace source_name = survey_name if source_name == ""
		drop mean_* adjusted* prop_urban parent_iso3 questionnaire case_name gpaq_* adjmean_* mean_* urbanicity_activity* urbanicity2_* gpaq logit_mean agegrp age_*4 age_70_100 age_all superregion approximated_se urbanicity


// PREPARE COLUMNS FOR EPI UPLOADER 

	// Make labels for urbanicity and representation
		label define urbanicity 0 "Unknown" 1 "Mixed/both" 2 "Urban" 3 "Rural" 4 "Suburban" 5 "Peri-urban", replace
		label values urbanicity_type urbanicity

		//replace urbanicity_type = 2 if urbanicity_type == . & regexm(file, "MAHARASHTRA") 
		//replace urbanicity_type = 1 if nid == 129594 & iso3 == "CHN_354" 

		decode urbanicity_type, gen(urbanicity_type_new) 
		drop urbanicity_type
		rename urbanicity_type_new urbanicity_type
*/
		rename national_type representative_name 
		label define national_new 1 "Nationally representative only" 2 "Representative for subnational location only" 3 "Not representative" 4 "Nationally and subnationally representative" /// 
		5 "Nationally and urban/rural representative" 6 "Nationally, subnationally and urban/rural representative" 7 "Representative for subnational location and below" /// 
		8 "Representative for subnational location and urban/rural" 9 "Representative for subnational location, urban/rural and below" 10 "Representative of urban areas only" /// 
		11 "Representative of rural areas only" 	
		label values representative_name national_new

		replace file = subinstr(file, "\", "/", .)

		split file, p("/") 
		replace representative_name = 3 if regexm(file, "WHO_STEPS") & regexm(file5, "([A-Z])") 
		replace representative_name = 3 if regexm(iso3, "BRA") & file == "" & nid != 129594 // just want to replace for VIGITEL
		replace representative_name = 1 if regexm(iso3, "BRA") & regexm(file, "RISK_FACTOR_MORBIDITY") & representative_name == . 
		drop if regexm(file, "2002_2003_CH8_PHYSICAL_ACTIVITY.PDF")
		drop file1 file2 file3 file4 file5 file6 file7 file8 file9 file10 file11

	// Specify "notrepresentative" covariate, which designates data points that are not nationally representative and do not correspond to a GBD subnational location
		gen cv_notrepresentative = inlist(representative_name, 3, 10, 11 )
		

		//replace cv_notrepresentative = 0 if representative_name != 1 & urbanicity_type == 1 & regexm(iso3, "[0-9]")


		// 	gen cv_notrepresentative = national_type == 3 | (national_type == 2 & substr(iso3, 1, 1) != "X")

	// DisMod uploader actually wants string values so decode representative_name and urbanicity_type

		decode representative_name, gen(rep_name_new)
		rename representative_name rep_name_numeric
		rename rep_name_new representative_name

	// Clean up uncertainty 
		rename orig_uncertainty_type uncertainty_type 
		replace uncertainty_type = "Effective sample size" if uncertainty_type == "ESS" 
		replace uncertainty_type = "Standard error" if uncertainty_type == "SE"
		replace uncertainty_type = "Confidence interval" if uncertainty_type == "CI" 
		gen uncertainty_type_value = 95 
		replace uncertainty_type_value = . if lower == . | upper == . 

	// Sex should be string for epi uploader 
		tostring sex, replace 
		replace sex = "Male" if sex == "1" 
		replace sex = "Female" if sex == "2" 
		replace sex = "Both" if sex == "3" 


// Set up to merge with GHDx database (based on file paths so need to parse file path) 
	replace file = subinstr(file,"\","/",.)
	gen example = regexs(0) if regexm(file,"[\/]([A-z.0-9]*)$")
	gen path = subinstr(file, example, "", .) 
	
	replace path = "`survey'" if path == "`survey'"
	replace path = "`survey'" if path == "`survey'"
	replace path = "`survey'" if path == "`survey'"
	
	tostring year_start year_end, replace
	replace path = path + "/" + year_start if regexm(path, "([A-Z])$") & regexm(path, "WHO_STEPS") 
	destring year_start year_end, replace 
	
	replace path = "`survey'" if path == "`survey'"
	replace path = "`survey'" if path == "`survey'" 
	replace path = "`survey'" if path == "`survey'"
	replace path = "`survey'" if path == "`survey'"
	
	replace path = "`survey'" if path == "`survey'"
	replace path = "`survey'" if path == "" & iso3 == "IRN" & year_start == 2007
	replace path = "`survey'" if path == "" & iso3 == "IRN" & year_start == 2011

	
	tempfile all 
	save `all', replace 
	
// Get citations

	// Prepare citations database 
	#delim ;
    odbc load, exec("SELECT fl.field_location_value file_path, fn.field_file_name_value file_name, records.nid record_nid, records.file_id file_nid
    FROM
    (SELECT entity_id nid, field_internal_files_target_id file_id
    FROM ghdx.field_data_field_internal_files) records
    JOIN ghdx.field_data_field_location fl ON fl.entity_id = records.file_id
    JOIN ghdx.field_data_field_file_name fn ON fn.entity_id= records.file_id
    ORDER BY record_nid") dsn(ghdx) clear;
    #delim  cr

 // format
    replace file_path = subinstr(file_path, "\", "/", .)
    drop file_nid
    // drop file_path nid duplicates
    duplicates drop file_path record_nid, force
    // keep single source
    duplicates tag file_path, gen(dupe)
    	// drop country/year duplicates
		duplicates drop file_path if dupe > 0 & regexm(file_path, "[0-9][0-9][0-9][0-9]$") & !regexm(file_path, "EUROBAROMETER"), force
    	drop dupe
    	duplicates tag file_path, gen(dupe)
    	// save remaining duplicates in a separate file
    	preserve
    		keep if dupe > 0 
    		duplicates drop file_path file_name, force
    		rename file_path path
    		tempfile ghdx_dupe
    		save `ghdx_dupe', replace
    	restore
    // save
    drop if dupe > 0 

     rename file_path path 

    tempfile ghdx
    save `ghdx', replace

// Bring in full file 
	use `all', clear

    merge m:1 path using `ghdx', keep(1 3) nogen
    replace nid = record_nid if nid == . 
    drop record_nid 


    replace example = subinstr(example, "/", "", .)
    replace file_name = example if file_name == ""

    tempfile ready
    save `ready', replace

    merge m:1 path file_name using `ghdx_dupe', keep(1 3) nogen
    replace nid = record_nid if nid == . & nid != 129886
  

	// Replace any nids that are still missing 
	
		// Etude Natoinale Nutrition Sante (ENNS)
		replace nid = 124439 if regexm(path, "INDEXERS")
		
		// New South Wales Population Health Survey 
		local nsw "`survey'"

		replace nid = 139725 if file == "`nsw'" & year_start == 2002
		replace nid = 139726 if file == "`nsw'" & year_start == 2003
		replace nid = 139728 if file == "`nsw'" & year_start == 2004
		replace nid = 139729 if file == "`nsw'" & year_start == 2005
		replace nid = 139730 if file == "`nsw'" & year_start == 2006
		replace nid = 139731 if file == "`nsw'" & year_start == 2007
		replace nid = 139732 if file == "`nsw'" & year_start == 2008 
		replace nid = 139733 if file == "`nsw'" & year_start == 2009 
		replace nid = 139734 if file == "`nsw'" & year_start == 2010 
		replace nid = 139735 if file == "`nsw'" & year_start == 2011
		replace nid = 139736 if file == "`nsw'" & year_start == 2012 
		
		// National Nutrition and Physical Activity Survey (NNPAS)
		
		replace nid = 113243 if path == "`survey'"
		
		// Global physical activity questionnaire nine country reliability and validity study 
		
		replace nid = 139425 if regexm(citation, "nine country reliability and validity study")
		replace nid = 30181 if regexm(file, "BRAZZAVILLE_2004") 

		// National Health Survey in Brazil (PNS) - 2013
		replace nid = 195010 if source_name == "Brazil National Health Survey"
		drop example

// SPLIT LOCATIONS FOR LIT 
		
		drop if regexm(file, "IND_STEPS_NCD_2007") & location_id == 163 // Initially extracted the national report (which reported states without urban/rural status) as well as subnational reports (with urban/rural status); just need to keep subnational reports that have urbanicity
		expand 2 if nid == 129594 & iso3 == "IND", gen(tag)
		replace location_id = 43904 if nid == 129594 & iso3 == "IND" & tag == 0 
		replace iso3 = "IND_43904" if location_id == 43904
		replace location_id = 43891 if nid == 129594 & iso3 == "IND" & tag == 1 
		replace iso3 = "IND_43891" if location_id == 43891

// Fill in location names that don't match location ids 
	/*
	replace location_id = "" if location_id == "." 
	replace iso3 = iso3 + "_" + location_id if location_id != "" & !regexm(iso3, "[0-9]")
	replace location_id = iso32 if location_id == "" & iso32 != "" 
*/	
	
	rename location_name location_name_old
	tempfile ready 
	save `ready', replace


	use `countrycodes', clear
	destring location_id, replace
	merge 1:m location_id using `ready', keep(3) nogen
	drop location_name_old 
	
	
// Source type 
	
	drop source_type
	gen source_type = 26 
	label define source 26 "Survey - other/unknown" 
	label values source_type source
	
// Fill in epi variables for Dismod
	rename sample_size effective_sample_size
	gen measure = "proportion"
	gen unit_value_as_published = 1 
	gen extractor = "lalexan1" 
	gen is_outlier = 0 
	gen underlying_nid = . 
	gen sampling_type = "" 
	gen recall_type = "Point" 
	gen recall_type_value = "" 
	gen unit_type = "Person" 
	gen input_type = "" 
	gen sample_size = . 
	gen cases = . 
	gen design_effect = . 
	gen site_memo = "" 
	gen case_name = "" 
	gen case_definition = "" 
	gen case_diagnostics = "" 
	gen response_rate = .
	gen note_SR = "" 
	gen note_modeler = "" 
	gen row_num = . 
	gen parent_id = . 
	gen data_sheet_file_path = "" 

	rename cv_notrepresentative cv_not_represent
	rename citation field_citation_value

// Check sources with missing mean

	replace mean = 0.204 if regexm(healthstate, "inactive") & regexm(file, "BRAZZAVILLE") 
	replace mean = 0.796 if regexm(healthstate, "lowmodhigh") & regexm(file, "BRAZZAVILLE") 
	replace effective_sample_size = 2095 if regexm(file, "BRAZZAVILLE") 


// Sample size check - drop if too small 
	drop if effective_sample_size < 10 

// Few missing age_ends 
	replace age_end = age_start + 4 if age_end == . 
	replace age_end = 100 if age_start == 80 & age_end == . 

// Missing urbanicity type 
	replace urbanicity_type = "Mixed/both" if urbanicity_type == "" 

	tempfile final 
	save `final', replace

// Outsheet compiled dataset 
	outsheet using "`dataset'", comma names replace 

/*
// Brazil National Health Survey (PNS), also reassigning VIGITEL for Distrito Federal
	replace location_id = 4756 if source_name == "VIGITEL" & location_id == 4651
	replace iso3 = "BRA_4756" if source_name == "VIGITEL" & location_id == 4756

	keep if nid == 195010 | (location_id == 4756 & source_name == "VIGITEL")
*/

	keep if survey_name == "Behavioral Risk Factor Surveillance System"

	save `final', replace 
	
// Physical inactivity and low physical activity, inactive (9356)

	keep if healthstate == "activity_inactive" 
	gen modelable_entity_id = 9356
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, inactive"

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile inactive
	save `inactive', replace

	
	export excel "`filepath'", firstrow(variables) sheet("extraction") replace


	// Physical inactivity and low physical activity, low active (9357) 
	use `final', clear
	keep if healthstate == "activity_low" 
	gen modelable_entity_id = 9357
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, low active" 

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile low 
	save `low', replace

	export excel "`filepath'", firstrow(variables) sheet("extraction") replace

	// Physical inactivity and low physical activity, moderately active (9358) 
	use `final', clear 
	keep if healthstate == "activity_mod" 
	gen modelable_entity_id = 9358
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, moderately active" 

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile mod
	save `mod', replace

	export excel "`filepath'", firstrow(variables) sheet("extraction") replace


	// Physical inactivity and low physical activity, highly active (9359)
	use `final', clear
	keep if healthstate == "activity_high" 
	gen modelable_entity_id = 9359
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, highly active " 

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile high 
	save `high', replace

	export excel "`filepath'", firstrow(variables) sheet("extraction") replace


	// Physical inactivity and low physical activity, low/moderately/highly active (9360)
	use `final', clear 
	keep if healthstate == "activity_lowmodhigh" 
	gen modelable_entity_id = 9360
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, low/moderately/highly active" 

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile lowmodhigh
	save `lowmodhigh', replace

	cap export excel "`filepath'", firstrow(variables) sheet("extraction") replace

	// Physical inactivity and low physical activity, moderately/highly active (9361)
	use `final', clear 
	keep if healthstate == "activity_modhigh" 
	gen modelable_entity_id = 9361
	gen modelable_entity_name = "GBD 2016 Physical inactivity and low physical activity, moderately/highly active" 

	destring location_id, replace 

	local varlist row_num modelable_entity_id modelable_entity_name measure	nid location_name	location_id	/// 
	sex	year_start	year_end	age_start	age_end	measure	mean	lower	upper	standard_error	effective_sample_size	/// 
	uncertainty_type uncertainty_type_value	representative_name	urbanicity_type	extractor ///
	unit_value_as_published source_type is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name  case_definition /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id cv_not_represent

	keep `varlist'
	order `varlist' 

	tempfile modhigh
	save `modhigh', replace

	cap export excel "`filepath'", firstrow(variables) sheet("extraction") replace
