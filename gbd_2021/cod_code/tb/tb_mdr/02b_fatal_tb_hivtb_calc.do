// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		This code computes HIV-TB and HIV-other deaths utilizing a PAF
// Author:		USERNAME
// Edited:      USERNAME
// Description:	1: run a mixed effects regression to predict proportions of HIV-TB among all TB cases
//				2: calculate PAF using the predicted proportions above and global RR
// 				3: generate HIV-TB death age pattern by pulling HIV mortality pattern
//				4: calculate hivtb deaths based on high quality VR data and proportions of HIV positive TB cases
//				5: capping hivtb deaths if hivtb/hiv>45%
//				6: upload results
// Variables:	acause, custom_version, tb_model, male_hiv, female_hiv, hiv_model, decomp_step
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************

**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************

// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set scheme s1color

	// Define focal drives
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
	}

	// Close any open log file
	cap log close
	
// Load helper objects and directories
	
	// Load locals
	local acause hiv_tb
	local custom_version v1.8
	local tb_model 637511_636083
	local male_hiv 637130
	local female_hiv 637133
	local hiv_model 637130_637133_new
	local decomp_step step4

	// Make folders on cluster
	capture mkdir "FILEPATH"
	capture mkdir "FILEPATH/`custom_version'"
	capture mkdir "FILEPATH"
	capture mkdir "FILEPATH/`custom_version'"

	// Define filepaths
	local outdir "FILEPATH/`custom_version'/"
	local indir "FILEPATH/data"
	local tempdir "FILEPATH/temp"
	
	cap mkdir `outdir'

**********************************************************************************************************************
** STEP 1A: GATHER INPUTS (HIVTB PROPORTIONS, COVARIATE, POPULATION) FOR MIXED EFFECTS REGRESSION
**********************************************************************************************************************

	// Load direct coded HIV-TB before redistribution
	insheet using "`indir'/hivtb_prop.csv", comma names clear 
	keep location_id year_id data
	rename (year_id data) (year raw_prop)
	
	// Drop 1 outlier from NGA
	drop if location_id==214 & year==2003
	
	// Save tempfile
	tempfile prop
	save `prop', replace
		
	// Load location information
	use "`tempdir'/iso3.dta", clear
	replace location_name="USA Georgia" if location_id==533
	replace location_name="MEX Distrito Federal" if location_id==4651
	duplicates drop location_name, force
	
	// Save tempfile
	tempfile iso3
	save `iso3', replace
	  
	// Pull popultions
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') gbd_round_id(6) clear
	rename population mean_pop
	
	// Save tempfile for all populations
	tempfile pop_all
	save `pop_all', replace

	// Save tempfile for all-ages population
	keep if age_group_id==22
	tempfile pop
	save `pop', replace
		
	// Pull adult both sexes HIV mortality rate covariate
	clear all
	adopath + "FILEPATH"
	get_covariate_estimates, covariate_id(1240) decomp_step(`decomp_step') model_version_id(34899) gbd_round_id(6)
	keep if sex_id==3
	save "`indir'/adult_hiv_death_rate.dta", replace

	// Clean covariate table
    duplicates drop 
	gen ln_rate=ln(mean_value)
	keep location_id year_id ln_rate
	gen year=year_id
	
	// Merge proportions and location information
	merge 1:m location_id year using `prop', keepusing(raw_prop) nogen 
	merge m:1 location_id using "`tempdir'/iso3.dta", keepusing(iso3) keep(3)nogen
	
	// Logit transform HIV-TB proportion
	gen logit_prop_tbhiv=logit(raw_prop)
	
	// Save input
	tempfile tmp_reg_dta
	save `tmp_reg_dta', replace
    save "`outdir'/tmp_reg_dta", replace
		
**********************************************************************************************************************
** STEP 1B: RUN A MIXED EFFECTS REGRESSION TO PREDICT PROPORTIONS OF HIV-TB AMONG ALL TB CASES
**********************************************************************************************************************

	// Load inputs
	use `tmp_reg_dta', clear
	drop if year<1980	
	
	// Merge region information
	merge m:1 location_id using "FILEPATH/locations_22.dta", keepusing(super_region_id region_id standard_location) keep(3)nogen
	
	// Run regression with standard locations
	log using "`outdir'/hivtb_prop_log_`custom_version'.smcl", replace
		xtmixed logit_prop_tbhiv ln_rate || super_region_id: || region_id: || location_id: if standard_location==1 
	cap log close 		

	// predictions with only fixed effects and subtract the pred from the dependent variable
	predict pred, xb
	gen logit_prop_tbhiv2 = logit_prop_tbhiv-pred
	
	// Extract coefficients from mixed effects regressions with standard locations
	matrix m = e(b)'
	matrix m = m[1..(rowsof(m)-4),1]
	local covars: rownames m
	local num_covars: word count `covars'
	local betas
	forvalues j = 1/`num_covars' {
		local this_covar: word `j' of `covars'
		local betas `betas' b_`this_covar'
	}
	
	matrix C = e(V)
	matrix C = C[1..(colsof(C)-4), 1..(rowsof(C)-4)]
	drawnorm `betas', means(m) cov(C)
	** just save 1,000 betas... 
	preserve
		qui keep b*
		qui drop if _n>1000
		qui gen id=_n
		qui tempfile tmp_betas
		save `tmp_betas', replace 
	restore
	qui drop b_*
	qui duplicates drop
	
	// Run regression with no intercept with all input using subtracted DV
	log using "`outdir'/hivtb_prop_log_`custom_version'_random_effects_reg.smcl", replace
		xtmixed logit_prop_tbhiv2, noconstant || super_region_id: || region_id: || location_id:
	cap log close

	// Extract random effects from second regression --
	// Extracting location id random effects
	predict sr_RE reg_RE iso_RE, reffects
	preserve
	collapse (mean) iso_RE, by(iso3 region_id super_region_id)
	rename iso_RE iso_RE_new
	tempfile iso_RE
	save `iso_RE', replace

	// Extracting region id random effects
	restore 
	preserve
	collapse (mean) reg_RE, by(region_id super_region_id)
	rename reg_RE reg_RE_new
	tempfile region_RE
	save `region_RE', replace

	// Extracting super region random effects
	restore 
	preserve
	collapse(mean) sr_RE, by(super_region_id)
	rename sr_RE sr_RE_new
	tempfile SR_RE
	save `SR_RE', replace
	restore
	
	// Merge random effects
	merge m:1 iso3 using `iso_RE', keepusing(iso_RE_new) nogen
	merge m:1 region_id using `region_RE', keepusing(reg_RE_new) nogen
	merge m:1 super_region_id using `SR_RE', keepusing(sr_RE_new) nogen

	// Save tempfile
	tempfile all
	save `all', replace 
	
	// Extract national location random effects where there are subnationals		
	use `iso_RE', clear /* add Eth=179, Iran=142, NZL=72, NOR=90, RUS=62 */
	preserve
	keep if iso3=="CHN"
	local iso_RE_CHN=iso_RE
	restore
	
	preserve
	keep if iso3=="MEX"
	local iso_RE_MEX=iso_RE
	restore
	
	preserve
	keep if iso3=="GBR"
	local iso_RE_GBR=iso_RE
	restore
	
	preserve
	keep if iso3=="USA"
	local iso_RE_USA=iso_RE
	restore
	
	preserve
	keep if iso3=="BRA"
	local iso_RE_BRA=iso_RE
	restore
	/*
	preserve
	keep if iso3=="IND"
	local iso_RE_IND=iso_RE
	local iso_RE_se_IND=iso_RE_se
	restore
	
	preserve
	keep if iso3=="KEN"
	local iso_RE_KEN=iso_RE
	local iso_RE_se_KEN=iso_RE_se
	restore
	*/
	preserve
	keep if iso3=="JPN"
	local iso_RE_JPN=iso_RE
	restore
	

	preserve
	keep if iso3=="SWE"
	local iso_RE_SWE=iso_RE
	restore
	/*
	preserve
	keep if iso3=="ZAF"
	local iso_RE_ZAF=iso_RE
	local iso_RE_se_ZAF=iso_RE_se
	restore
	*/
	
	/*
	preserve
	keep if iso3=="SAU"
	local iso_RE_SAU=iso_RE
	local iso_RE_se_SAU=iso_RE_se 
	restore
	*/
	preserve
	keep if iso3=="ETH"
	local iso_RE_ETH=iso_RE
	restore
	
	preserve
	keep if iso3=="IRN"
	local iso_RE_IRN=iso_RE
	restore
	
	preserve
	keep if iso3=="NZL"
	local iso_RE_NZL=iso_RE
	restore
	
	preserve
	keep if iso3=="NOR"
	local iso_RE_NOR=iso_RE
	restore
	
	preserve
	keep if iso3=="RUS"
	local iso_RE_RUS=iso_RE
	restore
	
	preserve
	keep if iso3=="IDN"
	local iso_RE_IDN=iso_RE
	restore
	
	preserve
	keep if iso3=="PAK"
	local iso_RE_PAK=iso_RE
	restore
	
	preserve
	keep if iso3=="PHL"
	local iso_RE_PHL=iso_RE
	restore
	
	preserve
	keep if iso3=="ITA"
	local iso_RE_ITA=iso_RE
	restore
	
	preserve
	keep if iso3=="NGA"
	local iso_RE_NGA=iso_RE
	restore
	
	preserve
	keep if iso3=="POL"
	local iso_RE_POL=iso_RE
	restore
	
	// Load prediction table back in
	use `all', clear

	// missing subnational random effects and SEs are replaced with country random effects and SEs
	replace iso_RE=`iso_RE_CHN' if regexm(iso3,"CHN_")
	/* replace iso_RE_se=`iso_RE_se_CHN' if regexm(iso3,"CHN_") */
	/* replace iso_RE=`iso_RE_IND' if regexm(iso3,"IND_")
	replace iso_RE_se=`iso_RE_se_IND' if regexm(iso3,"IND_") */
	replace iso_RE=`iso_RE_GBR' if regexm(iso3,"GBR_")
	/* 	replace iso_RE_se=`iso_RE_se_GBR' if regexm(iso3,"GBR_") */
	replace iso_RE=`iso_RE_MEX' if regexm(iso3,"MEX_")
	/*	replace iso_RE_se=`iso_RE_se_MEX' if regexm(iso3,"MEX_") */
	replace iso_RE=`iso_RE_USA' if regexm(iso3,"USA_")
	/*	replace iso_RE_se=`iso_RE_se_USA' if regexm(iso3,"USA_") */
	replace iso_RE=`iso_RE_BRA' if regexm(iso3,"BRA_")
	/*	replace iso_RE_se=`iso_RE_se_BRA' if regexm(iso3,"BRA_") */
	/* replace iso_RE=`iso_RE_KEN' if regexm(iso3,"KEN_")
	replace iso_RE_se=`iso_RE_se_KEN' if regexm(iso3,"KEN_") */
	replace iso_RE=`iso_RE_JPN' if regexm(iso3,"JPN_")
	/*	replace iso_RE_se=`iso_RE_se_JPN' if regexm(iso3,"JPN_") */
	replace iso_RE=`iso_RE_SWE' if regexm(iso3,"SWE_")
	/*	replace iso_RE_se=`iso_RE_se_SWE' if regexm(iso3,"SWE_") */
	/* replace iso_RE=`iso_RE_ZAF' if regexm(iso3,"ZAF_")
	replace iso_RE_se=`iso_RE_se_ZAF' if regexm(iso3,"ZAF_") */
	/*  replace iso_RE=`iso_RE_SAU' if regexm(iso3,"SAU_") */
	/*	replace iso_RE_se=`iso_RE_se_SAU' if regexm(iso3,"SAU_") */
	replace iso_RE=`iso_RE_IDN' if regexm(iso3,"IDN_")
	/*	replace iso_RE_se=`iso_RE_se_IDN' if regexm(iso3,"IDN_") */

	// GBD2017 subnationals
	replace iso_RE=`iso_RE_ETH' if regexm(iso3,"ETH_")
	replace iso_RE=`iso_RE_IRN' if regexm(iso3,"IRN_")
	replace iso_RE=`iso_RE_NZL' if regexm(iso3,"NZL_")
	replace iso_RE=`iso_RE_NOR' if regexm(iso3,"NOR_")
	replace iso_RE=`iso_RE_RUS' if regexm(iso3,"RUS_")

	// GBD2019 subnationals
	replace iso_RE=`iso_RE_PAK' if regexm(iso3,"PAK_")
	replace iso_RE=`iso_RE_PHL' if regexm(iso3,"PHL_")
	replace iso_RE=`iso_RE_ITA' if regexm(iso3,"ITA_")
	replace iso_RE=`iso_RE_NGA' if regexm(iso3,"NGA_")
	replace iso_RE=`iso_RE_POL' if regexm(iso3,"POL_")

	// Missing country random effects are replaced with the average random effect at the global level (i.e., 0)
	replace iso_RE=0 if iso_RE==.
	replace reg_RE=reg_RE_new if reg_RE==.
	replace sr_RE=sr_RE_new if sr_RE==.

	// Merge 1000 draws of fixed effects from standard location model
	qui gen id=_n
	merge 1:1 id using "`tmp_betas'", nogen 
	drop id

	// Generate 1000 estimates 
	forvalues j = 1/1000 {
		di in red "Generating Draw `j'"
			qui gen prop_tbhiv_xb_d`j'=ln_rate*b_ln_rate[`j']+b__cons[`j']+iso_RE+reg_RE+sr_RE
			qui replace prop_tbhiv_xb_d`j'=invlogit(prop_tbhiv_xb_d`j')
	}
		
	// Drop duplicates
	duplicates drop 
	tempfile tmp_prop_xb
	save `tmp_prop_xb', replace 
	
	 // Compute mean, upper, and lower
	 egen mean_prop=rowmean(prop_tbhiv*)
	 egen lower_prop=rowpctile(prop_tbhiv*), p(2.5)
	 egen upper_prop=rowpctile(prop_tbhiv*), p(97.5)
	 drop prop_tbhiv*

	 // Save
	 save "`outdir'/Prop_tbhiv_mean_ui_`custom_version'.dta", replace
	 save "FILEPATH/Prop_tbhiv_mean_ui.dta", replace
	 
**********************************************************************************************************************
** STEP 1C: GRAPH PREDICTIONS OF HIVTB PROPORTIONS
**********************************************************************************************************************

	// Initialize pdfmaker
	set scheme s1color
	set printcolor asis
	capture confirm file "FILEPATH"
	if _rc == 0 {
		do "FILEPATH/pdfmaker.do"
	}
	else {
		do "FILEPATH/pdfmaker_Acrobat11.do"
	}
	
	// Load results
	use "`outdir'/Prop_tbhiv_mean_ui_`custom_version'.dta", clear
	duplicates drop location_id year, force
	tempfile prop
	save `prop', replace
	
	// Load input data
	use "`outdir'/tmp_reg_dta", clear
	duplicates drop location_id year, force
	tempfile reg
	save `reg', replace
	
	// Merge data
	use `prop', clear
	merge m:1 location_id year using `reg', nogen 
	merge m:1 location_id using "`tempdir'/iso3.dta", keepusing(iso3) keep(3)nogen
	
	// Prep for plotting
	gen pred_prop=mean_prop
	gen pred_prop_lower=lower_prop
	gen pred_prop_upper=upper_prop
	drop if year<1980	
	drop if (location_id>=4841 & location_id<=4875) | location_id==44538
	
	// Plot data
	pdfstart using "`outdir'/Prop_tbhiv_scatter_`custom_version'.pdf"
		sort iso3 year
		levelsof iso3 , local(isos)
		foreach i of local isos {
			scatter pred_prop year if iso3=="`i'" || scatter raw_prop year if iso3=="`i'", title("`i'")  ///
			legend(order(1 "Predicted proportions" 2 "Raw proportions") col(2))
			pdfappend
		}
	pdffinish
	
**********************************************************************************************************************
** STEP 2: COMPUTE PAF USING THE PREDICTED PROPORTIONS FROM STEP 1 AND MEDIAN GLOBAL RR
**********************************************************************************************************************	

	// Load in 4-5 star countries		 
	insheet using "`indir'/hiv_tb_icd10_decomp4.csv", comma names clear 
	gen iso3=ihme_loc_id

	// Compute HIV-TB deaths
	preserve
	keep if acause=="hiv_tb_other"
	drop if deaths==0 | deaths==.
	collapse (sum) deaths, by (acause iso3 year) 
	rename deaths deaths_hivtb
	keep iso3 year deaths_hivtb
	tempfile hivtb
	save `hivtb', replace
	restore

	// Compute TB no-HIV deaths
	keep if acause=="tb_other" | acause=="tb"
	drop if deaths==0 | deaths==.
	collapse (sum) deaths, by (iso3 year) 
	rename deaths deaths_tb
	keep iso3 year deaths_tb
	tempfile tb
	save `tb', replace

	// Pull predictions
	use "`outdir'/Prop_tbhiv_mean_ui_`custom_version'.dta", clear
	keep iso3 year mean_prop raw_prop
	rename year year_id
	tempfile prop
	save `prop', replace

	// Load in HIVTB and TB no-HIV deaths
	use `hivtb', clear
	merge 1:1 iso3 year using `tb', keep(3) nogen

	// Drop if the number of hivtb deaths is 10 or less
	drop if deaths_hivtb <=10

	// Compute fractions
	gen frac=deaths_hivtb/(deaths_tb+deaths_hivtb)
	drop if frac>1

	// Merge in proportions
	merge 1:1 iso3 year using `prop', keep (3) nogen

	// Compute RR
	gen RR=((frac*mean_prop)-frac)/((frac*mean_prop)-mean_prop)
	gen RR_2=((frac*raw_prop)-frac)/((frac*raw_prop)-raw_prop)

	// Save
	save "`outdir'/RR_updated_all_`custom_version'.dta", replace
	
	// Outlier RUSSIA
	drop if regexm(iso3, "RUS")

	// Drop if RR less than 1
	drop if RR<1 & RR !=.

	// Generate locals for mean and median RRs
	sum RR, detail
	gen RR_mean=`r(mean)'
	gen RR_median=`r(p50)'

	// Save RR
	save "`outdir'/global_RR_updated_`custom_version'.dta", replace

	// Calculate PAR
	use `tmp_prop_xb', clear
	gen RR_mean=`r(mean)'
	gen RR_median=`r(p50)'

	// Rename draws
	forvalues i = 1/1000 {
		local i1 = `i' - 1
		rename prop_tbhiv_xb_d`i' prop_tbhiv_xb_d`i1'
	}

	// Compute PAF
	forvalues j=0/999 {
		di in red "draw `j'"
		gen par_based_on_median_rr_`j'=(prop_tbhiv_xb_d`j'*RR_median)/((prop_tbhiv_xb_d`j'*RR_median)+(1-prop_tbhiv_xb_d`j'))
	}

	// Clean and save
	keep location_id year_id par_based_on_median_rr_*	
	save "`outdir'/predicted_PAR_`custom_version'.dta", replace

**********************************************************************************************************************
** STEP 3: GENERATE HIV MORTALITY AGE PATTERN
**********************************************************************************************************************

	clear all
	adopath + "FILEPATH"

	// Load male mortality age pattern
	get_model_results, gbd_team("cod") gbd_id(298) model_version_id(`hiv_male') decomp_step(iterative) gbd_round_id(6) clear
	tempfile tmp
	save `tmp', replace 

	// Load female mortality age pattern
	get_model_results, gbd_team("cod") gbd_id(298) model_version_id(`hiv_female') decomp_step(iterative) gbd_round_id(6) clear
	append using `tmp'

	// Save HIV mortality
	duplicates drop
	save "`indir'/hiv_deaths.dta", replace

	// Clean table
	use "`indir'/hiv_deaths.dta", clear
	keep if (age_group_id>=4 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	keep location_id year_id sex_id age_group_id mean_death_rate
	rename mean_death_rate hiv_death_rate

	// Save age pattern
	save "`indir'/hiv_death_rate_age_pattern.dta", replace

**********************************************************************************************************************
** STEP 4: COMPUTE HIV-TB DEATHS BASED ON HIGH QUALITY VR DATA AND PROPORTIONS OF HIVTB CASES
**********************************************************************************************************************

	// Load data
	use "`outdir'/predicted_PAR_`custom_version'.dta", clear
	tempfile frac
	save `frac', replace

	// Generate TB-HIV death numbers
	use FILEPATH/tb_codem_draws_`tb_model'.dta, clear
	duplicates drop location_id year_id age_group_id sex_id, force
	fastcollapse draw_*, type(sum) by(location_id year_id) 

	// Merge poroportions
	merge 1:1 location_id year_id using `frac', keep(3)nogen
			
	// Compute HIV-TB Deaths 
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=0 if draw_`i'==.
		gen tbhiv_d`i'=(par_based_on_median_rr_`i'/(1-par_based_on_median_rr_`i'))*draw_`i'
		drop draw_`i' 
		replace tbhiv_d`i'=0 if tbhiv_d`i'==.
	}

	// Save as tempfile
	tempfile hivtb
	save `hivtb', replace

	// Save HIVTB deaths		
	save FILEPATH/HIVTB_mortality_draws_`custom_version'.dta, replace

	// Pull populations
	clear all
	adopath + "FILEPATH"

	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') gbd_round_id(6) clear
	rename population mean_pop
	tempfile pop_all
	save `pop_all', replace

	// Clean population data
	use `pop_all', clear
	drop if year_id<1980
	drop if location_id==1
	keep if (age_group_id>=4 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	drop if sex_id==3
	tempfile tmp_pop
	save `tmp_pop', replace

	// Pull HIV mortality age pattern
	use "`indir'/hiv_death_rate_age_pattern.dta", clear 
	duplicates drop
	tempfile age_pattern
	save `age_pattern', replace

	// Prep for age-split
	use FILEPATH/HIVTB_mortality_draws_`custom_version'.dta, clear
	merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
	merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

	// Create variables
	rename mean_pop sub_pop
	gen rate_sub_pop=hiv_death_rate*sub_pop

	// Sum rates by location and year
	preserve
	collapse (sum) rate_sub_pop, by(location_id year_id) fast
	rename rate_sub_pop sum_rate_sub_pop
	tempfile sum
	save `sum', replace

	// Merge sum
	restore
	merge m:1 location_id year_id using `sum', keep(3)nogen

	// Apply age pattern
	forvalues i=0/999 {
		di in red "draw `i'"
		gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
		drop tbhiv_d`i' 
	}

	// Clean and save age and sex specific HIVTB deaths
	keep location_id year_id age_group_id sex_id draw_*
	tempfile hivtb_cyas
	save `hivtb_cyas', replace
	save FILEPATH/HIVTB_cyas_`custom_version'.dta, replace

**********************************************************************************************************************
** STEP 5A: CAP HIVTB DEATHS IF HIVTB/HIV > 45%
**********************************************************************************************************************

	// Load HIV deaths
	use FILEPATH/hiv_draws_`hiv_model'.dta, clear
	duplicates drop location_id year_id age_group_id sex_id, force

	// Rename HIV draws
	forvalues i = 0/999 {
	  rename draw_`i' hiv_`i'
	}

	// Save as tempfie
	tempfile hiv
	save `hiv', replace

	// Load HIV-TB deaths
	use FILEPATH/HIVTB_cyas_`custom_version'.dta, clear
	duplicates drop location_id year_id age_group_id sex_id, force

	// Save as tempfile
	tempfile hivtb
	save `hivtb', replace

	// Merge draws together
	use `hiv', clear
	merge 1:1 location_id year_id age_group_id sex_id using `hivtb', keep(3) nogen 

	// Loop through draws and cap if needed
	forvalues i=0/999 {
		gen frac_`i'=draw_`i'/hiv_`i'
		replace draw_`i'=hiv_`i'*0.45 if frac_`i'>0.45
		replace draw_`i'=0 if draw_`i'==.
	}

	// Clean
	drop hiv_* frac_*
	tempfile hivtb_capped
	save `hivtb_capped', replace

	// Save
	drop measure_id
	replace cause_id=299		
	save FILEPATH/hivtb_capped_`custom_version'.dta, replace 

**********************************************************************************************************************
** STEP 5B: COMPUTE HIV OTHER
**********************************************************************************************************************

	// Merge HIV deaths and HIVTB deaths together
	use `hiv', clear
	merge 1:1 location_id year_id age_group_id sex_id using `hivtb_capped', keep(3) nogen

	// Subtract HIVTB deaths from HIV deaths 
	forvalues i=0/999 {
		replace draw_`i'=hiv_`i'-draw_`i'
	}

	// Clean
	replace cause_id=300		
	drop measure_id	metric_id hiv_*

	// Save as csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** STEP 6: UPLOAD RESULTS FOR HIV OTHER
**********************************************************************************************************************

	// save results for HIV other
	run "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(300) description(hiv_other custom `custom_version';`decomp_step' HIV model/cov) input_file_pattern({location_id}.csv) input_dir(FILEPATH) model_version_type_id(6) decomp_step(`decomp_step') mark_best(True) gbd_round_id(6) clear
	
	
