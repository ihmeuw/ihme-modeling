/// *****************************************************************
// ******************************************************************
// Purpose:		Prep and crosswalk IPV dataset for Dismod
** ******************************************************************
** RUNTIME CONFIGURATION
** ******************************************************************

// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set mem 12g
		set maxvar 32000
	// Set to run all selected code without pausing
		set more off
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		cap restore, not
	// Reset timer 
		timer clear	
	// Close previous logs
		cap log close	
	// Install carryforward command for use later
		cap ssc install carryforward
	
// Make locals for relevant files and folders 
	** Individual-level microdata from the WHO Multi-country Study on Women's Health and Domestic Violence against Women
	local eg_cw_data				"FILEPATH"
	** Age group id converstion 
	local age_groups				"FILEPATH"
	** Ever been partnered DisMod output 
	local ever_partnered			"FILEPATH"
	** Log directory for crosswalk regression
	local logs						"FILEPATH"
	** Directory for compiled tabulated data
	local data_dir 					"FILEPATH"
	** Directory for files to be uploaded to Dismod
	local outdir 					"FILEPATH"
	** Turn on/off crosswalk block (1="on", 0="off")
	local crosswalk					1
	
** Bring in newest population envelope 
	adopath + "FILEPATH" // load in shared functions
	get_demographics, gbd_team(cov)
	get_populations, year_id($year_ids) location_id($location_ids) sex_id($sex_ids) age_group_id($age_group_ids) clear

	rename year_id year 
	rename sex_id sex 
	rename age_group_id age_id
	
	tempfile pops 
	save `pops', replace

	// Age group ids to age groups 
	insheet using "`age_groups'/convert_to_new_age_ids.csv", comma names clear 
	rename age_group_id age_id
	merge 1:m age_id using `pops'
	drop age_id _m 
	rename age_start age

	keep if sex == 2 
	keep if age >= 15
	save `pops', replace

// Prep database from DHS of fraction that have ever been in a relationship 
	
	use "`ever_partnered'", clear
	
	// Clean up 
		rename sex_id sex 
		label define sex 2 "Female"
		label values sex sex

		tempfile ever_partnered_data
		save `ever_partnered_data', replace 

		insheet using "`age_groups'/convert_to_new_age_ids.csv", comma names clear 
		merge 1:m age_group_id using `ever_partnered_data', keep(3) nogen

		rename age_start age 
		drop if age < 15
		
		egen mean = rowmean(draw*)

		rename year_id year
		keep location_id year age sex mean draw_*
		
		tempfile ever_relationship
		save `ever_relationship'
	
	
// Load population data
	use  `pops', clear
	//keep if age >= 15  & age <= 80 & sex == 2 // only estimate IPV in females above the age of 15
	duplicates drop year sex pop age location_id, force // drop duplicates
	drop if year < 1990 
	
	merge 1:1 location_id year age sex using `ever_relationship', keep(3) nogen 
		
	// Broaden age groups to ten instead of five years and make age start and end variables to match IPV dataset
		rename age age_start
		replace age_start = age_start - 5 if inlist(age_start, 30, 40, 50, 60, 70, 80)
		gen age_end = age_start + 9 if !inlist(age_start, 15, 20)
		replace age_end = age_start + 4 if inlist(age_start, 15, 20)
		replace age_end = 100 if age_start == 80
	
	// Duplicate observations so that we can calculate an "all ages" and a reproductive age range estimate
		expand 2, gen(dup)
		replace age_start = 15 if dup == 1 
		replace age_end = 100 if dup == 1
		drop dup
		
		expand 2, gen(dup)
		replace age_start = 15 if dup == 1
		replace age_end = 49 if dup == 1
	
	// Combine older age groups 
		replace age_start = 45 if age_start >=45 
		replace age_end = 100 if age_start >=45 
		
	// Prepare  proportion ever in a relationship variable for population-weighting
		forvalues i= 0/999 { 
			gen total_ever_`i' = draw_`i' * pop
		}

	// Collapse to population weight
		fastcollapse pop total_ever_*, type(sum) by(location_id year age_start age_end sex)

	// Create fraction in rate space now that it's been population-weighted

		forvalues i= 0/999 { 
			gen ever_partnered_`i' = total_ever_`i' / pop
		}
		
		drop total_ever_*

	// Make an age group variable
		tostring age_start age_end, replace
		gen agegrp = age_start + "_" + age_end
		replace agegrp = "all" if agegrp == "15_100"
		replace agegrp = "45+" if agegrp == "45_100"
		destring age_start age_end, replace
		
		drop if year < 1990 

	tempfile eversexfinal
	save `eversexfinal', replace

** *****************************************************************
// STEP 1: Prep regression results (using crosswalk data from expert group)
** *****************************************************************
if `crosswalk' == 1 {
		use using "`eg_cw_data'", clear
		drop if age == .
		decode agegrp_gbd, gen(agegrp)
		
	// Generate various metrics of IPV
		gen prev_GS = (physvio == 1 | sexvio == 1)
		gen prev_curr = (sexphcur == 1)
		gen prev_curr_phys = (sexphcur == 1 & physvio == 1) 
		gen prev_curr_sex = (sexphcur == 1 & sexvio == 1)
		gen prev_phys = (physvio == 1)
		gen prev_sev = (severe == 1)
		gen prev_sev_curr = (severe == 1 & sexphcur == 1)
		gen prev_sev_curr_phys = (severe == 1 & sexphcur == 1 & physvio == 1)
		gen prev_sev_curr_sex = (severe == 1 & sexphcur == 1 & sexvio == 1)
		gen prev_sev_phys = (severe == 1 & physvio == 1)
		gen prev_sev_sex = (severe == 1 & sexvio == 1)
		gen prev_sex = (sexvio == 1)
		
	// Estimate prevalence parameters for gold standard (everany) and alternate metrics
		preserve
			collapse prev*, by(countloc)
			gen agegrp = "all"
			tempfile all
			save `all', replace
			
			line prev_GS prev_GS || ///
			scatter prev_curr-prev_sex prev_GS, jitter(5) ///
			legend(size(small) row(4)) ///
			title("IPV, F: Gold Standard v. Alternative Metrics")
		restore
		
		collapse prev*, by(countloc agegrp)
		replace age = subinstr(age, "-", "_", .)
		replace age = "45plus" if age == "45_54"
		levelsof age, local(ages)
		append using `all'
	
	// Logit transform the variables
		foreach v of varlist prev* {
			replace `v' = logit(`v')
		}
		
	// Create age dummies (all = absorbed)
		foreach a in `ages' {
			gen age_`a' = (agegrp == "`a'")
		}
		
	// Run the crosswalk regressions
		log using "`logs'/crosswalk_results.smcl", replace
		foreach alt of varlist prev_curr - prev_sex {
			cap log close 
			log using "`logs'/crosswalk_results.smcl", append
			di in red "`alt'"
			if strmatch("`alt'", "*curr*") xi: reg prev_GS `alt' age_*
			else rreg prev_GS `alt'
			cap log close 
			
			qui {
				// Store the RMSE	
					local rmse_`alt' = e(rmse)
				
				// Create a columnar matrix (rather than dUSERt, which is row) by using the apostrophe
					matrix m_`alt' = e(b)'
					
				// Create a local that corresponds to the variable name for each parameter
					local covars_`alt': rownames m_`alt'
					
				// Create a local that corresponds to total number of parameters
					local num_covars_`alt': word count `covars_`alt''
					
				// Create an empty local that you will fill with the name of each beta (for each parameter)
					local betas_`alt'

				// Fill in this local
					forvalues j = 1/`num_covars_`alt'' {
						local this_covar: word `j' of `covars_`alt''
						local covar_fix = subinstr("`this_covar'","b.","",.)
						local covar_rename = subinstr("`covar_fix'",".","",.)
						local betas_`alt' `betas_`alt'' b_`covar_rename'
					}

				// Store the covariance matrix
					matrix C_`alt' = e(V)
					
					set obs 1000
				// Use the "drawnorm" function to create draws using the mean and standard deviations from your covariance matrix
					drawnorm `betas_`alt'', means(m_`alt') cov(C_`alt')
					rename b__cons b__cons_`alt'
					if strmatch("`alt'", "*curr*") {
						foreach age in `ages' {
							rename b_age_`age' b_age_`age'_`alt'
						}
					}
			}
		}
		gen id = _n
		keep id b_*
		tempfile reg_results
		save `reg_results', replace
}	
	
** *******************************************************************	
// STEP 2: Prep Actual Data, applying regression results to non-gold standard data
** *******************************************************************
	// Bring in raw exposure prevalence
		import excel using "`data_dir'/compiled_unadjusted_tabulations_full_dataset.xlsx", firstrow clear

		rename mean parameter_value
			
	// Create age groups that correspond to regression results above
		gen agegrp = "all" if age_start <= 25 & age_end > 45
		replace agegrp = "45+" if age_start >= 45 
		replace agegrp = "15_49" if age_start >=14 & age_end <=49
		replace agegrp = "15_19" if age_start >= 15 & age_end <=19
		replace agegrp = "20_24" if age_start >= 20 & age_end <=24
		replace agegrp = "25_34" if age_start >=25 & age_end <= 34
		replace agegrp = "35_44" if (age_start >=35 & age_end <= 44) | (age_start >=30 & age_end >=50)
	
	// Create age dummies (all = absorbed)
		foreach a in `ages' {
			gen age_`a' = (agegrp == "`a'")
		}	
		
	// Generate a dummy to indicate where data is for the "current" period (last 1 to 2 years)
		gen curr = (cv_recall_1yr == 1 | cv_past2yr == 1)
		gen ptype = "prev"
		replace ptype = ptype + "_sev" if cv_case_severe == 1
		replace ptype = ptype + "_curr" if curr == 1
		replace ptype = ptype + "_sex" if cv_sexual_ipv == 1
		replace ptype = ptype + "_phys" if cv_phys_ipv  == 1
		replace ptype = "prev_GS" if ptype == "prev"
		
	// Take log of proportion for regression
		levelsof ptype, local(types)
		foreach t in `types' {
			gen `t' = logit(parameter_value) if ptype == "`t'"
		}
	
	// Merge on fraction ever_sex
		gen year = int((year_start + year_end)/2)

		gen dismod_year = . 

		forvalues i=1990(5)2015 {
			replace dismod_year = `i' if abs(`i' - year < 3)
		}

		replace dismod_year = 1990 if year < 1990

		drop year
		rename dismod_year year

		merge m:1 location_id year agegrp using `eversexfinal', keep(3) nogen

// if `crosswalk' == 1 {	
// Prep for regression
	// 1000 draws
		forvalues d = 1/1000 {
			gen draw`d' = .
		}
		
	// Merge in regression coefficients 
		gen id = _n
		merge 1:1 id using `reg_results', nogen
		drop id
	
			// Run crosswalks
			compress
			foreach alt in `types' {
				if "`alt'" == "prev_GS" continue
			di in red "`alt'"
			qui {
				local counter = 0
				
				forvalues j = 1/1000 {
					local counter = `counter' + 1
					quietly generate temp_d`j' = 0
					if !strmatch("`alt'", "*curr*") {
						replace temp_d`j' = temp_d`j' + b__cons_`alt'[`j'] + `alt'*b_`alt'[`j']
					}
					else {
						replace temp_d`j' = temp_d`j' + b__cons_`alt'[`j'] + `alt'*b_`alt'[`j'] ///
											+ b_age_15_19_`alt'[`j']*age_15_19 + b_age_20_24_`alt'[`j']*age_20_24 ///
											+ b_age_25_34_`alt'[`j']*age_25_34 + b_age_35_44_`alt'[`j']*age_35_44 ///
											+ b_age_45plus_`alt'[`j']*age_45plus
					}
					replace draw`j' = temp_d`j' if (temp_d`j' != . & draw`j' == . & ptype == "`alt'") 
					drop temp_d`j'
				}
				drop b_*`alt'
			}
		}
				
		egen double mean = rowmean(draw*)
			replace mean = invlogit(mean)
			replace mean = parameter_value if ptype == "prev_GS"
			recode mean .=0 if parameter_value == 0
		egen double lower_ci = rowpctile(draw*), p(2.5)
			replace lower_ci = invlogit(lower_ci)
			replace lower_ci = lower if ptype == "prev_GS" | parameter_value == 0
		egen double upper_ci = rowpctile(draw*), p(97.5)
			replace upper_ci = invlogit(upper_ci) 
			replace upper_ci = upper if ptype == "prev_GS" | parameter_value == 0
	
	// Prep variables for DisMod
		drop lower upper
		rename upper_ci upper
		rename lower_ci lower

	tempfile int
	save `int', replace
	
// Adjust for fraction who have had sex

	// Create 1,000 draws based on mean and standard error to multiply by 1,000 draws from DisMod model 

		// First fill out those data poitns that just have effective_sample_size
		replace cases = mean * effective_sample_size 
		replace standard_error = sqrt(cases) / effective_sample_size if standard_error == . & upper == . & lower == . 

		replace standard_error = ((upper - lower) / 3.92) if standard_error == . & upper != . & lower != . 
		
		gen standard_deviation = standard_error * sqrt(effective_sample_size)

		// Take logit of mean and delta method for standard error  
		gen variance = (standard_error)^2
		replace mean = .00001 if mean == 0 
		gen variance_new = variance * (1/(mean*(1-mean)))^2
		gen standard_error_new = sqrt(variance)
		gen standard_deviation_new = standard_error_new * sqrt(effective_sample_size)

		replace mean = logit(mean) 

		 forvalues draw = 0/999 {
			gen draw_`draw' = invlogit(rnormal(mean, standard_deviation_new))
		}

	// Multiply 1,000 draws of raw data by 1,000 draws of mean ever partnered if cv_pstatall == 1 or cv_pstatcurr == 1 
		// Pstatall=0 if whole population surveyed, 1 if only ever-partnered population surveyed (parter-status: ever)
		// Pstatcurr=0 if not only currently partnered population surveryed, 1 if only currently partnered population surveyed (partner-status: current)

		forvalues i = 0/999 { 
			gen mean_`i' = draw_`i' * ever_partnered_`i' if (cv_pstatall == 1 | cv_pstatcurr == 1)	
		} 

		replace mean = invlogit(mean) // inverse logit all means, which are still in logit space 

		// calculate mean and upper and lower space 
		egen mean_new = rowmean(mean_*)
		egen upper_new = rowpctile(mean_*), p(97.5)
		egen lower_new = rowpctile(mean_*), p(2.5)

		foreach v in mean upper lower { 
			replace `v' = `v'_new if (cv_pstatall == 1 | cv_pstatcurr == 1) & `v'_new != . 
		}

	// Clean up 
		drop mean_* ever_partnered_* draw_* 
		replace uncertainty_type = "Confidence interval" if cv_pstatall == 1 | cv_pstatcurr == 1 

// Make a variable for original case name
	gen case_name = substr(ptype, 6, .)
	replace case_name = "ever_any" if case_name == "GS"
	replace case_name = "ipv" + "_" + case_name
	
// Indicator for prevalence in last year(s) (vs. lifetime)
	replace cv_recall_1yr = 1 if cv_past2yr == 1 | cv_recall_1yr == 1
	replace cv_recall_1yr = 0 if cv_past2yr != 1 & cv_recall_1yr != 1

// Validation checks
	drop if mean == . | (upper == . & lower == . & effective_sample_size == . & standard_error == .)
	drop if mean < lower & lower != .
	drop if upper < mean 
	drop if mean > 1

// Drop extraneous data points

	** If a study has breakdown by age, exclude the all age data points
	egen sourcenum = group(nid iso3 year_start year_end sex case_name recall_type)
	gen agedif = age_end - age_start
	bysort sourcenum: egen maxagedif = max(agedif)
	bysort sourcenum: egen minagedif = min(agedif)
	gen x = 1
	bysort sourcenum: egen sum = total(x)
	gen data_status = "excluded" if agedif == maxagedif & sum != 1 & minagedif != maxagedif
	 
		
		//egen sourcenum = group(nid iso3 year_start year_end sex)
		duplicates tag sourcenum age_start, gen(dup)
		bysort sourcenum: egen sum_dup = sum(dup) 
		replace data_status = "" if data_status == "excluded" & sum_dup == 0 

		drop sourcenum agedif maxagedif minagedif x sum sum_dup dup

	** If a study has ipv_any for an age group exclude other data points that were crosswalked
	egen sourcenum = group(nid iso3 year_start year_end sex age_start age_end)
	bysort sourcenum: gen goldstd = case_name == "ipv_ever_any"
	bysort sourcenum: egen sum = total(goldstd)
	replace data_status = "excluded" if goldstd == 0 & sum != 0
	drop sourcenum goldstd sum
	

		// Drop specific cases that were also wider age groups that weren't caught in above logic
		replace data_status = "excluded" if iso3 == "UKR" & year_start == 1999 & age_start == 17 & age_end == 44
		
		replace data_status = "" if iso3 == "TUR" & year_start == 2014 // shouldn't be dropped but caught in above logic


	** If a study has lifetime and current estimates for ipv_phys or ipv_sex, exclude the year specific points and keep just the lifetime points active
	// ipv_phys
	egen sourcenum = group(nid iso3 year_start year_end sex)
	gen has_ipv_phys = case_name == "ipv_phys" 
	gen has_ipv_phys_curr = case_name == "ipv_curr_phys"
	bysort sourcenum: egen max_has_ipv_phys = max(has_ipv_phys)
	bysort sourcenum: egen max_has_ipv_phys_curr = max(has_ipv_phys_curr)
	bysort sourcenum: gen has_both = max_has_ipv_phys + max_has_ipv_phys_curr
	replace data_status = "excluded" if has_both == 2 & case_name == "ipv_curr_phys"
	drop has_ipv_phys_curr max_has_ipv_phys max_has_ipv_phys_curr has_both
	
	// ipv_sex
	gen has_ipv_sex = case_name == "ipv_sex"
	gen has_ipv_sex_curr = case_name == "ipv_curr_sex"
	bysort sourcenum: egen max_has_ipv_sex = max(has_ipv_sex)
	bysort sourcenum: egen max_has_ipv_sex_curr = max(has_ipv_sex_curr)
	bysort sourcenum: gen has_both = max_has_ipv_sex + max_has_ipv_sex_curr
	replace data_status = "excluded" if has_both == 2 & case_name == "ipv_curr_sex"
	drop has_ipv_sex_curr max_has_ipv_sex max_has_ipv_sex_curr has_both 
	
	** if a study has severe and all severity categories for same recall_type, exclude the severe observations
	foreach type in sex phys {
		// Remove severe physical and severe sexual, when more inclusive severity definition is available
			gen has_ipv_sev_`type' = case_name == "ipv_sev_`type'"
			bysort sourcenum: egen max_has_ipv_`type' = max(has_ipv_`type')
			bysort sourcenum: egen max_has_ipv_sev_`type' = max(has_ipv_sev_`type')
			bysort sourcenum: gen has_both = max_has_ipv_`type' + max_has_ipv_sev_`type'
			replace data_status = "excluded" if has_both == 2 & case_name == "ipv_sev_`type'"
			drop has_ipv_`type' has_ipv_sev_`type' max_has_ipv_`type' max_has_ipv_sev_`type' has_both
	
		// Remove current severe physical and current severe sexual, when more inclusive severity definition for current exposure is available
			gen has_curr_`type' = case_name == "ipv_curr_`type'"
			gen has_curr_sev_`type' = case_name == "ipv_sev_curr_`type'"
			bysort sourcenum: egen max_has_curr_`type' = max(has_curr_`type')
			bysort sourcenum: egen max_has_curr_sev_`type' = max(has_curr_sev_`type')
			bysort sourcenum: gen has_both = max_has_curr_`type' + max_has_curr_sev_`type'
			replace data_status = "excluded" if has_both == 2 & case_name == "ipv_sev_curr_`type'"
			drop has_curr_`type' has_curr_sev_`type' max_has_curr_`type' max_has_curr_sev_`type' has_both
			
		// If a study only has severe  definition for sexual violence or phys, keep lifetime over current 
			gen has_sev_`type' = case_name == "ipv_sev_`type'" 
			gen has_curr_sev_`type' = case_name == "ipv_sev_curr_`type'"
			bysort sourcenum: egen max_has_sev_`type' = max(has_sev_`type')
			bysort sourcenum: egen max_has_curr_sev_`type' = max(has_curr_sev_`type')
			bysort sourcenum: gen has_both = max_has_sev_`type' + max_has_curr_sev_`type'
			replace data_status = "excluded" if has_both == 2 & case_name == "ipv_sev_curr_`type'"
			drop has_sev_`type' has_curr_sev_`type' max_has_sev_`type' max_has_curr_sev_`type' has_both
	}
	
// Create an indicator for whether a point was crosswalked. If data is gold standard (case_name = ipv_ever_any) it is not crosswalked, in all other cases it is o
	gen cv_physcrosswalk = case_name != "ipv_ever_any" & regexm(case_name, "phys")
	gen cv_sexcrosswalk = case_name != "ipv_ever_any" & regexm(case_name, "sex")
	replace uncertainty_type = "Confidence interval" if case_name != "ipv_ever_any" & (upper != . & lower != .) 
	replace uncertainty_type_value = 95 if uncertainty_type == "Confidence interval" 
	

// Use group preview function to keep non-reference case definitions but not have them inform estimates or show up in viz tool 

	tempfile ready 
	save `ready', replace 

	bysort nid: gen flag = 1 if data_status == "excluded" 
	bysort nid: egen max_flag = sum(flag)

	levelsof nid if max_flag != 0, local(nids_with_exclusions)

	gen group = . 
	local counter = 1 

	foreach nid of local nids_with_exclusions { 
		
		replace group = `counter' if nid == `nid'

		local counter = `counter' + 1
         di "`counter'"

	}

	// make group_review variable
		// group_review = 1 if gold standard definition 
		// group_review = 0 if alternative definition

		gen group_review = 1 if group != . & data_status == "" 
		gen specificity = "gold standard definition" if group_review == 1 
		replace group_review = 0 if group != . & data_status == "excluded"
		replace specificity = "non-reference case definition" if group_review == 0 

// MARK OUTLIERS

	// Mark Korea outliers
		replace is_outlier = 1 if inlist(nid, 150604, 151544)
		replace data_status = ""

	// Mark USA as outlier for GENACIS -- unreasonable high estimates due to missingness
		replace is_outlier = 1 if inlist(nid, 169751, 169752) & location_id == 102 


// Replace nids for United States Natoinal Youth Risk Behavior Surveys (which somehow were not assigned NIDs before)
	replace nid = 163912 if nid == . & year_start == 2003
	replace nid = 163915 if nid == . & year_start == 2009 
	replace nid = 163917 if nid == . & year_start == 2013
	replace nid = 163914 if nid == . & year_start == 2007 
	replace nid = 163913 if nid == . & year_start == 2005 
	replace nid = 163916 if nid == . & year_start == 2011 

// Fix sex variable for dismod 

	tostring sex, replace
	gen sex_new = "Female" if sex == "2" 
	replace sex_new = "Male" if sex == "1" 
	drop sex 
	rename sex_new sex 

	drop sourcenum 
	drop if sex == "Male" 
	replace sex = "Female" if sex == ""

	//gen case_name = "" 

	drop cv_past2yr
	rename cv_spouseonly cv_spouse_only

// Make cv_national covariate 
	gen cv_not_represent = 1 if regexm(representative_name, "subnational location only") & urbanicity_type != "Mixed/both" & !regexm(iso3, "IND") 
	replace cv_not_represent = 0 if cv_not_represent == . 

// replace site_memo
	tostring site_memo, replace
	replace site_memo = "jakarta" if iso3 == "IDN" & effective_sample_size == 769
	replace site_memo = "purworejo" if iso3 == "IDN" & effective_sample_size == 820
	replace site_memo = "jayapura" if iso3 == "IDN" & effective_sample_size == 858

// Drop duplicated DEU datapoint 
	drop if iso3 == "DEU" & year_start == 2003 & effective_sample_size == 10264

// Only keep necessary variables
	local variables nid iso3 year_start year_end sex age_start age_end effective_sample_size  mean standard_error cv_* file path location_id upper lower uncertainty_type representative_name source_type /// 
	unit_value_as_published unit_type urbanicity_type uncertainty_type_value row_num modelable_entity_id modelable_entity_name recall_type measure extractor is_outlier underlying_nid /// 
	sampling_type recall_type recall_type_value unit_type input_type sample_size cases design_effect site_memo case_name /// 
	case_diagnostics response_rate note_SR note_modeler data_sheet_file_path parent_id case_name group_review group specificity new 
	
	keep `variables'
	
// Organize
	order `variables'
	sort iso3 year_start year_end sex age_start age_end

// Aggregate age groups if sample size is very low (in this case using similar threshold as epi - cases less than 5 
	
	tempfile all 
	save `all', replace

	// Save study info to merge on later 
	
	preserve
	drop effective_sample_size cases age_start age_end standard_error mean 
	duplicates drop file nid location_id source_type year_start year_end cv_* recall_type case_name urbanicity_type site_memo, force 
	tempfile info 
	save `info', replace
	restore


gen flag = . 

foreach age_tail in front rear {
     count if cases < 5
     local countdown = `r(N)'
     while `countdown' != 0 {
         quietly {
            count if cases < 5
                 local prev_count = `r(N)'
                 sort nid location_id source_type year_start year_end age_start age_end cv_* recall_type case_name urbanicity_type site_memo
                 egen all_age_group = group(file nid location_id source_type year_start year_end cv_* recall_type case_name urbanicity_type site_memo)
                  
                  levelsof all_age_group, local(groups)
                        foreach group of local groups {
                             if "`age_tail'" == "front" {
                                    replace age_end = age_end[_n+1] if cases < 5 & all_age_group == `group' & all_age_group[_n+1] == `group'
                                   replace flag = 1 if cases < 5 & all_age_group == `group' & all_age_group[_n+1] == `group'
                                    replace age_start = age_start[_n-1] if cases[_n-1] < 5 & all_age_group == `group' & all_age_group[_n-1] == `group'
                                   replace flag = 1 if cases[_n-1] < 5 & all_age_group == `group' & all_age_group[_n-1] == `group'

                                    }
                                else if "`age_tail'" == "rear" {
                                    replace age_start = age_start[_n-1] if cases < 5 & all_age_group == `group' & all_age_group[_n-1] == `group'
                                    replace flag = 1 if cases < 5 & all_age_group == `group' & all_age_group[_n-1] == `group'
                                    replace age_end = age_end[_n+1] if cases[_n+1] < 25 & all_age_group == `group' & all_age_group[_n+1] == `group'
                                    replace flag =1 if cases[_n+1] < 25 & all_age_group == `group' & all_age_group[_n+1] == `group'
                                    }
                                }
                  collapse (sum) cases effective_sample_size flag (mean) upper lower standard_error mean, by(file nid location_id source_type year_start year_end cv_* recall_type case_name urbanicity_type site_memo age_start age_end) fast
				  gen mean_aggregated = cases / effective_sample_size 
                        }
                        count if cases < 5
                        local countdown = `prev_count' - `r(N)'
                        }
                    //go to rear 
                    }

 // Merge to study info 
	merge m:1 file nid location_id source_type year_start year_end cv_* recall_type case_name urbanicity_type site_memo using `info', nogen 

// Replace mean where there was aggregation
	replace mean = mean_aggregated if flag != 0 

// Replace uncertainty type with effective sample size (ESS) if collapsing was done across age groups 
	foreach var in standard_error upper lower { 
		replace `var' = . if flag != 0 
	} 

	replace uncertainty_type = "Effective sample size" if flag != 0 
	replace standard_error = . if flag != 0 
	replace uncertainty_type_value = . if flag != 0 
	replace effective_sample_size = . if effective_sample_size == 0 
	replace standard_error = . if standard_error == 0 
	replace uncertainty_type_value = 95 if upper != . & lower != . 
	replace uncertainty_type_value = . if upper == . & lower == . 
	replace uncertainty_type = "Confidence interval" if upper != . & lower != . & standard_error == . 
	replace uncertainty_type = "Effective sample size" if upper == . & lower == . & standard_error == . 
	drop flag mean_aggregated 

// Reorder variables 
	
	order `variables'
	sort `variables'

// replace means that were replaced with a small value before to take logit 
	replace mean = 0 if mean == .00001
	
// Save clean and crosswalked dataset 
	export excel "FILEPATH/FILE.xlsx", sheet("Data") firstrow(variables) sheetreplace


	rename case_name case_definition 
	gen case_name = "" 
	
	tempfile new_data
	save `new_data', replace


// Just add new studies
	keep if nid == 126441 

	export excel "FILEPATH/FILE.xlsx", firstrow(variables) sheet("extraction") replace
