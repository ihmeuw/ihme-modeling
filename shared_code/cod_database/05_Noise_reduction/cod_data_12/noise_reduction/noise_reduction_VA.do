** ************************************************************************************************* **
** Purpose: Apply noise reduction to VA by acause
** ************************************************************************************************* **
// Prep 
	clear
	set more off
	if c(os) == "Unix" {
		set odbcmgr unixodbc
		global prefix "/home/j"
		do "$prefix/Usable/Tools/ADO/pdfmaker.do"
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		do "$prefix/Usable/Tools/ADO/pdfmaker_Acrobat11.do"
	}
	
// What source
	local source "`1'"
	
// Where to read/write
	local temp_dir "`2'"
	
// Timestamp of update
	local timestamp "`3'"
	
// Which cause?
	local acause "`4'"
	
// What super region/PfPR group?
	local categ "`5'"
	
// Label sexes
	local sex1 Male
	local sex2 Female
	
// Graph?
	local graph 1

// Log
	capture log close
	log using "`temp_dir'/11_VA_noise_reduction/`source'/logs/`acause'_`categ'_log.smcl", replace

** *********************************************************************************************
// Get age sex restrictions
	use "$prefix/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", clear
	** keep if cause_version == 2
	keep acause male female yll_age_*
	foreach s in male female {
		capture destring `s', replace
		replace `s' = 1 if `s' == .
	}
	capture destring yll*, replace
	replace yll_age_start = 0 if yll_age_start == .
	replace yll_age_end = 80 if yll_age_end == .
	tempfile agesexrestrictions
	cap confirm new file `agesexrestrictions'
	while _rc {
		noisily di "tempfile existed already!"
		tempfile agesexrestrictions
		cap confirm new file `agesexrestrictions'
	}
	save `agesexrestrictions', replace
	
// Load source dataset
	use "`temp_dir'/11_VA_noise_reduction/`source'/`acause'_`categ'_raw.dta", clear
	capture drop *deaths*
	capture drop maxobserved is_zero_or_one pct_zero_or_one smoothvalue
	
// Standard error using Wilson method
	gen double std_error = sqrt(1/sample_size*cf_final*(1-cf_final)+1/(4*sample_size^2)*1.96^2)
	
// Convert to deaths
	gen deaths = cf_final*sample_size
	
// Re-enforce restrictions
	merge m:1 acause using `agesexrestrictions', keep(1 3) nogen
	** standardize ages
	recast double age yll_age_start yll_age_end
	foreach age_var in age yll_age_start yll_age_end {
		replace `age_var' = 0 if `age_var' == 91
		replace `age_var' = .01 if (`age_var' >.009 & `age_var' < .011) | `age_var' == 93
		replace `age_var' = .1 if (`age_var' >.09 & `age_var' < .11) | `age_var' == 94
	}
	drop if age < yll_age_start | age > yll_age_end | (sex==1 & male == 0) | (sex==2 & female == 0)
	** put ages back
	replace age = 91 if age == 0
	replace age = 93 if age >.009 & age < .011
	replace age = 94 if age >.09 & age < .11
	
// Run regressions by sex (already split by cause and super region)
	tempfile both
	save `both', replace
	levelsof sex, local(sexes)
	foreach sex of local sexes {
		use `both', clear
		** Matlab and SCD are time-series
		if index("`source'","Matlab") {
			keep if index(source,"Matlab")
		}
		if index("`source'","India_SCD") | index("`source'","India_SRS") {
			keep if index(source,"India_SCD") | index(source,"India_SRS")
			** Double standard error for SCD
			replace std_error = std_error*2
		}
		** Drop Indonesia, too imposing
		drop if source == "Indonesia_VA_comp" & "`source'" != "Indonesia_VA_comp"
		keep if sex == `sex'
		if !(index("`source'","Matlab") | index("`source'","India_SCD") | index("`source'","India_SRS")) {
			count if !(index(source,"Matlab") | index(source,"India_SCD") | index(source,"India_SRS"))
			local total = `r(N)'
			count if cf_final > 0 & !(index(source,"Matlab") |index(source,"India_SCD") | index(source,"India_SRS"))
			local cf_count = `r(N)'
		}
		else {
			count
			local total = `r(N)'
			count if cf_final > 0
			local cf_count = `r(N)'
		}
		** Continue if meets minumum required observations...
		if `total' > 6  & `cf_count' > 0 {
			di in red "Running `sex`sex''s..."
			replace subdiv = "/NONE/" if subdiv == "" 
			replace location_id = 0 if location_id == .
			egen subreg = group(NID location_id iso3 subdiv)
			** Run random or fixed effects regression on non-time series VAs
			if !(index("`source'","Matlab") | index("`source'","India_SCD") | index("`source'","India_SRS")) {
				** Attempt random effects if not SE Asia, E Asia, Oceania (will do so for all non-time-series malaria)
				if "`categ'" != "4" {
					drop if index(source,"Matlab") | index(source,"India_SCD") | index(source,"India_SRS")
					** All causes except malaria, and malaria outside of sub-Saharan Africa
					if substr("`categ'",1,3) != "SSA" {
						noisily di in red "RANDOM EFFECT ON STUDY"
						capture noisily mepoisson deaths i.age, exposure(sample_size) iterate(50) || _all: R.subreg
						if _rc | e(converged) == 0 {
							di in red "... RANDOM EFFECT FAILED, FIXED EFFECT INSTEAD"
							poisson deaths i.age i.subreg, exposure(sample_size) iterate(50)
							preserve
								clear
								file open finish using "`temp_dir'/11_VA_noise_reduction/`source'/logs/_FE_`acause'_`categ'_`sex'.txt", write replace
								file close finish
							restore
						}
					}
					** Malaria-specific for Africa
					else if substr("`categ'",1,3) == "SSA" {
						noisily di in red "FIXED EFFECT ON STUDY-YEAR"
						egen subreg_yr = group(NID iso3 subdiv year)
						poisson deaths i.age i.subreg_yr, exposure(sample_size) iterate(50)
					}
				}
				** Or just used fixed for SE Asia to avoid overinfluence of the large VAs in that region
				else if "`categ'" == "4" {
					di in red "SE ASIA, E ASIA, OCEANIA - FIXED EFFECTS ON STUDY"
					poisson deaths i.age i.subreg, exposure(sample_size) iterate(50)
				}
			}
			** Do seperate regression for time series
			else if index("`source'","Matlab") {
				noisily di in red "TIME SERIES REGRESSION (AGE + YEAR DUMMIES)"
				poisson deaths i.age i.year, exposure(sample_size) iterate(50)
			}
			else if index("`source'","India_SCD") | index("`source'","India_SRS") {
					di in red "... RANDOM EFFECT FAILED, FIXED EFFECT INSTEAD"
					poisson deaths i.age i.year i.location_id, exposure(sample_size) iterate(50)
					preserve
						clear
						file open finish using "`temp_dir'/11_VA_noise_reduction/`source'/logs/_FE_`acause'_`categ'_`sex'.txt", write replace
						file close finish
					restore
				** }
			}
			predict double cf_pred, xb nooffset
			predict double se_pred, stdp
			replace cf_pred = exp(cf_pred)
			replace se_pred = exp(se_pred-1)*cf_pred
			tempfile sex_`sex'
			save `sex_`sex'', replace
		}
		** ... Or create empty variables for those studies that do not have enough observations
		else {
			di in red "Not enough `sex`sex'' observations"
			gen cf_pred = .
			gen se_pred = .
			tempfile sex_`sex'
			save `sex_`sex'', replace
		}
	}
	
// Compile regressions
	clear
	foreach sex of local sexes {
		append using `sex_`sex''
	}
	
// Only keep the source being uploaded
	keep if source == "`source'"
	
// Get posterior
	gen double cf_post = ((se_pred^2/(se_pred^2 + std_error^2))*cf_final) + ((std_error^2/(std_error^2 + se_pred^2))*cf_pred)
	gen double var_post = (se_pred^2*std_error^2)/(se_pred^2+std_error^2)
	
// Swap in values if not null
	replace cf_final = cf_post if cf_post != .

// Graph & save output for review
	count
	if `r(N)' > 0 & `graph' == 1 {
		preserve
			capture mkdir "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'"
			capture mkdir "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`timestamp'"
			capture copy "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`acause'_`categ'_results.pdf" "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`timestamp'/`acause'_`categ'_results.pdf"
			capture erase "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`acause'_`categ'_results.pdf"
			pdfstart using "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`acause'_`categ'_results.pdf"
			label define sexlbl 1 "Males" 2 "Females"
			label values sex sexlbl
			replace age = 0 if age == 91
			replace age = 0.01 if age == 93
			replace age = 0.1 if age == 94
			egen obs_group = group(NID subdiv iso3 year)
			sort obs_group year sex age
			levelsof obs_group, local(obs_groups)
			foreach obs_group of local obs_groups {
				levelsof year if obs_group == `obs_group', local(year) c
				levelsof iso3 if obs_group == `obs_group', local(iso_use) c
				levelsof subdiv if obs_group == `obs_group', local(subdiv_use) c
				summ sample_size if obs_group == `obs_group'
				local ss = round(`r(sum)')
				scatter cf_before_smoothing age if obs_group == `obs_group', by(sex) mcolor(red) msymbol(O) msize(small) connect(l) lcolor(red) || scatter cf_pred age if obs_group == `obs_group', by(sex) mcolor(blue) msymbol(O) msize(small) connect(l) lcolor(blue)  || scatter cf_post age if obs_group == `obs_group', by(sex, title("`iso_use', `subdiv_use' (`year')""`acause' cause fractions [Sample size: `ss']", size(med))) mcolor(black) msymbol(O) msize(small) connect(l) lcolor(black) ytitle("Cause fraction", size(vsmall)) xtitle("Age", size(vsmall)) legend(size(vsmall) symxsize(*.5) symysize(*.5) label(1 "Input") label(2 "Prediction") label(3 "Posterior"))
				pdfappend
			}
			pdffinish
			capture erase "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`acause'_`categ'_results.log"
			keep NID iso3 location_id national region subdiv year sex age acause cf_final cf_before_smoothing sample_size cf_pred std_error se_pred var_post
			save "$prefix/WORK/03_cod/01_database/02_programs/VA_utilities/NR_graphs/`source'/`acause'_`categ'_results.dta", replace
		restore
	}
	
// Fix ages & subdiv
	replace subdiv = "" if subdiv == "/NONE/"
	replace location_id = . if location_id == 0
	
// Format
	order source* list NID iso3 location_id national region subdiv year sex age acause cf_raw cf_corr cf_rd cf_final cf_before_smoothing sample_size cf_pred cf_post std_error se_pred var_post
	sort source* list NID iso3 location_id national region subdiv year sex age acause cf_raw cf_corr cf_rd cf_final cf_before_smoothing sample_size cf_pred cf_post std_error se_pred var_post
	
// Save
	keep source* list NID iso3 location_id national region subdiv year sex age acause cf_raw cf_corr cf_rd cf_final cf_before_smoothing sample_size cf_pred cf_post std_error se_pred var_post
	gen VA_nr_group = "`categ'"
	save "`temp_dir'/11_VA_noise_reduction/`source'/`acause'_`categ'_results.dta", replace
	capture saveold "`temp_dir'/11_VA_noise_reduction/`source'/`acause'_`categ'_results.dta", replace

capture log close

** *********************************************************************************************
** *********************************************************************************************
