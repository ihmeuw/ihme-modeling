** Description: Generate HIV-TB proportions to split nonfatal TB

// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "ADDRESS"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "ADDRESS"
				}
			
			// Close any open log file
				cap log close
				
			// local

				

** *************************************************************************************************************************************************
// locals
local acause hiv_tb
local custom_version nonfatal

// define filepaths
	cap mkdir "$ADDRESS"
	cap mkdir "$ADDRESS"
	
	local outdir "$ADDRESS"
	local indir "$ADDRESS"
	local tempdir "$ADDRESS"
	
** **************************************************************************************************************************************************
** Step (1): run a mixed effects regression to predict proportions of HIV-TB among all TB cases
** **************************************************************************************************************************************************

// get direct coded HIV-TB before redistribution

		insheet using "`FILEPATH", comma names clear 
		/*gen year_mean=(year_start+year_end)/2	 
        gen year=round(year_mean, 1) */
		
		// drop 1 outlier from NGA
		
		drop if location_id==214 & year_id==2003
		
		keep location_id year_id data
		
		rename year_id year
		
		rename data raw_prop
		
				
		tempfile prop
		save `prop', replace
		
		/*
		preserve
		keep if location_id==44539 | location_id==44540
		collapse (sum) cases sample_size, by (location_id year)
		gen mean=cases/sample_size
		tempfile six_minor_ter
		save `six_minor_ter', replace
		restore
		
		drop if location_id==44539 | location_id==44540
		append using `six_minor_ter'
		
		rename mean raw_prop
		drop cases sample_size
		
		// drop outliers
		drop if location_id==196 & (year==2002 | year==2003)
		drop if location_id==130 & (year==2006 | year==2007)
		drop if inlist(location_id,43882,43884,43885,43886,43895,43888,43894,43895,43903,43918,43920,43922,43924,43926,43931) & year==2008
		drop if inlist(location_id,43894,43903,43911,43930,43921,43939) & year==2009
		drop if location_id==6 & year==2006
		drop if location_id==11
		tempfile prop
		save `prop', replace		
	    */

      // get iso3
	  
	  use "FILEPATH", clear
	  replace location_name="USA Georgia" if location_id==533
	  replace location_name="MEX Distrito Federal" if location_id==4651
	  duplicates drop location_name, force
	  tempfile iso3
	  save `iso3', replace
	  
      // get population

		clear all
		adopath + "ADDRESS"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
		rename population mean_pop
		tempfile pop_all
		save `pop_all', replace
		
		keep if age_group_id==22
        tempfile pop
		save `pop', replace
		
	 // get the covariate
	 
		clear all
		adopath + "ADDRESS"
		get_covariate_estimates, covariate_name_short(adult_hiv_death_rate)
		keep if sex_id==3
		save "FILEPATH", replace

        duplicates drop 

	  gen ln_rate=ln(mean_value)
	  
	  keep location_id year_id ln_rate
	
	  gen year=year_id
		merge 1:m location_id year using `prop', keepusing(raw_prop) nogen 
			
		gen logit_prop_tbhiv=logit(raw_prop)
		
		merge m:1 location_id using "FILEPATH", keepusing(iso3) keep(3)nogen
		
		tempfile tmp_reg_dta
	    save `tmp_reg_dta', replace
        save "FILEPATH", replace
		
** *****************************
// Predict Fraction TB-HIV in TB
** *****************************

		use `tmp_reg_dta', clear
		drop if year<1980	
		
		merge m:1 location_id using "FILEPATH", keepusing(super_region_id region_id) keep(3)nogen
		
		log using "FILEPATH", replace
	
		//  regression 
			xtmixed logit_prop_tbhiv ln_rate || super_region_id: || region_id: || location_id: 
		
	    cap log close 		
		** store location_id

		/*
			preserve
				predict u_location_id, reffects
				keep location_id u*
				duplicates drop 
				outsheet using "FILEPATH", delim(",") replace 
			restore
		*/
	
	predict sr_RE reg_RE iso_RE, reffects
	preserve
	collapse (mean) iso_RE, by(iso3 region_id super_region_id)
	rename iso_RE iso_RE_new
	tempfile iso_RE
	save `iso_RE', replace
	
	restore 
 	preserve
	collapse (mean) reg_RE, by(region_id super_region_id)
	rename reg_RE reg_RE_new
	tempfile region_RE
	save `region_RE', replace
	
	restore 
	preserve
	collapse(mean) sr_RE, by(super_region_id)
	rename sr_RE sr_RE_new
	tempfile SR_RE
	save `SR_RE', replace
	restore
	
	merge m:1 iso3 using `iso_RE', keepusing(iso_RE_new) nogen
	merge m:1 region_id using `region_RE', keepusing(reg_RE_new) nogen
	merge m:1 super_region_id using `SR_RE', keepusing(sr_RE_new) nogen
	
		tempfile all
		save `all', replace 
		
				
		use `iso_RE', clear
		preserve
		keep if iso3=="CHN"
		local iso_RE_CHN=iso_RE
	/*	local iso_RE_se_CHN=iso_RE_se */
		restore
		
		preserve
		keep if iso3=="MEX"
		local iso_RE_MEX=iso_RE
	/*	local iso_RE_se_MEX=iso_RE_se */
		restore
		
		preserve
		keep if iso3=="GBR"
		local iso_RE_GBR=iso_RE
	/*	local iso_RE_se_GBR=iso_RE_se */
		restore
		
		preserve
		keep if iso3=="USA"
		local iso_RE_USA=iso_RE
	/*	local iso_RE_se_USA=iso_RE_se */
		restore
		
		preserve
		keep if iso3=="BRA"
		local iso_RE_BRA=iso_RE
	/*	local iso_RE_se_BRA=iso_RE_se */
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
	/*	local iso_RE_se_JPN=iso_RE_se */
		restore
		
	
	    preserve
		keep if iso3=="SWE"
		local iso_RE_SWE=iso_RE
	/*	local iso_RE_se_SWE=iso_RE_se */
		restore
		
		/*
		preserve
		keep if iso3=="ZAF"
		local iso_RE_ZAF=iso_RE
		local iso_RE_se_ZAF=iso_RE_se
		restore
		*/
		
		preserve
		keep if iso3=="SAU"
		local iso_RE_SAU=iso_RE
	 /* local iso_RE_se_SAU=iso_RE_se */
		restore
		
		preserve
		keep if iso3=="IDN"
		local iso_RE_IDN=iso_RE
	/*	local iso_RE_se_IDN=iso_RE_se */
		restore
		
    use `all', clear
    
	/* merge m:1 super_region using `super_RE', nogen	*/
		
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
	replace iso_RE=`iso_RE_SAU' if regexm(iso3,"SAU_")
/*	replace iso_RE_se=`iso_RE_se_SAU' if regexm(iso3,"SAU_") */
	replace iso_RE=`iso_RE_IDN' if regexm(iso3,"IDN_")
/*	replace iso_RE_se=`iso_RE_se_IDN' if regexm(iso3,"IDN_") */

	// missing country random effects are replaced with the average random effect at the global level (i.e., 0)
	replace iso_RE=0 if iso_RE==.
		
	/*
	// countries with missing standard errors are replaced with global standard deviation of the country random effects
	// run _diparm  to get global sd of random effects /* need to use lns1_1_1. xtmixed estimates the ln_sigma, the inverse function is exp(). The derivative of exp() is just exp() */
	_diparm lns1_1_1, f(exp(@)) d(exp(@))
	gen global_sd=`r(est)'
	
	replace iso_RE_se = global_sd if missing(iso_RE_se)
	
	*/
	
	replace reg_RE=reg_RE_new if reg_RE==.
	
	replace sr_RE=sr_RE_new if sr_RE==.
	
	
		// create draws from the covariance matrix to get parameter uncertainty
		
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
			** drop duplicates
			qui duplicates drop
			qui gen id=_n
			merge 1:1 id using "`tmp_betas'", nogen 
			drop id
	
			
		// Generate 1000 estimates - predict without RE
			forvalues j = 1/1000 {
				di in red "Generating Draw `j'"
				 qui gen prop_tbhiv_xb_d`j'=ln_rate*b_ln_rate[`j']+b__cons[`j']+iso_RE+reg_RE+sr_RE
				** qui gen prop_tbhiv_xb_d`j'=sex_2*b_sex_2[`j']+ln_tb_dth_rt*b_ln_tb_dth_rt[`j']+b__cons[`j']
				qui replace prop_tbhiv_xb_d`j'=invlogit(prop_tbhiv_xb_d`j')
			}
	
	** drop duplicates
	 duplicates drop 
	 
	 preserve
	 keep location_id year_id prop_tbhiv_xb_d*
	 save "FILEPATH", replace
		
	 restore
     // calculate mean, upper, and lower
     egen mean_prop=rowmean(prop_tbhiv*)
	 egen lower_prop=rowpctile(prop_tbhiv*), p(2.5)
	 egen upper_prop=rowpctile(prop_tbhiv*), p(97.5)
	 drop prop_tbhiv*

     save "FILEPATH", replace
	 
	 
** *****************************
// Graph predictions... prop tbhiv/tb
** *****************************
// Initialize pdfmaker

	set scheme s1color
	set printcolor asis
	capture confirm file "FILEPATH"
	if _rc == 0 {
		do "$FILEPATH"
	}
	else {
		do "FILEPATH"
	}
	
	
	use "FILEPATH", clear
	duplicates drop location_id year, force
	tempfile prop
	save `prop', replace
	
	use "FILEPATH", clear
	duplicates drop location_id year, force
	tempfile reg
	save `reg', replace
	
	    use `prop', clear
		merge m:1 location_id year using `reg', nogen 
		merge m:1 location_id using "FILEPATH", keepusing(iso3) keep(3)nogen
		gen pred_prop=mean_prop
		gen pred_prop_lower=lower_prop
		gen pred_prop_upper=upper_prop
		drop if year<1980	
		drop if (location_id>=4841 & location_id<=4875) | location_id==44538
		
		pdfstart using "FILEPATH"
			sort iso3 year
			levelsof iso3 , local(isos)
			foreach i of local isos {
					scatter pred_prop year if iso3=="`i'" || scatter raw_prop year if iso3=="`i'", title("`i'")    ///
					legend(order(1 "Predicted proportions" 2 "Raw proportions") col(2))
					pdfappend
			}
		pdffinish
		