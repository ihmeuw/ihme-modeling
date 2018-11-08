
// MI ratio regression

clear all
set more off

local date 07122018

local model haq_regionRE_`date'_fix_NAME


use "FILEPATH\MI_input_`date'.dta", clear

drop if iso3==""

merge m:1 location_id using "FILEPATH\location_22.dta", keepusing(region_id) keep(3)nogen

    
	cap log close 	
	log using "FILEPATH\log_`model'.smcl", replace


		//  regression 
		
		xtmixed logit_prop age0to4 age5to14 age15to24 age35to44 age45to54 age55to64 age65plus female haq || region_id: 


	cap log close 	
		
    preserve
		predict r_RE, reffects
		predict r_RE_se, reses
		collapse (mean) r_RE r_RE_se, by(region_id) 
		// use global RE for SSA & SEA 
		replace r_RE=0 if region_id==174
		replace r_RE=0 if region_id==9
		replace r_RE=0 if region_id==138

		tempfile r_RE
		save `r_RE', replace
		
	restore
		merge m:1 region_id using `r_RE', nogen
		tempfile all
		save `all', replace 
		
		 use `all', clear
    
	

	// missing SR random effects are replaced with the average random effect at the global level (i.e., 0)
	replace r_RE=0 if r_RE==.
	
	// countries with missing standard errors are replaced with global standard deviation of the SR random effects
	// run _diparm  to get global sd of random effects /* need to use lns1_1_1. xtmixed estimates the ln_sigma, the inverse function is exp(). 
	// The derivative of exp() is just exp() */
	_diparm lns1_1_1, f(exp(@)) d(exp(@))
	gen global_sd=`r(est)'
	
	replace r_RE_se = global_sd if missing(r_RE_se)
		
		// create draws from the covariance matrix to get parameter uncertainty  /* type - matrix list e(b) - to view the matrix  */
		
			matrix m = e(b)'  /* matrix list m */
			matrix m = m[1..(rowsof(m)-2),1]
			local covars: rownames m
			local num_covars: word count `covars'
			local betas
			forvalues j = 1/`num_covars' {
				local this_covar: word `j' of `covars'
				local betas `betas' b_`this_covar'
			}
			matrix C = e(V)
			matrix C = C[1..(colsof(C)-2), 1..(rowsof(C)-2)]
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
	
			
			
		// Generate 1000 estimates 
			forvalues j = 1/1000 {
				di in red "Generating Draw `j'"
				 qui gen xb_d`j'=haq*b_haq[`j']+b__cons[`j']+r_RE
				 quietly replace xb_d`j'=xb_d`j'+female*b_female[`j']
				quietly replace xb_d`j'=xb_d`j'+age0to4*b_age0to4[`j']
			quietly replace xb_d`j'=xb_d`j'+age5to14*b_age5to14[`j']
			quietly replace xb_d`j'=xb_d`j'+age15to24*b_age15to24[`j']
			quietly replace xb_d`j'=xb_d`j'+age35to44*b_age35to44[`j']
			quietly replace xb_d`j'=xb_d`j'+age45to54*b_age45to54[`j']
			quietly replace xb_d`j'=xb_d`j'+age55to64*b_age55to64[`j']
			quietly replace xb_d`j'=xb_d`j'+age65plus*b_age65plus[`j']
				qui replace xb_d`j'=invlogit(xb_d`j')
				qui replace xb_d`j'=xb_d`j'*1.139966
			}
			
		** drop duplicates
			duplicates drop 
		tempfile tmp_prop_xb
		save `tmp_prop_xb', replace 
		
     // calculate mean, upper, and lower
     egen mean_prop=rowmean(xb_d*)
	 egen lower_prop=rowpctile(xb_d*), p(2.5)
	 egen upper_prop=rowpctile(xb_d*), p(97.5)
	 drop xb_d*

     save "FILEPATH\cfr_pred_`model'.dta", replace



use "FILEPATH\cfr_pred_`model'.dta", clear	 
keep location_id location_name year_id sex_id haq age mean_prop lower_prop upper_prop
sort location_id year_id age sex_id
rename mean_prop cfr_pred
gen cfr_se=(upper_prop-lower_prop)/(2*1.96)
tempfile cfr
save `cfr', replace


use "FILEPATH\TB_TBHIV_mortality_tb_codcorrect_v84_v1.6_custom_age.dta", clear
keep location_id year_id sex_id age all_tb_death_mean all_tb_death_lower all_tb_death_upper
rename all_tb_death_mean all_tb_death
merge m:1 location_id using "FILEPATH\location_22.dta", keepusing(ihme_loc_id region_name) keep(3)nogen
rename ihme_loc_id iso3
tempfile death
save `death', replace


use "FILEPATH\tb_prev.dta", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
keep location_id year_id age_group_id sex_id mean 
merge m:1 age_group_id using "FILEPATH\age_group_ids.dta", keep(3)nogen
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH\population.dta", keepusing(population) keep(3)nogen
gen cases=mean*population
drop if age_group_id==22
            preserve
				qui keep if age_group_id>=18
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_group_id<=5
			drop if age_group_id>=18
			
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
			forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) cases population, by(location_id year sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
						
			qui drop if year<1990
			
gen prev= cases/population
tempfile prev
save `prev', replace


use "FILEPATH\hivtb_prev.dta", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
keep location_id year_id age_group_id sex_id mean 
merge m:1 age_group_id using "FILEPATH\age_group_ids.dta", keep(3)nogen
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH\population.dta", keepusing(population) keep(3)nogen
gen cases=mean*population
drop if age_group_id==22
            preserve
				qui keep if age_group_id>=18
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_group_id<=5
			drop if age_group_id>=18
			
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
			forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) cases population, by(location_id year sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
						
			qui drop if year<1990
		

gen hivtb_prev= cases/population

merge 1:1 location_id year_id age sex_id using `prev', keepusing(prev) keep(3)nogen
gen hiv_prop=hivtb_prev/prev

gen RR_median=1.684684

gen hiv_rr_cyas=hiv_prop*RR_median+(1-hiv_prop)*1

tempfile hiv
save `hiv', replace


use `death', clear
merge 1:1 location_id year_id age sex_id using `prev', keep(3)nogen
merge m:1 location_id year_id age sex_id using `cfr', keep(3)nogen
merge m:1 location_id year_id age sex_id using `hiv', keep(3)nogen

gen cfr_pred_hiv=cfr_pred*hiv_rr_cyas

gen csmr=all_tb_death/population

gen csmr_lower=all_tb_death_lower/population

gen csmr_upper=all_tb_death_upper/population

gen csmr_se=(csmr_upper-csmr_lower)/(2*1.96)

gen inc_cases=all_tb_death/cfr_pred

gen inc_cases_hiv=all_tb_death/cfr_pred_hiv

gen inc_rate=inc_cases_hiv/population


// compute combined se 

gen inc_rate_se=sqrt((csmr^2/cfr_pred_hiv^2)*((csmr_se^2/csmr^2)+(cfr_se^2/cfr_pred_hiv^2)))

save "FILEPATH\inc_from_death_cfr_`model'.dta", replace
