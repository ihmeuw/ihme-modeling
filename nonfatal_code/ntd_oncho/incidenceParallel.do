

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 10000
	set more off
  
	adopath + FILEPATH
	run FILEPATH/get_draws.ado
	run FILEPATH/get_demographics.ado
 
 
 
*** PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND ***  
	local location  "`1'"
	local meid      "`2'"
	local rootDir   "`3'"
	local logDir    "`4'"
	local csmr      "`5'"
	local meid_prev "`6'"


	capture log close
	log using `logDir'/`location'.smcl, replace
  

    macro dir
  
*** LOAD DATA FOR MODELLING AND PREP VARIABLES ***
 
	use `rootDir'/temp/`location'.dta, clear

	sum mean, meanonly
	if `r(mean)'==0 {
		local endemic 0
		}
	
	else {	
		local endemic 1
		
		levelsof age_group_id, local(ages) clean
		
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid_prev') location_ids(`location') status(best) measure_ids(5) age_group_ids(`ages') source(dismod) clear
		drop model_version_id
		
		rename draw_* prev_*
		
		merge 1:1 location_id year_id sex_id age_group_id using `rootDir'/temp/`location'.dta, assert(3) nogenerate

		
	*** IF ENDEMIC THEN PREP VARIABLES ***	

		gen age2 = age^2
		gen age3 = age^3

		foreach var in age age2 age3 {
			generate `var'_male = (sex_id==1) * `var'
			generate `var'_female = (sex_id==2) * `var'
			}
			
		foreach var in foa suscepPred suscepSe {
			generate `var' = .
			}

		forvalues i = 0 / 999 {
			foreach var in foa suscep rem inc {
				quietly generate `var'_`i' = .
				}
			}
			
		bysort year_id: egen maxAnnualPrev = max(mean)
		
		capture generate prNeg = 1 - mean
		
		capture drop remission
		generate remission = invlogit(fixed + randomFull) * (1.01 * maxMean) + 0.99*minMean
		
		generate toInterpolate = 0
			
			
	*** CYCLE THROUGH YEARS, RUN YEAR-SPECIFIC MODELS, AND CREATE DRAWS ***		
		foreach year in 1990 1995 2000 2005 2010 2016 {

			* Determine if prevalence > 0 in current year * 
			sum mean if year_id==`year', meanonly
			local yearMean = `r(mean)'
			
			* If >0 then run the model *
			if (`meid'==1495 & `yearMean'>0.0001) | (`meid'!=1495 & `yearMean'>0) | (`meid'==1495 & `location'==125 & `yearMean'> 2e-07) {
				glm prNeg age*male if year_id==`year', link(log) nocons
				predict suscepTmp, xb
				predict suscepTmpSe, stdp
				replace suscepPred = suscepTmp if year_id==`year'
				replace suscepSe   = suscepTmpSe if year_id==`year'
				drop suscepTmp*

				replace foa = -1 * (_b[age_male] + 2*_b[age2_male]*age + 3*_b[age3_male]*age^2) if sex_id==1 & year_id==`year'
				replace foa = -1 * (_b[age_female] + 2*_b[age2_female]*age + 3*_b[age3_female]*age^2) if sex_id==2 & year_id==`year'

				matrix m = e(b)'
				matrix C = e(V)
				local covars: rownames m
				drawnorm b_`=subinstr("`covars'", " ", " b_", .)', means(m) cov(C)


				forvalues i = 0 / 999 {
					quietly {
						replace foa_`i' = -1 * (b_age_male[`=`i'+1'] + 2*b_age2_male[`=`i'+1']*age + 3*b_age3_male[`=`i'+1']*age^2) if sex_id==1 & year_id==`year'
						replace foa_`i' = -1 * (b_age_female[`=`i'+1'] + 2*b_age2_female[`=`i'+1']*age + 3*b_age3_female[`=`i'+1']*age^2) if sex_id==2 & year_id==`year'
				
						replace suscep_`i' = 0 if year_id==`year'
						
						foreach covar of local covars {
							replace suscep_`i' = suscep_`i' + `covar' * b_`covar'[`=`i'+1'] if year_id==`year'
							}
						}
					}
				drop b_*
				}
			else if (`meid'==1495 & `yearMean'<=0.0001 & `yearMean'>0) & !(`meid'==1495 & `location'==125 & `yearMean'> 2e-07) {
				replace toInterpolate = 1 if year_id==`year'
				}
			}
			
			
	
			
			
		forvalues i = 0 / 999 {
			quietly {
				replace rem_`i' = invlogit(rnormal(fixed, fixedS) + rnormal(randomFull, randomFullSe)) * (1.01 * maxMean) + 0.99*minMean
				replace inc_`i' = foa * (1 - prev_`i') + (rem_`i' * prev_`i') 
				
				if `meid'==1495 {
						bysort location_id year_id sex_id (age_group_id): replace inc_`i' = inc_`i'[5] if age_group_id<6
						}
						
				replace inc_`i' = 0 if inc_`i'<0 | maxAnnualPrev==0
				
				bysort location_id sex_id age_group_id (year_id): replace inc_`i' = inc_`i'[_n-1] if toInterpolate==1
				}
			di "." _continue
			}
			
		fastrowmean inc_*, mean_var_name(incMean)
		generate incPoint = foa  * exp(suscepPred) + (remission * (1-exp(suscepPred))) if toInterpolate==0
		
		replace incPoint = 0 if incPoint<0 | incMean==0 | mean==0
		
		sum toInterpolate, meanonly
		if `r(mean)'>0 {
			generate incPrevRatio = incPoint / mean
			bysort location_id sex_id age_group_id (year_id): replace incPrevRatio = incPrevRatio[_n-1] if toInterpolate==1
			replace incPrevRatio = 0 if mean==0
			replace incPoint = mean * incPrevRatio if toInterpolate==1
			}
		
		if `meid'==1495 {
			bysort location_id sex_id age_group_id (year_id): replace incPoint = incPoint[_n-1] if year_id==2016 & mean>0 & incPoint>incPoint[_n-1]
			}
			
		forvalues i = 0 / 999 {
			quietly {
				replace inc_`i' = inc_`i' * incPoint / incMean
				}
			}
			
	*** CLEAN UP ***		
		rename inc_* draw_*
		keep location_id year_id sex_id age_group_id draw_*

		generate modelable_entity_id = `meid'
		generate measure_id = 6

		tempfile inc
		save `inc'
		
	}  // end block of code for processing in endemic countries here
 	
	
	levelsof age_group_id, local(ages) clean



*** PULL IN PREVALENCE DRAWS & EXPORT DRAW FILE ***
	levelsof age_group_id, local(ages) clean

	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') location_ids(`location') status(best) measure_ids(5) age_group_ids(`ages') source(dismod) clear
	drop model_version_id
	
	
	if `endemic'==0 {
		expand 2, gen(newObs)
		replace measure_id = measure_id + newObs
		drop newObs
		}
	
	else {		
		append using `inc'
		}

	export delimited using `rootDir'/draws/`location'.csv, replace
	
	
	
	file open progress using `rootDir'/progress/`location'.txt, text write replace
	file write progress "complete"
	file close progress
  
	log close


