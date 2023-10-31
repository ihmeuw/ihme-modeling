
*DENGUE CUSTOM MODEL - NEGATIVE BINOMIAL WITH RANDOM INTERCEPT ON LOCATION ID 

*NOTE ON TERMINOLOGY - 
	*TRAINING DATASET = THE DATASET USED TO FIT THE MODEL AND ESTIMATE COVARIATE BETAS, RANDOM INTERCEPTS, ETC.
	*ESTIMATION DATASET = THE DATASET OF ALL LOCATIONS YEARS, SEX, ETC. FOR WHICH WE MAKE PREDICTIONS 


*PULL IN ALL COVARIATE VALUES FOR LOCATIONS - THIS WILL BE USED TO BUILD THE ESTIMATION DATA SET

import delimited "FILEPATH/covariate_file_GBD2020.csv",clear


*KEEP YEARS FOR GBD STUDY
keep if year_id==1990 | year_id==1995 | year_id==2000 |year_id==2005 |year_id==2010 |year_id==2015 | year_id==2019 | year_id==2020 | year_id==2021 | year_id==2022 

*drop type mismatch variables - 
drop developed end_date

*PULL CASE DATA WHICH HAS COVARIATE VALUES STORED 
append using "FILEPATH/dengue_cases.dta"

*NOTE: you need to drop parent_level locations as they can skew the csmr covariate fit
drop if location_id==6 & model_dataset==1
drop if location_id==163 & model_dataset==1
drop if location_id==135 & model_dataset==1
drop if location_id==11 & model_dataset==1
drop if location_id==16 & model_dataset==1



*centering csmr covariate
summarize csmr_cov, meanonly
gen csmr_center = csmr_cov - r(mean)

*log transform pop density
generate ln_dens=log(popdens_cov)
*replace b/c some locations have pop density=0
replace ln_dens=log(.05) if popdens_cov==0

generate ln_prob=log(deng_prob_cov)



*if missing sample size, impute with population
replace sample_size=population if sample_size==.


drop if year_id<1990

generate inc_test=cases/sample_size
generate year_c=year_id-((2022-1990)/2)

menbreg cases deng_prob_cov csmr_center ln_dens if deng_prob_cov>0, exp(sample_size) intmethod(mvaghermite) || location_id: 

predict  re_loc, reffects reses(randomModelSeLoc) nooffset

 
  predict fixed, fixedonly fitted nooffset
  predict fixedSe, stdp fixedonly nooffset

*data management to impute random effects for locations that do not have data
bysort  location_id: egen countryRandom = mean(re_loc)
replace re_loc = countryRandom if missing(re_loc)

bysort super_region_id year_id: egen RegionRandom =mean(re_loc)
replace re_loc= RegionRandom if missing(re_loc)

bysort super_region_id: egen RegionRandom3 =mean(re_loc)
replace re_loc= RegionRandom3 if missing(re_loc)


bysort  location_id year_id: egen LocRandomSe = mean(randomModelSeLoc)
replace randomModelSeLoc = LocRandomSe if missing(randomModelSeLoc)


bysort super_region_id: egen RegionRandom2 =mean(randomModelSeLoc)
replace randomModelSeLoc= RegionRandom2 if missing(randomModelSeLoc)


* CREATE THE 1000 DRAWS for all locations 
forvalues i = 0/999 {
  local fixedTemp  = rnormal(0,1)
  local randomTemp = rnormal(0,1)
  
  quietly {
 
	 *predict from fixed effects for South Asia region only
	 generate draw_`i' = exp(rnormal(fixed, fixedSe)) * sample_size if region_id==159
	 
	 replace draw_`i' = exp((rnormal(fixed, fixedSe)) + rnormal(re_loc,randomModelSeLoc)) * sample_size  if region_id!=159
	 
	 *if observed is higher than modeled, we correct and draw from fixed effect SE to introduce uncertainty
	 replace draw_`i' = exp(rnormal((ln(inc_input)),fixedSe))*sample_size if inc_input> (draw_`i'/sample_size) & !missing(inc_input)
	 
	*inflate estimates from south Asia (except Bangladesh) by mean expansion factor to account for gross under-reporting (relative to large populations)
	*uniform dist 4.8 to 9.6
	 replace draw_`i' = draw_`i'* ((9.6-4.6)*runiform()+4.6) if region_id==159 & location_id!=161
	
	 *for any location not in the endemic set
	 replace  draw_`i' = 0 if deng_prob_cov==0 
	  
	 *NOTE: draws output are in CASE SPACE 
	 	  
	 }

  di "." _continue
  }



*DROP ROWS FROM TRAINING DATASET - THEY ARE ONLY NEEDED TO FIT THE MODEL


drop if model_dataset==1
*You are now left with an dataset that is the base of the estimation frame 

*KEEP LOCATION ID, YEAR ID, POPULATION, and DRAWS 
keep location_id year_id draw_* population 

*EXPAND DATASET NOW TO HAVE MALE AND FEMALE ROWS
expand 2, generate(sex_id)
replace sex_id=2 if sex_id==0

*EXPAND TO CONSTRUCT AGE CATEGORIES 
expand 25, generate(age_group_id)
bysort location_id year_id sex_id: generate counter= _n

replace age_group_id=counter
replace age_group_id=30 if counter==1
replace age_group_id=31 if counter==21
replace age_group_id=32 if counter==22
replace age_group_id=235 if counter==23
replace age_group_id=238 if counter==24
replace age_group_id=388 if counter==25
replace age_group_id=389 if age_group_id==5
replace age_group_id=34 if age_group_id==4
drop counter 
******************************************************************

**--------APPLY AGE/SEX PATTERN-------------------------**

* BRING IN AGE DISTRIBUTION DATASET *
rename population allAgePop

merge m:1 sex_id age_group_id using "FILEPATH/ageDistribution20.dta"

drop _merge

*MERGE POPULATION BY AGE 
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH/pop_age20.dta"

keep if _merge==3

*set early neonatal to zero per age restrictions (age_group_id=2)
replace incCurve = 0 if age_group_id==2
generate casesCurve = incCurve * population
bysort year_id location_id: egen totalCasesCurve = total(casesCurve)

forvalues i = 0/999 {
  quietly{
  replace draw_`i' = casesCurve * draw_`i' / totalCasesCurve
  replace  draw_`i' = draw_`i' / population
  }
   }
  
 
keep year_id location_id age_group_id sex_id draw_*
generate model_id= "ADDRESS"
generate measure_id = 6
  
*CREATE LOCATIONS INDEX FOR LOOP 
levelsof location_id, local(dLocs) clean

*LOOP THROUGH AND OUTPUT ALL DRAWS BY LOCATION_ID
	foreach location of local dLocs {
	{
		quietly{
				export delimited location_id year_id sex_id age_group_id measure_id model_iddraw_* using FILEPATH/`location'.csv if location_id==`location',  replace
		}
		}
}
