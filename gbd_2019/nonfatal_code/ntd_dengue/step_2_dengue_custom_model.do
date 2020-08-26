***********************************************************************

**Runs custom model code - negative binomial mixed effects regression using csmr and dengue_prob as covariates
*outcome of model is all-age both sex incidence, with sex and age splitting after model predictions are generated


**---------------------------------------------------------------------------------------------

*import dataset with covariate and estimation data frame
import delimited "FILEPATH",clear

*subset to estimation years
keep if year_id==1990 | year_id==1995 | year_id==2000 |year_id==2005 |year_id==2010 |year_id==2015 | year_id==2017 | year_id==2019

*append all-age, both sex case data
append using "FILEPATH"

replace sample_size=population if sample_size==.
*run the model and predict with and without the fixed effects
menbreg new_cases deng_prob_cov csmr_cov if deng_prob_cov>0, exp(sample_size) intmethod(mvaghermite)  || location_id:
predict randomModel, reffects reses(randomModelSe) nooffset
  predict fixed, fixedonly fitted nooffset
  predict fixedSe, stdp fixedonly nooffset
 

* CREATE THE 1000 DRAWS * *NOTE we run for India with random effects 
forvalues i = 0/999 {
  local fixedTemp  = rnormal(0,1)
  local randomTemp = rnormal(0,1)
  
  quietly {
 
	 
	 generate draw_`i' = exp(rnormal(fixed, fixedSe)) * sample_size
	*replace  draw_`i' = 0 if deng_prob_cov==0 
	 
	*generate draw_`i' = exp(rnormal(fixed, fixedSe) + rnormal(randomModel, randomModelSe)) * sample_size  
	* replace  draw_`i' = 0 if deng_prob_cov==0 
	 
	 
	 }

  di "." _continue
  }

*drop model training dataset
drop if model_dataset==1

*generate rows for age and sex-specific predictions
keep location_id year_id draw_* population 

expand 2, generate(sex_id)
replace sex_id=2 if sex_id==0

expand 23, generate(age_group_id)
bysort location_id year_id sex_id: generate counter= _n

replace age_group_id=counter
replace age_group_id=30 if counter==1
replace age_group_id=31 if counter==21
replace age_group_id=32 if counter==22
replace age_group_id=235 if counter==23

drop counter 


* BRING IN AGE DISTRIBUTION DATASET *
rename population allAgePop
*apply single age pattern, based off Brazil data
merge m:1 sex_id age_group_id using "FILEPATH"

drop _merge
*bring in dataset with age-specific populations 
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH"

keep if _merge==3

replace incCurve = 0 if age_group_id<4
generate casesCurve = incCurve * population
bysort year_id location_id: egen totalCasesCurve = total(casesCurve)

forvalues i = 0/999 {
  
  replace draw_`i' = casesCurve * draw_`i' / totalCasesCurve
  replace  draw_`i' = draw_`i' / population
  
   }
  

keep year_id location_id age_group_id sex_id draw_*
generate modelable_entity_id = ADDRESS
generate measure_id = 6
  
 *output draws
levelsof location_id, local(dLocs) clean

	foreach location of local dLocs {
	{

				export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using /ihme/ntds/ntd_models/ntd_models/ntd_dengue/resub/custom/fixed/`location'.csv if location_id==`location',  replace
		}
		}
