
* do "/filepath/other_intestinal_cod_custom.do"

*** BOILERPLATE ***
	clear all
	set more off

		local j "filepath"

	run "`j'/filepath/build_cod_dataset.ado"
	run `j'/filepath/select_xforms.ado
	run `j'/filepath/run_best_model.ado
	run "`j'/filepath/process_predictions.ado"

*** CREATE THE DATASET ***
	
	local cause_id 321
	*local covariate_ids 33 57 71 108 109 114 122 127 128 129 130 131 132 133 134 135 142 159 160 463 482 486 487 845 854 863 866 881 1099
	* Other possible covariates:
		* SEV Diarrhea 740
		* SEV Hygiene 2040
		* DTP3 coverage 32
		
		
	local covariate_ids 32 142 160 740 863 866 881 1099 2040
	local saveto `j'/filepath/otherIntest2019_decomp3.dta
	local minAge 4

** Building a COD dataset took 3.1GB of RAM (6 cores requested, 20.4% of requested memory) and took 21 minutes, 50 seconds. 
*** Done already!! ***	
   build_cod_dataset `cause_id', covariate_ids(`covariate_ids')  saveto(`saveto', replace) clear

*** RUN BEST MODEL AS MIXED EFFECTS ***

use `saveto', clear

generate popScaled =  study_deaths / rate

local covarListRaw sdi_22 haqi_22 water_prop_22

foreach covar of local covarListRaw {
	quietly {
		sum `covar'
		generate `covar'_C = (`covar' - `r(min)') / (`r(max)' - `r(min)')
		replace  `covar'_C = `covar'_C + ((0.5 - `covar'_C) * 0.000002)
		}
	}
	


local eta 1
sum water_prop_22_C
generate water_prop_22_Cx = (ln((normal((water_prop_22_C - `r(mean)') / `r(sd)') + `eta') / (1 - normal((water_prop_22_C - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))

capture generate logit_haqi_22 = logit(haqi_22_C)
capture generate cloglog_water_prop_22 = cloglog(water_prop_22_C)
capture generate ln_year = ln(year_id)

local covar cloglog_water_prop_22
sum `covar' 
generate xformTemp1 = (`covar' - `r(min)') / (`r(max)' - `r(min)')
replace xformTemp1 = xformTemp1 + ((0.5 - xformTemp1) * 0.000002)

local eta 1
sum xformTemp1
generate xformTemp2 = (ln((normal((xformTemp1 - `r(mean)') / `r(sd)') + `eta') / (1 - normal((xformTemp1 - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))
		
mkspline xformTempS1 0.65 xformTempS2 = xformTemp2
capture drop xformTempS*
mkspline xformTempS1 0.3 xformTempS2 = xformTemp2


mkspline sevWaterS = SEV_wash_water, cubic 

gen sqrtSdi = sqrt(sdi)
capture drop xformSdi
local eta 0.001
sum sqrtSdi
generate xformSdi = (ln((normal((sqrtSdi - `r(mean)') / `r(sd)') + `eta') / (1 - normal((sqrtSdi - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))
		
scatter xformSdi sqrtSdi


generate numerator = rate * population
generate logitCf_22 = logit(cf_22)
generate lnRate_22 = ln(rate_22)
generate lnRate = ln(rate)

/*
*mixed logitCf_22 logit_haqi_22 cloglog_water_prop_22 if aaIndex==1 || super_region_id: sex_id || region_id: || location_id: ln_year
*mixed logitCf_22 xformTemp2 if aaIndex==1 || super_region_id: sex_id || region_id: || location_id: 
*mixed logitCf_22 xformTemp2 i.sex_id if aaIndex==1 || region_id: || location_id:
*mixed logitCf_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: 
mixed logitCf_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: 
*mixed lnRate_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: 
*/

mixed logitCf_22 xformSdi i.sex_id if aaIndex==1 || region_id: 
*local varInflator = _se[xformSdi]

capture drop fixed* 
capture drop random*
capture drop stage1
predict fixed
predict fixedSe, stdp
predict randomMean*, reffects
predict randomSe*, reses
/*
foreach var of varlist random? {
	quietly sum `var' if e(sample)
	replace `var' = `r(mean)' if missing(`var')
	}
*/
foreach rVar of varlist randomMean* {
	sum `rVar' if e(sample)
	replace `rVar' = `r(mean)' if missing(`rVar')
	local seTemp = `r(sd)'
	sum `=subinstr("`rVar'", "Mean", "Se", .)' if e(sample)
	replace `=subinstr("`rVar'", "Mean", "Se", .)' = sqrt(`r(mean)'^2 + `seTemp'^2) if missing(`=subinstr("`rVar'", "Mean", "Se", .)')
	}		
	
egen randomMeanFull = rowtotal(randomMean?)
*gen stage1 = ln(invlogit(fixed + randomMeanFull))
gen stage1 = fixed + randomMeanFull

egen randomSeFull = rowtotal(randomSe?)
gen stage1Se = fixedSe + randomSeFull

forvalues i = 0/999 {
	quietly generate inflate_`i' = rnormal(0, stage1Se)
	di "." _continue
	}

*gen stage1 = fixed + randomFull
*tabstat stage1, by(year_id) stat(n mean sd min p25 p50 p75 max)
sum fixedSe, meanonly
local varInflator = `r(mean)'
drop fixed* random*
*/
*run_best_model mixed lnRate i.age_group_id stage1 , reffects(|| region_id: R.age_group_id)
*local description "All-age rate to age-specific (mixed lnRate_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: || location_id:)"

*run_best_model menbreg study_deaths i.age_group_id stage1, exp(popScaled) difficult reffects(|| location_id: R.age_group_id)
*local description "All-age rate to age-specific (mixed lnRate_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: || location_id:)"


*run_best_model menbreg study_deaths i.age_group_id stage1, exp(sample_size)  difficult reffects(|| region_id:)
*local description "All-age cf to age-specific (re test) (mixed logitCf_22 xformTempS1 xformTempS2 i.sex_id if aaIndex==1 || region_id: )"

*run_best_model glm study_deaths i.age_group_id  stage1, family(binomial sample_size) difficult 
*local description "All-age cf to age-specific binomial (mixed logitCf_22 logit_haqi_22 cloglog_water_prop_22 if aaIndex==1 || super_region_id: sex_id || region_id: || location_id: R.year_id)"
*local description "All-age cf to age-specific binomial (mixed logitCf_22 loglog_water_prop_22 if aaIndex==1 || super_region_id: sex_id ln_year || region_id: || location_id: )"
*/



replace is_outlier = 1 if inlist(location_id, 203, 215)  
replace is_outlier = 1 if (super_region_name=="Sub-Saharan Africa" | region_name=="Oceania") & cf==0

capture drop logitCf
sum cf if cf>0 & is_outlier==0, d
generate logitCf = logit(cf+`r(min)')


*bysort super_region_id: egen p1 = pctile(logitCf), p(1)
*bysort super_region_id: egen p99 = pctile(logitCf), p(99)

*mixed logitCf i.age_group_id i.sex_id haqi xformTempS1 xformTempS2, difficult || region_id:


*run_best_model menbreg study_deaths i.age_group_id i.sex_id haqi if outlier==0, exp(sample_size)  difficult reffects(|| region_id:)
*local description "Single-step me-cf model (menbreg study_deaths i.age_group_id i.sex_id haqi, exp(sample_size)  difficult reffects(|| region_id:))"

*run_best_model mixed logitCf i.age_group_id i.sex_id xformTemp2 if is_outlier==0, difficult reffects(|| super_region_id: || region_id:)
*local description "Single-step me-cf model (mixed logitCf i.age_group_id i.sex_id xformTemp2[eta=1], difficult reffects(|| super_region_id: || region_id:))"

*run_best_model mixed logitCf i.age_group_id i.sex_id water_prop_22_Cx sdi_22_C if is_outlier==0, difficult reffects(|| super_region_id: || region_id:)
*local description "Single-step me-cf model (mixed logitCf i.age_group_id i.sex_id water_prop_22_Cx sdi_22_C, difficult reffects(|| super_region_id: || region_id:))"

/*
run_best_model glm deaths i.age_group_id i.sex_id stage1 if is_outlier==0, family(binomial envelope)
local description "Simplified model with VA data"
process_predictions `cause_id', link(logit) random(no) min_age(`minAge') description("`description'") multiplier(envelope) 
*/
/*
glm deaths i.age_group_id i.sex_id stage1 SEV_wash_water if is_outlier==0, family(binomial envelope)
capture drop predTest
predict predTest
tabstat predTest if predIndex==1 & is_estimate==1 & !inlist(age_group_id, 2, 3, 22, 27), by(year_id) stat(n mean sum)
*/

run_best_model glm deaths i.age_group_id i.sex_id stage1 if is_outlier==0, family(binomial envelope)

/*
sum stage1
	local stage1Range = `r(max)' - `r(min)'
sum xformSdi
	local xformSdiRange = `r(max)' - `r(min)'
	
replace beta_covarTemp_25 =	beta_covarTemp_25 + rnormal(0,`varInflator' / (`stage1Range'/`xformSdiRange'))
*/

local description "Refresh 4, decomp 3"
process_predictions `cause_id', link(logit) random(no) min_age(`minAge') description("`description'") multiplier(envelope) inflator_draw_stub(inflate_) 


sum logitCf if is_outlier==0, d

// If save results crashes, use this: //
/*
run "/filepath/save_results_cod.ado"
save_results_cod, decomp_step(step3) cause_id(321) mark_best("False") description("Refresh 4, decomp 3") input_dir("/filepath/") input_file_pattern({location_id}.csv) clear
*/
