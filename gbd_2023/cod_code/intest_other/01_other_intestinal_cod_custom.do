
*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "/FILEPATH"
		local h "/FILEPATH"
		}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		local h "FILEPATH"
		}
	
	local description "DESCRIPTION"
	local codeDir "FILEPATH/Other_Intestinal"

	run "`codeDir'/build_cod_dataset_0701.ado"
	run "`codeDir'/select_xforms.ado"
	run "`codeDir'/run_best_model.ado"

	
*** CREATE THE DATASET ***
	
	local cause_id 321
	*local covariate_ids 33 57 71 108 109 114 122 127 128 129 130 131 132 133 134 135 142 159 160 463 482 486 487 845 854 863 866 881 1099
	* Other possible covariates:
		* SEV Diarrhea 740
		* SEV Hygiene 2040
		* DTP3 coverage 32
		
	local covariate_ids 32 160 740 863 866 881 1099 2040 
	local saveto "FILEPATH/otherIntest2023_codrefresh70.dta"
	local minAge 4
	
   build_cod_dataset `cause_id', covariate_ids(`covariate_ids')  saveto(`saveto', replace) clear

   
*** RUN MODEL AS MIXED EFFECTS ***

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

log using "FILEPATH/log_otherIntest2023_codrefresh70.smcl", replace
mixed logitCf_22 xformSdi i.sex_id if aaIndex==1 || region_id: 
*local varInflator = _se[xformSdi]

capture drop fixed* 
capture drop random*
capture drop stage1
predict fixed
predict fixedSe, stdp
predict randomMean*, reffects
predict randomSe*, reses

foreach rVar of varlist randomMean* {
	sum `rVar' if e(sample)
	replace `rVar' = `r(mean)' if missing(`rVar')
	local seTemp = `r(sd)'
	sum `=subinstr("`rVar'", "Mean", "Se", .)' if e(sample)
	replace `=subinstr("`rVar'", "Mean", "Se", .)' = sqrt(`r(mean)'^2 + `seTemp'^2) if missing(`=subinstr("`rVar'", "Mean", "Se", .)')
	}		
	
egen randomMeanFull = rowtotal(randomMean?)
gen stage1 = fixed + randomMeanFull

egen randomSeFull = rowtotal(randomSe?)
gen stage1Se = fixedSe + randomSeFull

forvalues i = 0/999 {
	quietly generate inflate_`i' = rnormal(0, stage1Se)
	di "." _continue
	}

sum fixedSe, meanonly
local varInflator = `r(mean)'
drop fixed* random*


replace is_outlier = 1 if inlist(location_id, 203, 215)  
replace is_outlier = 1 if (super_region_name=="Sub-Saharan Africa" | region_name=="Oceania") & cf==0

capture drop logitCf
sum cf if cf>0 & is_outlier==0, d
generate logitCf = logit(cf+`r(min)')

run_best_model glm deaths i.age_group_id i.sex_id stage1 if is_outlier==0, family(binomial envelope)


process_predictions `cause_id', link(logit) random(no) min_age(`minAge') description("`description'") multiplier(envelope) inflator_draw_stub(inflate_) 
log close

sum logitCf if is_outlier==0, d

