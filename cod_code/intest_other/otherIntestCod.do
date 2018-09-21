

*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "FILEPATH"
		}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}

	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model_dev.ado
	run FILEPATH/process_predictions.ado


*** CREATE THE DATASET ***
	
	local cause_id 321
	local covariate_ids 33 57 71 108 109 114 122 127 128 129 130 131 132 133 134 135 142 159 160 463 482 486 487 845 854 863 866 881 1089 1099
	local saveto FILEPATH/data/otherIntest.dta
	local minAge 4

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids')  saveto(`saveto', replace) clear

	


*** PREP DATA FOR MODELLING ***
	use `saveto', clear

	
	sum water_prop_22
	generate water_prop_22_C = (water_prop_22 - `r(min)') / (`r(max)' - `r(min)')
	replace  water_prop_22_C = water_prop_22_C + ((0.5 - water_prop_22_C) * 0.000002)

	generate cloglog_water_prop_22 = cloglog(water_prop_22_C)

	local covar cloglog_water_prop_22
	sum `covar' 
	generate xformTemp1 = (`covar' - `r(min)') / (`r(max)' - `r(min)')
	replace xformTemp1 = xformTemp1 + ((0.5 - xformTemp1) * 0.000002)

	local eta 1
	sum xformTemp1
	generate xformTemp2 = (ln((normal((xformTemp1 - `r(mean)') / `r(sd)') + `eta') / (1 - normal((xformTemp1 - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))
			

	sum cf if cf>0
	generate logitCf = logit(cf+`r(min)')




*** RUN MODEL ***
	run_best_model mixed logitCf i.age_group_id i.sex_id xformTemp2 if is_outlier==0, difficult reffects(|| super_region_id: || region_id:)

	
*** SUBMIT JOBS TO CREATE DRAWS AND SAVE RESULTS ***
	local description "Single-step me-cf model (mixed logitCf i.age_group_id i.sex_id xformTemp2[eta=1], difficult reffects(|| super_region_id: || region_id:))"
	process_predictions `cause_id', link(logit) random(yes) min_age(`minAge') description("`description'") multiplier(envelope) 
	
	
	

