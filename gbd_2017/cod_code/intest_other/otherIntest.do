

*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "/home/j"
		}
	else if c(os) == "Windows" {
		local j "J:"
		}

	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado


*** CREATE THE DATASET ***
	
	local cause_id 321
	local covariate_ids 142 160 863 866 881 1099
	local saveto "FILEPATH".dta
	local minAge 4

    build_cod_dataset `cause_id', covariate_ids(`covariate_ids')  saveto(`saveto', replace) clear

	


*** RUN BEST MODEL AS MIXED EFFECTS ***

	use `saveto', clear



	gen sqrtSdi = sqrt(sdi)
	capture drop xformSdi
	local eta 0.001
	sum sqrtSdi
	generate xformSdi = (ln((normal((sqrtSdi - `r(mean)') / `r(sd)') + `eta') / (1 - normal((sqrtSdi - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))
			


	generate logitCf_22 = logit(cf_22)


	mixed logitCf_22 xformSdi i.sex_id if aaIndex==1 || region_id: 

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

	drop fixed* random*



	capture drop logitCf
	sum cf if cf>0 & is_outlier==0, d
	generate logitCf = logit(cf+`r(min)')


	run_best_model glm deaths i.age_group_id i.sex_id stage1 if is_outlier==0, family(binomial envelope)


	local description "Two-stage model with xform[eta = 0.001] of sqrtSDI"
	process_predictions `cause_id', link(logit) random(no) min_age(`minAge') description("`description'") multiplier(envelope) inflator_draw_stub(inflate_) 

