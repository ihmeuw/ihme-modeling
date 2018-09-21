// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate shift in relative risks for lead based on SBP distribution
//				And calculate the PAFs
// *************************************************************************************************
// *************************************************************************************************

** *************************************************************************************************
** STATA
** *************************************************************************************************

	clear
	set more off
	set maxvar 32000
	pause on

** *************************************************************************************************
** WORKSPACE
** *************************************************************************************************

	cap ssc install moremata
	cap ssc install rsource
	set seed 370566

	// pull arguments from qsub
	local risk = "`1'"
	local rei_id = "`2'"
	local location_id = "`3'"
	local sex_id = "`4'"
	local year_ids : subinstr local 5 "_" " ", all
	local gbd_round_id "`6'"
	local n_draws "`7'"
	local code_dir = "`8'"
	local out_dir = "`9'"

	// load functions
	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"

    get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

	// pull vars
	import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
	keep if risk=="`risk'"
	levelsof inv_exp, local(inv_exp) c
	levelsof rr_scalar, local(rr_scalar) c
	levelsof tmred_para1, local(tmred_1) c
	levelsof tmred_para2, local(tmred_2) c
	levelsof maxrr, local(cap) c

** *************************************************************************************************
** PAF CALCULATION
** *************************************************************************************************

** pull SBP RRs ----------------------------------------------------------------------------------

	noi di c(current_time) + ": pull SBP RRs"
	get_draws, gbd_id_field(rei_id) gbd_id(107) location_ids(`location_id') year_ids(`year_ids') ///
		sex_ids(`sex_id') kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') source(risk) ///
		n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	replace location_id = `location_id'
	noi di c(current_time) + ": shift RRs"
	forvalues i = 0/`=`n_draws'-1' {
		qui replace rr_`i' = rr_`i'^(.61/10) // .61 mmHg shift in SBP and convert SBP unit space to 1 SBP
	}
	tempfile rr
	save `rr', replace

** REGRESS CV  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": begin SD regression"
	insheet using "FILEPATH/envir_lead_bone_data.csv", clear
	noi regress log_std_dev log_meas_value if age_group_id >= 10
	local intercept = _b[_cons]
	matrix b = e(b)
	matrix V = e(V)
	local bL = colsof(b)
	noi di c(current_time) + ": regression complete"

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get exposure draws for `risk'"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
		sex_ids(`sex_id') age_group_ids(`gbd_ages') kwargs(draw_type:exposure num_workers:10) source(risk) ///
		gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
	cap drop model*
	noi di c(current_time) + ": exposure draws read"
	rename draw_* exp_*
	fastrowmean exp*, mean_var_name(exp_mean)
	gen risk = "`risk'"
	tempfile E
	save `E', replace

	drawnorm b1-b`bL', double n(`n_draws') means(b) cov(V) clear
	forvalues i = 1/`bL' {
		local x = `i' - 1
		rename b`i' b`x'
	}
	local bL = `bL' - 1
	rename b0 coeff_
	keep coeff_ b`bL'
	gen n=_n
	replace n=n-1
	rename b`bL' intercept_
	gen risk = "`risk'"
	reshape wide coeff_ intercept_, i(risk) j(n)
	joinby risk using `E'
	forvalues i = 0/`=`n_draws'-1' {
		qui gen exp_sd_`i' = exp(intercept_`i' + coeff_`i' * ln(exp_mean))
		rename exp_`i' exp_mean_`i'
	}
	drop exp_mean coeff_* intercept_*

	tempfile exp
	save `exp', replace
	
** GENERATE TMREL ----------------------------------------------------------------------------------

	joinby location_id age_group_id sex_id using `rr'
	sort sex_id age_group_id cause_id modelable_entity_id mortality morbidity
	forvalues i = 0/`=`n_draws'-1' {
		qui gen double tmred_mean_`i' = ((`tmred_2'-`tmred_1')*runiform() + `tmred_1')
	}
	qui count
	local n = `r(N)'
	forvalues x = 0/`=`n_draws'-1' {
		qui levelsof tmred_mean_`x' in 1, local(t) c
		forvalues i = 1/`n' {
			qui replace tmred_mean_`x' = `t' in `i'
		}
	}
	
** MAX RR ----------------------------------------------------------------------------------

	gen maxrr=`cap'
	cap drop rei_id
	gen rei_id = `rei_id'
	merge m:1 rei_id using "`out_dir'/`risk'/exposure/exp_max_min.dta", keep(3) nogen
	gen cap = .
	replace cap = min_1_val_mean if cap ==. & `inv_exp'==1
	replace cap = max_99_val_mean if cap ==. & `inv_exp'!=1
	replace cap = maxrr if maxrr < cap & `inv_exp'!=1
	local FILE = "`out_dir'/`risk'/FILE_`location_id'_`sex_id'"
	outsheet using "`FILE'.csv", comma replace

** PAF CALC ----------------------------------------------------------------------------------

	// ENSEMBLE
	insheet using "FILEPATH/`risk'.csv", clear
	keep if sex_id == `sex_id'
	drop location_id year_id sex_id
	duplicates drop
	local W = "`out_dir'/`risk'/WEIGHT_`location_id'_`sex_id'"
    export delimited using "`W'.csv", replace
	noi di c(current_time) + ": begin PAF calc"
	noi rsource using "`code_dir'/core/paf_calc_cont_ensemble.R", rpath("FILEPATH/R") roptions(`" --vanilla --args "`rr_scalar'" "`W'" "`FILE'" "`inv_exp'"  "`code_dir'" "') noloutput
	noi di c(current_time) + ": PAF calc complete"
	erase "`W'.csv"
	import delimited using "`FILE'_OUT.csv", asdouble varname(1) clear
	erase "`FILE'_OUT.csv"
	erase "`FILE'.csv"

	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}

	** expand mortality and morbidity
	mortality_expand mortality morbidity 
	levelsof mortality, local(morts)

	keep cause_id location_id year_id sex_id age_group_id mortality morbidity paf_*
	gen rei_id = `rei_id'
	gen modelable_entity_id = .
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id mortality morbidity
	noi di c(current_time) + ": saving PAFs"
	foreach mmm of local morts {
		if `mmm' == 1 local mmm_string = "yll"
		else local mmm_string = "yld"
		outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
			 "`out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv" ///
			  if mortality == `mmm', comma replace
		no di "saved: `out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv"
	}
