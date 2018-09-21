// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Central PAF calculator
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

	// get arguments from qsub
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
	run "`code_dir'/core/paf_calc_categ.do"

    get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

	// pull vars
	import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
	keep if risk=="`risk'"
	levelsof maxval, local(maxval) c
	levelsof minval, local(minval) c
	levelsof inv_exp, local(inv_exp) c
	levelsof rr_scalar, local(rr_scalar) c
	levelsof calc_type, local(dist) c
	levelsof tmred_para1, local(tmred_1) c
	levelsof tmred_para2, local(tmred_2) c
	levelsof risk_type, local(risk_type) c
	levelsof maxrr, local(cap) c

** *************************************************************************************************
** CONTINUOUS CALCULATION
** *************************************************************************************************

	if "`risk_type'" == "2" {

** REGRESS FOR CV ----------------------------------------------------------------------------------
				
		if !inlist("`risk'","metab_sbp","metab_cholesterol") {
			noi di c(current_time) + ": begin regression"
			import delimited using "FILEPATH/`risk'_data.csv", clear
			if "`risk'" == "envir_radon" noi regress log_std_dev log_meas_value
			else noi regress log_std_dev log_meas_value if age_group_id >= 10
			local intercept = _b[_cons]
			matrix b = e(b)
			matrix V = e(V)
			local bL = colsof(b)
			noi di c(current_time) + ": regression complete"
		}

** PULL EXP, RR, and CALC PAFS ---------------------------------------------------------------------

	** PULL EXPOSURE ------------------------------------------------------------------------------

		noi di c(current_time) + ": get exposure draws"
		get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
			sex_ids(`sex_id') age_group_ids(`gbd_ages') kwargs(draw_type:exposure) source(risk) ///
			gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
		cap drop model*

		if inlist("`risk'","metab_sbp","metab_cholesterol") {
			rename draw_* exp_mean_*
			tempfile exp_mean
			save `exp_mean', replace
			if "`risk'" == "metab_sbp" local sdme = 15788
			if "`risk'" == "metab_cholesterol" local sdme = 15789
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`sdme') year_ids(`year_ids') sex_ids(`sex_id') ///
				gbd_round_id(`gbd_round_id') location_ids(`location_id') age_group_ids(`gbd_ages') ///
				source(epi) n_draws(`n_draws') resample("True") clear
			cap drop model_* measure_id
			rename draw_* exp_sd_*
			merge 1:1 location_id year_id age_group_id sex_id using `exp_mean', keep(3) nogen
			** SBP merge SD correction factors
			if "`risk'"=="metab_sbp" {
				merge m:1 age_group_id using "FILEPATH/sbp_correction.dta", keep(3) nogen
				forvalues i = 0/`=`n_draws'-1' {
					qui replace exp_sd_`i' = exp_sd_`i' * ratio
				}
				drop ratio
			}
		}
		else {
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
			local bLshift = `bL' - 1
			rename b0 coeff_
			keep coeff_ b`bLshift'
			gen n=_n
			replace n=n-1
			rename b`bLshift' intercept_
			gen risk = "`risk'"
			reshape wide coeff_ intercept_, i(risk) j(n)

			joinby risk using `E'

			forvalues i = 0/`=`n_draws'-1' {
				gen exp_sd_`i' = exp(intercept_`i' + coeff_`i' * ln(exp_mean))
				rename exp_`i' exp_mean_`i'
				drop coeff_`i' intercept_`i'
			}
			drop exp_mean
		}

		tempfile exp
		save `exp', replace
		noi di c(current_time) + ": exposure draws read"

	** PULL RRS ----------------------------------------------------------------------------------

		noi di c(current_time) + ": get relative risk draws"
		get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') ///
			year_ids(`year_ids') kwargs(draw_type:rr) gbd_round_id(`gbd_round_id') source(risk) ///
			sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
		replace location_id = `location_id'
		noi di c(current_time) + ": relative risk draws read"

	** MAX RR ----------------------------------------------------------------------------------

		gen maxrr=`cap'
		cap drop rei_id
		gen rei_id = `rei_id'
		merge m:1 rei_id using "`out_dir'/`risk'/exposure/exp_max_min.dta", keep(3) nogen
		gen cap = .
		replace cap = min_1_val_mean if cap ==. & `inv_exp'==1
		replace cap = max_99_val_mean if cap ==. & `inv_exp'!=1
		replace cap = maxrr if maxrr < cap & `inv_exp'!=1

		else joinby age_group_id sex_id location_id using `exp'

	** GEN TMRELS ----------------------------------------------------------------------------------

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

	** PAF CALC ----------------------------------------------------------------------------------

		local FILE = "`out_dir'/`risk'/FILE_`location_id'_`sex_id'"
		export delimited using "`FILE'.csv", replace
		if "`dist'" == "ensemble" {
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
		}
		else {
			rsource using "`code_dir'/core/paf_calc_cont.R", rpath("FILEPATH/R") roptions(`" --vanilla --args "`minval'" "`maxval'" "`rr_scalar'" "`dist'" "`FILE'" "`inv_exp'" "') noloutput
		}
		
		noi di c(current_time) + ": PAF calc complete"
		import delimited using "`FILE'_OUT.csv", asdouble varname(1) clear
		erase "`FILE'_OUT.csv"
		erase "`FILE'.csv"

	} // end continuous loop

** *************************************************************************************************
** CATEGORICAL CALCULATION
** *************************************************************************************************

	if "`risk_type'"=="1" {

	** PULL EXPOSURES  ----------------------------------------------------------------------------------	

		noi di c(current_time) + ": get exposure draws"
		if "`risk'"=="smoking_direct_prev" local year_ids 1985 1990 1995 2000 2001 2005 2011
		if "`risk'" == "nutrition_vitamina" {
			// vitamin a exposure
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(16446) location_ids(`location_id') sex_ids(`sex_id') ///
				age_group_ids(`gbd_ages') year_ids(`year_ids') measure_ids(5) source(epi) gbd_round_id(`gbd_round_id') ///
				resample("True") n_draws(`n_draws') clear
			gen parameter="cat1"
			fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id) append flag(dup)
			replace parameter = "cat2" if dup == 1
			forvalues i = 0/`=`n_draws'-1' {
				qui replace draw_`i' = 1 - draw_`i' if dup == 1
			}
			drop dup
		}
		else {
			get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') gbd_round_id(`gbd_round_id') ///
				sex_ids(`sex_id') age_group_ids(`gbd_ages') location_ids(`location_id') source(risk) ///
				kwargs(draw_type:exposure) n_draws(`n_draws') resample("True") clear
		}
		** smoking prevalence is 5 year lagged
		if "`risk'"=="smoking_direct_prev" {
			local year_ids 1990 1995 2000 2005 2006 2010 2016
			qui replace year_id = year_id + 5
			qui replace age_group_id = age_group_id + 1
			qui replace age_group_id = 30 if age_group_id == 21
			qui replace age_group_id = 235 if age_group_id == 33
		}
		rename draw_* exp_*
		keep age_group_id location_id sex_id year_id parameter exp_*
		noi di c(current_time) + ": exposure draws read"
		tempfile exp
		save `exp', replace

		if "`risk'" == "abuse_ipv_exp" {
			// 12 month exposure
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(16526) location_ids(`location_id') sex_ids(`sex_id') ///
				age_group_ids(`gbd_ages') year_ids(`year_ids') measure_ids(5) source(epi) gbd_round_id(`gbd_round_id') ///
				n_draws(`n_draws') resample("True") clear
			gen parameter="cat1"
			fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id) append flag(dup)
			replace parameter = "cat2" if dup == 1
			forvalues i = 0/`=`n_draws'-1' {
				qui replace draw_`i' = 1 - draw_`i' if dup == 1
			}
			drop dup
			rename draw_* abort_exp_*
			keep age_group_id location_id sex_id year_id parameter abort_exp_*
			merge 1:1 age_group_id location_id sex_id year_id parameter using `exp', nogen keep(3)
			save `exp', replace
		}

	** PULL RRs  ----------------------------------------------------------------------------------	

		noi di c(current_time) + ": get relative risk draws"
		if "`risk'"=="occ_carcino_smoke" { // use SHS RRs for occ SHS RRs
			get_draws, gbd_id_field(rei_id) gbd_id(100) location_ids(`location_id') status(best) sex_ids(`sex_id') ///
				year_ids(`year_ids') kwargs(draw_type:rr) gbd_round_id(`gbd_round_id') ///
				source(risk) resample("True") n_draws(`n_draws') clear
			expand 2 if parameter == "cat1", gen(dup)
			replace parameter = "cat3" if parameter == "cat2"
			replace parameter = "cat2" if dup == 1
			drop dup
		}
		else {
			get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') status(best) sex_ids(`sex_id') ///
				year_ids(`year_ids') kwargs(draw_type:rr) gbd_round_id(`gbd_round_id') source(risk) ///
				resample("True") n_draws(`n_draws') clear
		}
		replace location_id = `location_id'
		mortality_expand mortality morbidity 
		noi di c(current_time) + ": relative risk draws read"
		
		joinby age_group_id parameter sex_id location_id  using `exp'

		if regexm("`risk'","vacc") {
			// RR needs to be inverted
			forvalues i = 0/`=`n_draws'-1' {
				qui replace rr_`i' = 1/rr_`i'
			}
			// for vaccines, cat1 represents the proportion covered
			gen n = 1 if parameter == "cat2"
			replace parameter = "cat1" if n == 1
			replace parameter = "cat2" if n == .
			drop n
		}

		** use 12-month IPV exposure model for the IPV-abortion PAF
		if "`risk'"=="abuse_ipv_exp" {
			forvalues i = 0/`=`n_draws'-1' {
				qui replace exp_`i' = abort_exp_`i' if cause_id == 371
			}
			drop abort_*
		}

	** GENERATE TMREL  ----------------------------------------------------------------------------------	

		levelsof parameter, c
		local L : word count `r(levels)'
		forvalues i = 0/`=`n_draws'-1' {
			qui gen tmrel_`i' = 0
			qui replace tmrel_`i' = 1 if parameter=="cat`L'"
		}
		
	** CALC PAFS  ----------------------------------------------------------------------------------	

		noi di c(current_time) + ": calc PAFs"
		gen rei_id = `rei_id'
		calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id mortality morbidity)
		// replace paf with 0 if vaccine not introduced
		if inlist("`rei'","vacc_hib3","vacc_pcv3","vacc_rotac") {
			tempfile vacc
			save `vacc', replace
			noi di c(current_time) + ": get vaccine introduction"
			get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') sex_ids(`sex_id') ///
				year_ids(`year_ids') kwargs(draw_type:tmrel) source(risk) clear
			rename tmrel_0 vacc_intro
			drop modelable_entity_id model_version_id tmrel_*
			replace location_id = `location_id'
			merge 1:m age_group_id sex_id location_id year_id using `vacc', keep(3) nogen
			forvalues i = 0/`=`n_draws'-1' {
				qui replace paf_`i' = 0 if vacc_intro == 0
			}
		}

	} // end categorical loop

** *************************************************************************************************
** SAVE PAFS
** *************************************************************************************************	

	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0 & !inlist(rei_id,165,166)
	}
	noi di c(current_time) + ": PAF calc complete"

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

	** smoking injuries adjustment
	if "`risk'"=="smoking_direct_prev" {
		noi di c(current_time) + ": begin smoking injuries comp"
		local year_ids : subinstr local 5 " " "_", all
		noi do "`code_dir'/core/custom_calc/custom_calc_smoking_injuries.do" `risk' `rei_id' `location_id' `sex_id' "`year_ids'" `gbd_round_id' `n_draws' "`code_dir'" "`out_dir'"
		noi di c(current_time) + ": smoking injury calc done"
	}
	noi di c(current_time) + ": DONE!"

// END
