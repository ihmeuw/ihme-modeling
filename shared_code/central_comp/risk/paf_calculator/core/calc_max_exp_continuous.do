// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calc global max/min value to use as cap before calculating PAFs
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

	// get argument from qsub
	local risk = "`1'"
	local rei_id = "`2'"
	local gbd_round_id "`3'"
	local code_dir = "`4'"
	local out_dir = "`5'"

	// load functions
	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"

	create_connection_string, server(modeling-epi-db)
    local epi_string = r(conn_string)

	// pull vars from risk_variables spreadsheet
	import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
	keep if risk=="`risk'"
	levelsof maxval, local(maxval) c
	levelsof minval, local(minval) c
	levelsof inv_exp, local(inv_exp) c
	levelsof rr_scalar, local(rr_scalar) c
	levelsof calc_type, local(dist) c
	levelsof tmred_para1, local(tmred_1) c
	levelsof tmred_para2, local(tmred_2) c
	levelsof maxrr, local(cap) c

	// pull populations
	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'
	local gbd_locs `r(location_ids)'
	local year_ids `r(year_ids)'
    get_population, year_id("`year_ids'") location_id("`r(location_ids)'") sex_id("`r(sex_ids)'") ///
    	age_group_id("`gbd_ages'") gbd_round_id("`gbd_round_id'")
	tempfile pops 
	save `pops', replace

** *************************************************************************************************
** REGRESS FOR CV
** *************************************************************************************************

	if inlist("`risk'","metab_bmd","envir_radon","envir_lead_bone") {

		noi di c(current_time) + ": begin regression"
		if ("`risk'"=="metab_bmd") {
			risk_info, risk(`risk') draw_type("exposure") gbd_round_id(`gbd_round_id') clear
			levelsof best_model_version, local(model_version_id) c
			pull_dismod, model_version_id(`model_version_id') clear
			noi regress log_std_dev log_meas_value if age_lower >= 25
		}
		else {
			import delimited using "FILEPATH/`risk'_data.csv", clear
			if "`risk'" == "envir_radon" noi regress log_std_dev log_meas_value
			else noi regress log_std_dev log_meas_value if age_group_id >= 10
		}
		local intercept = _b[_cons]
		matrix b = e(b)
		matrix V = e(V)
		local bL = colsof(b)
		noi di c(current_time) + ": regression complete"
		
	}

** *************************************************************************************************
** READ EXPOSURES
** *************************************************************************************************

	if inlist("`risk'","metab_bmd","envir_radon","envir_lead_bone","metab_sbp","metab_cholesterol") {

		noi di c(current_time) + ": begin get draws"
		get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') kwargs(draw_type:exposure) ///
			sex_ids(1 2) gbd_round_id(`gbd_round_id') source(risk) age_group_ids(`gbd_ages') clear
		cap drop modelable_entity_id
		noi di c(current_time) + ": exposure draws read"

		if inlist("`risk'","metab_bmd","envir_radon","envir_lead_bone") {
			rename draw_* exp_*
			fastrowmean exp*, mean_var_name(exp_mean)
			gen risk = "`risk'"
			tempfile E
			save `E', replace

			drawnorm b1-b`bL', double n(1000) means(b) cov(V) clear
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

			forvalues i = 0/999 {
				gen exp_sd_`i' = exp(intercept_`i' + coeff_`i' * ln(exp_mean))
				rename exp_`i' exp_mean_`i'
				drop coeff_`i' intercept_`i'
			}
		}
		else if inlist("`risk'","metab_sbp","metab_cholesterol") {
			cap drop model*
			rename draw_* exp_mean_*
			tempfile exp_mean
			save `exp_mean', replace
			if "`risk'" == "metab_sbp" local sdme = 15788
			if "`risk'" == "metab_cholesterol" local sdme = 15789
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`sdme') sex_ids(1 2)  ///
				source(epi) age_group_ids(`gbd_ages') gbd_round_id(`gbd_round_id') clear
			cap drop model_* measure_id
			rename draw_* exp_sd_*
			merge m:1 location_id year_id age_group_id sex_id using `exp_mean', keep(3) nogen
		}

		** SBP merge SD correction factors
		if "`risk'"=="metab_sbp" {
			merge m:1 age_group_id using "FILEPATH/sbp_correction.dta", keep(3) nogen
			forvalues i = 0/999 {
				qui replace exp_sd_`i' = exp_sd_`i' * ratio
			}
			drop ratio
		}

	} 
	else {

		clear
		tempfile exp
		save `exp', emptyok
		foreach lid of local gbd_locs {
			import delimited using "FILEPATH/`lid'.csv", clear
			keep location_id year_id age_group_id sex_id draw mean sd
			duplicates drop
			append using `exp'
			save `exp', replace
		}
		reshape wide mean sd, i(location_id year_id sex_id age_group_id) j(draw)
		rename (mean* sd*) (exp_mean_* exp_sd_*)

	}

	noi di c(current_time) + ": read exposure"

** *************************************************************************************************
** SAVE IT OUT!! 
** *************************************************************************************************

	merge m:1 location_id year_id age_group_id sex_id using `pops', keep(3) nogen keepusing(population)
	replace population = round(population,1)

	cap drop exp_mean
	fastrowmean exp_mean_*, mean_var_name(exp_mean)	
	fastrowmean exp_sd_*, mean_var_name(exp_sd)	
	keep location_id year_id age_group_id sex_id population exp_mean exp_sd

	local FILE = "`out_dir'/`risk'/exposure/FILE"
	outsheet using "`FILE'.csv", comma replace
	noi di c(current_time) + ": begin generating sample"
	noi rsource using "`code_dir'/core/cal_max_exp.R", rpath("FILEPATH/R") roptions(`" --vanilla --args "`dist'" "`FILE'""')
	noi di c(current_time) + ": sample generate and percentiles calculated"
	import delimited using "`FILE'_OUT.csv", asdouble varname(1) clear
	** clean up the tempfiles
	erase "`FILE'_OUT.csv"
	erase "`FILE'.csv"

	gen risk="`risk'"
	gen rei_id = `rei_id'
	keep risk rei_id min_1_val_mean max_99_val_mean
	save "`out_dir'/`risk'/exposure/exp_max_min.dta", replace
