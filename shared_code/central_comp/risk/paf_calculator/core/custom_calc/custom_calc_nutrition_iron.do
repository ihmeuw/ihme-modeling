// *************************************************************************************************
// Purpose:		Calculate iron deficiency PAFs using anemia outputs
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

	local exp_dir = "FILEPATH"
	local tmrel_dir = "FILEPATH"

	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"

	// pull vars
	import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
	keep if risk=="`risk'"
	levelsof maxval, local(maxval) c
	levelsof minval, local(minval) c
	levelsof inv_exp, local(inv_exp) c
	levelsof rr_scalar, local(rr_scalar) c
	levelsof calc_type, local(dist) c

	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** DATA
** *************************************************************************************************

** PULL RRS ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get relative risk draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') year_ids(`year_ids') ///		
		kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') ///
		source(risk) sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	replace location_id = `location_id'
	noi di c(current_time) + ": relative risk draws read"
	tempfile rr
	save `rr', replace

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": read exp"
	insheet using "`exp_dir'/exp_`location_id'.csv", clear
	replace location_id = `location_id'
	keep if sex_id == `sex_id'
	keep location_id year_id age_group_id sex_id parameter exp*
	reshape wide exp_*, i(location_id year_id age_group_id sex_id) j(parameter) string
	rename (exp_*mean exp_*sd) (exp_mean_* exp_sd_*)
	local y : subinstr local year_ids " " ",", all
	keep if inlist(year_id,`y')
	tempfile exp
	save `exp', replace

** PULL TMRELS ----------------------------------------------------------------------------------
	
	noi di c(current_time) + ": read TMREL"
	insheet using "`tmrel_dir'/tmrel_`location_id'.csv", clear
	keep if parameter == "mean"
	replace location_id = `location_id'
	keep if sex_id == `sex_id'
	keep location_id year_id age_group_id sex_id tmrel*
	local y : subinstr local year_ids " " ",", all
	keep if inlist(year_id,`y')
	rename tmrel_* tmred_mean_*
	merge 1:1 location_id year_id age_group_id sex_id using `exp', keep(3) nogen
	joinby location_id age_group_id sex_id using `rr'

** PAF CALC ----------------------------------------------------------------------------------

	local FILE = "`out_dir'/`risk'/FILE_`location_id'_`sex_id'"
	outsheet using "`FILE'.csv", comma replace
	noi di c(current_time) + ": begin PAF calc"
	rsource using "`code_dir'/core/paf_calc_cont_nocap.R", rpath("FILEPATH/R") roptions(`" --vanilla --args "`minval'" "`maxval'" "`rr_scalar'" "`dist'" "`FILE'" "`inv_exp'" "') noloutput
	noi di c(current_time) + ": PAF calc complete"

	import delimited using "`FILE'_OUT.csv", asdouble varname(1) clear		
	** clean up the tempfiles
	erase "`FILE'_OUT.csv"
	erase "`FILE'.csv"
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}

	** expand mortliaty and morbidity
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

// END