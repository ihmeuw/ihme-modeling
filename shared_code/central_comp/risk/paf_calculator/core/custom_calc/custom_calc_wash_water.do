// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate unsafe water PAFs
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

** *************************************************************************************************
** PAF CALC
** *************************************************************************************************

** PULL RRS  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get relative risk draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') year_ids(`year_ids') ///		
		status(best) kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') source(risk) ///
		sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	replace location_id = `location_id'
	noi di c(current_time) + ": relative risk draws read"
	tempfile rr
	save `rr', replace

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	risk_info, risk(`risk') draw_type(exposure) gbd_round_id(`gbd_round_id') clear
	keep me_id parameter
	rename (me_id) (modelable_entity_id)
	levelsof modelable_entity_id, local(MEs) c
	tempfile M
	save `M', replace

	noi di c(current_time) + ": pull exposures"
	local x = 0
	foreach me of local MEs {
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me') location_ids(`location_id') year_ids(`year_ids') ///
			source(epi) gbd_round_id(`gbd_round_id') age_group_ids(`gbd_ages') sex_ids(`sex_id') ///
			n_draws(`n_draws') resample("True") status(latest) clear
		local x = `x' + 1
		tempfile `x'
		save ``x'', replace	
	}
	clear
	forvalues i = 1/`x' {
		append using ``i''
	}
	merge m:1 modelable_entity_id using `M', keep(3) nogen
	noi di c(current_time) + ": exposure draws read"
	drop model*

** SQUEEZE EXPOSURES  ----------------------------------------------------------------------------------

	** merge on RRs to get categories first then squeeze exposure
	joinby age_group_id parameter sex_id location_id using `rr'
	drop cause_id rr* mortality morbidity
	duplicates drop
	levelsof parameter, c
	local L : word count `r(levels)'
	drop if parameter=="cat`L'"
	
	forvalues i = 0/`=`n_draws'-1' {
		bysort location_id year_id age_group_id sex_id: egen scalar = total(draw_`i')
		qui replace draw_`i' = draw_`i' / scalar if scalar > 1
		drop scalar
	}

	bysort location_id age_group_id year_id sex_id: gen level = _N
	levelsof level, local(ref_cat) c
	local ref_cat = `ref_cat' + 1
	drop level

	fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id) append flag(dup)
	replace parameter = "cat`ref_cat'" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		qui replace draw_`i' = 1 - draw_`i' if dup == 1
	}
	drop dup

** GEN TMREL  ----------------------------------------------------------------------------------

	levelsof parameter, c
	local L : word count `r(levels)'
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		qui replace tmrel_`i' = 1 if parameter=="cat`L'"
	}
	rename draw_* exp_*
	tempfile exp
	save `exp', replace

** CALC PAFS  ----------------------------------------------------------------------------------

	** merge on RRs
	joinby age_group_id parameter sex_id location_id using `rr'

	noi di c(current_time) + ": calc PAFs"
	calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id location_id sex_id year_id cause_id mortality morbidity)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
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
		outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf* using ///
			 "`out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv" ///
			  if mortality == `mmm', comma replace
		no di "saved: `out_dir'/`risk'/paf_`mmm_string'_`location_id'_`sex_id'.csv"
	}

// END
