// *************************************************************************************************
// Purpose:		Compute IKF from CKD DisMoD models
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

	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"
	run "`code_dir'/core/paf_calc_categ.do"

  	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** DATA
** *************************************************************************************************

** PULL CKD  EXPOSURE ----------------------------------------------------------------------------------

	** Pull stage 5 model
	noi di c(current_time) + ": read in Stage V"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16445) location_ids(`location_id') year_ids(`year_ids') ///
		measure_ids(5) source(epi) gbd_round_id(`gbd_round_id') age_group_ids(`gbd_ages') sex_ids(`sex_id') ///
		n_draws(`n_draws') resample("True") clear
	gen parameter = "cat1" 
	tempfile V
	save `V', replace

	** Pull stage 4 model
	noi di c(current_time) + ": read in Stage IV"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16444) location_ids(`location_id') year_ids(`year_ids') ///
		measure_ids(5) source(epi) gbd_round_id(`gbd_round_id') age_group_ids(`gbd_ages') sex_ids(`sex_id') ///
		n_draws(`n_draws') resample("True") clear
	gen parameter = "cat2"
	tempfile IV
	save `IV', replace

	** Pull stage 3 model
	noi di c(current_time) + ": read in Stage III"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16443) location_ids(`location_id') year_ids(`year_ids') ///
		measure_ids(5) source(epi) gbd_round_id(`gbd_round_id') age_group_ids(`gbd_ages') sex_ids(`sex_id') ///
		n_draws(`n_draws') resample("True") clear
	gen parameter = "cat3"
	tempfile III
	save `III', replace

	** Pull Albuminuria
	noi di c(current_time) + ": read in Albuminuria"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(16442) location_ids(`location_id') year_ids(`year_ids') ///
		measure_ids(18) source(epi) gbd_round_id(`gbd_round_id') age_group_ids(`gbd_ages') sex_ids(`sex_id') ///
		n_draws(`n_draws') resample("True") clear
	gen parameter = "cat4"

	append using `III'
	append using `IV'
	append using `V'
	gen rei_id = `rei_id'

	** squeeze if sum>1 and create counterfactual category
	forvalues i = 0/`=`n_draws'-1' {
		bysort location_id year_id age_group_id sex_id rei_id: egen scalar = total(draw_`i')
		qui replace draw_`i' = draw_`i' / scalar if scalar > 1
		drop scalar
	}
	bysort location_id age_group_id year_id sex_id rei_id: gen level = _N
	levelsof level, local(ref_cat) c
	local ref_cat = `ref_cat' + 1
	drop level

	fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id rei_id) append flag(dup)
	replace parameter = "cat`ref_cat'" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		replace draw_`i' = 1 - draw_`i' if dup == 1
	}
	drop dup
	rename draw_* exp_*
	drop model* measure*
	tempfile exp
	save `exp', replace

** PULL RRS ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get relative risk draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') year_ids(`year_ids') ///		
		status(best) kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') source(risk) ///
		sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	replace location_id = `location_id'
	noi di c(current_time) + ": relative risk draws read"

	joinby age_group_id parameter sex_id location_id using `exp'

** GEN TMRELS ----------------------------------------------------------------------------------

	bysort age_group_id year_id sex_id cause_id mortality morbidity: gen level = _N
	levelsof level, local(tmrel_param) c
	drop level
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		replace tmrel_`i' = 1 if parameter=="cat`tmrel_param'"
	}

** PAF CALC ----------------------------------------------------------------------------------

	noi di c(current_time) + ": calc PAFs"
	cap drop rei_id
	gen rei_id = `rei_id'
	calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mortality morbidity)
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}
	noi di c(current_time) + ": PAF calc complete"

	** expand mortality and morbidity
	mortality_expand mortality morbidity 
	levelsof mortality, local(morts)

	** save PAFs
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