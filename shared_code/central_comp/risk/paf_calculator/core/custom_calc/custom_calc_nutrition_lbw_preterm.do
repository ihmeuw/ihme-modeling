// *************************************************************************************************
// *************************************************************************************************
// Purpose:		preterm/lbw paf calculation
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

** *************************************************************************************************
** CATEGORICAL CALCULATION
** *************************************************************************************************

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get exposure draws"
    get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
    	age_group_ids(`gbd_ages') sex_ids(`sex_id') kwargs(draw_type:exposure num_workers:10) ///
    	source(risk) gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
	rename draw_* exp_*
	tempfile exp
	save `exp', replace
	noi di c(current_time) + ": exposure draws read"

** ADD TMREL CATEGORY ----------------------------------------------------------------------------------	

	import delimited using "FILEPATH/nutrition_lbw_preterm_tmrel.csv", clear
	keep if sex_id == `sex_id'
	gen preterm = .
	replace preterm = 1 if regexm(modelable_entity_name,"\[0, 24\) wks")
	replace preterm = 2 if regexm(modelable_entity_name,"\[24, 26\) wks")
	replace preterm = 3 if regexm(modelable_entity_name,"\[26, 28\) wks")
	replace preterm = 4 if regexm(modelable_entity_name,"\[28, 30\) wks")
	replace preterm = 5 if regexm(modelable_entity_name,"\[30, 32\) wks")
	replace preterm = 6 if regexm(modelable_entity_name,"\[32, 34\) wks")
	replace preterm = 7 if regexm(modelable_entity_name,"\[34, 36\) wks")
	replace preterm = 8 if regexm(modelable_entity_name,"\[36, 37\) wks")
	replace preterm = 9 if regexm(modelable_entity_name,"\[37, 38\) wks")
	replace preterm = 10 if regexm(modelable_entity_name,"\[38, 40\) wks")
	replace preterm = 11 if regexm(modelable_entity_name,"\[40, 42\) wks")
	gen lbw = .
	replace lbw = 1 if regexm(modelable_entity_name,"\[0, 500\) g")
	replace lbw = 2 if regexm(modelable_entity_name,"\[500, 1000\) g")
	replace lbw = 3 if regexm(modelable_entity_name,"\[1000, 1500\) g")
	replace lbw = 4 if regexm(modelable_entity_name,"\[1500, 2000\) g")
	replace lbw = 5 if regexm(modelable_entity_name,"\[2000, 2500\) g")
	replace lbw = 6 if regexm(modelable_entity_name,"\[2500, 3000\) g")
	replace lbw = 7 if regexm(modelable_entity_name,"\[3000, 3500\) g")
	replace lbw = 8 if regexm(modelable_entity_name,"\[3500, 4000\) g")
	replace lbw = 9 if regexm(modelable_entity_name,"\[4000, 4500\) g")

	merge 1:m modelable_entity_id parameter sex_id using `exp', keep(3) assert(3) nogen
	drop model*_id measure_id old_id
	save `exp', replace

** PULL RRs  ----------------------------------------------------------------------------------	

	noi di c(current_time) + ": get relative risk draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') location_ids(`location_id') year_ids(`year_ids') ///
		status(best) kwargs(draw_type:rr num_workers:10) gbd_round_id(`gbd_round_id') source(risk) ///
		sex_ids(`sex_id') n_draws(`n_draws') resample("True") clear
	drop year_id 
	duplicates drop
	expand 2 if parameter == "cat55", gen(dup)
	replace parameter = "cat56" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		qui replace rr_`i' = 1 if dup == 1
	}
	drop dup
	replace location_id = `location_id'
	noi di c(current_time) + ": relative risk draws read"
	tempfile rr
	save `rr', replace

** *************************************************************************************************
** PRETERM AND LBW INDIVIDUAL PAFS
** *************************************************************************************************

	foreach child in preterm lbw {	

		if "`child'" == "preterm" {
			local max 11
			local child_id 334
		} 
		if "`child'" == "lbw" {
			local max 9
			local child_id 335
		} 

		use `exp', clear
		noi di c(current_time) + ": calc `child' PAFs"
		forvalues k = 1/`max' {
			preserve
				keep if `child' == `k'

				** SCALE RR such that TMREL is 1 ---------------------------------
				joinby age_group_id parameter sex_id using `rr'
				forvalues i = 0/`=`n_draws'-1' {
					sort location_id year_id age_group_id sex_id cause_id tmrel_`child'
					bysort location_id year_id age_group_id sex_id cause_id : gen scalar = rr_`i'[1]
					qui replace rr_`i' = rr_`i' / scalar
					qui replace rr_`i' = 1 if tmrel_`child' == 1
					drop scalar
				}

				tempfile `k'
				save ``k'', replace
			restore	
		}
		clear
		forvalues i = 1/`max' {
			append using ``i''
		}

		** CALC PAFS  --------------------------------------------------------------------------
		forvalues i = 0/`=`n_draws'-1' {
			qui gen paf_`i' = (exp_`i'*(rr_`i'-1))/((exp_`i'*(rr_`i'-1)+1))
		}
		noi di c(current_time) + ": collapse `child' PAFs by row/column (multiplicative sum)"

		forvalues i = 0/`=`n_draws'-1' {
			bysort age_group_id location_id sex_id year_id cause_id mortality morbidity: egen scalar = total(paf_`i')
			qui replace paf_`i' = paf_`i' / scalar if scalar > 1
			drop scalar
		}
		foreach var of varlist paf_* {
			qui replace `var' = log(1 - `var')
		}
		fastcollapse paf*, type(sum) by(age_group_id location_id sex_id year_id cause_id mortality morbidity)
		foreach var of varlist paf_* {
			qui replace `var' = 1 - exp(`var')
			qui replace `var' = 1 if `var' == . 
		}

		forvalues i = 0/`=`n_draws'-1' {
			qui replace paf_`i' = 0 if paf_`i' == .
			qui replace paf_`i' = 1 if paf_`i' > 1
			qui replace paf_`i' = 0 if paf_`i' < 0
		}
		noi di c(current_time) + ": `child' PAF calc complete"

		** expand mortality and morbidity
		mortality_expand mortality morbidity 
		levelsof mortality, local(morts)

		keep cause_id location_id year_id sex_id age_group_id mortality morbidity paf_*
		rename paf_* `child'_paf_*
		tempfile `child'
		save ``child'', replace
	}

** *************************************************************************************************
** JOINT PRETERM/LOW BIRTH WEIGHT
** *************************************************************************************************
	
	use `exp', clear
	noi di c(current_time) + ": calc pretrem/lbw PAFs"

	** RESCALE EXPOSURES to sum to 1 ---------------------------------------------------

	drop if tmrel_lbw_preterm == 1
	forvalues i = 0/`=`n_draws'-1' {
		bysort location_id year_id age_group_id sex_id: egen scalar = total(exp_`i')
		qui replace exp_`i' = exp_`i' / scalar if scalar > 1
		drop scalar
	}
	fastcollapse exp*, type(sum) by(location_id year_id age_group_id sex_id) append flag(dup)
	replace parameter = "cat56" if dup == 1
	forvalues i = 0/`=`n_draws'-1' {
		qui replace exp_`i' = 1 - exp_`i' if dup == 1
	}
	drop dup

	** GEN TMREL AND RR --------------------------------------------------------------------

	joinby age_group_id parameter sex_id using `rr'
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		qui replace tmrel_`i' = 1 if parameter=="cat56"
		qui replace rr_`i' = 1 if parameter=="cat56"
	}

	** CALC PAFS  --------------------------------------------------------------------------
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

** *************************************************************************************************
** RESCALE CHILD PAF TO ADULT PAF
** *************************************************************************************************
	
	merge 1:1 cause_id location_id year_id sex_id age_group_id mortality morbidity using `lbw', keep(3) nogen
	merge 1:1 cause_id location_id year_id sex_id age_group_id mortality morbidity using `preterm', keep(3) nogen

	forvalues i = 0/`=`n_draws'-1' {
	  qui gen sum = lbw_paf_`i' + preterm_paf_`i'
	  qui replace lbw_paf_`i' = (lbw_paf_`i'/sum)*paf_`i'
	  qui replace preterm_paf_`i' = (preterm_paf_`i'/sum)*paf_`i'
	  qui drop sum
	}

	foreach child in preterm lbw {	
		preserve
			if "`child'" == "preterm" local child_id 334
			if "`child'" == "lbw" local child_id 335
			keep cause_id location_id year_id sex_id age_group_id mortality morbidity `child'_paf_*
			rename `child'_paf_* paf_*
			gen rei_id = `child_id'
			gen modelable_entity_id = .
			order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id mortality morbidity
			noi di c(current_time) + ": saving `child' PAFs"
			cap mkdir "`out_dir'/nutrition_`child'"
			foreach mmm of local morts {
				if `mmm' == 1 local mmm_string = "yll"
				else local mmm_string = "yld"
				outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf* using ///
					 "`out_dir'/nutrition_`child'/paf_`mmm_string'_`location_id'_`sex_id'.csv" ///
					  if mortality == `mmm', comma replace
				no di "saved: `out_dir'/nutrition_`child'/paf_`mmm_string'_`location_id'_`sex_id'.csv"
			}
		restore
	}

//END