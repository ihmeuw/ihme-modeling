// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate drug use - suicide PAFs, RRs from GBD 2010
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
	run "`code_dir'/core/paf_calc_categ.do"
	adopath + "`code_dir'/helpers"

    get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
**  INVIDUAL PAF CALC
** *************************************************************************************************

	** coacaine, amphetamine, opioid
	local n = 0
	foreach me in 16437 16438 16439 { 

** PULL EXPOSURES  ----------------------------------------------------------------------------------

		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me') location_ids(`location_id') ///
			year_ids(`year_ids') measure_ids(5) age_group_ids(`gbd_ages') sex_ids(`sex_id') source(epi) ///
			gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
		gen parameter="cat1"
		expand 2, gen(dup)
		replace parameter = "cat2" if dup == 1
		forvalues i = 0/`=`n_draws'-1' {
			qui replace draw_`i' = 1 - draw_`i' if dup == 1
		}
		rename draw_* exp_*
		levelsof age_group_id, local(age) sep(,)
		tempfile exp
		save `exp', replace

** PULL RRS  ----------------------------------------------------------------------------------

		clear
		set obs 1

		if `me' == 16438 {
			gen modelable_entity_id = `me'
			gen sd = ((ln(16.94)) - (ln(3.93))) / (2*invnormal(.975))
			forvalues i = 0/`=`n_draws'-1' {
				gen double rr_`i' = exp(rnormal(ln(8.16), sd))
			}
		}
		if `me' == 16439 {
			gen modelable_entity_id = `me'
			gen sd = ((ln(16.94)) - (ln(3.93))) / (2*invnormal(.975))
			forvalues i = 0/`=`n_draws'-1' {
				gen double rr_`i' = exp(rnormal(ln(8.16), sd))
			}
		}
		if `me' == 16437 {
			gen modelable_entity_id = `me'
			gen sd = ((ln(10.53)) - (ln(4.49))) / (2*invnormal(.975))
			forvalues i = 0/`=`n_draws'-1' {
				gen double rr_`i' = exp(rnormal(ln(6.85), sd))
			}
		}

		gen cause_id = 721
		expand 2, gen(dup)
		replace cause_id = 723 if dup == 1
		drop dup
		gen parameter = "cat1"
		gen n = 1
		tempfile r
		save `r', replace

		clear
		set obs 235
		gen age_group_id = _n
		keep if inlist(age_group_id,`age')
		gen n = 1
		joinby n using `r'
		keep cause_id age_group_id modelable_entity_id parameter rr*

		expand 2, gen(dup)
		forvalues i = 0/`=`n_draws'-1' {
			replace rr_`i' = 1 if dup==1
		}
		replace parameter="cat2" if dup==1 
		drop dup
		gen mortality=1
		gen morbidity=1
		gen sex_id = `sex_id'
		tempfile rr
		save `rr', replace

		joinby age_group_id modelable_entity_id parameter sex_id using `exp'

** GENERATE TMREL ----------------------------------------------------------------------------------
		
		bysort age_group_id year_id sex_id cause_id mortality morbidity: gen level = _N
		levelsof level, local(tmrel_param) c
		drop level
		forvalues i = 0/`=`n_draws'-1' {
			qui gen tmrel_`i' = 0
			replace tmrel_`i' = 1 if parameter=="cat`tmrel_param'"
		}

** PAF CALC ----------------------------------------------------------------------------------

		cap drop rei_id
		gen rei_id = `rei_id'
		calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id cause_id modelable_entity_id mortality morbidity)
		local n = `n' + 1
		tempfile `n'
		save ``n'', replace

	} // end me loop

** *************************************************************************************************
** APPEND PAFS AND CALCULATE JOINT FOR ALL DRUG USE
** *************************************************************************************************

	clear
	forvalues i = 1/`n' {
		qui append using ``i''
	}

	foreach var of varlist paf* {
		qui replace `var' = log(1 - `var')
	}
	fastcollapse paf*, type(sum) by(age_group_id location_id sex_id year_id cause_id rei_id)
	** Exponentiate and complement.
	foreach var of varlist paf* {
		qui replace `var' = 1 - exp(`var')
		qui replace `var' = 1 if `var' == . 
	}

	** apply cap
	local logit_mean = logit(.84488)
	local logit_sd = (logit(.896145) - logit(.785724)) / (2*invnormal(.975))
	forvalues i = 0/`=`n_draws'-1' {
		qui gen paf_tot_cap_`i' = invlogit(rnormal(`logit_mean',`logit_sd')) if _n==1
		qui replace paf_tot_cap_`i' = paf_tot_cap_`i'[1]
		qui gen paf_tot_temp_`i' = min(paf_`i',paf_tot_cap_`i')
		qui replace paf_`i' = paf_`i' * (paf_tot_temp_`i' / paf_`i') if paf_tot_temp_`i'!=0
	}
	drop paf_tot_cap* paf_tot_temp*

	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}

	** expand mortality and morbidity
	gen mortality=1
	gen morbidity=1
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