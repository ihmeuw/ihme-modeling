// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate occ hearing PAFs by applying PAF of sequela level results and calculating 
//				YLD burden for cause level hearing loss
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
	run "`code_dir'/core/paf_calc_categ.do"

  	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** PAF CALC
** *************************************************************************************************

** PULL RRS  ----------------------------------------------------------------------------------

	insheet using "FILEPATH/rr.csv", clear	
	keep age_group_id location_id rei_id sex_id mortality morbidity parameter sequela_id rr_*
	tempfile rr
	save `rr', replace

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get exposure draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
		age_group_ids(`gbd_ages') sex_ids(`sex_id') kwargs(draw_type:exposure num_workers:10) source(risk) ///
		gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
	rename draw_* exp_*
	cap drop modelable_entity_id
	joinby age_group_id parameter sex_id location_id using `rr'

** GENERATE TMREL ----------------------------------------------------------------------------------

	levelsof parameter, c
	local L : word count `r(levels)'
	forvalues i = 0/`=`n_draws'-1' {
		qui gen tmrel_`i' = 0
		qui replace tmrel_`i' = 1 if parameter=="cat`L'"
	}

** PAF CALC ----------------------------------------------------------------------------------

	noi di c(current_time) + ": calc PAFs"
	cap drop rei_id
	gen rei_id = `rei_id'
	calc_paf_categ exp_ rr_ tmrel_ paf_, by(age_group_id rei_id location_id sex_id year_id sequela_id)
	noi di c(current_time) + ": PAF calc complete"

	keep age_group_id rei_id location_id sex_id year_id sequela_id paf*
	tempfile paf
	save `paf', replace

** SEQUELA TO CAUSE STUFF --------------------------------------------------------------------------

	noi di c(current_time) + ": read sequela"
	keep sequela_id
	duplicates drop
	levelsof sequela_id, local(all) c
	gen n = _n
	local count = _N
	tempfile s_list
	save `s_list', replace

	** read in seqeula for each row
	local y = 5000
	forvalues iii = 1/`count' {
		use if n==`iii' using `s_list', clear
		levelsof sequela_id, local(ss) c
		local x = 0
		foreach s of local ss {
			get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`s') age_group_ids(`gbd_ages') ///
				gbd_id_field(sequela_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) ///
				sex_ids(`sex_id') kwargs(num_workers:10) n_draws(`n_draws') status(203) resample("True") clear 
			local x = `x' + 1
			tempfile `x'
			save ``x'', replace
		}
		clear
		forvalues n = 1/`x' {
			append using ``n''
		}
		renpfix draw_ yld_
		fastcollapse yld_*, type(sum) by(age_group_id location_id sex_id year_id)
		gen sequela_id = "`ss'"
		noi di "appended row `iii' sequela: `ss'"
		merge 1:m age_group_id sex_id location_id year_id sequela_id using `paf', keep(3) nogen
		forvalues i = 0/`=`n_draws'-1' {
			qui replace yld_`i' = yld_`i' * paf_`i'
		}
		keep age_group_id rei_id location_id sex_id year_id sequela_id yld*
		local y = `y' + 1
		tempfile `y'
		save ``y'', replace
	}
	clear
	forvalues w = 5001/`y' {
		append using ``w''
	}
	fastcollapse yld*, type(sum) by(age_group_id rei_id location_id sex_id year_id)
	gen cause_id = 674 // hearing loss
	tempfile ab
	save `ab', replace

	get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(674) age_group_ids(`gbd_ages') ///
		gbd_id_field(cause_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) ///
		sex_ids(`sex_id') kwargs(num_workers:10) n_draws(`n_draws') status(203) resample("True") clear
	rename draw_* yld_*
	append using `ab'

	gen denominator = .
	replace denominator = (rei_id == .)
	fastfraction yld*, by(year_id sex_id cause_id age_group_id) denominator(denominator) prefix(paf_) 
	keep if rei_id==`rei_id'
	rename paf_yld_* paf_*
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}
	
	keep rei_id year_id sex_id cause_id age_group_id paf*
	gen location_id = `location_id'
	gen modelable_entity_id=.
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id 
	noi di c(current_time) + ": saving PAFs"
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv"
// END
