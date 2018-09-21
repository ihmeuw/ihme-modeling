// *************************************************************************************************
// Purpose:		Calculate BMD PAFs
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

	create_connection_string, server(ADDRESS)
    local epi_string = r(conn_string)

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
** PAF CALC
** *************************************************************************************************

** REGRESS CV  ----------------------------------------------------------------------------------

	risk_info, risk(`risk') draw_type("exposure") gbd_round_id(`gbd_round_id') clear
	levelsof best_model_version, local(model_version_id) c
	pull_dismod, model_version_id(`model_version_id') clear

	noi regress log_std_dev log_meas_value if age_lower >= 25
	local intercept = _b[_cons]
	matrix b = e(b)
	matrix V = e(V)
	local bL = colsof(b)

** PULL EXPOSURES  ----------------------------------------------------------------------------------

	** get exposure draws
	noi di c(current_time) + ": get exposure draws"
	get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
		sex_ids(`sex_id') age_group_ids(`gbd_ages') kwargs(draw_type:exposure num_workers:10) ///
		source(risk) gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
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
		qui gen exp_sd_`i' = exp(intercept_`i' + coeff_`i' * ln(exp_mean))
		rename exp_`i' exp_mean_`i'
	}
	drop exp_mean coeff_* intercept_*

	tempfile exp
	save `exp', replace

** PULL RRS ----------------------------------------------------------------------------------

	noi di c(current_time) + ": get relative risk draws"
	insheet using "FILEPATH/rr.csv", comma double clear
	drop risk year_id
	joinby age_group_id sex_id using `exp'
	tempfile rr
	save `rr', replace

** PULL TMRELS ----------------------------------------------------------------------------------

	insheet using "FILEPATH/tmred.csv", comma double clear
	drop if parameter=="sd"
	drop parameter risk year_id
	merge 1:m age_group_id sex_id using `rr', keep(3) nogen
	rename tmred_* tmred_mean_*

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
	export delimited using "`FILE'.csv", replace

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

	keep acause location_id year_id sex_id age_group_id mortality morbidity paf_*
	gen rei_id = `rei_id'
	gen modelable_entity_id = .
	order modelable_entity_id rei_id acause location_id year_id sex_id age_group_id mortality morbidity paf_*
	save "`out_dir'/`risk'/paf_`location_id'_`sex_id'.dta", replace
	tempfile P
	save `P', replace

** YLD PAFS ---------------------------------------------------------------------------------
	
	import excel using "FILEPATH/Matrix.xlsx", firstrow clear sheet("Matrix for YLD")
		keep if inlist(acause,"inj_trans_road_pedest","inj_trans_road_pedal","inj_trans_road_2wheel", ///
		"inj_trans_road_4wheel","inj_trans_road_other","inj_trans_other","inj_falls") | ///
		inlist(acause,"inj_mech_other","inj_animal_nonven","inj_homicide_other","inj_disaster")
	reshape long N, i(acause) j(healthstate) string
	rename (acause N) (inj acause)
	replace healthstate = "N" + healthstate
	replace acause="hip" if acause=="Hip PAF"
	replace acause="non-hip" if acause=="non-hip PAF"
	gen morbidity=1
	tempfile yld
	save `yld', replace
	levelsof inj, local(acause) sep("','") c // "
	odbc load, exec("SELECT cause_id, acause as inj FROM shared.cause WHERE acause IN ('`acause'')") `epi_string' clear
	tempfile causes
	save `causes', replace
	joinby inj using `yld'
	tempfile yld
	save `yld', replace

	levelsof cause_id, local(CAUSES) c
	clear
	tempfile epi
	save `epi', emptyok
	foreach cid of local CAUSES {
		get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`cid') age_group_ids(`gbd_ages') ///
			gbd_id_field(cause_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) ///
			sex_ids(`sex_id') status(203) kwargs(num_workers:10) n_draws(`n_draws') resample("True") clear
		append using `epi'
		save `epi', replace
	}
	joinby cause_id using `yld'
	tempfile m
	save `m', replace

	clear
	tempfile ne
	save `ne', emptyok
	foreach yid of local year_ids {
		insheet using "FILEPATH/NEmatrix_`location_id'_`yid'_`sex_id'.csv", clear
		gen year_id=`yid'
		append using `ne'
		save `ne', replace
	}
	gen sex_id=`sex_id'
	rename (ncode ecode) (healthstate inj)
	rename draw_* matrix_*
	save `ne', replace

	rename matrix_* prop_*
	** merge on COMO YLDs and YLD matrix (inj is inj_falls etc, healthstate is N code)
	joinby age_group_id healthstate inj sex_id year_id using `m'
	** merge PAF
	joinby age_group_id sex_id acause year_id using `P'
	gen matrix = 1
	** merge on total matrix to re-scale proportions to COMO output
	joinby age_group_id healthstate inj sex_id year_id using `ne', unmatched(using)
	forvalues i = 0/`=`n_draws'-1' {
		qui {
			** carryforward draw
			bysort year_id age_group_id sex_id inj: egen total = max(draw_`i')
			replace draw_`i' = total
			drop total
			** re-scale matrix inputs
			bysort year_id age_group_id sex_id inj: egen total = total(matrix_`i')
			replace prop_`i' = (matrix_`i' * draw_`i')/total
			** calculate PAF
			replace draw_`i' = (paf_`i' * prop_`i')
			drop total
		}
	}
	keep if matrix==1

	fastcollapse draw*, type(sum) by(cause_id age_group_id year_id sex_id)
	gen risk = "`risk'"
	append using `epi'
	keep year_id sex_id cause_id age_group_id risk draw*
	gen denominator = .
	replace denominator = (risk == "")
	duplicates drop
	fastfraction draw*, by(year_id sex_id cause_id age_group_id) denominator(denominator) prefix(paf_) 
	keep if risk=="`risk'"
	rename paf_draw_* paf_*
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < -0
	}

	keep cause_id year_id sex_id age_group_id paf_*
	gen location_id = `location_id'
	gen rei_id = `rei_id'
	gen modelable_entity_id = .
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id 
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv"

** YLL PAFS ---------------------------------------------------------------------------------
	
	use "FILEPATH/proportions_of_hospital_deaths.dta", clear
	tempfile hosp_deaths 
	save `hosp_deaths', replace

	use `P', clear
	joinby age_group_id sex_id acause using `hosp_deaths'
		keep if inlist(acause,"inj_trans_road_pedest","inj_trans_road_pedal","inj_trans_road_2wheel", ///
		"inj_trans_road_4wheel","inj_trans_road_other","inj_trans_other","inj_falls") | ///
		inlist(acause,"inj_mech_other","inj_animal_nonven","inj_homicide_other","inj_disaster")
	duplicates drop
	tempfile f
	save `f', replace

	clear
	tempfile cod
	save `cod', emptyok
	foreach cid of local CAUSES {
		get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`cid') age_group_ids(`gbd_ages') ///
			gbd_id_field(cause_id) measure_ids(1) gbd_round_id(`gbd_round_id') source(codcorrect) ///
			sex_ids(`sex_id') status(66) kwargs(num_workers:10) n_draws(`n_draws') resample("True") clear
		append using `cod'
		save `cod', replace
	}
	keep age_group_id sex_id cause_id year_id location_id draw_*
	merge m:1 cause_id using `causes', keep(3) nogen
	save `cod', replace

	joinby inj age_group_id sex_id year_id location_id using `f'
	forvalues i = 0/`=`n_draws'-1' {
		qui replace draw_`i' = (paf_`i' * draw_`i' * fraction)
	}
	fastcollapse draw*, type(sum) by(cause_id age_group_id year_id sex_id)
	cap gen modelable_entity_id=.
	gen risk = "`risk'"
	append using `cod'

	keep year_id sex_id cause_id age_group_id risk draw*
	gen denominator = .
	replace denominator = (risk == "")
	duplicates drop
	fastfraction draw*, by(year_id sex_id cause_id age_group_id) denominator(denominator) prefix(paf_) 
	keep if risk=="`risk'"
	keep year_id sex_id cause_id age_group_id risk paf*
	rename paf_draw_* paf_*
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < -0
	}

	keep cause_id sex_id age_group_id year_id paf_*
	gen rei_id = `rei_id'
	gen location_id = `location_id'
	gen modelable_entity_id = .
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id 
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv"

//END
