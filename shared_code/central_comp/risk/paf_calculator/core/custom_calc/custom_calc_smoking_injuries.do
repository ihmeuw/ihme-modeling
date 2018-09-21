// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate injuries from smoking
// *************************************************************************************************
// *************************************************************************************************

** *************************************************************************************************
** STATA
** *************************************************************************************************

	clear

** *************************************************************************************************
** WORKSPACE
** *************************************************************************************************

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

	create_connection_string, server(ADDRESS)
	local epi_string = r(conn_string)

  	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** 
** *************************************************************************************************

	clear
	tempfile P
	save `P', emptyok
	foreach yid of local year_ids {
		insheet using "`out_dir'/smoking_direct_prev/paf_yld_`location_id'_`sex_id'.csv", clear
		append using `P'
		save `P', replace
	}
	** keep the injuries parts
	keep if inlist(cause_id,878,923)
	gen acause=""
	replace acause="hip" if cause_id==878
	replace acause="non-hip" if cause_id==923
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
			gbd_id_field(cause_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) status(203) ///
			sex_ids(`sex_id') kwargs(num_workers:10) n_draws(`n_draws') resample("True") clear
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
	joinby year_id age_group_id healthstate inj sex_id using `m'
	** merge PAF
	joinby year_id age_group_id sex_id acause using `P'
	gen matrix = 1
	** merge on total matrix to re-scale proportions to COMO output
	joinby year_id age_group_id healthstate inj sex_id using `ne', unmatched(using)
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
	tempfile yld
	save `yld', replace

	insheet using "`out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv", clear
	drop if inlist(cause_id,878,923)
	qui append using `yld'
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv"

** YLL PAFS ---------------------------------------------------------------------------------

	use "FILEPATH/proportions_of_hospital_deaths.dta", clear
	tempfile hosp_deaths 
	save `hosp_deaths', replace

	clear
	tempfile P
	save `P', emptyok
	foreach yid of local year_ids {
		insheet using "`out_dir'/smoking_direct_prev/paf_yll_`location_id'_`sex_id'.csv", clear
		append using `P'
		save `P', replace
	}
	** keep the injuries parts
	keep if inlist(cause_id,878,923)
	gen acause=""
	replace acause="hip" if cause_id==878
	replace acause="non-hip" if cause_id==923
	joinby age_group_id sex_id acause using `hosp_deaths'
	keep if inlist(inj,"inj_trans_road_pedest","inj_trans_road_pedal","inj_trans_road_2wheel", ///
		"inj_trans_road_4wheel","inj_trans_road_other","inj_trans_other","inj_falls") | ///
		inlist(inj,"inj_mech_other","inj_animal_nonven","inj_homicide_other","inj_disaster")
	duplicates drop
	tempfile f
	save `f', replace

	clear
	tempfile cod
	save `cod', emptyok
	foreach cid of local CAUSES {
		get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`cid') age_group_ids(`gbd_ages') ///
			gbd_id_field(cause_id) measure_ids(1) gbd_round_id(`gbd_round_id') source(codcorrect) ///
			sex_ids(`sex_id') kwargs(num_workers:10) n_draws(`n_draws') resample("True") status(66) clear
		append using `cod'
		save `cod', replace
	}
	keep age_group_id sex_id cause_id year_id location_id draw_*
	merge m:1 cause_id using `causes', keep(3) nogen
	save `cod', replace

	joinby inj age_group_id sex_id year_id using `f'
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
	tempfile yll
	save `yll', replace

	insheet using "`out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv", clear
	drop if inlist(cause_id,878,923)
	qui append using `yll'
		order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id 
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv"

// END