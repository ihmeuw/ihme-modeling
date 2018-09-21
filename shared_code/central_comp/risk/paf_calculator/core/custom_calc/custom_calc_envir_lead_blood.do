// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Calculate Lead ID PAFs
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

    get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local gbd_ages `r(age_group_ids)'

** *************************************************************************************************
** PAF CALC
** *************************************************************************************************

** PULL EXPOSURES  ----------------------------------------------------------------------------------

    get_draws, gbd_id_field(rei_id) gbd_id(`rei_id') year_ids(`year_ids') location_ids(`location_id') ///
		age_group_ids(`gbd_ages') sex_ids(`sex_id') kwargs(draw_type:exposure num_workers:10) source(risk) ///
		gbd_round_id(`gbd_round_id') n_draws(`n_draws') resample("True") clear
    rename draw_* shift_*
	cap drop model*
	tempfile iqshift
	save `iqshift', replace

** *************************************************************************************************
** INTELLECTUAL DISABILITY
** *************************************************************************************************

	local x = 0
	foreach s in 487 488 489 490 491 {
		get_draws, location_ids(`location_id') year_ids(`year_ids') gbd_id(`s') age_group_ids(`gbd_ages') ///
			gbd_id_field(sequela_id) measure_ids(3) gbd_round_id(`gbd_round_id') source(como) ///
			sex_ids(`sex_id') kwargs(num_workers:10) status(203) n_draws(`n_draws') resample("True") clear
		qui gen severity = ""
		qui replace severity = "borderline" if `s'==487
		qui replace severity = "mild" if `s'==488
		qui replace severity = "moderate" if `s'==489
		qui replace severity = "severe" if `s'==490
		qui replace severity = "profound" if `s'==491
		local x = `x' + 1
		tempfile `x'
		save ``x'', replace
	}
	clear
	forvalues i = 1/`x' {
		append using ``i''
	}
	keep year_id location_id age_group_id sex_id severity draw*
	reshape wide draw_*, i(year_id location_id age_group_id sex_id) j(severity) string
	rename (draw_*borderline draw_*mild draw_*moderate draw_*severe draw_*profound) ///
		(borderline_* mild_* moderate_* severe_* profound_*)
	tempfile intellectual_dis
	save `intellectual_dis', replace

	merge 1:1 year_id location_id sex_id age_group_id using `iqshift', keep(3) nogen
		
	** generate cumulative prevalences for each severity level
	forvalues x = 0/`=`n_draws'-1' { 
		qui gen double profound_cum`x' = profound_`x'
		qui gen double severe_cum`x' =  severe_`x' + profound_`x'
		qui gen double moderate_cum`x' = moderate_`x' + severe_`x' + profound_`x'
		qui gen double mild_cum`x' = mild_`x' + moderate_`x' + severe_`x' + profound_`x'
		qui gen double borderline_cum`x' = borderline_`x' + mild_`x' + moderate_`x' + severe_`x' + profound_`x'
	}

	** reshape severity levels long
	rename (borderline_*  mild_* moderate_* severe_* profound_*) ///
		(draw*_borderline draw*_mild draw*_moderate draw*_severe draw*_profound)
	global numbers = ""
	forvalues x = 0/`=`n_draws'-1' {
		global numbers = "${numbers} draw`x'_  drawcum`x'_"
	}
	reshape long $numbers, i(year_id location_id age_group_id sex_id) j(severity) string
		
	// create a variable that is the IQ cutoff for each severity level
	gen IQ_cutoff = .
	replace IQ_cutoff = 85 if severity == "borderline"
	replace IQ_cutoff = 70 if severity == "mild"
	replace IQ_cutoff = 50 if severity == "moderate"
	replace IQ_cutoff = 35 if severity == "severe"
	replace IQ_cutoff = 20 if severity == "profound"

	local assumed_mean = 100
	forvalues x = 0/`=`n_draws'-1' {	
		qui replace drawcum`x'_ = 0.000000000000000000000000001 if drawcum`x'_ == 0
		qui gen sd_`x' = (IQ_cutoff - `assumed_mean') / invnorm(drawcum`x'_)
		qui bysort year_id location_id sex_id age_group_id: egen sd_max_`x' = mean(sd_`x')
		qui gen drawcum`x'_shifted = normal((IQ_cutoff - (`assumed_mean' + shift_`x'))/sd_max_`x')
		qui gen drawcum`x'_unshifted = normal((IQ_cutoff - `assumed_mean')/sd_max_`x')
		qui gen paf_`x' =(drawcum`x'_unshifted - drawcum`x'_shifted)/drawcum`x'_unshifted
	}
	forvalues x = 0/`=`n_draws'-1' {		
		qui replace draw`x'_ = 0.000000000000000000000000001 if draw`x'_ == 0
		qui bysort year_id location_id age_group_id sex_id: egen total_prev_`x' = total(draw`x'_)
		qui gen weighted_paf_`x' = (paf_`x' * draw`x'_) / total_prev_`x'
		qui gen diff_paf_`x' = paf_`x' -  weighted_paf_`x'
	}

	fastcollapse weighted_paf*, type(sum) by(year_id location_id age_group_id sex_id)
	rename weighted_paf_* paf_*

	** save out
	forvalues i = 0/`=`n_draws'-1' {
		qui replace paf_`i' = 0 if paf_`i' == .
		qui replace paf_`i' = 1 if paf_`i' > 1
		qui replace paf_`i' = 0 if paf_`i' < 0
	}
	keep year_id location_id sex_id age_group_id paf*
	gen rei_id = `rei_id'
	gen cause_id = 582 // intellectual disability
	gen modelable_entity_id = .
	order modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id 
	noi di c(current_time) + ": saving PAFs"
	outsheet modelable_entity_id rei_id cause_id location_id year_id sex_id age_group_id paf_* using ///
		"`out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv", comma replace
	no di "saved: `out_dir'/`risk'/paf_yld_`location_id'_`sex_id'.csv"

// END
