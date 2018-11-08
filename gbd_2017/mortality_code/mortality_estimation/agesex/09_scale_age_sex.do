** Steps: read in GPR simulation files for all locs, want long by sex and wide by age
** want to also read in birth sex ratio, draw level 5q0
** want exponentiated values


clear all
set more off
cap restore, not

** Set up Stata

	local 5q0_dir "FILEPATH"
	log using "FILEPATH", replace

** save GPR 5q0 estimates for later use
	insheet using "FILEPATH", clear
	keep if location_id == `location_id' & estimate_stage_id == 3
	gen ihme_loc_id = "`ihme_loc_id'"
	gen year = year_id + 0.5
	drop if year > 2018 | year < 1949.5
	rename mean q5med
	rename lower q5lower
	rename upper q5upper
	keep ihme_loc_id year q5*
	isid ihme_loc_id year
	tempfile estimates
	save `estimates', replace

// Import scaled input
	import delimited "FILEPATH", clear
	destring q*, replace force

** going into conditional prob space
	gen pred_enn = q_enn_/q_u5_
	gen pred_lnn = (1-q_enn_)*q_lnn_/q_u5_
	gen pred_pnn = (1-q_enn_)*(1-q_lnn_)*q_pnn_/q_u5_
	gen pred_ch  = (1-q_enn_)*(1-q_lnn_)*(1-q_pnn_)*q_ch_/q_u5_
	gen pred_inf = q_inf_/q_u5_

** scale (this scales within each simulation, for males and females)
	gen scale = pred_inf + pred_ch
	foreach var in pred_inf pred_ch {
		replace `var' = `var'/scale
	}
	drop scale

	gen scale = (pred_enn + pred_lnn + pred_pnn) / pred_inf
	foreach var in pred_enn pred_lnn pred_pnn {
		replace `var' = `var'/scale
	}
	drop scale

** going back to qx space
	replace q_enn_ = (q_u5_ * pred_enn)
	replace q_lnn_ = (q_u5_ * pred_lnn) / ((1-q_enn_))
	replace q_pnn_ = (q_u5_ * pred_pnn) / ((1-q_enn_)*(1-q_lnn_))
	replace q_ch_  = (q_u5_ * pred_ch)  / ((1-q_enn_)*(1-q_lnn_)*(1-q_pnn_))
	replace q_inf_ = (q_u5_ * pred_inf)
	drop pred*

** generate estimates for both sexes combined (for each simulation)
	reshape wide q_enn_ q_lnn_ q_pnn_ q_ch_ q_inf_ q_u5_, i(ihme_loc_id year simulation birth_sexratio) j(sex, string)

** ratio of live males to females at the beginning of each period
	gen r_enn = birth_sexratio
	gen r_lnn = r_enn*(1-q_enn_male)/(1-q_enn_female)
	gen r_pnn = r_lnn*(1-q_lnn_male)/(1-q_lnn_female)
	gen r_ch  = r_pnn*(1-q_pnn_male)/(1-q_pnn_female)

	drop q_enn_both q_lnn_both q_pnn_both q_ch_both q_inf_both

	gen q_enn_both = (q_enn_male) * (r_enn/(1+r_enn)) + (q_enn_female) * (1/(1+r_enn))
	gen q_lnn_both = (q_lnn_male) * (r_lnn/(1+r_lnn)) + (q_lnn_female) * (1/(1+r_lnn))
	gen q_pnn_both = (q_pnn_male) * (r_pnn/(1+r_pnn)) + (q_pnn_female) * (1/(1+r_pnn))
	gen q_ch_both  = (q_ch_male)  * (r_ch/(1+r_ch))   + (q_ch_female)  * (1/(1+r_ch))
	gen q_inf_both = (q_inf_male) * (r_enn/(1+r_enn)) + (q_inf_female) * (1/(1+r_enn))

** scale each estimate for both sexes combined (for each simulation)
	gen prob_enn_both = q_enn_both/q_u5_both
	gen prob_lnn_both = (1-q_enn_both)*q_lnn_both/q_u5_both
	gen prob_pnn_both = (1-q_enn_both)*(1-q_lnn_both)*q_pnn_both/q_u5_both
	gen prob_ch_both  = (1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*q_ch_both/q_u5_both
	gen prob_inf_both = q_inf_both/q_u5_both

	gen scale = prob_inf_both + prob_ch_both
	replace prob_inf_both = prob_inf_both / scale
	replace prob_ch_both = prob_ch_both / scale
	drop scale

	gen scale = (prob_enn_both + prob_lnn_both + prob_pnn_both) / prob_inf_both
	foreach age in enn lnn pnn {
		replace prob_`age'_both = prob_`age'_both / scale
	}
	drop scale

	replace q_enn_both = (q_u5_both * prob_enn_both)
	replace q_lnn_both = (q_u5_both * prob_lnn_both) / ((1-q_enn_both))
	replace q_pnn_both = (q_u5_both * prob_pnn_both) / ((1-q_enn_both)*(1-q_lnn_both))
	replace q_inf_both = (q_u5_both * prob_inf_both)
	replace q_ch_both  = (q_u5_both * prob_ch_both)  / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both))

** Some formatting
	drop prob* r*

	reshape long q_enn q_lnn q_pnn q_inf q_ch q_u5, i(ihme_loc_id year simulation birth_sexratio) j(sex, string)
	replace sex = subinstr(sex, "_", "", 1)
	isid ihme_loc_id year sex simulation

** Generate neonatal estimates
	gen q_nn = 1 - (1-q_enn)*(1-q_lnn)


** Save simulations
	preserve
		keep ihme_loc_id year sex simulation q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5
		gen rat_enn = q_enn/q_u5
		gen rat_lnn = q_lnn/q_u5
		gen rat_nn = q_nn/q_u5
		gen rat_pnn = q_pnn/q_u5
		gen rat_inf = q_inf/q_u5
		gen rat_ch = q_ch/q_u5
		sort ihme_loc_id sex year

		foreach rat in enn lnn nn pnn inf ch {
			noisily: di "`rat'"
			by ihme_loc_id sex year: egen rat_`rat'_lower = pctile(rat_`rat'), p(2.5)
			by ihme_loc_id sex year: egen rat_`rat'_upper = pctile(rat_`rat'), p(97.5)
			drop rat_`rat'
		}

		drop simulation
		order ihme_loc_id sex year rat*
		drop q*
		duplicates drop
		isid ihme_loc_id sex year
		saveold "FILEPATH", replace

	restore, preserve

		keep ihme_loc_id year sex simulation q_enn q_lnn q_pnn q_ch q_u5

		** 2/2/2017 capping the draws of qx. This was potentially causing problems in the MLT
		foreach var of varlist q* {
			replace `var' = 0.99 if `var' >.99 | `var' == .
		}

		outsheet using "FILEPATH", comma replace
	restore

** Calculate rates of decline
	sort simulation ihme_loc_id sex year
	foreach q in enn lnn nn pnn inf ch u5 {
		by simulation ihme_loc_id sex: gen change_`q'_70_90 = -100*ln(q_`q'[41]/q_`q'[21])/(1990-1970)
		by simulation ihme_loc_id sex: gen change_`q'_90_10 = -100*ln(q_`q'[61]/q_`q'[41])/(2010-1990)
		by simulation ihme_loc_id sex: gen change_`q'_90_08 = -100*ln(q_`q'[59]/q_`q'[41])/(2008-1990)
	}

** ***************************
** Collapse and force consistency in final estimates
** ***************************
** Collapse across to find the 2.5 and 97.5 percentiles and mean in all qx estimates and rates of change, by sex, country, and year
	sort ihme_loc_id sex year simulation
	isid ihme_loc_id sex year simulation

	foreach q in enn lnn nn pnn inf ch u5 {
		noisily: di "`q'"
		by ihme_loc_id sex year: egen q_`q'_med = mean(q_`q')
		by ihme_loc_id sex year: egen q_`q'_lower = pctile(q_`q'), p(2.5)
		by ihme_loc_id sex year: egen q_`q'_upper = pctile(q_`q'), p(97.5)
		drop q_`q'

		foreach date in 70_90 90_10 90_08 {
			noisily: di "`date'"
			by ihme_loc_id sex: egen change_`q'_`date'_med = mean(change_`q'_`date')
			by ihme_loc_id sex: egen change_`q'_`date'_lower = pctile(change_`q'_`date'), p(2.5)
			by ihme_loc_id sex: egen change_`q'_`date'_upper = pctile(change_`q'_`date'), p(97.5)
			drop change_`q'_`date'
		}
	}

	drop simulation
	order ihme_loc_id sex year birth_sexratio q* change*
	duplicates drop
	isid ihme_loc_id sex year

** Merge on GPR 5q0; replace the combined 5q0 estimates with these
** (they are almost the same but differ slightly due to rounding differences and the way that shocks are being applied, and we want to keep the GPR estimates)
	merge m:1 ihme_loc_id year using `estimates'
	foreach est in med lower upper {
		replace q_u5_`est' = q5`est' if sex == "both"
	}
	drop q5* _m

	preserve
		drop *lower *upper
		rename *med *med_prescale
		tempfile prescale
		save `prescale', replace
	restore

** Force consistency in the male and female 5q0 with the combined 5q0 (medium estimates only)
	sort ihme_loc_id year sex
	by ihme_loc_id year: gen scale = q_u5_med[1] / (q_u5_med[3]*(birth_sexratio/(1+birth_sexratio)) + q_u5_med[2]*(1/(1+birth_sexratio)))
	replace q_u5_med = q_u5_med*scale if sex!="both"
	gen scale_sexes_u5_to_both_u5 = scale
	gen srb = birth_sexratio
	drop scale

** Force consistency in the enn, lnn, nn, pnn, inf, and ch estimates for males and females (medium estimates only)
	gen prob_enn_med = q_enn_med/q_u5_med
	gen prob_lnn_med = (1-q_enn_med)*q_lnn_med/q_u5_med
	gen prob_pnn_med = (1-q_enn_med)*(1-q_lnn_med)*q_pnn_med/q_u5_med
	gen prob_ch_med  = (1-q_enn_med)*(1-q_lnn_med)*(1-q_pnn_med)*q_ch_med/q_u5_med
	gen prob_inf_med = q_inf_med/q_u5_med

	gen scale = prob_inf_med + prob_ch_med
	replace prob_inf_med = prob_inf_med / scale
	replace prob_ch_med = prob_ch_med / scale
	gen scale_mf_infch_u5 = scale
	drop scale

	gen scale = (prob_enn_med + prob_lnn_med + prob_pnn_med) / prob_inf_med
	foreach age in enn lnn pnn {
		replace prob_`age'_med = prob_`age'_med / scale
	}
	gen scale_mf_ennlnnpnn_inf = scale
	drop scale

	replace q_enn_med = (q_u5_med * prob_enn_med)
	replace q_lnn_med = (q_u5_med * prob_lnn_med) / ((1-q_enn_med))
	replace q_pnn_med = (q_u5_med * prob_pnn_med) / ((1-q_enn_med)*(1-q_lnn_med))
	replace q_inf_med = (q_u5_med * prob_inf_med)
	replace q_ch_med  = (q_u5_med * prob_ch_med)  / ((1-q_enn_med)*(1-q_lnn_med)*(1-q_pnn_med))
	replace q_nn_med  = 1-(1-q_enn_med)*(1-q_lnn_med)
	drop prob*

** Recalculate the both sexes combined estimates for enn, lnn, nn, pnn, inf, and child (medium estimates only)
	sort ihme_loc_id year sex
	** ratio of live males to females at the beginning of each period
	gen r_enn = birth_sexratio
	by ihme_loc_id year: gen r_lnn = r_enn*(1-q_enn_med[3])/(1-q_enn_med[2])
	by ihme_loc_id year: gen r_pnn = r_lnn*(1-q_lnn_med[3])/(1-q_lnn_med[2])
	by ihme_loc_id year: gen r_ch  = r_pnn*(1-q_ch_med[3])/(1-q_ch_med[2])

	by ihme_loc_id year: replace q_enn_med = (q_enn_med[3]) * (r_enn/(1+r_enn)) + (q_enn_med[2]) * (1/(1+r_enn)) if _n == 1
	by ihme_loc_id year: replace q_lnn_med = (q_lnn_med[3]) * (r_lnn/(1+r_lnn)) + (q_lnn_med[2]) * (1/(1+r_lnn)) if _n == 1
	by ihme_loc_id year: replace q_pnn_med = (q_pnn_med[3]) * (r_pnn/(1+r_pnn)) + (q_pnn_med[2]) * (1/(1+r_pnn)) if _n == 1
	by ihme_loc_id year: replace q_ch_med  = (q_ch_med[3])  * (r_ch/(1+r_ch))   + (q_ch_med[2])  * (1/(1+r_ch))  if _n == 1
	by ihme_loc_id year: replace q_inf_med = (q_inf_med[3]) * (r_enn/(1+r_enn)) + (q_inf_med[2]) * (1/(1+r_enn)) if _n == 1
	drop r* birth_sexratio

** Force consistency in the both sexes combined estimates (medium estimates only)
	gen prob_enn_med = q_enn_med/q_u5_med
	gen prob_lnn_med = (1-q_enn_med)*q_lnn_med/q_u5_med
	gen prob_pnn_med = (1-q_enn_med)*(1-q_lnn_med)*q_pnn_med/q_u5_med
	gen prob_ch_med  = (1-q_enn_med)*(1-q_lnn_med)*(1-q_pnn_med)*q_ch_med/q_u5_med
	gen prob_inf_med = q_inf_med/q_u5_med

	gen scale = prob_inf_med + prob_ch_med
	replace prob_inf_med = prob_inf_med / scale
	replace prob_ch_med = prob_ch_med / scale
	gen scale_both_infch_u5 = scale
	drop scale

	gen scale = (prob_enn_med + prob_lnn_med + prob_pnn_med) / prob_inf_med
	foreach age in enn lnn pnn {
		replace prob_`age'_med = prob_`age'_med / scale
	}
	gen scale_both_ennlnnpnn_inf = scale
	drop scale

	replace q_enn_med = (q_u5_med * prob_enn_med)
	replace q_lnn_med = (q_u5_med * prob_lnn_med) / ((1-q_enn_med))
	replace q_pnn_med = (q_u5_med * prob_pnn_med) / ((1-q_enn_med)*(1-q_lnn_med))
	replace q_inf_med = (q_u5_med * prob_inf_med)
	replace q_ch_med  = (q_u5_med * prob_ch_med)  / ((1-q_enn_med)*(1-q_lnn_med)*(1-q_pnn_med))
	replace q_nn_med  = 1-(1-q_enn_med)*(1-q_lnn_med)
	drop prob*

	preserve
		drop scale* srb
		merge 1:1 ihme_loc_id sex year using `prescale', nogen assert(3)
		foreach age in enn lnn pnn ch {
			gen scale_`age' = q_`age'_med/q_`age'_med_prescale
		}
		keep ihme_loc_id sex year scale*
		save "$output_dir/diagnostics/scaling/`ihme_loc_id'_scaling_numbers.dta", replace
	restore
	drop scale* srb

** Recalculate medium estimates of rates of change
	sort ihme_loc_id sex year
	foreach q in enn lnn nn pnn inf ch u5 {
		by ihme_loc_id sex: replace change_`q'_70_90_med = -100*ln(q_`q'_med[41]/q_`q'_med[21])/(1990-1970)
		by ihme_loc_id sex: replace change_`q'_90_10_med = -100*ln(q_`q'_med[61]/q_`q'_med[41])/(2010-1990)
		by ihme_loc_id sex: replace change_`q'_90_08_med = -100*ln(q_`q'_med[59]/q_`q'_med[41])/(2008-1990)
	}

** force probabilities <1
	foreach var of varlist q* {
		replace `var' = 0.99 if `var' >.99 | `var' == .
	}

** Format and save
	order ihme_loc_id sex year q* change*
	outsheet using "FILEPATH", comma replace
	cap log close

exit, clear
