// This is launched by 'winrun_lri_adjusted.do' and produces 
// an age-curve for Pneumococcal pneumonia PAF //
// Strongly recommend not making changes to this code
// as it is very sensitive and not supported by central
// comp. 

clear
set more off

* set j
	global j "filepath
	local dismodlink /filepath/bin


	local project 			$project  
	local datafile 			$datafile 
	local sample_interval	$sample_interval
	local num_sample		$num_sample
	local proportion 		$proportion
	local midmesh 			$midmesh
	local meshout			$meshout
	local prjfolder		$prjfolder
	local codefolder	"filepath"
	
	cap mkdir "`prjfolder'"
	di in red "`cause'_`risk'"
	
//insheet using "$datafile", comma clear

insheet using "`prjfolder'/data_input.csv", comma clear
		tostring meas_stdev, replace force
		local o = _N
		local age_s 0
		di `o'
 forval i = 1/2 {
		local o = `o' + 1
		set obs `o'
		replace integrand = "mtall" in `o'
		replace super = "none" in `o'
		replace region = "none" in `o'
		replace subreg = "none" in `o'
		di "part 1"
		replace time_lower = 2000 in `o'
		replace time_upper = 2000 in `o'
		replace age_lower = `age_s' in `o'
		local age_s = `age_s' + 5
		replace age_upper = `age_s' in `o'
		di "part 2"
		replace x_sex = 0 in `o'

		replace x_ones = 0 in `o'
		di "part 2.5"
		replace meas_value = .01 in `o'
		di "part 3"
		replace meas_stdev = "inf" in `o'
		di "part 4"
	}	
		qui sum age_upper
		local maxage = r(max)
		if `maxage' > 90 local maxage 100
		replace age_upper = `maxage' in `o'
		replace data_like = data_like[_n-1] if data_like == ""
outsheet using "`prjfolder'/data_in.csv", comma replace

	local studycovs
	foreach var of varlist x_* {
					local studycovs "`studycovs' `var'"
	}
	qui sum age_upper
	global mesh `midmesh' `maxage'
	global sample_interval `sample_interval'
	global num_sample `num_sample'
	global studycovs `studycovs'
	qui		do "`codefolder'/make_effect_ins.do"
	qui		do "`codefolder'/make_rate_ins.do"
	qui		do "`codefolder'/make_value_in.do"
	qui		do "`codefolder'/make_plain_in.do"
	
insheet using "`prjfolder'/rate_in.csv", comma clear case
	replace lower = "0" if type == "iota"
	replace upper = "1" if type == "iota"
outsheet using "`prjfolder'/rate_in.csv", comma replace
	
	clear
	set obs 1

	foreach var in integrand subreg region super   {
		gen `var' = "none"
	}
	foreach var in meas_value meas_stdev age_lower age_upper x_sex x_ones  {
		gen `var' = 0
	}
	local integs incidence mtall mtexcess mtother mtspecific mtstandard mtwith prevalence relrisk remission
	local l: list sizeof integs
	expand `l'
	local cnt 0
	foreach item of local integs {
		local cnt = `cnt' + 1
		replace integrand = "`item'" in `cnt'
	}
	tempfile pred
	tempfile main
	save `main'
	local t = wordcount("`meshout'") - 1
	di `t'
	forval cnt = 1/`t' {
		use `main', replace
		replace age_lower = real(word("`meshout'",`cnt'))
		replace age_upper = real(word("`meshout'",`cnt' + 1))
			if `cnt' == 1 save `pred', replace
			else {
				append using `pred'
				save `pred', replace
			}
	}
	drop if age_lower == .
	replace age_upper = real(word("`meshout'",`t')) if age_upper == .
	gen age = age_lower

	if $proportion == 1 keep if integrand == "incidence"
	foreach var of local studycovs {
			cap gen `var' = 0
	}
	gen data_like = "gaussian"
	sort integrand age_lower 
outsheet using "`prjfolder'/pred_in.csv", comma replace
	cd "`prjfolder'"
	
	! `dismodlink'/sample_post.py
	
insheet using "`prjfolder'/pred_in.csv", comma clear
	foreach var of local studycovs {
			cap gen `var' = 0
	}
	gen row_name = "null"
outsheet using "`prjfolder'/draw_in.csv", comma replace

	! `dismodlink'/stat_post.py scale_beta=false	
	! `dismodlink'/data_pred data_in.csv value_in.csv plain_in.csv rate_tmp.csv effect_in.csv sample_out.csv data_pred.csv
	! `dismodlink'/data_pred pred_in.csv value_in.csv plain_in.csv rate_tmp.csv effect_in.csv sample_out.csv pred_out.csv
	! `dismodlink'/model_draw value_in.csv plain_in.csv rate_in.csv effect_in.csv sample_out.csv draw_in.csv draw_out.csv

	! `dismodlink'/predict_post.py 10

	! `dismodlink'/data_pred model_in.csv value_in.csv plain_in.csv rate_tmp.csv effect_in.csv sample_out.csv model_out.csv
//	sleep 2000
	! `dismodlink'/plot_post.py  "`project'"
	! `dismodlink'/model_draw  draw_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv draw_out.csv

	//! /filepath/model_draw.py.in	draw_in.csv value_tmp.csv plain_tmp.csv rate_tmp.csv effect_tmp.csv sample_out.csv draw_out.csv
		insheet using "draw_out.csv", comma clear
				local n = _N
				keep if _n == 1 | _n > `n' - 1000
				xpose,clear
				gen id = _n
				tempfile draw_out
				save `draw_out'
		insheet using "draw_in.csv", comma clear
				gen id = _n
				merge 1:1 id using `draw_out'		
		//		rename v1 age
				drop v1
qui				forval i = 1/1000 {
						local j = `i' + 1
						//replace v`j' = 1 - v`j'
						rename v`j' draw`i'
				}
//				drop if age >80
				tostring age,force replace
				replace age = ".01" if age == ".0099999998"
				replace age = ".1" if age == ".1000000015"
				egen mean = rowmean(draw*)
				egen float lower = rowpctile(draw*), p(2.5)
				egen float upper = rowpctile(draw*), p(97.5)
				
				order integrand- id mean- upper
save reshaped_draws.dta, replace
export delimited "`prjfolder'/reshaped_draws_2019.csv", replace
drop draw*
export delimited "`prjfolder'/reshaped_draws.csv", replace
