// This launches DisMod at the global level for age patterns
// using the data for the Case fatality estimates of bacterial and viral
// pneumonia. The input data are the ratio of those ratios! //
// then do "/filepath/winrun_lri_cfr.do"

clear all
set more off

* set j
global j "filepath"
local dismodlink /filepath/bin

	local date = subinstr("`c(current_date)'"," ","_",.)

	import delimited "filepath", clear
	tempfile locs
	save `locs'
	
	import delimited "filepath", clear
	tempfile haqi
	save `haqi'
	
	import delimited "filepath", clear
	tempfile sdi
	save `sdi'
	
	**** Read and reformat your data, these variables are are mandatory***********	
	import excel "filepath", firstrow sheet("extraction") clear
	gen year_id = year_end
	merge m:1 location_id year_id using `haqi', keep(3) nogen
	merge m:1 location_id year_id using `sdi', keep(3) nogen

	gen meas_value = mean
	gen meas_stdev = standard_error
	gen time_lower = year_start
	gen time_upper = year_start
	gen age_lower = age_start
	gen age_upper = age_end
	gen integrand = "incidence"
	gen super = "none"
	//gen region = "none"		//other levels of random effect if required
	gen region = "none"
	//gen subreg = notes		// lowest level of random effect, mostly country 
	gen subreg = "none"
	cap gen x_sex = 0
	cap gen x_ones = 1
	
	egen mean_sdi = mean(sdi)
	egen mean_haqi = mean(haqi)
	egen low_sdi = pctile(sdi), p(33)
	egen mid_sdi = pctile(sdi), p(66)
	gen x_sdi = 0
	replace x_sdi = 1 if sdi < mean_sdi
	gen x_haqi = 0
	replace x_haqi = 1 if haqi < mean_haqi
	gen data_like = "log_gaussian"
	//export delimited "filepath", replace

	drop x_sdi x_haqi
// Outliers
	drop if meas_value > 10
	drop if meas_value == 0
	merge m:1 location_id using `locs', keep(3)

tempfile main
save `main'

	********* Prepare and run bradmod, customize the output folder and also you can change the options below if needed
	
			use `main', replace
			local project 			cfr_ratio_`date'_full
			global sample_interval	10
			global num_sample		2000
			global proportion 1
			global midmesh 0 1 2 5 20 60 80 100 // 0 30 45 60 70 80 // 0 1 2 3 4 5 6 7 8 9 10
			global meshout 0 .01 .1 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 // 0 1 2 3 4 5 6 7 8 9 10 //
			
			global prjfolder		"filepath"
			local prjfolder 	"filepath"
			local codefolder	"filepath"		// Copy and direct to your folder if you want to change central codes
			cap mkdir "`prjfolder'"
			outsheet using "`prjfolder'/data_input.csv", comma replace
			do "filepath/winrun_file.do"


cap log close	
