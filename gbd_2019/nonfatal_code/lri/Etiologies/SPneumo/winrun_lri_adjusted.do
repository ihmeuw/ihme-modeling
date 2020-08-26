// This launches DisMod at the global level for age patterns
// and for the 'Bonten' adjusted PCV estimates
// This should be run on the cluster

clear all
set more off

* set j
	global j "filepath"
	local dismodlink /filepath/bin

	local date = subinstr("`c(current_date)'"," ","_",.)

	**** Read and reformat your data, these variables are are mandatory***********	
	import delimited "filepath", clear
	//replace super = "none"
	gen super = "none"
	//gen region = "none"		//other levels of random effect if required
	replace region = "none"
	//gen subreg = notes		// lowest level of random effect, mostly country 
	replace subreg = "none"
	cap gen x_sex = 0
	cap gen x_ones = 1
	gen data_like = "log_gaussian"

	
	//gen integrand = "incidence"
	//drop if meas_value < 0.1
tempfile main
save `main'

	********* Prepare and run bradmod, customize the output folder and also you can change the options below if needed

			use `main', replace
			local project 			PCV_`date'_Updated2
			global sample_interval	10
			global num_sample		2000
			global proportion 1
			global midmesh 0 1 20 60 80 100 // 0 30 45 60 70 80 // 0 1 2 3 4 5 6 7 8 9 10
			global meshout 0 .01 .1 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 // 0 1 2 3 4 5 6 7 8 9 10 //
			
			global prjfolder		"filepath"
			local prjfolder 	"filepath"
			local codefolder	"filepath"		// Copy and direct to your folder if you want to change central codes
			cap mkdir "filepath"
			outsheet using "filepath", comma replace
			do "filepath/winrun_file.do"


cap log close	
