// NAME
// February 2014
// Save compartmental model mortality and progression parameters as inputs for Tim's SPECTRUM model

**************************************************************
** SET UP
**************************************************************

*going to see why its not saveing
log using "ADDRESS", replace

clear all
set maxvar 20000
set more off
cap restore, not

** Set code directory for Git
global user "`c(username)'"

if (c(os)=="Unix") {
	global root "ADDRESS"
	global code_dir "FILEPATH"
	local run_date = "`1'"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
	global code_dir "FILEPATH"
	local run_date = "20151222"
}

// Filepaths
local comp_dir "FILEPATH"
local graph_dir "FILEPATH"
local progression_path "FILEPATH"
local mortality_path "FILEPATH"

// Get locations
adopath + "FILEPATH"

// Loop vectors
local ages "15_25 25_35 35_45 45_100"
local types "mortality progression"

// Counter
local count = 1

// Seed
set seed 100
// Toggles
local visuals = 1
local format = 1

// Initialize pdfmaker
if c(os) == "Windows" {
	do "FILEPATH"
}
if c(os) == "Unix" {
	do "FILEPATH"
	set odbcmgr unixodbc
}

**************************************************************
** COMPILE ALL MORTALITY AND PROGRESSION PARAMETERS
**************************************************************
foreach a of local ages {
	foreach t of local types {
			
		** local a "35_45"
		** local t "progression"
		
		if "`t'"=="mortality" {
			local stub "mort"		
		}
		else if "`t'" == "progression" {
			local stub "prog"
		}
		
		insheet using "`comp_dir'/FILEPATH/`a'_sample_`t'.csv", clear comma names
		
		generate age = "`a'"
		generate draw = _n
		
		foreach var in `stub'* {
			cap tostring `var', replace force
		}
		
		reshape long `stub', i(age draw) j(cd4)
		
		cap replace `stub' = "" if `stub' == "NA"
		cap destring `stub', replace
		
		tostring cd4, replace
		replace cd4 = "LT50CD4" if cd4 == "7"
		replace cd4 = "50to99CD4" if cd4 == "6"
		replace cd4 = "100to199CD4" if cd4 == "5"
		replace cd4 = "200to249CD4" if cd4 == "4"
		replace cd4 = "250to349CD4" if cd4 == "3"
		replace cd4 = "350to500CD4" if cd4 == "2"
		replace cd4 = "GT500CD4" if cd4 == "1"
		
		replace age = subinstr(age, "_", "-", .)
		
		if `count' == 1 {
			tempfile `t'file
			save ``t'file', replace
		}
		else {
			append using ``t'file'
			tempfile `t'file
			save ``t'file', replace
		}
			
	}
	local count = `count' + 1	
}

**************************************************************
** RESAMPLE FROM DRAWS THAT DIDN'T CONVERGE
**************************************************************
// Need to deal with draws that didn't converge.  Following code will set to missing the entire draw that didn't converge (ie, it didn't converge for mortality, progression, or both) to missing, and then re-sample from an entire draw (ie, both mortality and progression parameters).  
// Output needs to ascertain that there is one draw labeled 1...1000 for every age/bin combination

// Combine files
use `mortalityfile', clear
rename mort prog
generate type = "mortality"
append using `progressionfile'
rename prog par
replace type = "progression" if type == ""
tempfile combined
save `combined', replace

*resampling draws that are below 0 or do not converge
generate below_0 = 0 
replace below_0 = 1 if par<0 
bysort draw: egen drop_draw_below = total(below_0)
bysort draw: egen drop_draw_no_converge = total(no_converge) 
generate drop_draw = drop_draw_below + drop_draw_no_converge
replace par = . if drop_draw > 0

// Need to drop unreasonable looking draws, but this is dependent on a couple of seeds. 
// replace par = . if inlist(draw, 43, 398, 1019, 1329, 1538)
// replace par = . if inlist(draw, 429, 527, 539, 567, 600, 670, 814, 1235, 1561)

drop no_converge drop_draw below_0 drop_draw_no_converge drop_draw_below

// Draw random integer - need to do this a few times, because it's possible that the draw number we generate to replace a missing draw is also a missing draw.
forvalues i = 1/4 {
	bysort draw: generate replacement_draw`i' = floor(1001*runiform() + 1000) if draw <= 1000 & par == . & type == "mortality" & age == "15-25" & cd4 == "LT50CD4"
	bysort draw: egen rep_draw`i' = max(replacement_draw`i')
	drop replacement_draw`i'
}

levelsof rep_draw1, local(reps1)
levelsof rep_draw2, local(reps2)
levelsof rep_draw3, local(reps3)
levelsof rep_draw4, local(reps4)

// Replace draw = replacement draw if the parameters didn't converge or threw up non-finite finite difference value error
reshape wide par rep_draw1 rep_draw2 rep_draw3 rep_draw4, i(age cd4 type) j(draw)
	
forvalues i = 1/1000 {
	di as text "`i' trying replacement draw 1"
	foreach j of local reps1 {
		qui replace par`i' = par`j' if rep_draw1`i' == `j' 
		cap assert par`i' != .
		
		if _rc != 0 {
			di in red "`i' trying replacement draw 2"
			foreach k of local reps2 {
				qui replace par`i' = par`k' if rep_draw2`i' == `k' & par`i' == .
			}
			cap assert par`i' != .
		}
		if _rc != 0 {
			di in red "`i' trying replacement draw 3"
			foreach l of local reps3 {
				qui replace par`i' = par`l' if rep_draw3`i' == `l' & par`i' == .
			}
			cap assert par`i' != .
		}	
		if _rc != 0 {
			di in red "`i' trying replacement draw 4"
			foreach m of local reps4 {
				qui replace par`i' = par`m' if rep_draw4`i' == `m' & par`i' == .
			}
			assert par`i' != .
		}
	}
}
	

**************************************************************
** FORMAT FOR GRAPHING IN R
**************************************************************	
drop rep_draw*
reshape long par, i(age cd4 type) j(draw)
keep if draw <= 1000

//  Saves parameter draws so they can be graphed in R
if `visuals' == 1 {

	preserve
		sort age type cd4 par
		by age type cd4: egen par_mean = mean(par)
		by age type cd4: generate par_lower = par[25]
		by age type cd4: generate par_upper = par[975]
		keep age cd4 type par_lower par_upper par_mean
		duplicates drop
		outsheet using "`comp_dir'/FILEPATH/re_age_specific_parameter_bounds.csv", replace comma names
	restore
	
}

**************************************************************
** CALCULATE COEFFICIENT OF VARIATION
**************************************************************	
preserve
	bysort age type cd4: egen stdev = sd(par)
	bysort age type cd4: egen mu = mean(par)
	generate coeff_var = stdev/mu
	
	duplicates drop
	sort type age cd4
	outsheet using "`comp_dir'/FILEPATH/coeff_var_table.csv", replace comma names
	
restore


**************************************************************
** SAVE FOR TIM TO USE IN SPECTRUM
**************************************************************		
preserve
	keep if type == "mortality"
	rename par mort
	drop type
	cd "`comp_dir'/FILEPATH"
	save "mortality_compiled.dta", replace
	outsheet using "mortality_compiled.csv", replace comma names
restore

preserve
	keep if type == "progression"
	rename par prog
	drop type
	cd "`comp_dir'/FILEPATH"
	save "progression_compiled.dta", replace
	outsheet using "progression_compiled.csv", replace comma names
restore
	

// Copy files to the proper locations
	local progression_path "FILEPATH"
	local mortality_path "FILEPATH"
	
	get_locations
	levelsof ihme_loc_id, local(isos) c 
	
			
	// Remove the results from the previous run
	! perl -e 'unlink <`progression_path'/\*>' 
	! perl -e 'unlink <`mortality_path'/\*>' 
	
	foreach country in `isos' {
		di "`country'"
		! cp "`comp_dir'/FILEPATH/progression_compiled.csv" "FILEPATH/`country'_progression_par_draws.csv"
		! cp "`comp_dir'/FILEPATH/mortality_compiled.csv" "FILEPATH/`country'_mortality_par_draws.csv"
	}

	/*
	// Launch filesaving jobs
		get_locations
		levelsof region_id, local(regions)
		levelsof ihme_loc_id, local(isos) c

		cd "$code_dir"
		foreach region in `regions' {
			di in red "Submitting Region `region'"
			! FILEPATHb -e FILEPATH -oFILEPATH -P proj_hiv -pe multi_slot 2 -N save_`region' "stata_shell.sh" "FILEPATH/05a_2_save_csvs.do" "`region'"
		}

	*/
	// Check that all file-saving jobs have finished before proceeding
		local ccc = 0
		local counter = wordcount("`isos'") 
		local counter = `counter' * 2 // Get number of expected files
		
		while `ccc' == 0 {
			local filecount = 0
			
			foreach i of local isos {
				capture confirm file "FILEPATH/`i'_progression_par_draws.csv"
				if !_rc local ++filecount

				capture confirm file "FILEPATH/`i'_mortality_par_draws.csv"
				if !_rc local ++filecount
			}
			
			di "Checking `c(current_time)': `filecount' of `counter' Save GBD files found"
			if (`filecount' == `counter') local ccc = 1
			else sleep 10000
		}	


log close
