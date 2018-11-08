// submit prevalence extraction jobs

local username = c(username)
if c(os) == "Unix" {
	local prefix "drive_name"
	global prefix "drive_name"
	set more off
	set odbcmgr unixodbc
}

set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix'/FILEPATH"
adopath + "`prefix'/FILEPATH"
local root "`prefix'/FILEPATH"

local repo = "/FILEPATH"

log using "$prefix/FILEPATH", replace name(log)

	use "$prefix/FILEPATH", clear
	drop if measure != "inc"
	levelsof bundle_id, l(bundles)

// Options
local initial_split = `1'
local map_and_collapse = `2'
local pre_nr_aggregate = `3'
local nr_and_save = `4'
	local inp_int = `5'

// set write_dir to either test or work
local write_dir = `6'

// Split to age/sex for ease
// NOTE this is deprecated for gbd 2016, use 01_submit_prevalence to split files
if `initial_split' == 1 {
	foreach year in 2000 2010 2012 {
		foreach dataset in ccae mdcr {
	 	!qsub -N split_`dataset'`year' -pe multi_slot 100 -l mem_free=200 "/FILEPATH" "`year' `dataset'"
	 	}
	}
}

// Extract incidence and map by dataset/year
// this is deprecated, we map with our claims process in python
if `map_and_collapse' == 1 {
	foreach dataset in ccae mdcr {
		foreach year in 2000 2010 2012 {
			forvalues age = 0/100 {
					forvalues sex = 1/2 {
					!qsub -N `dataset'`year'_`age'_`sex' -pe multi_slot 8 -l mem_free=16 "/FILEPATH" "`year' `dataset' `age' `sex'"
				}
			}
		}
	}
}

// Aggregate outputs by ME for modelers
if `pre_nr_aggregate' == 1 {
	foreach bid of local bundles {
		!qsub -N BID_`bid' -pe multi_slot 4 -l mem_free=8 "/FILEPATH" "`bid'"
	}
}

if `nr_and_save' == 1 {
	// Aggregate files for noise reduction
	if `inp_int' == 0 {
		local output_dir = "/FILEPATH"
	}
	if `inp_int' == 1 {
		local output_dir = "/FILEPATH"
	}
	if `inp_int' == 2 {
		local output_dir = "/FILEPATH"
	}
	if `inp_int' == 3 {
		local output_dir = "/FILEPATH"
	}
	local files: dir "`output_dir'" files "*.dta"
	clear
	tempfile all_files
	save `all_files', emptyok
	foreach file of local files {
		di "appending `file'..."
		append using "`output_dir'/`file'"
		count if year == .
		if r(N) != 0 {
			di "`file' HAS MISSING YEAR!!!!"
			BREAK
		}
	}
	// special data prep for endocarditis and rhd
	if `inp_int' == 3 {
		drop if acause != "cvd_endo#121"
	}
	save "/FILEPATH", replace

	do "`repo'/launch_noise_reduction.do" _Marketscan_incidence 2018_03_31 guest

	// Aggregate post-NR
	local date march_31_2018
	local measure incidence
	local short_measure inc
	if `inp_int' == 3 {
		local bid = 121
		!qsub -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "/FILEPATH" "`bid' `date' `measure' `short_measure' `inp_int' `write_dir'"
	}
	else {
		foreach bid of local bundles {
			!qsub -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "/FILEPATH" "`bid' `date' `measure' `short_measure' `inp_int' `write_dir'"
		sleep 500
		}
	//}
	}
}
clear
log close log 

// END
