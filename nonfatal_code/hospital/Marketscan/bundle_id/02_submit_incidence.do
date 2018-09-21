// submit prevalence extraction jobs

if c(os) == "Unix" {
	local prefix "FILEPATH"
	global prefix "FILEPATH"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "FILEPATH"
	global prefix "FILEPATH"
}
set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix'/FILEPATH"
adopath + "`prefix'/FILEPATH"
local root "`prefix'/FILEPATH"
local repo = "FILEPATH"

// get a list of bundle IDs
	use "$prefix/FILEPATH", clear
	drop if measure != "inc"
	levelsof bundle_id, l(bundles)

// Options
local initial_split = `1'
local map_and_collapse = `2'
local pre_nr_aggregate = `3'
local nr_and_save = `4'
	local inp_int = `5'

// Split to age/sex for ease
// NOTE this is deprecated for gbd 2016, use 01_submit_prevalence to split files
if `initial_split' == 1 {
	foreach year in 2000 2010 2012 {
		foreach dataset in ccae mdcr {
	 	!qsub -N split_`dataset'`year' -pe multi_slot 100 -l mem_free=200 "FILEPATH" "`repo'FILEPATH" "`year' `dataset'"
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
					!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N `dataset'`year'_`age'_`sex' -pe multi_slot 8 -l mem_free=16 "FILEPATH" "`repo'/02b_parallel_incidence_optimize.do" "`year' `dataset' `age' `sex'"
				}
			}
		}
	}
}

// Aggregate outputs by ME for modelers
if `pre_nr_aggregate' == 1 {
	foreach bid of local bundles {
		!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N BID_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/02c_aggregate_incidence_nocollapse.do" "`bid'"
	}
}

if `nr_and_save' == 1 {
		// Aggregate files for noise reduction
	if `inp_int' == 0 {
		local output_dir = "FILEPATH"
	}
	if `inp_int' == 1 {
		local output_dir = "FILEPATH"
	}
	if `inp_int' == 2 {
		local output_dir = "FILEPATH"
	}
	if `inp_int' == 3 {
		local output_dir = "FILEPATH"
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
	save "FILEPATH", replace
	do "FILEPATH" _Marketscan_incidence 2017_05_19 guest

	// Aggregate post-NR
	local date MM_DD_YYY
	local measure incidence
	local short_measure inc
	if `inp_int' == 3 {
		local bid = 121
		!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/aggregate_post_noise_reduction.do" "`bid' `date' `measure' `short_measure' `inp_int'"
	}
	else {
		foreach bid of local bundles {
			!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/aggregate_post_noise_reduction.do" "`bid' `date' `measure' `short_measure' `inp_int'"
		}
	//}
	}
}
clear
log close log
