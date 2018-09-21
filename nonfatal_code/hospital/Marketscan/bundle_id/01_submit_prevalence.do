// submit prevalence extraction jobs

if c(os) == "Unix" {
	local prefix "/home/j"
	global prefix "/home/j"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "J:"
	global prefix "J:"
}
set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix'FILEPATH"
adopath + "`prefix'FILEPATH"
local root "`prefix'FILEPATH"
local repo = "FILEPATH"

use "$prefix/FILEPATH", clear
drop if measure != "prev"
levelsof bundle_id, l(bundles)

// Options
local initial_split = `1'
local calculate_otp_visits = `2'
local make_sample_size = `3'
local map_and_collapse = `4'
local pre_nr_aggregate = `5'
local nr_and_save = `6' 
	local inp_int = `7'
local collapse_otp = `8'
local map_and_collapse_HF = `9' 

// Split initial files to be smaller by age/sex
// This will split for BOTH the prev and inc tracks
if `initial_split' == 1 {
	foreach year in 2000 2010 2012 {
		foreach dataset in ccae mdcr {
	 		if "`dataset'" == "ccae" & `year' != 2000 {
	 			!qsub -P proj_hospital -N new_split_`dataset'`year' -pe multi_slot 70 -l mem_free=140g "FILEPATH" "FILEPATH" "`year' `dataset'"
	 		}
	 		else {
	 			!qsub -P proj_hospital -N new_split_`dataset'`year' -pe multi_slot 20 -l mem_free=40g "FILEPATH" "FILEPATH" "`year' `dataset'"
	 		}
	 	}
	}
}

// Calculate outpatient prevalence average visits for hospital correction
if `calculate_otp_visits' == 1 {
	foreach year in 2000 2010 2012 {
		foreach dataset in ccaeo mdcro {
			forvalues age = 0/100 {
				forvalues sex = 1/2 {
					!qsub -P proj_codprep -N `dataset'`year'`age'`sex' -pe multi_slot 4 -l mem_free=8 "`repo'/FILEPATH" "`repo'/FILEPATH" "`year' `dataset' `age' `sex'"
				}
			}
		}
	}
}

// Collapse otp average visits and save
if `collapse_otp' == 1 {
	local otp_dir = "FILEPATH"
	local files: dir "`otp_dir'" files "*.dta"
	clear
	tempfile all_files
	save `all_files', emptyok
	foreach file of local files {
		di "appending `file'..."
		append using "`otp_dir'/`file'"
	}
	fastcollapse unique_encounter cases, type(sum) by(sex age_start age_end me_id)
	gen otp_vists = unique_encounter / cases 
	outsheet using "FILEPATH", comma names replace
}

// Reshape/collapse prevalence files
// this is deprecated, we map with our claims process in python
if `map_and_collapse' == 1 {
	foreach year in 2000 2010 2012 {
	//foreach year in 2010 2012 {
	//foreach year in 2000 {
		foreach dataset in ccae mdcr {
			forvalues age = 0/100 {
				forvalues sex = 1/2 {
					!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N `dataset'`year'_`age'_`sex' -pe multi_slot 8 -l mem_free=16  "FILEPATH" "`repo'/01b_parallel_prevalence_optimize.do" "`year' `dataset' `age' `sex'"
				}
			}
		}
	}
}

// OR map to HF codes
if `map_and_collapse_HF' == 1 {
	foreach year in 2000 2010 2012 {
		foreach dataset in ccae mdcr {
			forvalues age = 0/100 {
				forvalues sex = 1/2 {
					!qsub -P proj_hospital -N `dataset'`year' -pe multi_slot 8 -l mem_free=16 "FILEPATH" "FILEPATH" "`year' `dataset' `age' `sex'"  // changed code location
					//di `year' "`dataset'" `age' `sex'
				}
			}
		}
	}
}

// Save single age sample sizes for all jobs
if `make_sample_size' == 1 {
	foreach data_year in 2000 2010 2012 {
		di "saving `dataset'..."
		quietly {
		use "`prefix'/FILEPATH", clear 
		replace age = 100 if age > 100 
		gen age_start = age
		gen age_end = age
		gen sample_size = 1
		fastcollapse sample_size, type(sum) by(sex age_start age_end year egeoloc)
		save "FILEPATH", replace
		}
	}
}

// Aggregate collapsed prevalence files by bid
if `pre_nr_aggregate' == 1 {
	foreach bid of local bundles {
		!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N BID_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/01c_aggregate_prevalence_nocollapse.do" "`bid'"
	}
}

if `nr_and_save' == 1 {
	//foreach inp_int of numlist 0 1 2 {
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
	if `inp_int' == 4 {
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
	if `inp_int' == 3 | `inp_int' == 4 {
		drop if acause != "cvd_rhd#113"
	}
	save "FILEPATH", replace
	do "FILEPATH" _Marketscan_prevalence 2017_05_19 guest

	// Aggregate post-noise reduction for modelers
	local date may_19_2017
	local measure prevalence
	local short_measure prev
	if `inp_int' == 3 | `inp_int' == 4 {
		local bid = 113
		!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/aggregate_post_noise_reduction.do" "`bid' `date' `measure' `short_measure' `inp_int'"
	}
	else {
		foreach bid of local bundles {
			// change shell
			!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N post_nr_`bid' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "`repo'/aggregate_post_noise_reduction.do" "`bid' `date' `measure' `short_measure' `inp_int'"
			}
		//}
	}
}

clear
// END
