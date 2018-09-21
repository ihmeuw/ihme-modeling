/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2016 GATHER        
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 02
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH

** Set locals from arguments passed on to job
		local cause `1'
		local intensity `2'
		local loc `3'
		local sex `4'
		local i `loc'

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/interpolate_years__`location'_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



		tempfile pre_format mergingTemp  


	// Load draw data for years between which will be interpolated for prevalence (measure id = 5)
		foreach year in 1990 2005 {
			quietly insheet using "`tmp_dir'/`cause'_`intensity'/5_`i'_`year'_`sex'.csv", clear double

			quietly reshape long draw_, i(age) j(num_draw)

			sort age_group_id draw_
			rename draw_ dr`year'
			generate mu`year' = .

			levelsof age_group_id, local(ages)

			foreach age in `ages' {
				capture quietly summarize dr`year' if age_group_id == `age'
				capture replace mu`year' = `r(mean)' if age_group_id == `age'
			}

			bysort age_group_id: replace num_draw = _n-1

			capture merge 1:1 age_group_id num_draw using `mergingTemp', nogen
			save `mergingTemp', replace
		}

	
		summarize num_draw
		local draw_max = `r(max)'


		generate double rate_ann = (mu2005/mu1990)^(1/15)
		generate double dr1995 = dr1990 * rate_ann^5
		generate double dr2000 = dr1990 * rate_ann^10
		generate double dr2004 = dr1990 * rate_ann^14
		replace dr1995 = 0 if missing(dr1995)
		replace dr2000 = 0 if missing(dr2000)
		replace dr2004 = 0 if missing(dr2004)
		drop m* rate_ann
		quietly reshape wide dr*, i(age_group_id) j(num_draw)

		save `pre_format', replace


	// Export each year as a separate csv file.
		foreach year in 1995 2000 2004{
			use `pre_format', clear

			keep dr`year'* age_group_id
			rename dr`year'* draw_*

			format %16.0g age_group_id draw_*

			outsheet using "`tmp_dir'/`cause'_`intensity'/5_`i'_`year'_`sex'.csv", replace comma
		}








log close
exit
/* End of do-file */
