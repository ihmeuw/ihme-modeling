/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2016 GATHER       
Output:            Preprocess India Deworming Data from GBD Collaborator for STH prevalence extrapolation
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
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 01d
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
	*directory for output of draws
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


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/01d_preprocess_IndiaDeworming_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Location Metadata

	get_location_metadata,location_set_id(35) clear
		preserve
			keep if most_detailed==1
			levelsof location_id,local(gbdlocs) clean
		restore
		
		save "`local_tmp_dir'/`step'_metadata.dta", replace

	*Prepare map for location ids
		split path_to_top_parent,p(",")
		keep if path_to_top_parent4=="163"
		split location_name,p(", ")
		rename location_name original_location_name
		rename location_name1 location_name
		keep if location_name2!=""
		keep location_id location_name original_location_name
		save "`local_tmp_dir'/`step'_indiaIDs.dta", replace

/*====================================================================
                        2: Load and Clean Extrated India Deworming Survey Data
====================================================================*/


*--------------------2.1: Load RSOC Survey

	*Import
		import excel "FILEPATH/170116 India Deworming Coverage Data.xlsx", sheet("RSOC data") clear
	
	*Format
		drop in 1/4
		rename E location_name
		rename F natCovPreSAC2013
		drop if location_name=="All-India"
	
		destring natCovPreSAC2013, replace
		drop if natCovPreSAC2013==.
	
	*Assume the same coverage level across the two years since survey cross the 2 years
		gen natCovPreSAC2014=natCovPreSAC2013
	
	*Save
		save "`local_tmp_dir'/`step'_loadrsoc.dta", replace


*--------------------2.2: Load NDD Survey

	*Import
		import excel "FILEPATH/170116 India Deworming Coverage Data.xlsx", sheet("NDD") clear
	
	*basic formatting of table
		rename D location_name
		rename F natCovSAC20151
		rename H natCovSAC20161
		rename J natCovSAC20162
		destring natCovSAC2015 natCovSAC20161 natCovSAC20162 E G I, replace force
		drop in 1/7
	
	*Sum 6 minor territories into one unit
		preserve
			keep if location_name=="A&n Islands" | location_name=="D&n Haveli" | location_name=="Daman & Diu" | location_name=="Lakshadweep" | location_name=="Chandigarh" | location_name=="Puducherry"
			foreach covperc in natCovSAC20151 natCovSAC20161 natCovSAC20162{
				replace `covperc'=`covperc'/100
			}
			foreach txnum in E G I{
				replace `txnum'=`txnum'*100000
				*lakh=100,000
			}
			gen target20151=E/natCovSAC20151
			gen target20161=G/natCovSAC20161
			gen target20162=I/natCovSAC20162
			collapse (sum) E G I target20151 target20161 target20162
				*sum across all territories
			gen location_name="Union Territories other than Delhi"
			gen natCovSAC20151=E/target20151*100
			gen natCovSAC20161=G/target20161*100
			gen natCovSAC20162=I/target20162*100
				*keep in %*100 space to keep consistent with other data
			keep location_name natCovSAC20151 natCovSAC20161 natCovSAC20162
			gen C="0"
			tempfile sixminor
			save `sixminor', replace
		restore
		drop if location_name=="A&n Islands" | location_name=="D&n Haveli" | location_name=="Daman & Diu" | location_name=="Lakshadweep" | location_name=="Chandigarh" | location_name=="Puducherry"
		append using `sixminor'
	
	*Capture whether or not they have semi-annual or annual MDA
		destring C, replace force
		gen annual_rounds=2
		replace annual_rounds=1 if C>=28
		drop C E G I
	
	*Fix names
		drop if location_name=="" | location_name=="All India"
		replace location_name="Tamil Nadu" if location_name=="Tamil nadu"
		replace location_name="Sikkim" if location_name=="Sikkim "
		replace location_name="Tripura" if location_name=="Tripura "
		replace location_name="Jammu & Kashmir" if location_name=="Jammu & kashmir"
		replace location_name="West Bengal" if location_name=="West Bengal "
	
	*Save
		save "`local_tmp_dir'/`step'_ndd.dta", replace

*--------------------2.3: Merge

	*Merge to rsoc data to make assumptions about missing data for certain years
		merge 1:1 location_name using "`local_tmp_dir'/`step'_loadrsoc.dta", nogen

	*Save
		save "`local_tmp_dir'/`step'_loadcleanextractedsurvey.dta", replace


/*====================================================================
                        3: Extrapolation by Assumption for Missing Data
====================================================================*/

		use "`local_tmp_dir'/`step'_loadcleanextractedsurvey.dta", clear

*--------------------3.1: 

		rename natCovPreSAC2013 natCovPreSAC20131
		gen natCovPreSAC20132=natCovPreSAC20131 if annual_rounds==2
		replace natCovPreSAC20132=0 if annual_rounds==1

		rename natCovPreSAC2014 natCovPreSAC20141
		gen natCovPreSAC20142=natCovPreSAC20141 if annual_rounds==2
		replace natCovPreSAC20142=0 if annual_rounds==1
	
		gen natCovSAC20152=natCovSAC20151 if annual_rounds==2
		replace natCovSAC20152=0 if annual_rounds==1
	
		replace natCovSAC20162=natCovSAC20161 if natCovSAC20162==. & annual_rounds==2
		replace natCovSAC20162=0 if annual_rounds==1
		
		forval x=3/4{
			replace natCovPreSAC201`x'1= natCovPreSAC201`x'2 if natCovPreSAC201`x'1==.
			replace natCovPreSAC201`x'2= natCovPreSAC201`x'1 if natCovPreSAC201`x'2==.
		}
		forval x=5/6{
			replace natCovSAC201`x'1= natCovSAC201`x'2 if natCovSAC201`x'1==.
			replace natCovSAC201`x'2= natCovSAC201`x'1 if natCovSAC201`x'2==.
		}

		forval r=1/2{
			gen natCovPreSAC2015`r'=natCovSAC2015`r'
			gen natCovPreSAC2016`r'=natCovSAC2016`r'
		}
	
		forval y=1/2{
			egen meanto2015`y'=rowmean(natCovPreSAC2016`y' natCovPreSAC2014`y')
			replace natCovPreSAC2015`y'=meanto20151 if natCovPreSAC2015`y'==.
		}
		drop mean*

		egen mean=rowmean(natCovPreSAC20131 natCovPreSAC20132 natCovPreSAC20141 natCovPreSAC20142 natCovPreSAC20151 natCovPreSAC20152 natCovSAC20151 natCovSAC20152 natCovPreSAC20161 natCovPreSAC20162 natCovSAC20161 natCovSAC20162)
		foreach var in natCovPreSAC20131 natCovPreSAC20132 natCovPreSAC20141 natCovPreSAC20142 natCovPreSAC20151 natCovPreSAC20152 natCovSAC20151 natCovSAC20152 natCovPreSAC20161 natCovPreSAC20162 natCovSAC20161 natCovSAC20162{
			replace `var'=mean if `var'==.
		}
		drop mean*

	*Save
		save "`local_tmp_dir'/`step'_assumemissing.dta", replace


/*====================================================================
                        4: Calculate Cumulative Number of Treatments per Person
====================================================================*/


*--------------------4.1: Calculate cumulative number of treatments per person

	*Calculate cumulative number of treatments per person in population requiring PCT over 6 year intervals: 2004-2010, 2010-2016
	preserve
		keep if year >= 2004 & year < 2010
		collapse (sum) natCovPreSAC natCovSAC, by (location_id)
		rename natCovPreSAC cumPreSAC2004
		rename natCovSAC cumSAC2004
		tempfile cum2004
		save `cum2004', replace
	restore
	preserve
		keep if year >= 2010
		collapse (sum) natCovPreSAC natCovSAC, by (location_id)
		rename natCovPreSAC cumPreSAC2010
		rename natCovSAC cumSAC2010
		tempfile cum2010
		save `cum2010', replace
	restore

/*====================================================================
                        5: Format and Export
====================================================================*/

		use "`local_tmp_dir'/`step'_assumemissing.dta", clear

*--------------------5.1:

	*Format names for GBD merge
		replace location_name="Jammu and Kashmir" if location_name=="Jammu & Kashmir"
		replace location_name="Telangana" if location_name=="Telangana "
		
	*Merge with metadata to get location ids
		merge 1:m location_name using "`local_tmp_dir'/`step'_indiaIDs.dta", nogen
	
	*Reshape
		reshape long natCovSAC natCovPreSAC,i(location_name original_location_name location_id annual_rounds) j(round)
	
		replace natCovSAC=natCovPreSAC if natCovSAC==.
	
	*Make year variable
		gen year=.
		forval y=1/2{
			replace year=2013 if round==2013`y'
			replace year=2014 if round==2014`y'
			replace year=2015 if round==2015`y'
			replace year=2016 if round==2016`y'
		}
		forval r=2013/2016{
			replace round=1 if round==`r'1
			replace round=2 if round==`r'2
		}

	*Format
		drop location_name
		rename original_location_name location_name
		drop annual_rounds round
		drop if natCovSAC==. & natCovPreSAC==.
		replace natCovSAC=natCovSAC/100
		replace natCovPreSAC=natCovPreSAC/100
		gen parent_id=163
	
	*save
		save "`in_dir'/`step'_indiamda.dta", replace




log close
exit
/* End of do-file */
