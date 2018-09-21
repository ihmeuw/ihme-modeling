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
	local step 01b
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


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/01b_preprocess_WHOPCTdata_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Location Metadata

	get_location_metadata,location_set_id(35) clear
		
	keep if location_type=="admin0"
	keep location_id location_name ihme_loc_id
	
	save "`local_tmp_dir'/`step'_metadata.dta", replace


/*====================================================================
                        2: Load and Format WHO PCT Data
====================================================================*/

	insheet using "`in_dir'/who_pct_databank_coverage_STH_2016.csv", clear double

*--------------------2.1: Format locations

	*Rename variable to GBD name	
		rename country location_name

	*Rename country names to facilitate merge with location metadata
		replace location_name="Bolivia" if location_name=="Bolivia (Plurinational State of)"
		replace location_name="Brunei" if location_name=="Brunei Darussalam"
		replace location_name="Cape Verde" if location_name=="Cabo Verde"
		replace location_name="Cote d'Ivoire" if location_name=="C⌠te d'Ivoire"
		replace location_name="North Korea" if location_name=="Democratic People's Republic of Korea"
		replace location_name="The Gambia" if location_name=="Gambia"
		replace location_name="Laos" if location_name=="Lao People's Democratic Republic"
		replace location_name="Malaysia" if location_name=="Malasya"
		replace location_name="Federated States of Micronesia" if location_name=="Micronesia (Federated States of)"
		replace location_name="Moldova" if location_name=="Republic of Moldova"
		replace location_name="Macedonia" if location_name=="The former Yugoslav Republic of Macedonia"
		replace location_name="Tanzania" if location_name=="United Republic of Tanzania"
		replace location_name="Venezuela" if location_name=="Venezuela (Bolivarian Republic of)"
		replace location_name="Vietnam" if location_name=="Viet Nam"		

	*Merge with metadata to get location Ids
		merge m:1 location_name using "`local_tmp_dir'/`step'_metadata.dta", keep (matched) nogen


*--------------------2.2: Format variables

	*Cleaning - rename variables
		rename populationrequiringpcforsthpresa popPreSAC
		rename numberofpresactargeted targetPreSAC
		rename reportednumberofpresactreated treatPreSAC
		rename drugusedpresac drugPreSAC
		rename programmecoveragepresac progCovPreSAC
		rename nationalcoveragepresac natCovPreSAC
		rename populationrequiringpcforsthsac popSAC
		rename numberofsactargeted targetSAC
		rename reportednumberofsactreated treatSAC
		rename drugusedsac drugSAC
		rename programmecoveragesac progCovSAC
		rename nationalcoveragesac natCovSAC

	*Cleaning - change odd non-numeric values/strings and destring vars for use
		local numvars popPreSAC targetPreSAC treatPreSAC popSAC targetSAC treatSAC
		local percvars progCovPreSAC natCovPreSAC progCovSAC natCovSAC
		foreach var in `numvars'{
			replace `var'="" if `var'=="-" | `var'=="To be defined" | `var'=="No data available"
			replace `var'="1" if `var'=="No PC required"
			destring `var', replace
		}
		foreach var in `percvars'{
			replace `var'="" if `var'=="-"
			split `var',p(%)
			replace `var'=`var'1
			drop `var'1
			destring `var', replace
			replace `var'=`var'/100
		}

*--------------------2.3: Calculate Missing Measures

	*Recalculate national coverage since it is not uniformly calculated in the table
		replace natCovPreSAC=treatPreSAC/popPreSAC
		replace natCovPreSAC=1 if natCovPreSAC>1
	
		replace natCovSAC=treatSAC/popSAC
		replace natCovSAC=1 if natCovSAC>1
	
	*Replace missing coverage with zero
		replace natCovPreSAC = 0 if missing(natCovPreSAC)
		replace natCovSAC = 0 if missing(natCovSAC)

	*Drop if population did not require PC
		drop if popPreSAC==1 | popSAC==1

*--------------------2.4: Save

	save "`in_dir'/`step'_whopctdatabank.dta", replace




log close
exit
/* End of do-file */
