/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER         
Output:           Process GBD2013 EG Draws for Extrapolation
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
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
	local step 01
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
	foreach dir temp log progress {
		capture mkdir `dir'_dir
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/01_preprocessing_data_log_`date'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Get GBD Info
====================================================================*/

*--------------------1.1: Location Metadata
		
	get_location_metadata, location_set_id(35) clear
		keep ihme_loc_id location_id location_name
		save "`local_tmp_dir'/geonames.dta", replace


/*====================================================================
                        2: Import GBD 2013 EG Draws
====================================================================*/


*--------------------2.1: Import OCP Draws

	insheet using "`in_dir'/GBD 2013 onchocerciasis OCP draws.csv", clear double
	tempfile gbd_2013_ocp
	save `gbd_2013_ocp', replace

*--------------------2.2: Import APOC Draws
	
	insheet using "`in_dir'/GBD 2013 onchocerciasis APOC draws.csv", clear
	tempfile gbd_2013_apoc
	save `gbd_2013_apoc', replace


/*====================================================================
                        3: Format GBD 2013 EG Draws for OCP and APCO (loop)
====================================================================*/

	*Set up looping over data origin
		local dataset ocp apoc

		foreach set in `dataset'{
			use `gbd_2013_`set'', clear
			
*--------------------3.1: Get GBD location ids

			*Format names of locations to facilitate merge
				* OCP:
					replace location_name = "Guinea-Bissau" if location_name == "Guinea Bissau"
				
				* APOC:
					replace location_name = "Equatorial Guinea" if location_name == "Eq.Guinea"
					replace location_name = "Democratic Republic of the Congo" if location_name == "RDC"
					replace location_name = "Central African Republic" if location_name == "CAR"

			*Join with GBD Metadata to get location ids
				joinby location_name using "`local_tmp_dir'/geonames.dta", unmatched(none)
				drop location_name ihme_loc_id

*--------------------3.2: Rename/format variables
			
			*Rename variables to match updated GBD variable names
				rename year year_id
				rename sex sex_id
				
			*Format blindness values
				capture replace blindcases = "0" if blindcases == "Inf"
				cap destring blindcases, replace

*--------------------3.3: Convert to GBD2016 Age Groups

			*Convert old GBD 2013 age groups to new GBD 2016 age groups
				gen age_group_id=(age/5)+5
				replace age_group_id=5 if age==1
				replace age_group_id=28 if age==0
					*missing:	age=0.01	%>%		age_group_id=3
					*			age=0.1		%>%		age_group_id=4
					*			age=80		%>%		age_group_id=30 (instead of age=80+, age_group_id=21)
					*			age=85		%>%		age_group_id=31
					*			age=90		%>%		age_group_id=32
					*			age=95+		%>%		age_group_id=235
			
				drop age

*--------------------3.4: Reshape Data w/ Draws Long

			*Define locals for location anmes and outcomes
				*levelsof location_id,local(`set'_locs) clean
				local outcomes wormcases mfcases blindcases vicases osdcases1acute osdcases1chron osdcases2acute osdcases2chron osdcases3acute osdcases3chron
			
			*Reshape long so that causes are listed vertically - faster by tempfiles than using "reshape" command
				tempfile rawclean
				save `rawclean', replace
		
				tempfile longfile
				local i 1
				
				foreach var in `outcomes' {
					use `rawclean', clear
					keep location_id year_id age_group_id sex_id draw `var'
					gen outvar="`var'"
					rename `var' cases
					
					if `i'>1 append using `longfile'
					save `longfile', replace
					local ++i
				}
				
				use `longfile', clear		

*--------------------3.5: Reshape Data w/ Outcomes Wide
		
			*Reshape so that draws are wide (all differences accounted for in loc/year/age/sex/outvar) 
				tempfile widefile
				local a 1
				use `longfile', clear
				
				foreach outcome in `outcomes'{
					preserve
					
						quietly keep if outvar=="`outcome'"
						reshape wide cases, i(location_id age_group_id sex_id year_id outvar) j(draw)
						save "`local_tmp_dir'/reshaped_`set'_`outcome'.dta", replace
						
						if `a'>1 append using `widefile'
						save `widefile', replace
						local ++a
						
					restore
					drop if outvar=="`outcome'"
				}
					
*--------------------3.6: Reshape Data w/ Outcomes Wide	
				
			*SAVE FOR USE:
				use `longfile', clear
				save "`in_dir'/`set'_GBD2016_PreppedData.dta", replace

		}




log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.

