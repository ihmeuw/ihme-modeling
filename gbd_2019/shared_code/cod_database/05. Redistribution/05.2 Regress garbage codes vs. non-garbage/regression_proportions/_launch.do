		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "/home/j"
					global prefixh "FILEPATH"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
					global prefixh "H:"
				}

			// Set up PDF maker
				do "FILEPATH"

			// Get date
				local today = date(c(current_date), "DMY")
				local year = year(`today')
				local month = string(month(`today'),"%02.0f")
				local day = string(day(`today'),"%02.0f")
				local today = "`year'_`month'_`day'"


	** ****************************************************************
	** DEFINE LOCALS
	** ****************************************************************
		// Username
			local username
		
		// Location set version
			local location_set_version 46
			
		// Date
			local today ""
			
		// Schemes
			local schemes 2_sexes_6_ages
			** local schemes all 2_sexes 6_ages 2_sexes_6_ages
			
		// ICD input sources
			local input_sources ICD_cod
			
		// Specific input source for regressions
			local input_data_name ICD_cod
			
		// Geographic levels
			** local geographic_levels global super_region region
			
		// Set which processes to run
			local 01_prep_data				= 1
			local 01_prep_scheme_template	= 0
			local 01_prep_schemes			= 0
			local 02_generate_proportions	= 1
			local 03_rescale_proportions	= 1
			local 04_assemble_proportions	= 1
		
		// Code folder
			local code_folder "FILEPATH"	
		
		// Input folder
			local input_folder "FILEPATH"

		// Output folder
			local output_folder "FILEPATH"
			capture mkdir "`output_folder'"	
			
			
		// Packages
			use "FILEPATH", clear
			levelsof(computed_package_id), local(regression_packages) clean
			
			

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Prep data
	if `01_prep_data' == 1 {
		// Submit
		foreach input_source of local input_sources {
			capture rm "FILEPATH"
			capture rm "FILEPATH"
			!FILEPATH  -P  -pe multi_slot 30 -l mem_free=60g -N "RDP_prep_`input_source'" "FILEPATH" "FILEPATH" "`location_set_version' `input_source' `username'"
		}
		// Check
		foreach input_source of local input_sources {
			local checkfile "FILEPATH"
			display "Checking for `checkfile'"
			capture confirm file "`checkfile'"
			while _rc {
				sleep 5000
				capture confirm file "`checkfile'"
			}
			display "Found!"
		}
	}
	
	// Prep scheme template
	if `01_prep_scheme_template' == 1 {
		// Submit
			!FILEPATH  -P  -pe multi_slot 5 -l mem_free=10g -N "RDP_prep_scheme_template" "FILEPATH" "FILEPATH" "`location_set_version'"
		// Check
			local checkfile "FILEPATH"
			display "Checking for `checkfile'"
			capture confirm file "`checkfile'"
			while _rc {
				sleep 5000
				capture confirm file "`checkfile'"
			}
			display "Found!"
	}
	
	// Prep schemes
	if `01_prep_schemes' == 1 {
		// Submit
		foreach scheme of local schemes {
			capture rm "FILEPATH"
			!FILEPATH  -P -pe multi_slot 5 -l mem_free=10g -N "RDP_prep_`scheme'" "FILEPATH" "FILEPATH" "`scheme'"
		}
		// Check
		foreach scheme of local schemes {
			local checkfile "FILEPATH"
			display "Checking for `checkfile'"
			capture confirm file "`checkfile'"
			while _rc {
				sleep 5000
				capture confirm file "`checkfile'"
			}
			display "Found!"
		}
	}
	
	if `02_generate_proportions' == 1 {
		// Submit
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				capture confirm file "FILEPATH"
				if _rc {
					!FILEPATH -P  -pe multi_slot 10 -l mem_free=20g -N "RDP_regress_`package'_`scheme'" "FILEPATH" "FILEPATH" "`username' `scheme' `package' `input_data_name' `today'"
					sleep 7000
				}
			}
		}
		// Check
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				local checkfile "FILEPATH"
				display "Checking for 02_predicted for `package' `scheme'"
				capture confirm file "`checkfile'"
				while _rc {
					sleep 30000
					capture confirm file "`checkfile'"
				}
				display "Found!"
			}
		}
	}
	
	// Rescale proportions
	if `03_rescale_proportions' == 1 {
		// Submit
		foreach package of local regression_packages {
			foreach scheme of local schemes {

				capture confirm file "FILEPATH"
				if _rc {
					!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "RDP_rescale_`package'_`scheme'" "FILEPATH" "FILEPATH" "`username' `scheme' `package' `input_data_name' `today'"
				}
				
			}
		}
		// Check
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				local checkfile "FILEPATH"
				display "Checking for 03_rescaled for `package' `scheme'"
				capture confirm file "`checkfile'"
				while _rc {
					sleep 30000
					capture confirm file "`checkfile'"
				}
				display "Found!"
			}
		}
	}
	
	// Assemble
	if `04_assemble_proportions' == 1 {
		// Submit jobs
		foreach package of local regression_packages {
			capture rm "FILEPATH"
			capture confirm file "FILEPATH"
			if _rc {
				foreach scheme of local schemes {
					di "Assembling package `package'"
					quietly do "FILEPATH" `username' `scheme' `package' `input_data_name' `today'
				}
			}
		}
	}
	
	
	capture log close

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
