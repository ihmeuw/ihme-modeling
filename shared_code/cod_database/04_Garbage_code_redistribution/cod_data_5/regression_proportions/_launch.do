// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:Master run script for RDP regressions
** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the J drive (data), and setting a local for the date.
	**
	** ****************************************************************
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
					global prefixh "/homes/strUser"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
					global prefixh "H:"
				}

			// Set up PDF maker
				do "$prefix/Usable/Tools/ADO/pdfmaker_Acrobat10.do"

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
			local username strUser
		
		// Location set version
			local location_set_version 46
			
		// Date
			local today "strDate"
			
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
			local code_folder "$prefix/WORK/03_cod/01_database/02_programs/redistribution/regression_proportions"	
		
		// Input folder
			local input_folder "$prefix/WORK/03_cod/01_database/02_programs/redistribution/regression_proportions"

		// Output folder
			local output_folder "`input_folder'/_input_data"
			capture mkdir "`output_folder'"	
			
			
		// Packages
			use "`input_folder'/regression_input.dta", clear
			levelsof(computed_package_id), local(regression_packages) clean
			
		

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Prep data
	if `01_prep_data' == 1 {
		// Submit
		foreach input_source of local input_sources {
			capture rm "`input_folder'/_input_data/`input_source'.dta"
			capture rm "`input_folder'/_input_data/`input_source'_person_years.dta"
			!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 30 -l mem_free=60g -N "RDP_prep_`input_source'" "`code_folder'/shellstata.sh" "`code_folder'/01_prep_data__master.do" "`location_set_version' `input_source' `username'"
		}
		// Check
		foreach input_source of local input_sources {
			local checkfile "`input_folder'/_input_data/`input_source'_person_years.dta"
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
			!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "RDP_prep_scheme_template" "`code_folder'/shellstata.sh" "`code_folder'/01_prep_scheme_template.do" "`location_set_version'"
		// Check
			local checkfile "`output_folder'/_scheme_TEMPLATE.dta"
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
			capture rm "`input_folder'/_schemes/_scheme_`scheme'.dta"
			!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "RDP_prep_`scheme'" "`code_folder'/shellstata.sh" "`code_folder'/01_prep_schemes.do" "`scheme'"
		}
		// Check
		foreach scheme of local schemes {
			local checkfile "`input_folder'/_schemes/_scheme_`scheme'.dta"
			display "Checking for `checkfile'"
			capture confirm file "`checkfile'"
			while _rc {
				sleep 5000
				capture confirm file "`checkfile'"
			}
			display "Found!"
		}
	}
	
	// Generate proportions 
	if `02_generate_proportions' == 1 {
		// Submit
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				capture confirm file "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/`scheme'/`today'/02_predicted_`package'.dta"
				if _rc {
					!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 10 -l mem_free=20g -N "RDP_regress_`package'_`scheme'" "`code_folder'/shellstata.sh" "`code_folder'/02_generate_proportions_submit.do" "`username' `scheme' `package' `input_data_name' `today'"
					sleep 7000
				}
			}
		}
		// Check
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				local checkfile "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/`scheme'/`today'/02_predicted_`package'.dta"
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
				capture confirm file "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/`scheme'/`today'/03_rescaled_`package'.dta"
				if _rc {
					!/usr/local/bin/SGE/bin/lx24-amd64/qsub  -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "RDP_rescale_`package'_`scheme'" "`code_folder'/shellstata.sh" "`code_folder'/03_rescale_proportions.do" "`username' `scheme' `package' `input_data_name' `today'"
				}
				
			}
		}
		// Check
		foreach package of local regression_packages {
			foreach scheme of local schemes {
				local checkfile "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/`scheme'/`today'/03_rescaled_`package'.dta"
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
			capture rm "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/_assembled/`today'/04_assembled.dta"
			capture confirm file "/ihme/cod/prep/RDP_regressions/`input_data_name'/`package'/_assembled/`today'/04_assembled.dta"
			if _rc {
				foreach scheme of local schemes {
					di "Assembling package `package'"
					quietly do "`code_folder'/04_assemble.do" `username' `scheme' `package' `input_data_name' `today'
				}
			}
		}
	}
	
	
	capture log close

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
