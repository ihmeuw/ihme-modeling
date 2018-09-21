// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose: Generate schemes which will be used in regressions
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
		// Database connection
			local dsn "strConnection"

		// Scheme
			local scheme "`1'"
			
		** local scheme 2_sexes_5_ages

		// Input folder
			local input_folder "$prefix/WORK/03_cod/01_database/02_programs/redistribution/regression_proportions"

		// Output folder
			local output_folder "`input_folder'/_schemes"
			capture mkdir "`output_folder'"
			
** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Make schemes
		use "`output_folder'/_scheme_TEMPLATE.dta", clear
		// All
		if "`scheme'" == "all" {
			// Location
				gen group_location = 1
				gen scheme_location = "All"
			// Year
				gen group_year = 1
				gen scheme_year = "All"
			// Sex
				gen group_sex = 3
				gen scheme_sex = "Both"
			// Age
				gen group_age = 99
				gen scheme_age = "All"
		}
		// 2 Sexes
		if "`scheme'" == "2_sexes" {
			// Location
				gen group_location = 1
				gen scheme_location = "All"
			// Year
				gen group_year = 1
				gen scheme_year = "All"
			// Sex
				gen group_sex = sex
				gen scheme_sex = "Male" if group_sex == 1
				replace scheme_sex = "Female" if group_sex == 2
			// Age
				gen group_age = 99
				gen scheme_age = "All"
		}
		// 6 age groups
		if "`scheme'" == "6_ages" {
			// Location
				gen group_location = 1
				gen scheme_location = "All"
			// Year
				gen group_year = 1
				gen scheme_year = "All"
			// Sex
				gen group_sex = 3
				gen scheme_sex = "Both"
			// Age
				gen group_age = .
				replace group_age = 0 if age < 15
				replace group_age = 15 if age >= 15 & age < 50
				replace group_age = 50 if age >= 50 & age < 60
				replace group_age = 60 if age >= 60 & age < 70
				replace group_age = 70 if age >= 70 & age < 80
				replace group_age = 80 if age >= 80
				gen scheme_age = "<15" if group_age == 0
				replace scheme_age = "15-50" if group_age == 15
				replace scheme_age = "50-60" if group_age == 50
				replace scheme_age = "60-70" if group_age == 60
				replace scheme_age = "70-80" if group_age == 70
				replace scheme_age = "80+" if group_age == 80
		}
		* if "`scheme'" == "6_ages" {
		* 	// Location
		* 		gen group_location = 1
		* 		gen scheme_location = "All"
		* 	// Year
		* 		gen group_year = 1
		* 		gen scheme_year = "All"
		* 	// Sex
		* 		gen group_sex = 3
		* 		gen scheme_sex = "Both"
		* 	// Age
		* 		gen group_age = .
		* 		replace group_age = 0 if age < 15
		* 		replace group_age = 15 if age >= 15 & age < 30
		* 		replace group_age = 30 if age >= 30 & age < 45
		* 		replace group_age = 45 if age >= 45 & age < 60
		* 		replace group_age = 60 if age >= 60 & age < 75
		* 		replace group_age = 75 if age >= 75
		* 		gen scheme_age = "<15" if group_age == 0
		* 		replace scheme_age = "15-29" if group_age == 15
		* 		replace scheme_age = "30-44" if group_age == 30
		* 		replace scheme_age = "45-59" if group_age == 45
		* 		replace scheme_age = "60-74" if group_age == 60
		* 		replace scheme_age = "75+" if group_age == 75
		* }
		// 2 Sexes and 6 age groups
		if "`scheme'" == "2_sexes_6_ages" {
			// Location
				gen group_location = 1
				gen scheme_location = "All"
			// Year
				gen group_year = 1
				gen scheme_year = "All"
			// Sex
				gen group_sex = sex
				gen scheme_sex = "Male" if group_sex == 1
				replace scheme_sex = "Female" if group_sex == 2
			// Age
				gen group_age = .
				replace group_age = 0 if age < 15
				replace group_age = 15 if age >= 15 & age < 30
				replace group_age = 30 if age >= 30 & age < 45
				replace group_age = 45 if age >= 45 & age < 60
				replace group_age = 60 if age >= 60 & age < 75
				replace group_age = 75 if age >= 75
				gen scheme_age = "<15" if group_age == 0
				replace scheme_age = "15-29" if group_age == 15
				replace scheme_age = "30-44" if group_age == 30
				replace scheme_age = "45-59" if group_age == 45
				replace scheme_age = "60-74" if group_age == 60
				replace scheme_age = "75+" if group_age == 75
		}
	// Save
		compress
		save "`output_folder'/_scheme_`scheme'.dta", replace

	capture log close

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
