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
					global prefix "FILEPATH"
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
		// Database connection
			local dsn "ADDRESS"
			
		// Input data name
			local input_data_name "`1'"
			
		// Input source name
			local input_source "`2'"
			
		// Codebook type
			local codebook_name "`3'"
			
		// Username
			local username "`4'"
			
			
		// Input folder
			local input_folder "FILEPATH"
			
		
		// Temp folder
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			local temp_folder "FILEPATH"
		

				
** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Get data
		use "FILEPATH", clear
		gen codebook_name = "`codebook_name'"
	// Format causes
		gen orig_codebook = codebook_name
		replace codebook_name = "GBD" if acause != "_gc"
		replace cause = acause if codebook_name == "GBD"
	// Reshape age long
		keep iso3 location_id year sex cause codebook_name orig_codebook deaths*
		collapse (sum) deaths*, by(iso3 location_id year sex cause codebook_name orig_codebook) fast
		reshape long deaths, i(iso3 location_id year sex cause codebook_name orig_codebook) j(gbd_age)
		gen age = (gbd_age - 6) * 5 if gbd_age >= 7 & gbd_age <= 26
		replace age = 0 if gbd_age == 91
		replace age = 0.01 if gbd_age == 93
		replace age = 0.1 if gbd_age == 94
		replace age = 1 if gbd_age == 3
		drop if age == .
	// Make unique
		rename deaths metric
		drop if metric == 0
		collapse (sum) metric, by(year sex age cause codebook_name orig_codebook iso3 location_id) fast
	// Save
		compress
		save "FILEPATH", replace
	

	capture log close

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
