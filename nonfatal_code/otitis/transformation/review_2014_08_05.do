// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:	This code template is used for making changes to actual values (mean/upper/lower/SE/ESS) for all lit/non-lit data: age/sex-splitting, ESS-splitting, other transformations
//			This assumes that all raw data has been prepped, checked, and corrected as needed through 01_lit/02_nonlit; this should not be used to make corrections to raw data
//			It's imperative that we preserve the raw data prior to transformation; raw is defined as any lit extractions and non-lit data imported to the database
//			The review process is designed to be run iteratively, as modelers will often want to try multiple transformations on the same data to get the results they desire
//			Each run will download the raw data without the transformations of the previous review upload, so that new transformations will be applied to the same raw data
//			Here is an overview of the steps involved:
//				1. Save this code template renamed with timestamp (do not just use YYYY_MM_DD file because we will push updates to the template), and fill in "functional" and "date" globals
//				2. Download raw data for transformation, which includes only raw data prepped through 01_lit/02_nonlit, and excludes all previously transformed data
//				3. Run transformations on data by duplicating raw rows, marking new rows as is_raw=adjusted, and raw rows to be excluded as is_raw=excluded_review
//				4. Check that data meet template specifications, and save output sheet in 05_upload for upload by data analyst
//				5. The next iteration of the review process will download and restore all the raw data that went into the previous iteration, and drop the adjusted data from that iteration
// Description:	 age/sex-splitting

// set globals (required)
	// define the functional group for which you are prepping data (acause or impairment)
		global functional = "otitis"
	// define the date of the run (YYYY_MM_DD)
		global date = "DATE"
		
// prep stata (shouldn't need to modify except to increase memory)
	global process "03_review"
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
	}
	run "FILEPATH"   
	run "FILEPATH"  
	local code_dir "ADDRESS"
	local out_dir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap log close
	log using "FILEPATH", replace

// download and load lit data (this will grab the latest changes made to the database including marking outliers)
	/*do "FILEPATH" */
	insheet using "FILEPATH", comma clear
	
	// age-sex split data points given by age and by sex separately (example given below)
		// mark each group of datapoints to split with a unique group number: 1, 2, 3, etc.
		gen agesex_split_group = 1 if nid == 85057 & iso3=="AUS" & grouping=="chronic" & data_status=="" 
		agesexsplit ${functional}, date(${date}) tmpdir("ADDRESS")
	
// run checks and save in 05_upload; will break if error such as range check or missing required variable (correct and rerun), otherwise will save for upload by data analyst
	// this script will eventually upload directly to the database but we want to run several iterations of uploads centrally so we can build the proper checks; there are a ton of things that can go wrong during upload
	do "FILEPATH"
	cap log close
