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
// Description:	converting period to point prevalence, excluding data points that are not eligible to be included

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
	/*run "FILEPATH"
	run "FILEPATH" */
	local code_dir "ADDRESS"
	local out_dir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap log close
	log using "FILEPATH", replace

// download and load lit data (this will grab the latest changes made to the database including marking outliers)
	/* do "FILEPATH" */
	insheet using "FILEPATH", comma clear

// For nid==116111, data were extracted for cumulative incidence and period prevalence - drop the former and convert the latter into point prevalence   

replace data_status="excluded" if nid==116111 & parameter_type=="Incidence"
replace issues="cumulative incidence" if nid==116111 & parameter_type=="Incidence"
replace is_raw="excluded_review" if nid==116111 & parameter_type=="Prevalence"
replace issues="period prevalence" if nid==116111 & parameter_type=="Prevalence"
	// create new row with flag new=1 for 0-1 yr olds
			expand 2 in 34, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
			
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev
	// create new row with flag new=1 for 1-2 yr olds
			expand 2 in 39, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev			
	// create new row with flag new=1 for 2-3 yr olds
			expand 2 in 43, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev	
	// create new row with flag new=1 for 3-4 yr olds
			expand 2 in 44, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev	
	// create new row with flag new=1 for 4-5 yr olds
			expand 2 in 35, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev			
	// create new row with flag new=1 for 5-6 yr olds
			expand 2 in 38, gen(new)
			// null out row_id for new row
			replace row_id = . if new == 1
			// Calculate point prevalence based on period prevalence, duration and recall period  
			gen period_prev=mean if new==1
			gen point_prev=period_prev*(21/(21-1+365)) if new==1
			replace mean=point_prev if new==1
			replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
			replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
			replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
			replace data_status=" " if new==1
			replace is_raw="adjusted" if new==1
			drop new period_prev point_prev		
	
	// exclude a data point with very small sample size
			replace data_status="excluded" if sample_size==3 & iso3=="NGA"
			replace issues="very small sample size" if sample_size==3 & iso3=="NGA"
	
	// exclude a data point which is not AOM
			replace data_status="excluded" if nid==103419 & case_name=="Hearing Loss"
			replace issues="not AOM" if nid==103419 & case_name=="Hearing Loss" 
	
// run checks and save in 05_upload; will break if error such as range check or missing required variable (correct and rerun), otherwise will save for upload by data analyst
	// this script will eventually upload directly to the database but we want to run several iterations of uploads centrally so we can build the proper checks; there are a ton of things that can go wrong during upload
	do "FILEPATH"
	cap log close
