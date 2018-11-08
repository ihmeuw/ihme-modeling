// ***************************************************************************************************
// ***************************************************************************************************

// Purpose:	converting period to point prevalence

// set globals
	// define the functional group for which you are prepping data (acause or impairment)
		global functional = "uri"
	// define the date of the data prep run (YYYY_MM_DD)
		global date = "2014_02_21"
		
// prep stata
	clear all
	set more off
	cap restore, not
	cap log close
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
	}
	
// create directories and start log
	/* run "$prefix/WORK/04_epi/01_database/01_code/03_review/ess_split.ado"
	run "$prefix/WORK/04_epi/01_database/01_code/03_review/agesex_split.ado"  */
	local out_dir "ADDRESS"
	local tmp_dir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	capture log close
	log using "FILEPATH", replace


// download and load data
	** global step "review"
	** do "$prefix/WORK/04_epi/01_database/01_code/02_upload/epi_data_download.do"
	insheet using "FILEPATH", comma clear



// Converting period to point prevalence for two data points (Create two new observations with data_status=[blank] and is_raw=adjusted, and set the original data points as is_raw=excluded_review)

  //nid==115105 (common cold)
	// create new row with flag new=1 
	expand 2 in 1042 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Converting period to point prevalence
	gen period_prev= numerator/denominator if new == 1
	gen point_prev=period_prev*(5/(5-1+30)) if new == 1/* point prevalence= period prevalence*(duration/(duration-1+recall period))*/
	replace mean=point_prev if new == 1 
	replace data_status=" " if new == 1
	replace is_raw="adjusted" if new == 1
	replace case_definition= "common cold" if new == 1
	replace cv_mild=1 if new == 1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace is_raw="excluded_review" if nid==115105 & case_name=="common cold" & recall_type=="Point" & new!=1
	drop new period_prev point_prev

	//nid==115105 (tonsillopharyngitis)
	// create new row with flag new=1 
	expand 2 in 1041 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Converting period to point prevalence
	gen period_prev= numerator/denominator if new == 1
	gen point_prev=period_prev*(7/(7-1+30)) if new == 1/* point prevalence= period prevalence*(duration/(duration-1+recall period))*/
	replace mean=point_prev if new == 1 
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace data_status=" " if new == 1
	replace is_raw="adjusted" if new == 1
	replace case_definition= "tonsillopharyngitis" if new == 1
	replace cv_moderate_severe=1 if new == 1
	replace is_raw="excluded_review" if nid==115105 & case_name=="tonsillopharyngitis" & & recall_type=="Point" & new!=1
	drop new period_prev point_prev

// save final, send this path to data analyst for upload
	outsheet using "FILEPATH", comma replace

	
	log close
	