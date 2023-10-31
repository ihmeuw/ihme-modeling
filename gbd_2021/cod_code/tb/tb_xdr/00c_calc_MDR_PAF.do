// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Computes PAFs for MDR-TB disaggregated by HIV status and PAF for XDR-TB
// Author:		USERNAME
// Edited:      USERNAME
// Description: Compute PAF for MDR-TB among HIV positive individuals
//				Compute PAF for MDR-TB among HIV negative individuals
//				Compute PAF for XDR-TB
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
         
**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************		 
		 
// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set scheme s1color

	// Define focal drives
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
	}

	// Close any open log file
	cap log close

**********************************************************************************************************************
** STEP 1: COMPUTE PAF FOR MDR-TB AMONG HIV POSITIVE CASES
**********************************************************************************************************************

	// Get MDR relative risk draws
	insheet using "FILEPATH/MDR_pooled_RR_draws_step4.csv", clear
	tempfile mdr_rr
	save `mdr_rr', replace

	// Get XDR relative risk draws
	insheet using "FILEPATH/XDR_pooled_RR_draws_step4.csv", clear
	tempfile xdr_rr
	save `xdr_rr', replace

	// Pull MDR-HIVTB proportions
	use "FILEPATH/hiv_mdr_prop_draws.dta", clear

	// Merge on MDR relative risk draws
	gen acause="tb_drug"
	merge m:1 acause using `mdr_rr', keep(3)nogen

	// Compute PAF: gen PAR_based_on_mean_rr=(hiv_mdr_prop*RR_mean)/((hiv_mdr_prop*RR_mean)+(1-hiv_mdr_prop))
	forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (hiv_mdr_prop_`x'*rr_`x')/((hiv_mdr_prop_`x'*rr_`x')+(1-hiv_mdr_prop_`x'))
	}

	// Output csv
	keep location_id year_id age_group_id sex_id PAR_based_on_mean_rr_*
	save "FILEPATH/par_hivtb_mdr_draws.dta", replace

**********************************************************************************************************************
** STEP 2: COMPUTE PAF FOR MDR-TB AMONG NO-HIV POSITIVE CASES
**********************************************************************************************************************
	
	// Pull TB no-HIV MDR proportions
	use "FILEPATH/tb_nohiv_mdr_prop_draws.dta", clear

	// Merge on MDR relative risk
	gen acause="tb_drug"
	merge m:1 acause using `mdr_rr', keep(3)nogen

	// Compute PAF: gen PAR_based_on_mean_rr=(tb_nohiv_mdr_prop*RR_mean)/((tb_nohiv_mdr_prop*RR_mean)+(1-tb_nohiv_mdr_prop))
	forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (tb_nohiv_mdr_prop_`x'*rr_`x')/((tb_nohiv_mdr_prop_`x'*rr_`x')+(1-tb_nohiv_mdr_prop_`x'))
	}

	// Output csv
	keep location_id year_id age_group_id sex_id PAR_based_on_mean_rr_*
	save "FILEPATH/par_tb_mdr_draws.dta", replace

**********************************************************************************************************************
** STEP 3: COMPUTE PAF FOR XDR-TB
**********************************************************************************************************************
	
	// Pull XDR-TB proportions
	use "FILEPATH/xdr_prop_GBD2019.dta", clear

	// Merge in XDR relative risks
	gen acause="tb_xdr"
	merge m:1 acause using `xdr_rr', keep(3)nogen

	// calculate PAF : PAR_based_on_mean_rr=(tb_nohiv_mdr_prop*RR_mean)/((tb_nohiv_mdr_prop*RR_mean)+(1-tb_nohiv_mdr_prop))
	forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (xdr_prop*rr_`x')/((xdr_prop*rr_`x')+(1-xdr_prop))
	}

	// Clean
	drop rr_*	
	drop xdr_prop acause

	// Output csv
	save "FILEPATH/par_xdr_draws.dta", replace

