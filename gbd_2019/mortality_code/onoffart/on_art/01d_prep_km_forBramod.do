// THIS FILE'S PURPOSE IS TO PREPARE Split KM hiv free data AND FORMAT THE KAPLAN MEIER DATA SO IT CAN BE PUT INTO DISMOD 
	

// Updated by: NAME
// Last updated date: Feb 9, 2014

global user "`c(username)'"

// settings
	clear all
	set more off
	if (c(os)=="Unix") {
		global root "ADDRESS"
		global code_dir "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
		global code_dir "ADDRESS"
	}

	
// locals 

	local KM_data "FILEPATH"
	local dismod_templates "FILEPATH"
	local bradmod_dir "FILEPATH"
	
	local store_graphs "FILEPATH"
	local store_logs "FILEPATH"
	local store_data "FILEPATH" 
	
use "`bradmod_dir'/km_rates", clear 

*convert to probabilities 
drop all_rate
gen cond_prob = 1-(exp(-1*rate*1))  
replace cond_prob = 1-(1-cond_prob)^.5 if time_point == 6 | time_point == 12

************************
// Format variables for bradmod excels
************************

	// 1: Create variables needed for data_in csv
	
		gen sex_real=sex
		
		keep sex age time_lower time_upper cd4_start cd4_end cond_prob super iso3 cohort site pubmed_id nid subcohort_id  time_point age sex_real sample_size cd4_specificity 
		gen integrand="incidence" 
		rename cd4_start age_lower
		rename cd4_end age_upper
		rename cond_prob meas_value 
		tostring nid, replace 
		// gen subreg = "S" + nid // subreg is basically a country identifier in BradMod -- we want to have some sort of within-study variation so we use NID here to treat each study as a separate country
		gen subreg = "none"
		gen region="none"
		// super already exists
		gen x_sex=.
		replace sex=3 if sex==.
		replace x_sex=0 if sex==3 
		replace x_sex=.5 if sex==1
		replace x_sex=-.5 if sex==2
		gen x_ones=1

		
	// 2: Adjust CD4 counts (in bradmod they are in the 'age' columns since we are 'tricking' dismod)
		destring age_upper, replace 
		destring age_lower, replace 

		replace age_upper=age_upper/10
		replace age_lower=age_lower/10

	// 3: Order and save variables
		order pubmed_ nid subcohort_id super region subreg iso3 time_l time_u sex age_l age_u meas_v integ x_*  time_point
		tempfile tmp_adjusted
		save `tmp_adjusted', replace


***********************
// Convert to conditional probabilities before putting into bradmod
***********************


				
	// 2: Generate Standard Dev 

		// generate confidence interval
		gen lower_adj = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (meas_value + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * meas_value * (1 - meas_value) + 1/(4*sample_size^2) * invnormal(0.975)^2))  
		gen upper_adj = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (meas_value + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * meas_value * (1 - meas_value) + 1/(4*sample_size^2) * invnormal(0.975)^2))  
		
		// generate standard deviation 
		gen delta=(upper-lower)/(1.96*2)
		gen exp_delta=exp(delta)
		gen meas_stdev=(exp_delta-1)*meas_value 


		// need appropriate fix if the meas_value was 0 and therefore also the standard deviation calculated was zero. solution proposed by hmwe is to average the standard
			// deviation of other rows from the same study. or otherwise just replace with a very small value (.0001)
		bysort pubmed_id nid super iso3 time_lower time_upper age age_lower age_upper sex_real: egen mean_std=mean(meas_stdev)
		replace meas_stdev=mean_std if meas_stdev==0
		replace meas_stdev=.0001 if meas_stdev==0

	// 3: Final dismod formatting

		// Keep variables for dismod
		keep pubmed_id sample_size nid subcohort_id iso3 cohort site sex age super meas_value meas_stdev region subreg x_sex age_lower age_upper time_lower time_upper integrand x_ones  time_point cd4_specificity
		
		// string in order to append on those weird extra lines that bradmod requires in the data_in file
		tostring meas_stdev, replace force
		
		// Some studies did not have a point from 0-6 as a reference for the conditional prob; drop these.
		drop if meas_st=="."
		
		// dismod requires 2 extra lines at the bottom of the 'data_in' file. have saved these as a separate file that we append on here.
		// pull in bottom lines - save as csv so katrina can pull in temp file and run in stata 12 :)
		preserve
			insheet using "`dismod_templates'//dismod_append.csv", clear
			tempfile tmp_append 
			save `tmp_append', replace 
		restore
		
		append using "`tmp_append'"
		order pubmed_id nid subcohort_id sex super meas_value meas_stdev region subreg x_sex age_lower age_upper time_lower time_upper integrand x_ones
		
		// make any edits you need to for those extra 2 lines for data_in
		replace age_lower=0 if integrand=="mtall" & _n!=_N
		replace age_upper=20 if integrand=="mtall" & _n!=_N
		replace age_lower=20 if integrand=="mtall" & _n==_N
		replace age_upper=100 if integrand=="mtall" & _n==_N
		
/* 		tostring pubmed_id, replace
		egen unique_study_id=concat(pubmed_id subcohort_id)
		replace subreg=unique_study_id
		replace subreg="none" if subreg==".." */

	// 4: Save conditional probabilities ready for bradmod
		tempfile tmp_conditional
		save `tmp_conditional', replace
		save "`bradmod_dir'/tmp_conditional.dta", replace

	// 5: Also update the templates of the 'value_in' , 'plain_in' , 'effect in' and rate_in files. 
		// here we can adjust smoothing parameters, and number of draws,  'offset' (eta) value, 
		// zeta value (zcov) and prior of mortality dropping with cd4 (diota upper and lower)

		// add random effects for each study to effect in
		
	/* 	duplicates drop unique_study_id, force
		drop if unique_study_id== ".."
		keep unique_study_id
		rename unique_study_id name
		gen integrand="incidence"
		gen effect="subreg"
		gen lower=-2
		gen upper=2
		gen mean=0
		gen std=".01"
		
		tempfile study_effects
		save `study_effects', replace
 */
		
		insheet using "`dismod_templates'//effect_in.csv", comma names clear
			replace lower=0 if effect=="zcov"
			replace upper=1 if effect=="zcov"
			replace mean=.5 if effect=="zcov"
/* 			append using `study_effects' */
		outsheet using "`dismod_templates'//effect_in.csv", comma names replace
		
		insheet using "`dismod_templates'//value_in.csv", comma names clear
			replace value=".001" if name=="eta_incidence"
		outsheet using "`dismod_templates'//value_in.csv", comma names replace
		
		insheet using "`dismod_templates'//plain_in.csv", comma names clear
			replace lower=1 if name=="xi_iota" 
			replace upper=3 if name=="xi_iota"
			replace mean=2 if name=="xi_iota"
		outsheet using "`dismod_templates'//plain_in.csv", comma names replace
		
		insheet using "`dismod_templates'//rate_in.csv", comma names clear
			replace upper="0" if type=="diota"
		outsheet using "`dismod_templates'//rate_in.csv", comma names replace
		
		
	****** CHECK OUT OUR FINAL SAMPLE OF FACILITIES
		use "`bradmod_dir'/tmp_conditional.dta", clear
		gen geographic=iso
		replace geographic=site if geographic==""
		replace geographic=cohort if geographic==""


