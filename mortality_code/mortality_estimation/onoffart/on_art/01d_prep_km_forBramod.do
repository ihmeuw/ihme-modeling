// THIS FILE'S PURPOSE IS TO format Split KM hiv data SO IT CAN BE PUT INTO DISMOD ODE
	
global user "`c(username)'"

// settings
	clear all
	set more off
	if (c(os)=="Unix") {
		global root FILEPATH
		global code_dir FILEPATH
	}

	if (c(os)=="Windows") {
		global root FILEPATH
		global code_dir FILEPATH
	}

	
// locals 

	local dismod_templates FILEPATH
	local bradmod_dir FILEPATH
	
use "`bradmod_dir'/km_rates", clear 

*convert to conditional probabilities 
drop all_rate
gen cond_prob = 1-(exp(-1*rate*1))  
replace cond_prob = 1-(1-cond_prob)^.5 if time_point == 6 | time_point == 12

************************
// Format variables for bradmod excels
************************

	// 1: Create variables needed for data_in csv
	
		gen sex_real=sex
		
		keep sex age time_lower time_upper cd4_start cd4_end cond_prob super iso3 cohort site pubmed_id nid subcohort_id  time_point age sex_real sample_size 
		gen integrand="incidence" 
		rename cd4_start age_lower
		rename cd4_end age_upper
		rename cond_prob meas_value 
		tostring nid, replace 
		gen subreg = "none"
		gen region="none"
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

		// need appropriate fix if the meas_value was 0 
		bysort pubmed_id nid super iso3 time_lower time_upper age age_lower age_upper sex_real: egen mean_std=mean(meas_stdev)
		replace meas_stdev=mean_std if meas_stdev==0
		replace meas_stdev=.0001 if meas_stdev==0

	// 3: Final dismod formatting

		// Keep variables for dismod
		keep pubmed_id sample_size nid subcohort_id iso3 cohort site sex age super meas_value meas_stdev region subreg x_sex age_lower age_upper time_lower time_upper integrand x_ones  time_point
		
		// string in order to append on those extra lines that bradmod requires in the data_in file
		tostring meas_stdev, replace force
		
		// dismod requires 2 extra lines at the bottom of the 'data_in' file. have saved these as a separate file that we append on here.
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

	// 4: Save conditional probabilities ready for bradmod
		tempfile tmp_conditional
		save `tmp_conditional', replace
		save "`bradmod_dir'/tmp_conditional.dta", replace


		insheet using "`dismod_templates'//effect_in.csv", comma names clear
			replace lower=0 if effect=="zcov"
			replace upper=1 if effect=="zcov"
			replace mean=.5 if effect=="zcov"
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


