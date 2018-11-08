	clear
	clear mata
	clear matrix
	set maxvar 15000
	
	if (c(os)=="Unix") {
		global root "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
	}

// set locals

	local bradmod_dir "FILEPATH"
	local hr_dir "FILEPATH" 
	local data_dir "FILEPATH"
	
// GOAL OUTPUT: study level death rates (per person year), at the REGION-DURATION-SEX-AGE-SPECIFIC level

// INPUT: prepped KM data for splitting
use "`data_dir'/bradmod/KM_forsplit", clear
tostring age_start age_end, replace
gen age_joint=age_start+"-"+age_end
keep sex year_start dead_prop year_end cd4_start cd4_end dead_prop_adj dead_prop_lo dead_prop_hi super iso3 cohort site pubmed_id nid subcohort_id time_per time_point age_joint  sample_size  prop_male 
rename year_s time_lower
rename year_e time_upper 
destring cd4_start, replace 
destring cd4_end, replace 
drop if sex != 3


*drop incorrect data entry 
drop if dead_prop_adj > 1

*convert to conditional Probabilities 

order dead_prop_adj pubmed_id nid subcohort_id age_joint sex time_point
sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex time_point	
		
bysort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex: gen cond_prob=((dead_prop_adj-dead_prop_adj[_n-1])/(1 - dead_prop_adj[_n-1])) if time_point!=6
		
replace cond_prob=dead_prop_adj if time_point==6
order cond_prob

*convert to conditional Probabilities 

order dead_prop_adj pubmed_id nid subcohort_id age_joint sex time_point
sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex time_point	
		
bysort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex: gen cond_prob_all=((dead_prop-dead_prop[_n-1])/(1 - dead_prop[_n-1])) if time_point!=6
		
replace cond_prob_all=dead_prop if time_point==6
order cond_prob_all

*some studies cant calculate cond prob 
drop if missing(cond_prob_all) 

*convert india conditional probabilities to 1 yr 
replace cond_prob = 1-(1-cond_prob)^.25 if nid == 1993 & time_point == 24 
replace cond_prob_all = 1-(1-cond_prob_all)^.25 if nid == 1993 & time_point == 24 
*convert to death rates 
gen rate=.
replace rate=-(ln(1-cond_prob))/.5 if time_point == 6
replace rate=-(ln(1-cond_prob))/.5 if time_point == 12
replace rate=-(ln(1-cond_prob))/1 if time_point == 24   

keep if nid == 1993 


tempfile ltfu 
save `ltfu', replace  

use "`data_dir'/bradmod/km_rates", clear 
keep if nid == 1993
gen cond_prob = 1-(exp(-1*rate*1))  
replace cond_prob = 1-(1-cond_prob)^.5 if time_point == 6 | time_point == 12
gen cond_prob_all = 1-(exp(-1*all_rate*1))  
replace cond_prob_all = 1-(1-cond_prob_all)^.5 if time_point == 6 | time_point == 12 

gen cond_surv = 1-cond_prob 
gen cond_surv_all = 1-cond_prob_all

gen cum_surv = cond_surv if time_point == 6 
gen cum_surv_all = cond_surv_all if time_point == 6

sort age sex cd4_start cd4_end time_point  
bysort age sex cd4_start cd4_end: replace cum_surv = cond_surv*cum_surv[_n-1] if time_point != 6 

sort age sex cd4_start cd4_end time_point  
bysort age sex cd4_start cd4_end: replace cum_surv_all = cond_surv_all*cum_surv_all[_n-1] if time_point != 6 

gen surv_num = cum_surv*sample_size 
gen surv_num_all = cum_surv_all*sample_size 

collapse (sum) sample_size surv_num_all surv_num, by(cd4_start cd4_end time_point) 
gen surv_hiv = surv_num/sample_size 
gen surv_all = surv_num_all/sample_size 
gen death_hiv = 1- surv_hiv 
gen death_all = 1-surv_all
drop surv_hiv surv_all 

merge 1:1  cd4_start cd4_end time_point using `ltfu'

keep  cd4_start cd4_end death_hiv death_all dead_prop_adj dead_prop time_point sample_size cond_prob cond_prob_all

gen cond_surv = 1-cond_prob 
gen cond_surv_all = 1-cond_prob_all  

gen cum_surv = cond_surv if time_point == 6 
gen cum_surv_all = cond_surv_all if time_point == 6 

sort cd4_start cd4_end time_point 
bysort cd4_start cd4_end: replace cum_surv = cond_surv*cum_surv[_n-1] if time_point != 6
bysort cd4_start cd4_end: replace cum_surv_all = cond_surv_all*cum_surv_all[_n-1] if time_point != 6
gen LTFU_adj = 1 - cum_surv 
gen CR_estimate = 1-cum_surv_all

keep  cd4_start cd4_end death_hiv death_all time_point LTFU_adj CR_estimate
