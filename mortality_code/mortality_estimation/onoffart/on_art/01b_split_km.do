
// NOTES: all of this is occuring at the study level
	clear
	clear mata
	clear matrix
	set maxvar 15000
	
	if (c(os)=="Unix") {
		global root FILEPATH
	}

	if (c(os)=="Windows") {
		global root FILEPATH
	}

// set locals

	local bradmod_dir FILEPATH
	local hr_dir FILEPATH
	local data_dir FILEPATH
	
// GOAL OUTPUT: study level death rates (per person year), at the REGION-DURATION-SEX-AGE-SPECIFIC level, studies that are already sex/age specific will not be split

// INPUT: prepped KM data for splitting
use "`data_dir'/bradmod/KM_forsplit", clear
tostring age_start age_end, replace
gen age_joint=age_start+"-"+age_end
keep sex year_start year_end cd4_start cd4_end dead_prop_adj dead_prop_lo dead_prop_hi super iso3 cohort site pubmed_id nid subcohort_id time_per time_point age_joint age_start age_end  sample_size  prop_male 
rename year_s time_lower
rename year_e time_upper 
destring cd4_start, replace 
destring cd4_end, replace 


*convert to conditional Probabilities 

order dead_prop_adj pubmed_id nid subcohort_id age_joint sex time_point
sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex time_point	
		
bysort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex: gen cond_prob=((dead_prop_adj-dead_prop_adj[_n-1])/(1 - dead_prop_adj[_n-1])) if time_point!=6
		
replace cond_prob=dead_prop_adj if time_point==6
order cond_prob
drop dead_prop_adj

*some studies cant calculate cond prob 
drop if missing(cond_prob) 

*convert to death rates 
gen rate=.
replace rate=-(ln(1-cond_prob))/.5 if time_point == 6
replace rate=-(ln(1-cond_prob))/.5 if time_point == 12
replace rate=-(ln(1-cond_prob))/1 if time_point == 24  


//Now we must apply the HRS  

//first age split 
*should tab age_joint here to make sure it is appropiate to split all the cohorts as we assume they are representative of the median age for the region

*we will not split studies that have approximately our age ranges.  
destring age_start age_end, replace
gen age="" 
replace age="15_25" if age_start >=14 & ( 24<=age_end & age_end<=26)
replace age="25_35" if (age_start>=24 & age_start<=26) & (age_end>=34 & age_end <=36)
replace age="35_45" if (age_start>=34 & age_start<=36) & (age_end>=44 & age_end <=46)
replace age="45_55" if (age_start>=44 & age_start<=46) & (age_end>=54 & age_end <=56)
replace age="55_100" if (age_start>=54 & age_start<=56) & age_end >= 60
drop age_start age_end

gen exact_age = 0 
replace exact_age = 1 if age != ""
  
expand 5 if exact_age != 1
sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex time_point
bysort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end sex time_point: gen rownum=_n if exact_age != 1
replace age="15_25" if rownum==1
replace age="25_35" if rownum==2
replace age="35_45" if rownum==3
replace age="45_55" if rownum==4
replace age="55_100" if rownum==5
order age

gen merge_super = super  
merge m:1 merge_super age using "`hr_dir'/age_hazard_ratios.dta", nogen
egen hrmean = rowmean(age_hr_*) 
 
replace rate = rate*hrmean  if exact_age != 1
drop age_hr_* 
drop raw_hr_* 


*now sex HRs 
preserve
		insheet using "`hr_dir'/sex_hazard_ratios.csv", clear 
		gen merge_super=super
		tempfile tmp_sex_HRs
		save `tmp_sex_HRs', replace 
restore

merge m:1 merge_super using "`tmp_sex_HRs'", nogen  

		
drop rownum
replace sex = 3 if missing(sex) 
gen exact_sex = 0 
replace exact_sex = 1 if inlist(sex,1,2)
expand 2 if sex == 3

sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end  time_point age
bysort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end  time_point age: gen rownum=_n if sex == 3
replace sex=2 if rownum==1  
replace sex=1 if rownum==2 

*replace with region average if we are missing this variable
replace prop_male = pct_male_weighted if missing(prop_male)   
egen shrmean = rowmean(sex_hr*) 
drop sex_hr*

tostring sex, replace
replace sex = "male" if sex == "1"
replace sex = "female" if sex == "2" 

replace rate=rate/(1+(prop_male*(shrmean-1))) if sex=="female" & exact_sex != 1
sort pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end time_point age
by pubmed_id nid super iso3 cohort site time_lower time_upper age_joint cd4_start cd4_end  time_point age: replace rate=shrmean*rate[_n-1] if sex=="male" & exact_sex != 1 

replace sample_size = sample_size * prop_male if sex == "male" & exact_sex != 1
replace sample_size = sample_size *(1-prop_male) if sex == "female" & exact_sex != 1  



save "`data_dir'/bradmod/split_km", replace 



