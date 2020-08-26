
// NOTES: all of this is occuring at the study level
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
use "`data_dir'/FILEPATH/KM_forsplit", clear
tostring age_start age_end, replace
gen age_joint=age_start+"-"+age_end
keep sex year_start year_end cd4_start cd4_end dead_prop_adj dead_prop_lo dead_prop_hi super iso3 cohort site pubmed_id nid subcohort_id time_per time_point age_joint age_start age_end  sample_size  prop_male cd4_specificity 
rename year_s time_lower
rename year_e time_upper 
destring cd4_start, replace 
destring cd4_end, replace 
*drop incorrect data entry 
drop if dead_prop_adj > 1

*convert to conditional Probabilities 

order dead_prop_adj pubmed_id nid subcohort_id age_joint sex time_point
sort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end sex time_point	
		
bysort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end sex: gen cond_prob=((dead_prop_adj-dead_prop_adj[_n-1])/(1 - dead_prop_adj[_n-1])) if time_point!=6
		
replace cond_prob=dead_prop_adj if time_point==6
order cond_prob
drop dead_prop_adj

*some studies cant calculate cond prob 
drop if missing(cond_prob) 

*convert india conditional probabilities to 1 yr (12-24) from 4 year (12-60) 
replace cond_prob = 1-(1-cond_prob)^.25 if nid == 1993 & time_point == 24 

*convert to death rates 
gen rate=.
replace rate=-(ln(1-cond_prob))/.5 if time_point == 6
replace rate=-(ln(1-cond_prob))/.5 if time_point == 12
replace rate=-(ln(1-cond_prob))/1 if time_point == 24  



//Now we must apply the HRS  

//first age split 
*should tab age_joint here to make sure it is appropiate to split all the cohorts we assume they are representative of the median age for the region


*we will not split studies that have approximately our age ranges. starting off conservative since we dont have data like this for the moment 
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

*we dont neccessarily want to split to all the spectrum age groups so this is new method
gen split_ages = "15_25,25_35,35_45,45_55,55_100" if exact_age != 1
replace split_ages = "45_55,55_100" if exact_age != 1 & age_joint == "45-100"
replace split_ages = age if exact_age == 1
split split_ages, parse(",") 
rename split_ages org_split_ages
gen id = _n 
reshape long split_ages, i(id) j(num) 
drop if split_ages == ""
drop id num 
replace age = split_ages if exact_age != 1

  
/*
expand 5 if exact_age != 1
sort pubmed_id nid super iso3 cohort  site subcohort_id time_lower time_upper age_joint cd4_start cd4_end sex time_point
bysort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end sex time_point: gen rownum=_n if exact_age != 1
replace age="15_25" if rownum==1
replace age="25_35" if rownum==2
replace age="35_45" if rownum==3
replace age="45_55" if rownum==4
replace age="55_100" if rownum==5
order age
*/

gen merge_super = super  
gen baseline = "med" 
replace baseline = "45_100" if age_joint == "45-100"

merge m:1 merge_super age baseline using "`hr_dir'/age_hazard_ratios.dta", keep(1 3) nogen
egen hrmean = rowmean(age_hr_*) 

*trying somehting for india (a more study specific age pattern) 
replace hrmean = .84 if age == "15_25" & nid == 1993 
replace hrmean = .96 if age == "25_35" & nid == 1993 
replace hrmean = 1 if age == "35_45" & nid == 1993 
replace hrmean = 1.03 if age == "45_55" & nid == 1993 
replace hrmean = 1.46 if age == "55_100" & nid == 1993 
 
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

		
replace sex = 3 if missing(sex) 
gen exact_sex = 0 
replace exact_sex = 1 if inlist(sex,1,2)
expand 2 if sex == 3

sort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end  time_point age
bysort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end  time_point age: gen rownum=_n if sex == 3
replace sex=2 if rownum==1  
replace sex=1 if rownum==2 

*need to transition to study specific here
replace prop_male = pct_male_weighted if missing(prop_male) 
replace prop_male = .55 if nid == 1993   
egen shrmean = rowmean(sex_hr*) 
drop sex_hr*


tostring sex, replace
replace sex = "male" if sex == "1"
replace sex = "female" if sex == "2" 

replace shrmean = 1.36 if nid == 1993

replace rate=rate/(1+(prop_male*(shrmean-1))) if sex=="female" & exact_sex != 1
sort pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end time_point age
by pubmed_id nid super iso3 cohort site subcohort_id time_lower time_upper age_joint cd4_start cd4_end  time_point age: replace rate=shrmean*rate[_n-1] if sex=="male" & exact_sex != 1 

replace sample_size = sample_size * prop_male if sex == "male" & exact_sex != 1
replace sample_size = sample_size *(1-prop_male) if sex == "female" & exact_sex != 1  



save "`data_dir'/FILEPATH/split_km", replace 



