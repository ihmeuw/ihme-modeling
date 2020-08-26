*calculate cumulative mortality and survival for naco unaids gdb india
clear all
set more off

if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}





if 0==0 {
	if c(os) == "Windows" {
		global prefix "$root"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}

*naco
insheet using "FILEPATH\Naco_on_art.csv", clear  
gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point


replace mort_naco = 1-(1-mort_naco)^.5 if time_point == 6 
replace mort_naco = 1-(1-mort_naco)^.5 if time_point == 12






expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_naco 
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_naco 
rename cond_surv cond_surv_naco
outsheet using "FILEPATH\Naco_art_cum_surv.csv", replace 
tempfile naco 
save `naco', replace

*GBD 

insheet using "FILEPATH\PER_HIVonART.csv", clear

egen mort = rowmean(mort*) 
keep durationart cd4_category age sex cd4_lower cd4_upper mort

gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point

rename mort mort_gbd_other

replace mort_gbd_other = 1-(1-mort_gbd_other)^.5 if time_point == 6 
replace mort_gbd_other = 1-(1-mort_gbd_other)^.5 if time_point == 12






expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_gbd_other
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_gbd_other
rename cond_surv cond_surv_gbd_other
outsheet using "FILEPATH\gbd_art_cum_surv.csv", replace
tempfile gbd 
save `gbd', replace

*unaids south africa 
insheet using "FILEPATH\unaids_SA_art.csv", clear  
replace mort_sa = 1-exp(-mort_sa)
gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point


replace mort_sa = 1-(1-mort_sa)^.5 if time_point == 6 
replace mort_sa = 1-(1-mort_sa)^.5 if time_point == 12

rename mort_sa mort_sa_un




expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_sa_un 
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_sa_un 
rename cond_surv cond_surv_sa_un
outsheet using "FILEPATH\un_SA_art_cum_surv.csv", replace 
tempfile un 
save `un', replace 

*unaids asia 
insheet using "FILEPATH\unaids_asia_art.csv", clear  
gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point


replace mort = 1-(1-mort)^.5 if time_point == 6 
replace mort = 1-(1-mort)^.5 if time_point == 12

rename mort mort_asia_un




expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_asia_un 
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_asia_un 
rename cond_surv cond_surv_asia_un
outsheet using "FILEPATH\un_asia_art_cum_surv.csv",comma replace 
tempfile unas 
save `unas', replace 



*smind 

insheet using "FILEPATH\161031_IND_dt.csv", clear

egen mort = rowmean(mort*) 
keep durationart cd4_category age sex cd4_lower cd4_upper mort

gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point

rename mort mort_smind

replace mort_smind = 1-(1-mort_smind)^.5 if time_point == 6 
replace mort_smind = 1-(1-mort_smind)^.5 if time_point == 12






expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_smind
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_smind 
rename cond_surv cond_surv_smind
outsheet using "FILEPATH\smind_art_cum_surv.csv", replace
tempfile smind
save `smind', replace











*ART cc at haidongs request 
use "FILEPATH\tmp_conditional.dta", clear
keep if super == "high" & cohort == "ART-CC" & (iso3 == "AUT" | iso3== "NLD") 
replace age = subinstr(age, "_","-",1)
rename age_lower cd4_lower 
rename age_upper cd4_upper 
replace cd4_lower = cd4_lower*10 
replace cd4_upper = cd4_upper*10  
replace cd4_upper = 250 if cd4_upper == 249 
replace cd4_upper = 500 if cd4_upper == 499
replace cd4_upper = 1000 if cd4_upper == 500 & cd4_lower == 500
gen cd4_category="ARTLT50CD4" if cd4_lower==0
replace cd4_category="ART50to99CD4" if cd4_lower==50
replace cd4_category="ART100to199CD4" if cd4_lower==100
replace cd4_category="ART200to249CD4" if cd4_lower==200
replace cd4_category="ART250to349CD4" if cd4_lower==250
replace cd4_category="ART350to500CD4" if cd4_lower==350
replace cd4_category="ARTGT500CD4" if cd4_lower==500 
gen durationart="6to12Mo" if time_point == 12
replace durationart="LT6Mo" if time_point == 6
replace durationart="GT12Mo" if time_point == 24
keep meas_value sex age iso3 time_lower time_upper cd4_lower cd4_upper time_point cd4_category durationart  
gen cohort_id = iso3+ "_" + string(time_lower)+"_"+string(time_upper)
levelsof cohort_id, local(cohorts) 
drop time_lower time_upper iso3 
rename meas_value mort_
reshape wide mort_, i(sex age cd4_lower cd4_upper time_point) j(cohort_id) string
expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5

foreach c of local cohorts{ 
	gen cond_surv_`c' = 1 - mort_`c' 
	gen cum_surv_`c' = cond_surv_`c' if year == .5  

}

foreach c of local cohorts{ 
	foreach t in 1 2 3 4 5{

		bysort sex age cd4_category : replace cum_surv_`c' = cum_surv_`c'[_n-1]*cond_surv_`c'[_n] if year == `t'
	
	
	
	}



} 

tempfile artcc 
save `artcc', replace

*gbd ssa 

use "FILEPATH\mortality_probs_new_model.dta", clear 
keep if super == "ssa" 
drop super
egen mort = rowmean(mort*) 
keep durationart cd4_category age sex cd4_lower cd4_upper mort

gen time_point = 6 if durationart == "LT6Mo"
replace time_point = 12 if durationart == "6to12Mo"
replace time_point = 24 if durationart == "GT12Mo" 
sort sex age cd4_category time_point

rename mort mort_gbdsa

replace mort_gbdsa = 1-(1-mort_gbdsa)^.5 if time_point == 6 
replace mort_gbdsa = 1-(1-mort_gbdsa)^.5 if time_point == 12






expand 4 if time_point == 24 
sort sex age cd4_category time_point

bysort sex age cd4_category : gen year = _n
replace year = .5 if year == 1
replace year = year -1 if year != .5
gen cond_surv = 1 - mort_gbdsa 
gen cum_surv = cond_surv if year == .5   






foreach t in 1 2 3 4 5{

	bysort sex age cd4_category : replace cum_surv = cum_surv[_n-1]*cond_surv[_n] if year == `t'
	
	
	
}



rename cum_surv cum_surv_gbdsa 
rename cond_surv cond_surv_gbdsa
outsheet using "FILEPATH\gbdsa_art_cum_surv.csv", replace
tempfile gbdsa
save `gbdsa', replace


*graphing


merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `naco' , nogen 
merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `gbd' , nogen
merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `un' , nogen 
merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `smind', nogen
merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `artcc', nogen
merge 1:1 sex age cd4_category cd4_lower cd4_upper year using `unas', nogen



expand 2 if year == .5, gen(numyr) 
replace year = 0 if numyr == 1 
replace cum_surv_gbd_other = 1 if numyr == 1  
replace cum_surv_naco = 1 if numyr == 1
drop numyr 
sort sex age cd4_category year
replace mort_gbd_other =0 if year == 0 
replace mort_naco = 0 if year == 0 
replace cond_surv_gbd_other = 1 if year == 0 
replace cond_surv_naco = 1 if year == 0
drop durationart
drop time_point

gen APT_unadjusted_surv = .912 if year ==1 
replace APT_unadjusted_surv = .763 if year == 5 
gen APT_adjusted_surv = .706 if year == 5 
gen RAJ_unadjusted_surv = .906 if year ==1 
replace RAJ_unadjusted_surv = .783 if year == 5 
gen RAJ_adjusted_surv = .761 if year == 5 

tempfile final 
save `final', replace 

*now we bringing in sample sizes 
import excel "FILEPATH\lit_extraction_hiv_onART_brownav_Y2015_2016M06D08.xlsx", firstrow sheet(Kaplan-Meier) clear  
keep if NID == 1997 
drop if baseline == 1 
keep if cohort == "IeDEA Southern Africa"

keep sex age_start age_end cd4_start cd4_end sample_size 
duplicates drop sex age_start age_end cd4_start cd4_end sample_size, force

rename cd4_start cd4_lower 
rename cd4_end cd4_upper 
replace sex = "1" if sex == "male" 
replace sex = "2" if sex == "female" 
destring sex, replace 
replace age_end = age_end+1 if age_end != 100 & age_end != 55
replace cd4_upper = cd4_upper+1 if cd4_upper ==99 | cd4_upper == 199 | cd4_upper == 249 | cd4_upper == 349 | cd4_upper == 49
replace cd4_upper = 1000 if cd4_upper == 1500
gen age = string(age_start)+"-"+string(age_end) 

tempfile samples 
save `samples', replace 

use `final', clear 
merge m:1 sex age cd4_lower cd4_upper using `samples', keepusing(sample_size)
gen smind_weighted = cum_surv_smind * (sample_size/87781) if year == 5
egen fiveyr_weighted_smind = sum(smind_weighted)

gen gbdsa_weights = cum_surv_gbdsa*(sample_size/87781) if year ==5
egen fiveyr_weighted_gbdsa = sum(gbdsa_weights) 

gen unsa_weights = cum_surv_sa_un*(sample_size/87781) if year == 5 
egen fiveyr_weighted_unsa = sum(unsa_weights)

gen gbdother_weights = cum_surv_gbd_other*(sample_size/87781) if year == 5 
egen fiveyr_weighted_gbdother = sum(gbdother_weights) 

gen naco_weights = cum_surv_naco*(sample_size/87781) if year == 5 
egen fiveyr_weighted_naco = sum(naco_weights)

sort age sex cd4_lower cd4_upper year   

saveold "FILEPATH\master_22417",  replace

*grab cd4 specific survival curves 

local source asia_un smind gbdsa sa_un gbd_other naco `cohorts'



foreach s of local source{  
	di "`s'"
	gen surv_num_`s' = sample_size*cum_surv_`s'

}

collapse (sum) sample_size surv_num*, by(sex cd4_lower cd4_upper cd4_category year)

foreach s of local source{ 
	replace surv_num_`s' = sample_size if year == 0 
	gen surv_`s' = surv_num_`s'/sample_size

} 

tostring sex, replace 
replace sex = "Male" if sex == "1" 
replace sex = "Female" if sex == "2"

levelsof cd4_category, local(cd4)
levelsof sex, local(sex) 

cd "FILEPATH\surv_curvs"

pdfstart using "FILEPATH/cum_surv_compare.pdf"

foreach c of local cd4{ 
	foreach s of local sex { 
		preserve
		keep if cd4_category == "`c'" & sex == "`s'"
		twoway line  surv_smind surv_gbdsa surv_sa_un surv_gbd_other surv_naco surv_AUT_2002_2014 surv_NLD_1995_2001 surv_NLD_2002_2014 surv_asia_un year, scheme(plotplain) title("`c' `s'") ytitle("Survival") xtitle("Year from ART Initiation") legend(order(1 2 3 4 5 6 7 8 9) label(1 "GBD India") label(2 "GBD SSA") label(3 "UN SA") label(4 "GBD Other") label(5 "NACO") label(6 "ART-CC AUT 2002-2014") label(7 "ART-CC NLD 1995-2001") label(8 "ART-CC NLD 2002-2014") label(9 "Spectrum Asia")) /// 
		lpattern(solid solid dash solid solid solid dash dash_dot dash) lcolor(orange green green blue purple red red red orange) ylab(,nogrid) xlab(,nogrid)
		graph export "`s'_`c'_compare.pdf", replace
		pdfappend
		restore
	} 
}

pdffinish 

*overall survival

use "FILEPATH\master", clear 



local source asia_un smind gbdsa sa_un gbd_other naco `cohorts'

foreach s of local source{  
	di "`s'"
	gen surv_num_`s' = sample_size*cum_surv_`s'

} 

collapse (sum) sample_size surv_num*, by(year)

cd "FILEPATH\surv_curvs"


foreach s of local source{ 
	replace surv_num_`s' = sample_size if year == 0 
	gen surv_`s' = surv_num_`s'/sample_size

}  

twoway line  surv_smind surv_gbdsa surv_sa_un surv_gbd_other surv_naco surv_AUT_2002_2014 surv_NLD_1995_2001 surv_NLD_2002_2014 surv_asia_un year /// 
, scheme(plotplain) title("Overall Survival") ytitle("Survival") xtitle("Year from ART Initiation") legend(order(1 2 3 4 5 6 7 8 9) label(1 "GBD India") label(2 "GBD SSA") label(3 "UN SA") label(4 "GBD Other") label(5 "NACO") label(6 "ART-CC AUT 2002-2014") label(7 "ART-CC NLD 1995-2001") label(8 "ART-CC NLD 2002-2014") label(9 "Spectrum Asia"))  /// 
lpattern(solid solid dash solid solid solid dash dash_dot dash) lcolor(orange green green blue purple red red red orange) xlab(,nogrid) ylab(,nogrid)

graph export "allsurv_compare.pdf", replace


/*
levelsof cd4_category, local(cd4)
levelsof sex, local(sex) 
levelsof age, local(age)


pdfstart using "FILEPATH/cum_surv_compare.pdf"

foreach c of local cd4{ 
	foreach a of local age { 
		preserve
		keep if cd4_category == "`c'" & age == "`a'" 
		twoway line cum_surv_gbd year if sex == 1,  lcolor(green) /// 
		|| line cum_surv_gbd year if sex == 2,  lcolor(green) lpattern(dash) /// 
		|| line cum_surv_naco year if sex == 1,  lcolor(blue) /// 
		|| line cum_surv_naco year if sex == 2,  lcolor(blue) lpattern(dash) /// 
		|| line cum_surv_ap year if sex == 1, lcolor(yellow) /// 
		|| line cum_surv_ap year if sex == 2, lcolor(yellow) lpattern(dash) ///
		|| scatter APT_unadjusted_surv year, mcolor(red) msymbol(O) /// 
		|| scatter APT_adjusted_surv year, mcolor(red) msymbol(D) /// 
		|| scatter RAJ_unadjusted_surv year, mcolor(purple) msymbol(O) /// 
		|| scatter RAJ_adjusted_surv year, mcolor(purple) msymbol(D) ///
		legend( order(1 2 3 4 5 6) label(1 "GBD Males") label(2 "GBD Females") label(3 "NACO Males") label(4 "NACO Females") ///
		 label(5 "UNAIDS Male") label(6 "UNAIDS Female") size(small)) /// 
		title("`c', `a'") ytitle("Cumulative Survival") 
		pdfappend 
		restore
	
	
	}

}

pdffinish


*/


