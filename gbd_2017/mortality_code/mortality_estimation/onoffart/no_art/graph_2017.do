//purpose is to compare off art reg model to the previous off art reg model
clear all
set more off 

*compareing to itself right now since just started regular archiving of some of these
local compare_date = "NAME"
local compare_date_data ="NAME"

local compare_params = 1
local compare_surv = 1

if `compare_surv'{

	insheet using "FILEPATH", clear
	keep if regexm(source,"IHME")
	rename age age_cat
	rename surv_mean cum_surv_mean
	rename surv_lower cum_surv_lower 
	rename surv_upper cum_surv_upper
	gen model = "current"
	tempfile current
	save `current', replace 

	insheet using "FILEPATH", clear 
	keep if regexm(source,"IHME")
	rename age age_cat
	rename surv_mean cum_surv_mean
	rename surv_lower cum_surv_lower 
	rename surv_upper cum_surv_upper
	gen model = "old"
	append using `current'
	preserve   

	insheet using "FILEPATH", clear  
	rename age age_cat
	rename surv cum_surv_mean
	rename iso3 model 
	keep age_cat yr_since_sc cum_surv_mean model
	tempfile un17 
	save `un17', replace  
	
	
	use "FILEPATH", clear  
	gen age = "15_25" 
	replace age = "0_100" if age_cat == 0
	replace age = "25_35" if age_cat == 2 
	replace age = "35_45" if age_cat == 3 
	replace age = "45_100" if age_cat == 4 
	drop age_cat 
	rename age age_cat
	gen cum_surv_mean = 1-cum_mort_adj 
	gen model = "raw" 
	keep age_cat yr_since_sc cum_surv_mean model iso3
	tempfile raw 
	save `raw', replace  
	
	use "FILEPATH", clear  
	gen age = "15_25" 
	replace age = "0_100" if age_cat == 0
	replace age = "25_35" if age_cat == 2 
	replace age = "35_45" if age_cat == 3 
	replace age = "45_100" if age_cat == 4 
	drop age_cat 
	rename age age_cat
	gen cum_surv_mean = 1-cum_mort_adj 
	gen model = "raw_old" 
	keep age_cat yr_since_sc cum_surv_mean model
	tempfile raw_old 
	save `raw_old', replace 

	restore  
	append using `raw' 
	append using `raw_old'
	append using `un17'
	drop surv



	foreach a in 15_25 25_35 35_45 45_100{ 
		preserve 
		keep if age_cat == "`a'" 
		local title = subinstr("`a'","_"," to ",.)
		twoway line cum_surv_mean yr_since_sc if model == "old" & source=="IHME Statistical Model", color(red) lpattern(solid) || line cum_surv_lower yr_since_sc if model == "old" & source=="IHME Statistical Model", color(red) lpattern(dash) || line cum_surv_upper yr_since_sc if model == "old" & source=="IHME Statistical Model" , color(red) lpattern(dash) ///
		|| line cum_surv_mean yr_since_sc if model == "old" & source=="IHME Compartmental Model", color(black) lpattern(solid) || line cum_surv_lower yr_since_sc if model == "old" & source=="IHME Compartmental Model", color(black) lpattern(dash) || line cum_surv_upper yr_since_sc if model == "old" & source=="IHME Compartmental Model" , color(black) lpattern(dash) ///
		|| line cum_surv_mean yr_since_sc if model == "current" & source=="IHME Statistical Model" , color(green) lpattern(solid) || line cum_surv_lower yr_since_sc if model == "current" & source=="IHME Statistical Model", color(green) lpattern(dash) || line cum_surv_upper yr_since_sc if model == "current" & source=="IHME Statistical Model" , color(green) lpattern(dash) /// 
		|| line cum_surv_mean yr_since_sc if model == "current" & source=="IHME Compartmental Model", color(blue) lpattern(solid) || line cum_surv_lower yr_since_sc if model == "current" & source=="IHME Compartmental Model", color(blue) lpattern(dash) || line cum_surv_upper yr_since_sc if model == "current" & source=="IHME Compartmental Model" , color(blue) lpattern(dash) ///
		|| scatter cum_surv_mean yr_since_sc if model == "raw_old", msymbol(o) msize(small) color(red) || ///
		|| scatter cum_surv_mean yr_since_sc if model == "raw", msymbol(o) msize(small) color(green) ///
		|| scatter cum_surv_mean yr_since_sc if model == "UNAIDs Africa"|| scatter cum_surv_mean yr_since_sc if model == "UNAIDs Asia" || scatter cum_surv_mean yr_since_sc if model == "UNAIDs E/NA", || scatter cum_surv_mean yr_since_sc if model == "raw" & iso3 == "CIV", msymbol(o) msize(small) color(yellow) ///
		scheme(plotplain) ylab(0 .2 .4 .6 .8 1, nogrid) xlab(0 1 2 3 4 5 6 7 8 9 10 11 12,nogrid) legend(title("Source",size(vsmall)) ring(0) pos(9) size("tiny") label(1 "`compare_date' Statistical Model") label(4 "`compare_date' Compartmental Model") label(15 "UN Africa") label(16 "UN Asia") label(17 "UN NA/EU") label(18 "CIV 1220")  label(13 "`compare_date_data' Cohort Data") label(14 "Current Cohort Data") label(7 "Current Statistical model") label(10 "Current Compartmental model") order(1 4 7 10 13 14 15 16 17 18)) title("`title'") ytitle("Cumulative HIV Survival",size(small)) xtitle("Years from Seroconversion", size(small))
		graph export "FILEPATH", replace
		restore
	} 
	! pdftk FILEPATH output FILEPATH

if `compare_params'{ 
	
	*new mortality 
	use "FILEPATH", clear
	collapse (mean)  meanmort_new = mort (p3)  lomort_new = mort (p97)  himort_new = mort, by(age cd4) 
	tempfile new_mort 
	save `new_mort', replace  

	*old mort 
	insheet using "FILEPATH", clear 
	collapse (mean)  meanmort_old = mort (p3)  lomort_old = mort (p97)  himort_old = mort, by(age cd4) 
	tempfile old_mort 
	save `old_mort', replace 

	*new prog 
	use "FILEPATH", clear
	collapse (mean)  meanprog_new = prog (p3)  loprog_new = prog (p97)  hiprog_new = prog, by(age cd4)  
	tempfile new_prog 
	save `new_prog', replace 

	*old prog 
	insheet using "FILEPATH", clear 
	collapse (mean)  meanprog_old = prog (p3)  loprog_old = prog (p97)  hiprog_old = prog, by(age cd4) 

	merge 1:1 age cd4 using `new_prog', nogen 
	merge 1:1 age cd4 using `old_mort', nogen 
	merge 1:1 age cd4 using `new_mort', nogen  

	gen cd4_n = 1 
	replace cd4_n = 2 if cd4 == "350to500CD4" 
	replace cd4_n = 3 if cd4 == "250to349CD4" 
	replace cd4_n = 4 if cd4 == "200to249CD4" 
	replace cd4_n  = 5 if cd4 == "100to199CD4" 
	replace cd4_n = 6 if cd4 == "50to99CD4" 
	replace cd4_n = 7 if cd4 == "LT50CD4" 



	 

	twoway rcap lomort_old himort_old cd4_n,   lcolor(black) ///  
	|| rcap lomort_new himort_new cd4_n,  lcolor(red) ///  
	|| scatter meanmort_old cd4_n, msymbol(o)  mcolor(black)  ///   
	|| scatter meanmort_new cd4_n,  mcolor(red) msymbol(o) xlabel(1(1)7) xtitle("cd4 bin") legend( rows(1) order( 3 4) label(3 "`compare_date' model") label(4 "current model") )  by(age, note("") title("Off ART Mortality Parameters", size(medium)))
	graph export "FILEPATH", replace
		
	twoway rcap loprog_old hiprog_old cd4_n,   lcolor(black) ///  
	|| rcap loprog_new hiprog_new cd4_n,   lcolor(red) ///  
	|| scatter meanprog_old cd4_n, msymbol(o)  mcolor(black)  ///   
	|| scatter meanprog_new cd4_n,  mcolor(red) msymbol(o) xlabel(1(1)7) xtitle("cd4 bin") legend( rows(1) order( 3 4) label(3 "`compare_date' model") label(4 "current model") )  by(age, note("") title("Off ART Progression Parameters",size(medium)))
	graph export "FILEPATH", replace

	! pdftk FILEPATH output FILEPATH
	
	gen rd_prog = ((meanprog_new-meanprog_old)/(meanprog_old))*100 
	gen rd_mort = ((meanmort_new-meanmort_old)/(meanmort_old))*100 

	twoway scatter rd_prog cd4_n,xlabel(1(1)7) ytitle("Percent difference") xtitle("cd4 bin") legend(order(1) label(1 "Relative difference with `compare_date'")) by(age, note("") title("Off ART Progression Parameters RD",size(medium)))
	
	twoway scatter rd_mort cd4_n,xlabel(1(1)7) ytitle("Percent difference") xtitle("cd4 bin") legend(order(1) label(1 "Relative difference with `compare_date'")) by(age, note("") title("Off ART Mortality RD",size(medium)))
	
	
	
}
