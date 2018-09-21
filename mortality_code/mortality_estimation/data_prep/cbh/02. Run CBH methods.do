** Description: Runs the functions to estimate under-5 mortality from complete birth history data 

clear all 
macro drop _all 
capture cleartmp
set more off 
pause off 
set mem 1g
set matsize 11000

global sv = "strSurveyAbbreviation"

do "strDirectory/FUNCTION_person_months.ado"
do "strDirectory/FUNCTION_estimate_mort_indiv_surv.ado"

foreach sex in "both" "males" "females" { 
	global sex = "`sex'"
	// create the person months datasets 
	CBH_person_months, directory("strSurveyDirectory") /// 
					   sex("$sex") neonatal(1)
	// run CBH methods on 2 year periods for GPR
	CBH_estimate_mort_indiv_surv, directory("strSurveyDirectory") ///
					   sex("$sex") neonatal(1) period(2) save_GBD(1) save_sims(1)
	// run CBH methods on 5 year periods for age/sex model 
	CBH_estimate_mort_indiv_surv, directory("strSurveyDirectory") ///
					   sex("$sex") neonatal(1) period(5) save_GBD(0) save_sims(1)					   
} 
	