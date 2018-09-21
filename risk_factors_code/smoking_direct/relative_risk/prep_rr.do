// *************************************************************************************************************

** **********************************************************************************************************
// 1. SET UP STATA
** **********************************************************************************************************

clear all
set more off

if c(os)=="Unix" {
	global j "/home/j"
	local dismodlink FILEPATH
}

else {
	global j "J:"
	local dismodlink FILEPATH
}

local data_dir "FILEPATH"
local risk_version 4 
local out_dir_draws "FILEPATH"
local out_dir_means "$FILEPATH"

** **********************************************************************************************************
// 2. Change RRs (nasopharynx and mouth) to reflect the Kontis et al paper 
** **********************************************************************************************************

insheet using "FILEPATH", comma names clear
drop if parameter == "cat2"
drop parameter

replace rr_mean = 8.1 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 1
replace rr_lower = 5.70 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 1
replace rr_upper = 11.70 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 1

replace rr_mean = 6.00 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 2
replace rr_lower = 4.30 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 2
replace rr_upper = 8.50 if (acause == "neo_mouth" | acause == "neo_nasopharynx") & sex == 2

keep if gbd_age_start == 30 

// draw RRs from mean & CI

	gen rr_sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*1.96)
	replace rr_mean = ln(rr_mean)
	
	forvalues k=0/999 {
		quietly gen rr_`k' = rnormal(rr_mean,rr_sd) 
		quietly replace rr_`k' = exp(rr_`k') 
		
	
	}
	
	replace rr_mean = exp(rr_mean)
	drop rr_sd 

	expand 11
	bysort sex acause: gen id = _n
	
	destring gbd_age_start, replace force
	
	replace gbd_age_start = 30 if id == 1
	replace gbd_age_start = 35 if id==2
	replace gbd_age_start = 40 if id==3
	replace gbd_age_start = 45 if id==4
	replace gbd_age_start = 50 if id==5
	replace gbd_age_start = 55 if id==6
	replace gbd_age_start = 60 if id==7
	replace gbd_age_start = 65 if id==8
	replace gbd_age_start = 70 if id==9
	replace gbd_age_start = 75 if id==10
	replace gbd_age_start = 80 if id==11
	drop id

	replace gbd_age_end = gbd_age_start

// Create parameters 

expand 2, gen(dup) 

foreach var of varlist rr_* {

	replace `var' = 1 if dup == 1
	
	}
	

rename dup parameter 
tostring parameter, replace
replace parameter = "cat2" if parameter == "1" 
replace parameter = "cat1" if parameter == "0" 
drop if acause == "neo_lung"

tempfile all 
save `all', replace

// Bring in lung cancer RRs 

insheet using "FILEPATH", comma names clear
keep if acause == "neo_lung" 

append using `all'
tempfile combined 
save `combined', replace 

// Format & Save file for clustertemp 
sort risk acause gbd_age_start gbd_age_end sex whereami_id parameter year mortality morbidity rr_mean rr_lower rr_upper 

drop rr_mean rr_lower rr_upper

outsheet using "FILEPATH", comma names replace 

// Save file to the J Drive 

use `combined', clear 

rename rr_mean mean_rr 
rename rr_lower lower_rr
rename rr_upper upper_rr 

drop rr* 

rename mean_rr rr_mean
rename lower_rr rr_lower
rename upper_rr rr_upper

sort risk acause gbd_age_start gbd_age_end sex whereami_id parameter year mortality morbidity rr_mean rr_lower rr_upper 

outsheet using "FILEPATH", comma names replace 

