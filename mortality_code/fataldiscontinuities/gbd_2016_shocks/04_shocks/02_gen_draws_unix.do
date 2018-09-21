*** shock uncertainty on adult age groups ***
*** 10/24/2011
** NAME(edited by NAME 2/26/2015 to use causes)
** updating 5/5/2012
** Updated dec 2015 by NAME to use combined war/disaster dataset
clear
clear matrix
cap restore, not
set more off
set memory 8000m
set seed 1234567

	if c(os) == "Windows" {
		global prefix ""
	}
	else {
		global prefix ""
	}
 
 local user "USER"
 local input_folder "FILEPATH"
 global outdir "FILEPATH"
 local temp_folder "FILEPATH"
 if "`user'"=="USER" {
 	global shell_stata "FILEPATH"
	local code_folder "FILEPATH"
 }
 if "`user'"=="USER" {
 	global shell_stata "FILEPATH"
 	local code_folder "FILEPATH"
 }
 else {
 	global shell_stata "FILEPATH"
 	local code_folder "FILEPATH"
 }


** MUST REMOVE THE OLD FILES FIRST OR NO CHANGES WILL BE IMPLEMENTED (04/11/2016)
local datafiles: dir "`temp_folder'" files "*.dta"
foreach datafile of local datafiles {
	rm "`temp_folder'/`datafile'"
}

** Date
local date = string(date("`c(current_date)'", "DMY"), "%tdDD!_NN!_CCYY")

/*
*** step 1. get the uncertainty in deaths due to disaster
** get new war data
use "FILEPATH",clear
cap drop if  war_deaths_best< war_deaths_low
 
replace  war_deaths_low=2000 if iso3=="LBY" & year==2011
replace  war_deaths_high=30000 if iso3=="LBY" & year==2011

gen sds1=(war_deaths_best- war_deaths_low)/2
gen sds2=(war_deaths_high- war_deaths_best)/2
egen sds=rowmax(sds1 sds2)

replace war_deaths_best=war_rate*tot if sds==0
replace war_deaths_low=l_war_rate*tot if sds==0
replace war_deaths_high=u_war_rate*tot if sds==0

replace sds1=(war_deaths_best- war_deaths_low)/2 if sds==0
replace sds2=(war_deaths_high- war_deaths_best)/2 if sds==0
egen sds_rep =rowmax(sds1 sds2) 
replace sds=sds_rep if sds==0

keep if year>=1950 &  war_deaths_best!=0

gen threshhold=war_deaths_best/tot
drop if threshhold<0.000001
*/

** STEP 1. IMPORT DATA, GET UNCERTAINTY
//use "FILEPATH", clear
	use "`input_folder'/WAR_DISASTER_DEATHS.dta", clear
	gen sex = 9
	gen age = 26

**************************************************************************************************************************
	preserve
		gen low_to_mid = l_rate / rate 
		gen high_to_mid = u_rate / rate

		keep if iso3 == "YEM" & year == 2015 & cause == "inj_war_war"
		expand 6 if iso3 == "YEM" & year == 2015 & cause == "inj_war_war"
		gen new = _n - 1

		replace year = 2016 if new >= 3 
		replace age = 94 if mod(new, 3) == 0
		replace age = 3  if mod(new, 3) == 1
		replace age = 26 if mod(new, 3) == 2 

		local scale_15_16 = (9243 / 7354)
		replace deaths_best = 4225 if iso3 == "YEM" & year == 2015 & cause == "inj_war_war" & age == 94
		replace deaths_best = 5283 if iso3 == "YEM" & year == 2015 & cause == "inj_war_war" & age == 3
		replace deaths_best = 7354 if iso3 == "YEM" & year == 2015 & cause == "inj_war_war" & age == 26
		replace deaths_best = 4225 * `scale_15_16' if iso3 == "YEM" & year == 2016 & cause == "inj_war_war" & age == 94
		replace deaths_best = 5283 * `scale_15_16' if iso3 == "YEM" & year == 2016 & cause == "inj_war_war" & age == 3
		replace deaths_best = 9243 if iso3 == "YEM" & year == 2016 & cause == "inj_war_war" & age == 26
		
		replace rate = deaths_best / total_population if iso3 == "YEM" & (year == 2016 | year == 2015) & cause == "inj_war_war"
		replace l_rate = low_to_mid * rate if iso3 == "YEM" & (year == 2016 | year == 2015) & cause == "inj_war_war"
		replace u_rate = high_to_mid * rate if iso3 == "YEM" & (year == 2016 | year == 2015) & cause == "inj_war_war"

		drop new low_to_mid high_to_mid
		tempfile yemen_civil_war_15_16
		save `yemen_civil_war_15_16', replace
	restore
	drop if iso3 == "YEM" & year == 2015 & cause == "inj_war_war"
	append using `yemen_civil_war_15_16'

	**********************************************************************************************************************

// Create low, best, and high death counts
	gen deaths_low = l_rate * total_population
	gen deaths_high = u_rate * total_population
	
	foreach var of varlist deaths* {
		replace `var' = round(`var', 1)
	}
	
	//drop if deaths_best < deaths_low
	
	
		gen sds1 = (deaths_best - deaths_low) / 2
		gen sds2 = (deaths_high - deaths_best) / 2
		egen sds = rowmax(sds1 sds2)
		
		replace deaths_best = round(rate * tot, 1) if sds == 0
		replace deaths_low = round(l_rate * tot, 1) if sds == 0
		replace deaths_high = round(u_rate * tot, 1) if sds == 0
		
		
		replace sds1 = (deaths_best - deaths_low) / 2 if sds == 0
		replace sds2 = (deaths_high - deaths_best) / 2 if sds == 0
		egen sds_rep = rowmax(sds1 sds2)
		replace sds = sds_rep if sds == 0

		// some sds are too large and it pushes up the mean estimate because too many 0 draws are removed;
		// if they are above mean/1.96, then replace with mean/1.96 to ensure that 95% of normal distribution is positive
		gen max_sd = deaths_best / 1.96
		replace sds = max_sd if sds > max_sd

		
		// keep if deaths_best != 0
		keep if deaths_best != 0

		
		gen threshhold = deaths_best / total_population
		// execute thrsehold at national level (04/11/2016)
		// this avoids deflating the national level totals that were previously split
		gen nat_iso3 = substr(iso3, 1, 3)
		bysort nat_iso3 year cause: egen max_thresh = max(threshhold)
		drop if max_thresh < 0.000001
		
		//drop if deaths_best <=10 // Added after consultation with 12/16/2015
	

keep year iso3 cause age sex deaths_best sds
gen id=_n
local nn=_N
tempfile draws
noisily dis in red "Split file"

	// Partition dataset into 10 smaller sets and parallelize draws
	gen split = id - (floor(id / 10) * 10) + 1
	preserve
	forvalues i = 1/10 {
		keep if split == `i'
		drop split id
		
		save "`temp_folder'/pre_split_`i'.dta", replace
		
		!qsub -P proj_shocks -pe multi_slot 30 -l mem_free=60g -now no -N "split_`i'" "$shell_stata" "`code_folder'/04_shocks/02_gen_draws_unix_cluster.do" `i' -e ERROR_PATH -o OUT_PATH 
		
		restore
		drop if split == `i'
		preserve
	}

	// wait 5 minutes
	clear
	sleep 300000
	
	forvalues i = 1/10 {
		local check_file "`temp_folder'/post_split_`i'.dta"
		di "`check_file' starting"

		cap confirm file "`check_file'"
		while _rc {
			sleep 120000
			cap confirm file "`check_file'"
		}
		
		sleep 15000
		append using "`check_file'"
		di "`check_file' appended!"
	}


compress
save "$outdir/draws.dta", replace
save "$outdir/draws_`date'.dta", replace
/*
*/
