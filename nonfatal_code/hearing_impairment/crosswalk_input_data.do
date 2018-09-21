
**************************************
** Prep Stata
**************************************	
	
	clear all
	cap log close
	set mem 1g
	set maxvar 20000
	set more off
	cap restore, not
	set type double, perm

	
**************************************
** Prep input data
**************************************
	local data_2009_2010 FILEPATH
	local data_2007_2008 FILEPATH
	local data_2005_2006 FILEPATH
	local data_2003_2004 FILEPATH
	local data_2001_2002 FILEPATH
	local data_1999_2000 FILEPATH


	local wts_2009_2010 FILEPATH
	local wts_2007_2008 FILEPATH
	local wts_2005_2006 FILEPATH
	local wts_2003_2004 FILEPATH
	local wts_2001_2002 FILEPATH
	local wts_1999_2000 FILEPATH

*****************************************************************************
** Prep data 1999-2010
*****************************************************************************
if 1==1 {
	// loop through each year type
	local year_start 1999
	forvalues year_start=1999(2)2009 {
		local year_end=`year_start'+1
		local yrs "`year_start'_`year_end'"
		
		use `data_`yrs'', clear
		rename *, lower // Force variable names lower case
		gen year_start = "`year_start'"
		gen year_end = "`year_end'"
		tempfile NHANES_`yrs'_hearing
		save `NHANES_`yrs'_hearing', replace
		use `wts_`yrs'', clear
		rename *, lower // Force variable names lower case
		merge 1:1 seqn using `NHANES_`yrs'_hearing', keep(3)
		
		// set missingness to .
		local count=0
		foreach var in auxu1k1r auxu500r auxu2kr auxu3kr auxu4kr auxu6kr auxu8kr auxu1k1l auxu500l auxu2kl auxu3kl auxu4kl auxu6kl auxu8kl auxr1k1r auxr5cr auxr2kr auxr3kr auxr4kr auxr6kr auxr8kr auxr1k1l auxr5cl auxr2kl auxr3kl auxr4kl auxr6kl auxr8kl {
			replace `var'=. if `var'==888
			replace `var'=. if `var'==666
		}
		
		// replace with retest info if necessary
		rename *5c* *500*
		
		foreach x in 1k1 500 2k 3k 4k 6k 8k {
			replace auxu`x'r=auxr`x'r if auxr`x'r!=.
			replace auxu`x'l=auxr`x'l if auxr`x'l!=.
		}	
		
		// average decibel level at which hearing is lost across all frequencies for right and left ear separately
		egen db_loss_right = rowmean(auxu1k1r auxu500r auxu2kr auxu3kr auxu4kr)
		egen db_loss_left = rowmean(auxu1k1l auxu500l auxu2kl auxu3kl auxu4kl)
		
		// define hearing loss as what is from best ear
		gen db_loss=db_loss_right
		replace db_loss=db_loss_left if db_loss_left<db_loss_right
		
		// make psu- year specific
		tostring sdmvpsu, replace
		replace sdmvpsu=sdmvpsu+"_`yrs'"
		
		keep db_loss year_start year_end ridageyr sdmvstra wtmec2yr sdmvpsu riagendr
		tempfile clean_`yrs'
		save `clean_`yrs'', replace
	}
		
	** Combine years
	use `clean_2009_2010', clear
	tempfile clean
	save `clean', replace
	forvalues year_start=2001(2)2009 {
		local year_end=`year_start'+1
		local yrs "`year_start'_`year_end'"	
		append using `clean_`yrs''
		save `clean', replace
	}
	
	// Apply survey weighting and generate means for age/sex groups
	svyset sdmvpsu [pw=wtmec2yr], strata(sdmvstra) // Set survey weights and variables for the use of variance calculation, taken online reading of the NHANES tutorials from the CDC
	

	// Create variable to store GBD age groups
		rename ridageyr age
		rename riagendr sex
		gen gbd_age = .
		
		// 1 - 4 years
			replace gbd_age = 1 if age>=1 & age<5
		// 5 - 9 years
			replace gbd_age = 5 if age>=5 & age<10
		// 10 - 14 years
			replace gbd_age = 10 if age>=10 & age<15
		// 15 - 19 years
			replace gbd_age = 15 if age>=15 & age<20	
		// 20 - 24 years
			replace gbd_age = 20 if age>=20 & age<25
		// 25 - 29 years
			replace gbd_age = 25 if age>=25 & age<30
		// 30 - 34 years
			replace gbd_age = 30 if age>=30 & age<35
		// 35 - 39 years
			replace gbd_age = 35 if age>=35 & age<40
		// 40 - 44 years
			replace gbd_age = 40 if age>=40 & age<45
		// 45 - 49 years
			replace gbd_age = 45 if age>=45 & age<50
		// 50 - 54 years
			replace gbd_age = 50 if age>=50 & age<55
		// 55 - 59 years
			replace gbd_age = 55 if age>=55 & age<60
		// 60 - 64 years
			replace gbd_age = 60 if age>=60 & age<65
		// 65 - 69 years
			replace gbd_age = 65 if age>=65 & age<70
		// 70 - 74 years
			replace gbd_age = 70 if age>=70 & age<75
		// 75 - 79 years
			replace gbd_age = 75 if age>=75 & age<80
		// 80+ years
			replace gbd_age = 80 if age>=80 & age<120
			
	tempfile data
	save `data', replace
}

*****************************************************************************
** Make crosswalk estimates
*****************************************************************************

//CREATE TEMPLATE 
	//Threshold map 
	import excel FILEPATH, firstrow clear 
	destring bundle_id, replace 
	tempfile crosswalk_map
	save `crosswalk_map'
		//GBD thresholds
		keep gbd_lower gbd_upper 
		rename gbd_lower thresh_lower
		rename gbd_upper thresh_upper
		tempfile gbd_thresholds
		save `gbd_thresholds'

	//Data thresholds
	import excel FILEPATH, firstrow clear 
	tempfile sr
	save `sr'
	import excel FILEPATH, firstrow clear 
	tempfile sr_newborn
	save `sr_newborn'
	import excel FILEPATH, firstrow clear 
	tempfile collab
	save `collab'




	use `sr', clear 
	append using `collab', force 
	append using `sr_newborn'


	tempfile extraction 
	save `extraction'
	keep thresh_lower thresh_upper
	duplicates drop thresh_lower thresh_upper, force 
	drop if missing(thresh_lower)
	sort thresh_lower
	merge 1:1 thresh_lower thresh_upper using `crosswalk_map'
		count if _merge == 1 
		if `r(N)' > 0 di in red "Not all data severities mapped to crosswalk map - please add them! " BREAK  
		else drop _merge

	//Add GBD categories if missing 
	append using `gbd_thresholds'
	duplicates drop thresh_lower thresh_upper, force 
	drop if missing(thresh_lower)
	tempfile thresholds 
	gen n = _n 
	save `thresholds'

//CALCULATE PREVALENCES 
	count 
	local n_thresh = `r(N)'
	forval i = 1/`n_thresh' {
		use `thresholds' if n == `i', clear 
		local thresh_lower = thresh_lower
		local thresh_upper = thresh_upper
		use `data', clear
		gen prev = 0 
		replace prev = 1 if inrange(db_loss, `thresh_lower', `thresh_upper')

		svyset sdmvpsu [pw=wtmec2yr], strata(sdmvstra) 
		
			svy: mean prev
			matrix mean_matrix = e(b)
			matrix variance_matrix = e(V)
			local mean = mean_matrix[1,1]
			local variance = variance_matrix[1,1]
			local se = sqrt(`variance')		

			keep in 1 
			replace prev = `mean'
			gen prev_se`t' = `se'
			keep prev prev_se

		gen thresh_lower = `thresh_lower'
		gen thresh_upper = `thresh_upper'

		if `i' == 1 tempfile prev 
		else append using `prev'
		save `prev', replace
		}

//Add GBD prevalences to map 
use `crosswalk_map', clear 
	//Alternative thresholds
	merge 1:1 thresh_lower thresh_upper using `prev', keep(3) nogen
	rename prev alt_prev
	rename prev_se alt_prev_se
	rename thresh_lower thresh_lower_data
	rename thresh_upper thresh_upper_data

	//Reference thresholds
	rename gbd_lower thresh_lower
	rename gbd_upper thresh_upper
	merge m:1 thresh_lower thresh_upper using `prev', keep(1 3) nogen
	rename prev ref_prev
	rename prev_se ref_prev_se

	//Ratio 
	gen xwalk = ref_prev / alt_prev
	gen xwalk_logit = logit(ref_prev) / logit(alt_prev)

	drop if missing(xwalk)

	keep thresh_lower_data thresh_upper_data xwalk* bundle modelable_entity*
	rename thresh_upper_data thresh_upper
	rename thresh_lower_data thresh_lower
	tempfile crosswalk_map_final 
	save `crosswalk_map_final'




//Load and crosswalk data 
use `extraction', clear 
drop if missing(thresh_lower) | missing(thresh_upper)
cap drop bundle*
cap drop modelable_entity*
merge m:1 thresh_lower thresh_upper using `crosswalk_map_final', keep(3)


preserve 
keep if xwalk == 1 
tempfile no_xwalk_needed
save `no_xwalk_needed'
restore 

drop if xwalk == 1 

replace mean = cases / sample_size if missing(mean)
replace standard_error = sqrt(mean*(1 - mean) / (cases / mean)) if missing(standard_error) & missing(sample_size)
	count if missing(mean) 
	if `r(N)' > 0 di in red "Missing means" BREAK 
	count if missing(mean) & missing(standard_error)
	if `r(N)' > 0 di in red "Missing uncertainty" BREAK 


gen mean_1 = mean * xwalk 
gen mean_2 = invlogit(logit(mean) * xwalk_logit)
replace mean = mean_2 if mean != 0 
drop mean_1 mean_2 


replace xwalk = round(xwalk, 0.001)
tostring note_modeler thresh_lower thresh_upper xwalk, replace force 
replace note_modeler = note_modeler + "| crosswalked from data threshold (" + thresh_lower + " , " + thresh_upper + ") using xwalk coef " + xwalk

append using `no_xwalk_needed', force 

drop thresh_lower thresh_upper xwalk*

export excel "FILEPATH", firstrow(var) replace 
