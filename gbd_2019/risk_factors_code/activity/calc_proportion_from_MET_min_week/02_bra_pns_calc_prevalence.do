
// AUTHOR: 
// DATE: 
// PURPOSE: CLEAN AND EXTRACT PHYSICAL ACTIVITY DATA FROM Brazil National Health Survey (PNS) AND COMPUTE PHYSICAL ACTIVITY PREVALENCE IN 5 YEAR AGE-SEX GROUPS FOR EACH YEAR 

// NOTES: Doesn't use GPAQ or IPAQ but has duration, frequency and intensity of activity, which will allow us to calculate MET-min/week 


// Set up
	clear all
	set more off
	set mem 2g
	if c(os) == "Unix" {
		global j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
		version 11
	}

// arguments
	// test arguments local 1 "Para"
	
	args state

// add logs
	log using "FILEPATH", replace 
	
	//di "`state'"

// Make locals for relevant files and folders
	local data_dir "FILEPATH"
	local out_dir "FILEPATH"

// Bring in dataset 
	
	use "`data_dir'/FILEPATH", clear 

	// Keep relevant physical activity variables and rename 
	rename c008 age 
	rename c006 sex 

//***********************************
// Recode physical activity variables 
//***********************************

 	//***********************************
	// LEISURE/SPORT ACTIVITIES
	//***********************************

	rename p034 exercise_leisure 
	replace exercise_leisure = "" if exercise_leisure == "NA"
	destring exercise_leisure, replace
	recode exercise_leisure (2 = 0)

	rename p035 leisure_days 
	rename p036 leisure_activity 

	replace leisure_activity = subinstr(leisure_activity, "0", "", .)

	gen met1 = 3 // walking
	gen met2 = 4 // walking on a treadmill 
	gen met3 = 7.5 // running 
	gen met4 = 7.5 // running on a treadmill 
	gen met5 = 6.0 // weight lifting 
	gen met6 = 8.5  // aerobics/step
	gen met7 = 4.0 // water aerobics 
	gen met8 = 4.0 // gymnastics, general 
	gen met9 = 7.0 // swimming (going to assume swimming laps, frestyle, slow, moderate, or light effort)
	gen met10 = 10.0 // different types of martial arts 
	gen met11 = 7.0 // bicycling, stationary, general 
	gen met12 = 8.5 // soccer (ranges between 7 and 10 depending on how casual or competitive so took the mean)
	gen met13 = 8.0 // basketball 
	gen met14 = 6.0 // volleyball (ranges from 4 to 8 depending on how casual or competitive so took the mean)
	gen met15 = 7.0 // tennis, general
	gen met16 = 6.5 // dancing, aerobic general 
	gen met_other = 4 // assign moderate amount since we don't know the activity 

	// convert hours / minutes
	replace p03701 = "" if p03701 == "NA" 
	replace p03702 = "" if p03702 == "NA"
	replace leisure_days = "" if leisure_days == "NA"
	destring p03701, replace 
	destring p03702, replace
	destring leisure_activity, replace
	destring leisure_days, replace

	gen leisure_minutes = p03701 * 60 
	replace leisure_minutes = leisure_minutes + p03702 

	gen leisure_mets = . 

	forvalues i = 1/16 { 
		replace leisure_mets = leisure_minutes * leisure_days * met`i' if leisure_activity == `i'
	}

	replace leisure_mets = 0 if exercise_leisure == 0 


 	//***********************************
	// WORK ACTIVITIES
	//***********************************

	rename p038 work_exercise 
	destring work_exercise, replace
	recode work_exercise (2= 0) 
 	rename p03901 work_days
 	replace work_days = "" if work_days == "NA" 
 	replace p03902 = "" if p03902 == "NA" 
 	replace p03903 = "" if p03903 == "NA" 
 	destring p03902, replace
 	destring p03903, replace 
 	destring work_days, replace 

 	gen work_minutes = p03902 * 60 
 	replace work_minutes = work_minutes + p03903 

 	gen work_mets = work_days * work_minutes * 8 // vigorous work activity so use MET of 8 

 	replace work_mets = 0 if work_exercise == 0 


 	//***********************************
	// TRANSPORT ACTIVITIES
	//***********************************

	rename p040 transport_exercise 
	destring transport_exercise, replace 
	recode transport_exercise (3 = 0) 

 	replace p04101 = "" if p04101 == "NA" 
 	replace p04102 = "" if p04102 == "NA"
 	destring p04101, replace
 	destring p04102, replace

 	gen transport_minutes = p04101 * 60 
 	replace transport_minutes = transport_minutes + p04102

 	gen transport_mets = transport_minutes * 5 * 4 // going to assume they work 5 days a week and use MET of 4 

 	replace transport_mets = 0 if transport_exercise == 0 

 	//***********************************
	// DOMESTIC ACTIVITIES
	//***********************************
	
	rename p044 domestic_exercise 
	destring domestic_exercise, replace
	recode domestic_exercise (2 = 0)

 	rename p04401 domestic_days 
 	replace domestic_days = "" if domestic_days == "NA" 
 	replace p04403 = "" if p04403 == "NA" 
 	replace p04404 = "" if p04404 == "NA"

 	destring p04403, replace
 	destring p04404, replace

 	destring domestic_days, replace

 	gen domestic_minutes = p04403 * 60 
 	replace domestic_minutes = domestic_minutes + p04404

 	gen domestic_mets = domestic_days * domestic_minutes * 8 

 	replace domestic_mets = 0 if domestic_exercise == 0 

//***********************************
// Checks for accuracy and total met calculations
//***********************************

	egen total_mets = rowtotal(leisure_mets work_mets transport_mets domestic_mets)
	egen total_miss = rowmiss(leisure_mets work_mets transport_mets domestic_mets)
	replace total_mets = . if total_miss >2  
	drop total_miss
	
// Check to make sure total reported activity time is plausible	
	egen total_time = rowtotal(leisure_minutes work_minutes transport_minutes domestic_minutes) // Shouldn't be more than 6720 minutes (assume no more than 16 active waking hours per day on average)
	replace total_mets = . if total_time > 6720
	 //drop total_time 

// Remove those that have no recorded physical activity (zero edit)
	*drop if total_mets == 0

// Make categorical physical activity variables
	drop if total_mets == .
	gen inactive = total_mets < 600
	gen lowactive = total_mets >= 600 & total_mets < 4000
	gen lowmodhighactive = total_mets >= 600
	gen modactive = total_mets >= 4000 & total_mets < 8000
	gen modhighactive = total_mets >= 4000 
	gen highactive = total_mets >= 8000



//***********************************
// Extract prevalence
//***********************************
	
	// Set age groups
	drop if age < 25 | age == .
	egen age_start = cut(age), at(25(5)120)
	replace age_start = 80 if age_start > 80 & age_start != .
	levelsof age_start, local(ages)
	drop age

// Create empty matrix for storing proportion of a country/age/sex subpopulation in each physical activity category 
	mata 
		state = J(1,1,"todrop")
		age_start = J(1,1,999)
		sex = J(1,1,999)
		sample_size = J(1,1,999)
		inactive_mean = J(1,1,999)
		inactive_se = J(1,1,999)
		lowmodhighactive_mean = J(1,1,999)
		lowmodhighactive_se = J(1,1,999)
		modhighactive_mean = J(1,1,999)
		modhighactive_se = J(1,1,999)
		lowactive_mean = J(1,1,999)
		lowactive_se = J(1,1,999)
		modactive_mean = J(1,1,999)
		modactive_se = J(1,1,999)
		highactive_mean = J(1,1,999)
		highactive_se = J(1,1,999)
		total_mets_mean = J(1,1,999)
		total_mets_se = J(1,1,999)
	end
		
// Set survey weights
	destring v00291, replace
	rename v00291 pweight

	svyset upa [pweight=pweight], strata(v0024)
	
	rename v0001 uf_code 

	tempfile all 
	save `all', replace

// Merge on state and region names 
	import excel "FILEPATH", firstrow clear 
	merge 1:m uf_code using `all', nogen

// Compute prevalence at national level for age/sex group 
	
	tempfile all 
	save `all', replace

	di in red "`state'"

	foreach sex in 1 2 {	
		foreach age of local ages {

			use `all', clear
							
			di in red "State: `state' Age: `age' Sex: `sex'"
			count if state == "`state'" & age_start == `age' & sex == `sex' & total_mets != .
			local sample_size = r(N)
			if `sample_size' > 0 {
				// Calculate mean and standard error for each activity category
					foreach category in inactive lowactive modactive highactive lowmodhighactive modhighactive total_mets {
						svy linearized, subpop(if state == "`state'" & age_start ==`age' & sex == `sex'): mean `category'
						matrix `category'_stats = r(table)
						
						local `category'_mean = `category'_stats[1,1]
						mata: `category'_mean = `category'_mean \ ``category'_mean'
						
						local `category'_se = `category'_stats[2,1]
						mata: `category'_se = `category'_se \ ``category'_se'
					}
						
				// Extract other key variables	
					mata: state = state \ "`state'"
					mata: age_start = age_start \ `age'
					mata: sex = sex \ `sex'
					mata: sample_size = sample_size \ `sample_size'
			}
		}
	}


// Get stored prevalence calculations from matrix
		clear

		getmata state age_start sex sample_size highactive_mean highactive_se modactive_mean modactive_se lowactive_mean lowactive_se inactive_mean inactive_se lowmodhighactive_mean lowmodhighactive_se modhighactive_mean modhighactive_se total_mets_mean total_mets_se
		drop if _n == 1 // drop top row which was a placehcolder for the matrix created to store results

		tempfile mata_calc
		save `mata_calc', replace 
		
// Replace standard error as missing if its zero
	recode *_se (0 = .)


// Make variables and variable names consistent with other sources
	label define sex 1 "Male" 2 "Female"
	label values sex sex
	gen survey_name = "Brazil National Health Survey"
	gen questionnaire = "other"
	gen iso3 = "BRA"
	gen year_start = 2013
	gen year_end = 2013
	gen age_end = age_start + 4

// Save survey weighted prevalence estimates 
		save "FILEPATH", replace

	
	
			



	


