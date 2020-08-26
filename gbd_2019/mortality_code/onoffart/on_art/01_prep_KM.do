// THIS FILE'S PURPOSE IS TO PREPARE AND FORMAT THE KAPLAN MEIER DATA SO IT CAN BE PUT INTO DISMOD FOR 
	// (1) IDENTIFY POSITIVE DEVIATE IN SS AFRICA 
	// (2) RUN ALL DATA IN DISMOD. 
	// PREP STEPS INCLUDE 
		// STANDARDIZING VARIABLES, APPLYING THE LTFU CORRECTION, 
		// CALCULATING % MALE AND MEDIAN AGE BY REGION FOR THE PURPOSE OF APPLYING HAZARD RATIOS, 
		// AND TURNING CUMULATIVE INTO CONDITIONAL PROBS

// Updated by: NAME
// Last updated date: Feb 9, 2014

global user "`c(username)'"

// settings
	clear all
	set more off
	if (c(os)=="Unix") {
		global root "ADDRESS"
		global code_dir "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
		global code_dir "ADDRESS"
	}

	
// locals 

	local KM_data "FILEPATH"
	local dismod_templates "FILEPATH"
	local bradmod_dir "FILEPATH"
	
	local store_graphs "FILEPATH"
	local store_logs "FILEPATH"
	local store_data "FILEPATH"
	
	global add_new_ltfu = 1 
	global glmm = 1
	global hai_corr = 0
** *****************************
// Prep KM data
** *****************************

	// 1: Apply changes that affect both KM and HR calculations (regions)
		
		import excel using "`KM_data'", clear firstrow
		rename *LTFU* *ltfu*

		keep if include==1
		
		// Fill in Missing years (necessary for cd4 adjustment based on study period)
			replace year_start=year_end if year_start==.
		
		// Standardize Regions
			tab gbd_region

			gen super="ssa" if inlist(gbd_region, "Southern Sub-Saharan Africa", "Western Sub-Saharan Africa", "Eastern Sub-Saharan Africa", "Sub-Saharan Africa and Other", "Sub-Saharan Africa") ///
				| inlist(pubmed_id, 22972859, 16905784) | (pubmed_id==16530575 & site=="Low Income") ///
				| iso3 == "CIV"
			replace super="other" if inlist(gbd_region, "Tropical Latin America", "North Africa and Middle East", "East Asia", "Australasia", "Southeast Asia", "Latin America and Caribbean", "Latin America/Caribbean", "South Asia", "Eastern Europe") ///
				| regexm(gbd_region,"Latin")
			replace super="high" if inlist(gbd_region, "High-Income", "Western Europe", "High-income North America", "High-income") | (pubmed_id == 16530575 & site=="High Income")
			*check if super is missing
			count if missing(super) 
			if `r(N)' != 0 { 
				do "some cohorts are not assigned super regions  " 
			}
			
		// CD4 categories
		
			// Standarize entries
			replace cd4_start = . if cd4_start == .
			replace cd4_start=50 if cd4_start==51
			replace cd4_start=100 if cd4_start==101
			replace cd4_start=150 if cd4_start==151
			replace cd4_start=200 if cd4_start==201
			replace cd4_start=250 if cd4_start==251
			replace cd4_start=350 if cd4_start==351
			replace cd4_start=450 if cd4_start==451

			replace cd4_end = 1500 if cd4_end == 1500
			replace cd4_end=50 if cd4_end==49
			replace cd4_end=100 if cd4_end==99
			replace cd4_end=200 if cd4_end==199
			replace cd4_end=350 if cd4_end==349 
		
			// Adjust CD4 based on guidelines in place at time of study - in developing countries would not have initiated patients on art unless they had a cd4 meeting the guideline
				//need a new variable for cd4_specificity 
				gen cd4_specificity = "specified"
				replace cd4_specificity = "unspecified" if cd4_end == 1500 & cd4_start == 0 
				replace cd4_specificity = "lower specified" if cd4_start != 0 & cd4_end == 1500
				
				// Rwanda-specific guidelines cited in paper. they were ahead of other countries and implemented the 350 guideline earlier
				replace cd4_end=500 if iso=="RWA" &  cd4_end==1500 & cd4_start >= 350
				replace cd4_end=350 if iso=="RWA" &  cd4_end==1500 & cd4_start < 350
				
				// developing countries:
				// 2013 on: 500
				// 2010-2013: 350
				// pre 2010: 200			
				gen year_mean = (year_start + year_end) / 2
				replace cd4_end=200 if year_mean < 2010 & cd4_end==1500 & cd4_start==0 & (super=="ssa" | super=="other")
				replace cd4_end=350 if year_mean >= 2010 & year_mean < 2013 & cd4_end==1500 & cd4_start==0 & (super=="ssa" | super=="other")
				replace cd4_end=500 if cd4_end==1500 & (super=="ssa" | super=="other") // if there are still remaining 1500s left for categories starting at 200 or 350, limit the max to 500
				drop year_mean

				// developed:
				// always use 500 to be liberal? http://www.thelancet.com/journals/lancet/article/PIIS0140-6736(05)61719-9/fulltext
				replace cd4_end=500 if super=="high" & cd4_end==1500 & cd4_start<500 & super=="high"
				replace cd4_end=1000 if super=="high" & cd4_end==1500 & cd4_start>=500 & cd4_start!=. & super=="high"

		
		tempfile KM_data_clean
		save `KM_data_clean', replace

		
	// 2: Calculate region-specific median age and % male from KM data to be later used in applying Hazard Ratios. Should be weighted based on the size of the study.
		
		use `KM_data_clean', clear
		keep if baseline==1
		drop if prop_male==. | age_med==.
		keep pubmed_id nid subcohort_id sample_size prop_male age_med super
		
		gen num_male=prop_male*sample_size
		gen age_weight=age_med*sample_size
		collapse (sum) sample_size num_male age_weight, by(super)
		gen pct_male_weighted=num_male/sample_size
		gen age_med_weight=age_weight/sample_size
		drop sample_size num_male age_weight
			
		outsheet using "`store_data'/pct_male_med_age/pct_male_med_age.csv", delim(",") replace 
		

	// 3: Prep data for KM DisMod

		use `KM_data_clean', clear 
		
		drop if baseline==1
		keep include pubmed_id nid subcohort_id super iso site cohort year* sex prop_m age* cd4* treat* sample_size ltfu_prop* dead_prop* ltfu_def extractor notes cd4_specificity
		drop dead_prop_alt
		** 44 papers - 73 sites/cohorts ** 
		foreach var in cd4_start cd4_end {
			replace `var' = round(`var',.01)
		}
		tostring cd4_start, replace
		tostring cd4_end, replace 
		
		// standardize CD4 categories
		gen cd4_joint=cd4_start+"-"+cd4_end
		split cd4_joint, p("-")  // "
		replace cd4_start=cd4_joint1
		replace cd4_end=cd4_joint2
		drop cd4_joint1 cd4_joint2

		// Already manually reviewed data to update the LTFU column to 0 if it should be zero
		/* 	** replace LTFU=0 when the LTFU definition says it should be
			tab ltfu_def
			foreach var in ltfu_prop ltfu_prop_lo ltfu_prop_hi {
				replace `var'=0 if ltfu_def=="LTFU corrected with VR"
				replace `var'=0 if ltfu_def=="adj LTFU with VR data"
				replace `var'=0 if ltfu_def=="adjusted mortality for LTFU with invers"
			} */
			
		
		// Generate aggreate duration
		tostring treat_mo_s, replace
		tostring treat_mo_e, replace 
		gen time_per=treat_mo_s+"_"+treat_mo_en
		
		// geneate an additional time point variable that is numeric and will sort properly
		gen time_point=6 if time_per=="0_6"
		replace time_point=12 if time_per=="0_12"
		replace time_point=24 if time_per=="0_24"
		
		// keep observations that have the time periods of interest; we will lose some observations this way but not too many
		keep if time_per=="0_6" | time_per=="0_12" | time_per=="0_24"
	
	//  4: Tempfile our prepped file
		tempfile tmp_prepped
		save `tmp_prepped', replace 


*********************
// APPLY LTFU CORRECTION 
*********************

	// Run code

		use `tmp_prepped', clear 
		do "$code_dir/01a_adjust_survival_for_ltfu.do"
			drop if dead_prop_adj==.
			destring treat_mo_end, replace 


			
			
			
			
*India 
replace cd4_end = "1000" if cd4_end == "500" & nid == 1993 & cd4_start == "500"	 	

*we are going to split the 45-100 cohort using HRs into 45-55 & 55-100
replace age_end = 100 if age_end == 99 
drop if nid == 1997 & age_start == 55
replace age_end = 100 if age_start == 45 & age_end == 55 & nid == 1997 

save "`bradmod_dir'/KM_forsplit", replace	

//get extra age_ranges for Dismod
keep if nid == 1997 
keep age_start age_end age_med super 
*drop spectrum ages 
drop if age_start >=14 & ( 24<=age_end & age_end<=26)
drop if (age_start>=24 & age_start<=26) & (age_end>=34 & age_end <=36)
drop if (age_start>=34 & age_start<=36) & (age_end>=44 & age_end <=46)
drop if (age_start>=44 & age_start<=46) & (age_end>=54 & age_end <=56)
drop if (age_start>=54 & age_start<=56) & age_end >= 60 
*median age of region is more informative for these cohorts
drop if age_start <= 20 & age_end >= 80
replace age_start = age_med if !missing(age_med) 
replace age_end = age_med if !missing(age_med)
keep age_start age_end super 
rename (age_start age_end) (age_lower age_upper) 
duplicates drop 
tempfile master 
save `master', replace 

foreach sup in high ssa other{ 
	
	use `master', clear
	keep if super == "`sup'"
	drop super
	gen ind = 1 
	preserve 
	insheet using "FILEPATH/draw_in.csv", clear
	drop age_lower age_upper row_name 
	gen ind = 1 
	duplicates drop
	tempfile mod 
	save `mod', replace 
	restore 
	merge m:1 ind using `mod', nogen 
	drop ind 
	gen row_name = "`sup'" + "_" + string(age_lower) + "_" + string(age_upper)
	local filepath =  "`dismod_templates'/extra_`sup'" 
	save "`filepath'", replace


}
