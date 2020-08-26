
insheet using  "FILEPATH", comma names clear

// THIS FILE'S PURPOSE IS TO PREPARE AND FORMAT THE KAPLAN MEIER DATA SO IT CAN BE PUT INTO DISMOD FOR 
	// (1) IDENTIFY POSITIVE DEVIATE IN SS AFRICA 
	// (2) RUN ALL DATA IN DISMOD. 
	// PREP STEPS INCLUDE 
		// STANDARDIZING VARIABLES, APPLYING THE LTFU CORRECTION, 
		// CALCULATING % MALE AND MEDIAN AGE BY REGION FOR THE PURPOSE OF APPLYING HAZARD RATIOS, 
		// AND TURNING CUMULATIVE INTO CONDITIONAL PROBS

// Updated by: NAME
// Last updated date: Feb 9, 2014


// settings
	clear all
	set more off

// Initialize pdfmaker
if 0==0 {
	if c(os) == "Windows" {
		global prefix "ADDRESS"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}



	
// locals 

	local KM_data "FILEPATH"

	
** *****************************
// Prep KM data
** *****************************

	// 1: Apply changes that affect both KM and HR calculations (regions)
		
		insheet using "`KM_data'", clear comma names

		keep if include==1
		
		// Fill in Missing years (necessary for cd4 adjustment based on study period)
			replace year_start=year_end if year_start==.
		
		// Standardize Regions
			tab gbd_region

			gen super="ssa" if inlist(gbd_region, "Southern Sub-Saharan Africa", "Western Sub-Saharan Africa", "Eastern Sub-Saharan Africa", "Sub-Saharan Africa and Other", "Sub-Saharan Africa") ///
				| inlist(pubmed_id, 22972859, 16905784) | (pubmed_id == 16530575 & site=="Low Income")
			replace super="other" if inlist(gbd_region, "Tropical Latin America", "North Africa and Middle East", "East Asia", "Australasia", "Southeast Asia", "Latin America and Caribbean", "Latin America/Caribbean", "South Asia")
			replace super="high" if inlist(gbd_region, "High-Income", "Western Europe", "High-income North America") | (pubmed_id == 16530575 & site=="High Income")

		
		// CD4 categories
		
			// Standarize entries
			replace cd4_start=50 if cd4_start==51
			replace cd4_start=100 if cd4_start==101
			replace cd4_start=150 if cd4_start==151
			replace cd4_start=200 if cd4_start==201
			replace cd4_start=250 if cd4_start==251
			replace cd4_start=350 if cd4_start==351
			replace cd4_start=450 if cd4_start==451

			replace cd4_end=50 if cd4_end==49
			replace cd4_end=100 if cd4_end==99
			replace cd4_end=200 if cd4_end==199
			replace cd4_end=350 if cd4_end==349
		
			// Adjust CD4 based on guidelines in place at time of study - in developing countries would not have initiated patients on art unless they had a cd4 meeting the guideline
				
				// Rwanda-specific guidelines cited in paper. they were ahead of other countries and implemented the 350 guideline earlier
				replace cd4_end=500 if iso=="RWA" & cd4_end==1500 & cd4_start==350
				replace cd4_end=350 if iso=="RWA" & cd4_end==1500
				
				// developing countries:
				// 2013 on: 500
				// 2010-2013: 350
				// pre 2010: 200			
				replace cd4_end=200 if year_end<2010 & cd4_end==1500 & cd4_start==0 & (super=="ssa" | super=="other")
				replace cd4_end=350 if year_start>=2010 & year_end>=2010 & cd4_end==1500 & cd4_start==0 & (super=="ssa" | super=="other")
				replace cd4_end=200 if year_start<2010 & year_end==2010 & cd4_end==1500 & cd4_start==0 & (super=="ssa" | super=="other")
				replace cd4_end=500 if cd4_end==1500 & (super=="ssa" | super=="other") // if there are still remainign 1500s left for categories starting at 200 or 350, limit the max to 500

				// developed:
				// always use 500 to be liberal? http://www.thelancet.com/journals/lancet/article/PIIS0140-6736(05)61719-9/fulltext
				replace cd4_end=500 if super=="high" & cd4_end==1500 & cd4_start<500 & super=="high"
				replace cd4_end=1000 if super=="high" & cd4_end==1500 & cd4_start>=500 & cd4_start!=. & super=="high"
			
			// If the study reports the mean or median cd4 count, input this as both the upper and lower cd4, to try and account for the actual composition of cd4
				replace cd4_end=cd4_med if cd4_med!=.
				replace cd4_start=cd4_med if cd4_med!=.
		


		

	// 3: Prep data for KM DisMod

		drop if baseline==1
		keep include pubmed_id super iso site cohort year* sex prop_m age* cd4* treat* sample_size ltfu_prop* dead_prop* ltfu_def extractor notes
		drop dead_prop_alt
		** 44 papers - 73 sites/cohorts ** 
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
		replace time_point=36 if time_per=="0_36"
		replace time_point=48 if time_per=="0_48"
		replace time_point=60 if time_per=="0_60"
		
		// keep observations that have the time periods of interest; we will lose some observations this way but not too many
		keep if time_point!=.
	


*********************
// APPLY LTFU CORRECTION (Written by NAME)
*********************

	// Run Code

		do "FILEPATH"
			drop if dead_prop_adj==.
			destring treat_mo_end, replace 

// convert to conditional probabilities			
	

	// 1: Create variables needed for data_in csv
		tostring age_start age_end, replace
		gen age_joint=age_start+"-"+age_end
		gen sex_real=sex
		
		keep sex year_start year_end cd4_start cd4_end dead_prop_adj dead_prop_lo dead_prop_hi super iso3 cohort site pubmed_id time_per time_point age_joint sex_real sample_size
		rename year_s time_lower
		rename year_e time_upper
		gen integrand="incidence" 
		rename cd4_start age_lower
		rename cd4_end age_upper
		rename dead_prop_adj meas_value 
		gen subreg="none"
		gen region="none"
		// super already exists
		gen x_sex=.
		replace sex=3 if sex==.
		replace x_sex=0 if sex==3 
		replace x_sex=.5 if sex==1
		replace x_sex=-.5 if sex==2
		gen x_ones=1

		
	// 2: Adjust CD4 counts (in bradmod they are in the 'age' columns since we are 'tricking' dismod)
		destring age_upper, replace 
		destring age_lower, replace 

		replace age_upper=age_upper/10
		replace age_lower=age_lower/10

	// 3: Order and save variables
		order pubmed_ super region subreg iso3 time_l time_u sex age_l age_u meas_v integ x_* time_per time_point
	
	
		order meas_value pubmed_id age_joint sex_real time_point
		sort pubmed_id super iso3 cohort site time_lower time_upper age_joint age_lower age_upper sex_real time_point	
		

		
		bysort pubmed_id super iso3 cohort site time_lower time_upper age_joint age_lower age_upper sex_real: ///
			gen cond_prob=((meas_value-meas_value[_n-1])/(1 - meas_value[_n-1])) if time_point!=6
		
		bysort pubmed_id super iso3 cohort site time_lower time_upper age_joint age_lower age_upper sex_real: ///
			gen time_passed=(time_point-time_point[_n-1]) 
			
			replace time_passed=time_point if time_passed==.
			replace time_passed=time_passed/12
			

		replace cond_prob=meas_value if time_passed==(time_point/12)
		
		gen rate=-ln(1-cond_prob)/time_passed
		
		order rate

		drop if rate<0
		
		gen real_start_time=time_point-(time_passed*12)
		
		egen real_time_per=concat(real_start time_point), p(-)
		

		collapse rate, by(super real_time_per age_lower age_upper)
		
		egen mean_cd4=rowmean(age_lower age_upper)
		replace mean_cd4=mean_cd4*10
		
		drop if rate>.1 & super=="high"
		
		pdfstart using "FILEPATH"
		foreach super in high other ssa {
			preserve
			keep if super=="`super'"
			twoway 	scatter rate mean_cd4 if real_time_per=="12-24", mcolor(black)  || ///
					scatter rate mean_cd4 if real_time_per=="12-36", mcolor(blue)   || ///
					scatter rate mean_cd4 if real_time_per=="24-36", mcolor(green)   || ///
					scatter rate mean_cd4 if real_time_per=="36-48", mcolor(yellow)   || ///
					scatter rate mean_cd4 if real_time_per=="36-60", mcolor(orange)   || ///
					scatter rate mean_cd4 if real_time_per=="48-60", mcolor(red)   ///
					legend(lab(1 "12-24 months") lab(2 "12-36 months") lab(3 "24-36 months") lab(4 "36-48 months") lab(5 "36-60 months") lab(6 "48-60 months")) title("`super'") ///
					ytitle("Mortality rate (per-person year)") xtitle("CD4 midpoint") note("X-axis is either:" "(A) Studies reporting CD4-stratum-specific data: Mid-point of the CD4 stratum (eg 0-50 is at 25)" "(B) Studies not reporting CD4-specific data: Median CD4 for the whole study")
				pdfappend
			restore
		}
		
		pdffinish
		
		
		
		
		