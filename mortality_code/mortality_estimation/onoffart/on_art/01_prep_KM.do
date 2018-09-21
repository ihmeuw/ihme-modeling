// THIS FILE'S PURPOSE IS TO PREPARE AND FORMAT THE KAPLAN MEIER DATA SO IT CAN BE SEX-AGE SPLIT USING HR FROM THE META ANALYSIS

global user "`c(username)'"

// settings
	clear all
	set more off
	if (c(os)=="Unix") {
		global root FILEPATH
		global code_dir "FILEPATH
	}

	if (c(os)=="Windows") {
		global root FILEPATH
		global code_dir FILEPATH
	}

	
// locals 

	local KM_data "FILEPATH/HIV_extract_KM_2015.xlsx"
	local dismod_templates FILEPATH
	local bradmod_dir FILEPATH

	local store_data FILEPATH
	
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
				// always use 500 to be liberal http://www.thelancet.com/journals/lancet/article/PIIS0140-6736(05)61719-9/fulltext
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
		

	// 3: Prep data for sex-age splitting

		use `KM_data_clean', clear 
		
		drop if baseline==1
		keep include pubmed_id nid subcohort_id super iso site cohort year* sex prop_m age* cd4* treat* sample_size ltfu_prop* dead_prop* ltfu_def extractor notes
		drop dead_prop_alt 
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

	// Run correction

		use `tmp_prepped', clear 
		do "$code_dir/01a_adjust_survival_for_ltfu.do"
			drop if dead_prop_adj==.
			destring treat_mo_end, replace 
			

save "`bradmod_dir'/KM_forsplit", replace			
